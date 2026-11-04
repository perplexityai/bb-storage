package blobstore

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const storedFrameSize = 1 << 20

type compressedCASBlobAccess struct {
	BlobAccess
	directory          string
	maximumStagedBytes int64
	bytes              *semaphore.Weighted
	writes             *semaphore.Weighted
	pool               zstd.Pool
}

// NewCompressedCASBlobAccess stores validated compressed representations in a
// local block store using NewCompressedCASReadBufferFactory. Staging is bounded
// before reading an upload, and unlinked files are reclaimed even after a crash.
// The directory must reside on a filesystem supporting unlink of open files.
func NewCompressedCASBlobAccess(base BlobAccess, directory string, maximumStagedBytes, maximumWrites int64, pool zstd.Pool) (BlobAccess, error) {
	if err := ValidateCompressedCASConfiguration(directory, maximumStagedBytes, maximumWrites, pool); err != nil {
		return nil, err
	}
	return &compressedCASBlobAccess{BlobAccess: base, directory: directory, maximumStagedBytes: maximumStagedBytes, bytes: semaphore.NewWeighted(maximumStagedBytes), writes: semaphore.NewWeighted(maximumWrites), pool: pool}, nil
}

// ValidateCompressedCASConfiguration checks staging requirements before a
// configuration builder opens or truncates the storage device.
func ValidateCompressedCASConfiguration(directory string, maximumStagedBytes, maximumWrites int64, pool zstd.Pool) error {
	minimum, _ := zstdframes.MaximumStoredSize(0, storedFrameSize)
	if directory == "" || maximumStagedBytes < minimum || maximumWrites <= 0 || pool == nil {
		return status.Errorf(codes.InvalidArgument, "Compressed storage requires a staging directory, at least %d staging bytes, a positive write budget, and a codec pool", minimum)
	}
	info, err := os.Stat(directory)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return status.Error(codes.InvalidArgument, "Compression staging path is not a directory")
	}
	return nil
}

func (ba *compressedCASBlobAccess) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	return buffer.WithZSTDContext(ba.BlobAccess.Get(ctx, d), ctx)
}

func (ba *compressedCASBlobAccess) GetFromComposite(ctx context.Context, parent, child digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	// Physical offsets in compressed blocks are not logical slice offsets.
	b, _ := slicer.Slice(ba.Get(ctx, parent), child)
	return b
}

func (ba *compressedCASBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	if err := ctx.Err(); err != nil {
		b.Discard()
		return status.FromContextError(err).Err()
	}
	// This runs inside Put, after any upload authorization. Checking in the
	// protocol handler via FindMissing would instead apply read-side policy.
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil && status.Code(err) != codes.Unavailable && status.Code(err) != codes.Internal && status.Code(err) != codes.DataLoss && status.Code(err) != codes.Unknown {
		b.Discard()
		return err
	}
	if err == nil && missing.Empty() {
		return buffer.SkipUpload(b)
	}

	reservation, err := zstdframes.MaximumStoredSize(d.GetSizeBytes(), storedFrameSize)
	if err != nil {
		b.Discard()
		return err
	}
	if reservation > ba.maximumStagedBytes {
		b.Discard()
		return status.Error(codes.InvalidArgument, "Blob cannot fit the configured staging budget")
	}
	if !ba.writes.TryAcquire(1) {
		b.Discard()
		return status.Error(codes.ResourceExhausted, "Compressed upload slots exhausted")
	}
	defer ba.writes.Release(1)
	if !ba.bytes.TryAcquire(reservation) {
		b.Discard()
		return status.Error(codes.ResourceExhausted, "Compressed upload staging budget exhausted")
	}
	defer ba.bytes.Release(reservation)
	f, err := os.CreateTemp(ba.directory, "bb-storage-compressed-")
	if err != nil {
		b.Discard()
		return stagingError(err)
	}
	defer f.Close()
	if err := os.Remove(f.Name()); err != nil {
		f.Close()
		os.Remove(f.Name())
		b.Discard()
		return err
	}
	r := b.ToReader()
	w := &stagingWriter{file: f, remaining: reservation}
	encodeErr := zstdframes.Encode(ctx, w, d, r, storedFrameSize, ba.pool)
	closeErr := r.Close()
	if encodeErr != nil {
		return encodeErr
	}
	if err := closeErr; err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	physicalSize := reservation - w.remaining
	if d.GetSizeBytes() <= 4096 && physicalSize >= d.GetSizeBytes() {
		// Tiny records do not justify an index. Larger incompressible records
		// retain frame checksums so indexed random reads remain efficient.
		// Decode staging directly into the raw allocation without another file.
		logical := NewCompressedCASReadBufferFactory(ba.pool).NewBufferFromReaderAt(d, &stagingReader{ctx: ctx, File: f}, physicalSize, func(bool) {})
		return ba.BlobAccess.Put(ctx, d, buffer.WithZSTDContext(logical, ctx))
	}
	return ba.BlobAccess.Put(ctx, d, buffer.NewValidatedBufferFromReaderAt(&stagingReader{ctx: ctx, File: f}, physicalSize))
}

type stagingWriter struct {
	file      *os.File
	remaining int64
}

func (w *stagingWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, status.Error(codes.ResourceExhausted, "Compressed upload exceeded staging reservation")
	}
	n, err := w.file.Write(p)
	w.remaining -= int64(n)
	return n, stagingError(err)
}

func stagingError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return status.Error(codes.ResourceExhausted, "Compression staging filesystem is full")
	}
	return status.Errorf(codes.Internal, "Compression staging I/O failed: %v", err)
}

type stagingReader struct {
	ctx context.Context
	*os.File
}

func (r *stagingReader) ReadAt(p []byte, off int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, status.FromContextError(err).Err()
	}
	return r.File.ReadAt(p, off)
}

type compressedCASReadBufferFactory struct{ pool zstd.Pool }

// NewCompressedCASReadBufferFactory reads indexed compressed or legacy raw CAS
// records. Encoded checksums always remain enabled, including during refresh.
// This factory is intended for random-access local block storage.
func NewCompressedCASReadBufferFactory(pool zstd.Pool) ReadBufferFactory {
	return &compressedCASReadBufferFactory{pool: pool}
}

func (f *compressedCASReadBufferFactory) NewBufferFromReaderAt(d digest.Digest, r buffer.ReadAtCloser, size int64, callback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewCASBufferFromDeferredReaderAt(d, r, buffer.BackendProvided(callback), func(r buffer.ReadAtCloser) buffer.Buffer {
		index, err := zstdframes.Open(r, size, d)
		if err == nil {
			return buffer.NewCASBufferFromZSTDRepresentation(d, r, size, index, f.pool, buffer.BackendProvided(callback))
		}
		if err == zstdframes.ErrUnsupportedVersion && size == d.GetSizeBytes() {
			// Raw records may end in format-like bytes. Validate that interpretation,
			// but do not evict a possibly newer representation if it does not match.
			return CASReadBufferFactory.NewBufferFromReaderAt(d, r, size, func(valid bool) {
				if valid {
					callback(true)
				}
			})
		}
		if err != zstdframes.ErrInvalidRepresentation {
			r.Close()
			return buffer.NewBufferFromError(err)
		}
		if size == d.GetSizeBytes() {
			// A raw blob may happen to end in format-like bytes. Only full logical
			// validation can accept the legacy interpretation of such a record.
			return CASReadBufferFactory.NewBufferFromReaderAt(d, r, size, callback)
		}
		r.Close()
		callback(false)
		return buffer.NewBufferFromError(err)
	})
}

func (f *compressedCASReadBufferFactory) NewBufferFromByteSlice(d digest.Digest, p []byte, callback buffer.DataIntegrityCallback) buffer.Buffer {
	return f.NewBufferFromReaderAt(d, &sliceReaderAt{data: p}, int64(len(p)), callback)
}

func (*compressedCASReadBufferFactory) NewBufferFromReader(_ digest.Digest, r io.ReadCloser, _ buffer.DataIntegrityCallback) buffer.Buffer {
	r.Close()
	return buffer.NewBufferFromError(status.Error(codes.Unimplemented, "Compressed local storage requires a random-access reader"))
}

type sliceReaderAt struct{ data []byte }

func (r *sliceReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, status.Error(codes.InvalidArgument, "Negative read offset")
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (*sliceReaderAt) Close() error { return nil }
