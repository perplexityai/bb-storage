package blobstore

import (
	"context"
	"io"
	"os"

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
	directory string
	bytes     *semaphore.Weighted
	writes    *semaphore.Weighted
	pool      zstd.Pool
}

// NewCompressedCASBlobAccess stores validated compressed representations in a
// local block store using NewCompressedCASReadBufferFactory. Staging is bounded
// before reading an upload, and unlinked files are reclaimed even after a crash.
// The directory must reside on a filesystem supporting unlink of open files.
func NewCompressedCASBlobAccess(base BlobAccess, directory string, maximumStagedBytes, maximumWrites int64, pool zstd.Pool) (BlobAccess, error) {
	if directory == "" || maximumStagedBytes <= 0 || maximumWrites <= 0 || pool == nil {
		return nil, status.Error(codes.InvalidArgument, "Compressed storage requires a staging directory, positive byte/write budgets, and a codec pool")
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, status.Error(codes.InvalidArgument, "Compression staging path is not a directory")
	}
	return &compressedCASBlobAccess{BlobAccess: base, directory: directory, bytes: semaphore.NewWeighted(maximumStagedBytes), writes: semaphore.NewWeighted(maximumWrites), pool: pool}, nil
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
	reservation, err := zstdframes.MaximumStoredSize(d.GetSizeBytes(), storedFrameSize)
	if err != nil {
		b.Discard()
		return err
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
		return err
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
	return ba.BlobAccess.Put(ctx, d, buffer.NewValidatedBufferFromReaderAt(&stagingReader{ctx: ctx, File: f}, reservation-w.remaining))
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
	return n, err
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
	index, err := zstdframes.Open(r, size, d)
	if err == nil {
		return buffer.NewCASBufferFromZSTDRepresentation(d, r, size, index, f.pool, buffer.BackendProvided(callback))
	}
	if size == d.GetSizeBytes() {
		// A raw blob may happen to end in format-like bytes. Only full logical
		// validation can accept the legacy interpretation of such a record.
		return CASReadBufferFactory.NewBufferFromReaderAt(d, r, size, callback)
	}
	r.Close()
	callback(false)
	return buffer.NewBufferFromError(err)
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
