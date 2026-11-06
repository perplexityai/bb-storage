package blobstore

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"syscall"
	"time"

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
	bytes              *stagingBudget
	requests           *semaphore.Weighted
	pool               zstd.Pool
}

// NewCompressedCASBlobAccess stores validated compressed representations in a
// local block store using NewCompressedCASReadBufferFactory. Staging is bounded
// before reading an upload, and unlinked files are reclaimed even after a crash.
// The directory must reside on a filesystem supporting unlink of open files.
// pool must be dedicated to uploads, separate from the read-buffer factory's pool.
func NewCompressedCASBlobAccess(base BlobAccess, directory string, maximumStagedBytes, maximumWrites int64, pool zstd.Pool) (BlobAccess, error) {
	if err := ValidateCompressedCASConfiguration(directory, maximumStagedBytes, maximumWrites, pool); err != nil {
		return nil, err
	}
	return &compressedCASBlobAccess{BlobAccess: base, directory: directory, maximumStagedBytes: maximumStagedBytes, bytes: &stagingBudget{available: maximumStagedBytes, changed: make(chan struct{})}, requests: semaphore.NewWeighted(4 * maximumWrites), pool: pool}, nil
}

// ValidateCompressedCASConfiguration checks staging requirements before a
// configuration builder opens or truncates the storage device.
func ValidateCompressedCASConfiguration(directory string, maximumStagedBytes, maximumWrites int64, pool zstd.Pool) error {
	minimum, _ := zstdframes.MaximumStoredSize(0, storedFrameSize)
	if directory == "" || maximumStagedBytes < minimum || maximumWrites <= 0 || maximumWrites > math.MaxInt64/4 || pool == nil {
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

// Presence requires readable metadata: otherwise clients that check before
// uploading can never repair unsupported records. Get also refreshes old blocks.
func (ba *compressedCASBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing := digest.NewSetBuilder(digests.Length())
	for _, d := range digests.Items() {
		if err := ctx.Err(); err != nil {
			return digest.EmptySet, status.FromContextError(err).Err()
		}
		err := ba.probeRecord(ctx, d)
		switch status.Code(err) {
		case codes.OK:
		case codes.NotFound, codes.FailedPrecondition, codes.DataLoss:
			missing.Add(d)
		default:
			return digest.EmptySet, err
		}
	}
	return missing.Build(), nil
}

func (ba *compressedCASBlobAccess) probeRecord(ctx context.Context, d digest.Digest) error {
	r := buffer.NewReaderFromChunkReader(ba.Get(ctx, d).ToChunkReader(d.GetSizeBytes(), 1))
	var probe [1]byte
	_, err := r.Read(probe[:])
	if closeErr := r.Close(); closeErr != nil && (err == nil || err == io.EOF) {
		err = closeErr
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (ba *compressedCASBlobAccess) Put(ctx context.Context, d digest.Digest, b buffer.Buffer) error {
	if err := ctx.Err(); err != nil {
		b.Discard()
		return status.FromContextError(err).Err()
	}
	if !ba.requests.TryAcquire(1) {
		b.Discard()
		return status.Error(codes.ResourceExhausted, "Compressed upload queue is full")
	}
	defer ba.requests.Release(1)
	// This runs inside Put, after any upload authorization. Checking in the
	// protocol handler via FindMissing would instead apply read-side policy.
	missing, err := ba.BlobAccess.FindMissing(ctx, d.ToSingletonSet())
	if err != nil && status.Code(err) != codes.Unavailable && status.Code(err) != codes.Internal && status.Code(err) != codes.DataLoss && status.Code(err) != codes.Unknown && status.Code(err) != codes.FailedPrecondition {
		b.Discard()
		return err
	}
	repair := err != nil
	if err == nil && missing.Empty() {
		// Opening the logical end validates the index without decoding the
		// payload. Presence alone cannot identify unsupported or unreadable
		// records, and must not suppress an authorized repair upload.
		readErr := ba.probeRecord(ctx, d)
		if readErr == nil || readErr == io.EOF {
			return buffer.SkipUpload(b)
		}
		switch status.Code(readErr) {
		case codes.Canceled, codes.DeadlineExceeded, codes.PermissionDenied, codes.Unauthenticated, codes.ResourceExhausted:
			b.Discard()
			return readErr
		}
		repair = true
	}
	put := ba.BlobAccess.Put
	if repair {
		if local, ok := ba.BlobAccess.(interface {
			PutRepair(context.Context, digest.Digest, buffer.Buffer) error
		}); ok {
			put = local.PutRepair
		}
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
	if err := acquireUploadCapacity(ctx, ba.bytes, reservation); err != nil {
		b.Discard()
		return err
	}
	defer ba.bytes.Release(reservation)
	f, err := ba.createStagingFile()
	if err != nil {
		b.Discard()
		return err
	}
	defer f.Close()
	var physicalSize int64
	if err := buffer.ConsumeCASUpload(ctx, b, ba.pool, func(input buffer.Buffer) (result error) {
		r := input.ToReader()
		defer func() {
			if err := r.Close(); result == nil {
				result = err
			}
		}()
		// A retry replaces the entire output, including any longer previous tail.
		if err := f.Truncate(0); err != nil {
			return &buffer.UploadDestinationError{Err: stagingError(err)}
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return &buffer.UploadDestinationError{Err: stagingError(err)}
		}
		w := &stagingWriter{file: f, remaining: reservation}
		if err := zstdframes.Encode(ctx, w, d, r, storedFrameSize, ba.pool); err != nil {
			var encoding *zstdframes.EncodingError
			if errors.As(err, &encoding) {
				return &buffer.UploadDestinationError{Err: encoding.Err}
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		physicalSize = reservation - w.remaining
		return nil
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if d.GetSizeBytes() <= 4096 && physicalSize >= d.GetSizeBytes() {
		// Tiny records do not justify an index. Larger incompressible records
		// retain frame checksums so indexed random reads remain efficient.
		// Decode staging directly into the raw allocation without another file.
		reader := &stagingReader{ctx: ctx, File: f}
		index, err := zstdframes.Open(reader, physicalSize, d)
		if err != nil {
			return err
		}
		logical := buffer.NewCASBufferFromZSTDRepresentation(d, reader, physicalSize, index, ba.pool, buffer.BackendProvided(func(bool) {}))
		return put(ctx, d, buffer.WithZSTDContext(logical, ctx))
	}
	return put(ctx, d, buffer.NewValidatedBufferFromReaderAt(&stagingReader{ctx: ctx, File: f}, physicalSize))
}

// A fitting upload may pass a larger waiter. The request semaphore bounds the
// number of waiters; the acquisition deadline bounds starvation of large blobs.
type stagingBudget struct {
	mu        sync.Mutex
	available int64
	changed   chan struct{}
}

func (b *stagingBudget) Acquire(ctx context.Context, amount int64) error {
	for {
		b.mu.Lock()
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return err
		}
		if amount <= b.available {
			b.available -= amount
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *stagingBudget) Release(amount int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.available += amount
	close(b.changed)
	b.changed = make(chan struct{})
}

func acquireUploadCapacity(ctx context.Context, capacity *stagingBudget, amount int64) error {
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := capacity.Acquire(waitCtx, amount); err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Error(codes.ResourceExhausted, "Timed out waiting for compressed upload capacity")
	}
	return nil
}

func (ba *compressedCASBlobAccess) createStagingFile() (*os.File, error) {
	f, err := os.CreateTemp(ba.directory, "bb-storage-compressed-")
	if err != nil {
		return nil, stagingError(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		f.Close()
		return nil, stagingError(err)
	}
	return f, nil
}

type stagingWriter struct {
	file      *os.File
	remaining int64
}

func (w *stagingWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, &buffer.UploadDestinationError{Err: status.Error(codes.ResourceExhausted, "Compressed upload exceeded staging reservation")}
	}
	n, err := w.file.Write(p)
	w.remaining -= int64(n)
	if err != nil {
		return n, &buffer.UploadDestinationError{Err: stagingError(err)}
	}
	return n, nil
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

type compressedCASReadBufferFactory struct {
	pool    zstd.Pool
	indexes *zstdframes.IndexCache
}

// NewCompressedCASReadBufferFactory reads indexed compressed CAS and tiny raw
// records. Encoded checksums always remain enabled, including during refresh.
// This factory is intended for random-access local block storage.
func NewCompressedCASReadBufferFactory(pool zstd.Pool) ReadBufferFactory {
	return &compressedCASReadBufferFactory{pool: pool, indexes: zstdframes.NewIndexCache(64 << 20)}
}

func (f *compressedCASReadBufferFactory) NewBufferFromReaderAt(d digest.Digest, r buffer.ReadAtCloser, size int64, callback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewCASBufferFromDeferredReaderAt(d, r, buffer.BackendProvided(callback), func(r buffer.ReadAtCloser) buffer.Buffer {
		// Raw records are produced only by the tiny-record optimization. Full
		// validation is bounded to 4 KiB; larger unindexed records are rejected.
		if size == d.GetSizeBytes() && size <= 4096 {
			return CASReadBufferFactory.NewBufferFromReaderAt(d, r, size, callback)
		}
		index, err := f.indexes.Open(r, size, d)
		if err == nil {
			return buffer.NewCASBufferFromZSTDRepresentation(d, r, size, index, f.pool, buffer.BackendProvided(callback))
		}
		if err != zstdframes.ErrInvalidRepresentation {
			r.Close()
			return buffer.NewBufferFromError(err)
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
