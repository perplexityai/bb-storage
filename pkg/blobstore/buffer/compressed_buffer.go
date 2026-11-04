package buffer

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ZSTDChunkReader consumes a buffer's encoded representation when available.
// False leaves the buffer untouched. Encoded forwarding trusts the backend's
// logical validation; clients must still validate their logical digest.
func ZSTDChunkReader(b Buffer, maximumChunkSizeBytes int) (ChunkReader, bool) {
	return ZSTDChunkReaderAt(b, 0, maximumChunkSizeBytes)
}

// ZSTDChunkReaderAt reads a compressed suffix using logical byte offsets.
// Retrying decorators fall back to logical reads so replacements never resume
// an encoded stream at an offset belonging to a different representation.
func ZSTDChunkReaderAt(b Buffer, offset int64, maximumChunkSizeBytes int) (ChunkReader, bool) {
	if c, ok := b.(interface {
		zstdChunkReaderAt(int64, int) (ChunkReader, bool)
	}); ok {
		return c.zstdChunkReaderAt(offset, maximumChunkSizeBytes)
	}
	return nil, false
}

// NonRetryingErrorHandler identifies observers that only annotate errors. A
// retrying handler must keep logical-offset reads: compressed offsets cannot
// safely be resumed against an alternative encoding of the same logical blob.
type NonRetryingErrorHandler interface {
	ErrorHandler
	NonRetrying()
}

func (b *casErrorHandlingBuffer) zstdChunkReaderAt(offset int64, limit int) (ChunkReader, bool) {
	if _, ok := b.errorHandler.(NonRetryingErrorHandler); !ok {
		return nil, false
	}
	r, ok := ZSTDChunkReaderAt(b.base, offset, limit)
	if !ok {
		return nil, false
	}
	return &annotatingCompressedReader{ChunkReader: r, handler: b.errorHandler}, true
}

type annotatingCompressedReader struct {
	ChunkReader
	handler ErrorHandler
}

func (r *annotatingCompressedReader) Read() ([]byte, error) {
	p, err := r.ChunkReader.Read()
	if err != nil && err != io.EOF {
		replacement, translated := r.handler.OnError(err)
		if replacement != nil {
			replacement.Discard()
			return nil, status.Error(codes.Internal, "Non-retrying error handler returned a replacement")
		}
		return nil, translated
	}
	return p, err
}
func (r *annotatingCompressedReader) Close() { r.CloseWithError() }

func (r *annotatingCompressedReader) CloseWithError() error {
	err := closeChunkReader(r.ChunkReader)
	r.handler.Done()
	return err
}

func (b *casBufferWithBackgroundTask) zstdChunkReaderAt(offset int64, limit int) (ChunkReader, bool) {
	r, ok := ZSTDChunkReaderAt(b.base, offset, limit)
	if !ok {
		return nil, false
	}
	return b.decorateChunkReader(r), true
}

type storedRepresentation struct {
	r     ReadAtCloser
	size  int64
	index *zstdframes.Reader
	refs  atomic.Int64
}

func (s *storedRepresentation) release() {
	if s.refs.Add(-1) == 0 {
		s.r.Close()
	}
}

type compressedBuffer struct {
	ctx         context.Context
	digest      digest.Digest
	source      Source
	pool        zstd.Pool
	encoded     io.ReadCloser
	stored      *storedRepresentation
	upload      bool
	readEncoded func(int64) (io.ReadCloser, error)
}

// NewCASBufferFromZSTDReader preserves encoded bytes from a trusted backend.
// Logical consumers acquire a decoder and validate size and digest normally.
// This constructor must not be used to bypass validation of user uploads.
func NewCASBufferFromZSTDReader(ctx context.Context, d digest.Digest, r io.ReadCloser, pool zstd.Pool, source Source) Buffer {
	return &compressedBuffer{ctx: ctx, digest: d, encoded: r, pool: pool, source: source}
}

// NewCASBufferFromZSTDReadFactory defers a trusted backend read until its
// logical offset is known. The factory owns each returned stream; it is called
// at most once, and Discard before consumption requires no RPC or codec.
func NewCASBufferFromZSTDReadFactory(ctx context.Context, d digest.Digest, read func(int64) (io.ReadCloser, error), pool zstd.Pool, source Source) Buffer {
	return &compressedBuffer{ctx: ctx, digest: d, readEncoded: read, pool: pool, source: source}
}

// NewCASBufferFromZSTDUpload retains an untrusted upload until a logical
// consumer validates it. Only an RPC upload to a validating CAS server may
// forward these bytes without decoding; they are never eligible for reads.
func NewCASBufferFromZSTDUpload(ctx context.Context, d digest.Digest, r io.ReadCloser, pool zstd.Pool) Buffer {
	return &compressedBuffer{ctx: ctx, digest: d, encoded: zstd.LimitUpload(r, d.GetSizeBytes()), pool: pool, source: UserProvided, upload: true}
}

// ZSTDUploadReader consumes an encoded upload for forwarding to a CAS server
// that validates the uncompressed digest and size before returning success.
// False leaves the buffer untouched. Logical consumers always validate locally.
func ZSTDUploadReader(b Buffer) (io.ReadCloser, bool) {
	switch b := b.(type) {
	case *compressedBuffer:
		r, err := b.encodedReader()
		if err != nil {
			return newErrorReader(err), true
		}
		return r, true
	case *deferredCASBuffer:
		return ZSTDUploadReader(b.resolve())
	case *casBufferWithBackgroundTask:
		if r, ok := ZSTDUploadReader(b.base); ok {
			return b.decorateReader(r), true
		}
	case *casErrorHandlingBuffer:
		if _, ok := b.errorHandler.(NonRetryingErrorHandler); ok {
			if r, ok := ZSTDUploadReader(b.base); ok {
				return newChunkReaderBackedReader(&annotatingCompressedReader{ChunkReader: newReaderBackedChunkReader(r, 64*1024), handler: b.errorHandler}), true
			}
		}
	}
	return nil, false
}

// ConsumeCASUpload validates a streaming upload using the destination's codec
// pool while retaining source retries, integrity callbacks, and completion.
// Each retry invokes consume with the entire replacement. The callback must
// reset its output, close the reader, and publish nothing before success.
func ConsumeCASUpload(ctx context.Context, b Buffer, pool zstd.Pool, consume func(Buffer) error) error {
	err := consumeCASUpload(ctx, b, pool, consume)
	var destination *UploadDestinationError
	if errors.As(err, &destination) {
		return destination.Err
	}
	return err
}

// UploadDestinationError prevents failures of the staging destination from
// being attributed to the source or triggering a fallback to another source.
type UploadDestinationError struct{ Err error }

func (e *UploadDestinationError) Error() string { return e.Err.Error() }
func (e *UploadDestinationError) Unwrap() error { return e.Err }

func consumeCASUpload(ctx context.Context, b Buffer, pool zstd.Pool, consume func(Buffer) error) error {
	if err := ctx.Err(); err != nil {
		b.Discard()
		return status.FromContextError(err).Err()
	}
	switch b := b.(type) {
	case *casErrorHandlingBuffer:
		defer b.errorHandler.Done()
		base := b.base
		for {
			err := consumeCASUpload(ctx, base, pool, consume)
			var destination *UploadDestinationError
			if err == nil || err == io.EOF || ctx.Err() != nil || errors.As(err, &destination) {
				return err
			}
			base, err = b.errorHandler.OnError(err)
			if err != nil {
				return err
			}
		}
	case *casBufferWithBackgroundTask:
		err := consumeCASUpload(ctx, b.base, pool, consume)
		<-b.task.completion
		if err != nil {
			return err
		}
		return b.task.err
	case *deferredCASBuffer:
		return consumeCASUpload(ctx, b.resolve(), pool, consume)
	case *compressedBuffer:
		r, err := b.encodedReader()
		if err != nil {
			return err
		}
		copy := *b
		copy.ctx = ctx
		copy.pool = pool
		copy.encoded = zstd.LimitUpload(r, b.digest.GetSizeBytes())
		copy.stored = nil
		copy.readEncoded = nil
		return consume(&copy)
	default:
		return consume(b)
	}
}

// NewCASBufferFromZSTDRepresentation takes ownership of an already opened,
// checksummed local representation and its backing reader.
func NewCASBufferFromZSTDRepresentation(d digest.Digest, r ReadAtCloser, size int64, index *zstdframes.Reader, pool zstd.Pool, source Source) Buffer {
	s := &storedRepresentation{r: r, size: size, index: index}
	s.refs.Store(1)
	return &compressedBuffer{ctx: context.Background(), digest: d, stored: s, pool: pool, source: source}
}

// WithZSTDContext binds local decoder waits to the request after storage has
// acquired its block reference. Call before consuming or sharing the buffer.
func WithZSTDContext(b Buffer, ctx context.Context) Buffer {
	switch b := b.(type) {
	case *deferredCASBuffer:
		b.ctx = ctx
		if b.resolved != nil {
			WithZSTDContext(b.resolved, ctx)
		}
	case *compressedBuffer:
		b.ctx = ctx
	case *casBufferWithBackgroundTask:
		WithZSTDContext(b.base, ctx)
	case *casErrorHandlingBuffer:
		WithZSTDContext(b.base, ctx)
	}
	return b
}

func (b *compressedBuffer) encodedReader() (io.ReadCloser, error) {
	if b.readEncoded != nil {
		return b.readEncoded(0)
	}
	if b.stored == nil {
		return b.encoded, nil
	}
	return newChunkReaderBackedReader(&storedFrameReader{ctx: b.ctx, stored: b.stored, source: b.source}), nil
}

func (b *compressedBuffer) logical() Buffer {
	r, err := b.encodedReader()
	if err != nil {
		return NewBufferFromError(err)
	}
	var uploadSource *uploadErrorReader
	if b.upload {
		uploadSource = &uploadErrorReader{ReadCloser: r}
		r = uploadSource
	}
	decoded, err := zstd.NewReadCloser(b.ctx, b.pool, r)
	if err != nil {
		r.Close()
		return NewBufferFromError(err)
	}
	if uploadSource != nil {
		decoded = &uploadDecodedReader{ReadCloser: decoded, source: uploadSource}
	}
	return NewCASBufferFromReader(b.digest, decoded, b.source)
}

func (b *compressedBuffer) GetSizeBytes() (int64, error) { return b.digest.GetSizeBytes(), nil }
func (b *compressedBuffer) IntoWriter(w io.Writer) error { return b.logical().IntoWriter(w) }
func (b *compressedBuffer) ReadAt(p []byte, off int64) (int, error) {
	if b.stored == nil {
		return b.logical().ReadAt(p, off)
	}
	defer b.Discard()
	if off < 0 {
		return 0, status.Error(codes.InvalidArgument, "Negative read offset")
	}
	if off > b.digest.GetSizeBytes() {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < b.digest.GetSizeBytes() {
		data, _, err := b.stored.index.ReadFrameAt(b.ctx, off, false, b.pool)
		if err != nil {
			if err == zstdframes.ErrCorruptFrame {
				b.source.dataIntegrityCallback(false)
			}
			return n, err
		}
		copied := copy(p[n:], data)
		n += copied
		off += int64(copied)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b *compressedBuffer) ToProto(m proto.Message, limit int) (proto.Message, error) {
	return b.logical().ToProto(m, limit)
}

func (b *compressedBuffer) ToByteSlice(limit int) ([]byte, error) {
	return b.logical().ToByteSlice(limit)
}

func (b *compressedBuffer) ToChunkReader(off int64, limit int) ChunkReader {
	if b.stored != nil && off > 0 {
		return newReaderBackedChunkReader(newChunkReaderBackedReader(&storedSuffixReader{buffer: b, offset: off}), limit)
	}
	return b.logical().ToChunkReader(off, limit)
}
func (b *compressedBuffer) ToReader() io.ReadCloser { return b.logical().ToReader() }
func (b *compressedBuffer) toUnvalidatedChunkReader(off int64, limit int) ChunkReader {
	return b.logical().toUnvalidatedChunkReader(off, limit)
}

func (b *compressedBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return b.logical().toUnvalidatedReader(off)
}

func (b *compressedBuffer) Discard() {
	if b.stored != nil {
		b.stored.release()
	} else if b.encoded != nil {
		b.encoded.Close()
	}
}

func (b *compressedBuffer) CloneCopy(limit int) (Buffer, Buffer) {
	if b.stored == nil {
		return b.logical().CloneCopy(limit)
	}
	return b.CloneStream()
}

func (b *compressedBuffer) CloneStream() (Buffer, Buffer) {
	if b.stored == nil {
		return b.logical().CloneStream()
	}
	b.stored.refs.Add(1)
	clone := *b
	return b, &clone
}

func (b *compressedBuffer) WithTask(task func() error) Buffer {
	return newCASBufferWithBackgroundTask(b, b.digest, b.source, task)
}

func (b *compressedBuffer) applyErrorHandler(h ErrorHandler) (Buffer, bool) {
	return newCASErrorHandlingBuffer(b, h, b.digest, b.source), false
}

func (b *compressedBuffer) zstdChunkReaderAt(offset int64, limit int) (ChunkReader, bool) {
	if b.upload {
		return nil, false
	}
	if offset < 0 || offset > b.digest.GetSizeBytes() {
		b.Discard()
		return newErrorChunkReader(status.Error(codes.OutOfRange, "Logical offset is out of range")), true
	}
	if b.readEncoded != nil {
		r, err := b.readEncoded(offset)
		if err != nil {
			return newErrorChunkReader(err), true
		}
		return newReaderBackedChunkReader(r, limit), true
	}
	if offset == 0 {
		r, err := b.encodedReader()
		if err != nil {
			return newErrorChunkReader(err), true
		}
		return newReaderBackedChunkReader(r, limit), true
	}
	if b.stored != nil {
		return newReaderBackedChunkReader(newChunkReaderBackedReader(&storedSuffixReader{buffer: b, offset: offset, compressed: true}), limit), true
	}
	return nil, false
}

type storedSuffixReader struct {
	buffer     *compressedBuffer
	offset     int64
	compressed bool
	done       bool
}

func (r *storedSuffixReader) Read() ([]byte, error) {
	if err := r.buffer.ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r.done {
		return nil, io.EOF
	}
	b := r.buffer
	data, next, err := b.stored.index.ReadFrameAt(b.ctx, r.offset, r.compressed, b.pool)
	if err == zstdframes.ErrCorruptFrame {
		b.source.dataIntegrityCallback(false)
	}
	if err != nil {
		return nil, err
	}
	r.offset = next
	r.done = next == b.digest.GetSizeBytes()
	return data, nil
}
func (r *storedSuffixReader) Close() { r.buffer.Discard() }

type storedFrameReader struct {
	ctx    context.Context
	stored *storedRepresentation
	source Source
	i      int
}

func (r *storedFrameReader) Read() ([]byte, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r.i == r.stored.index.FrameCount() {
		r.source.notifyDataValid()
		return nil, io.EOF
	}
	data, err := r.stored.index.ReadCompressedFrame(r.ctx, r.i)
	if err == zstdframes.ErrCorruptFrame {
		r.source.dataIntegrityCallback(false)
	}
	if err == nil {
		r.i++
	}
	return data, err
}
func (r *storedFrameReader) Close() { r.stored.release() }

// ForLocalRefresh consumes the physical representation when refreshing a local
// record with its existing physical size. Never use for an ordinary Put, whose
// destination may use a different representation. Other data access methods
// fail with FailedPrecondition; GetSizeBytes reports only the physical length.
func ForLocalRefresh(b Buffer) Buffer {
	if d, ok := b.(*deferredCASBuffer); ok {
		return ForLocalRefresh(d.resolve())
	}
	if c, ok := b.(*compressedBuffer); ok && c.stored != nil {
		return &representationCopyBuffer{Buffer: NewValidatedBufferFromReaderAt(&storedReference{c.stored}, c.stored.size), original: c}
	}
	return b
}

type storedReference struct{ *storedRepresentation }

func (*storedReference) ReadAt(p []byte, off int64) (int, error) {
	return 0, status.Error(codes.FailedPrecondition, "Physical refresh buffers support only IntoWriter")
}
func (r *storedReference) Close() error { r.release(); return nil }

type representationCopyBuffer struct {
	Buffer
	original *compressedBuffer
}

func (b *representationCopyBuffer) IntoWriter(w io.Writer) error {
	defer b.Buffer.Discard()
	err := b.original.stored.index.WriteRepresentation(b.original.ctx, w)
	if err == zstdframes.ErrCorruptFrame {
		b.original.source.dataIntegrityCallback(false)
	}
	return err
}

// Preserve transport failures while classifying malformed encoded input as a
// client error. The decoder itself does not distinguish those error domains.
type uploadErrorReader struct {
	io.ReadCloser
	mu  sync.Mutex
	err error
}

func (r *uploadErrorReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil {
		r.mu.Lock()
		r.err = err
		r.mu.Unlock()
	}
	return n, err
}

func (r *uploadErrorReader) sourceError() error { r.mu.Lock(); defer r.mu.Unlock(); return r.err }

type uploadDecodedReader struct {
	io.ReadCloser
	source *uploadErrorReader
}

func (r *uploadDecodedReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		if sourceErr := r.source.sourceError(); sourceErr != nil && sourceErr != io.EOF {
			return n, sourceErr
		}
		if _, ok := status.FromError(err); !ok {
			return n, status.Errorf(codes.InvalidArgument, "Invalid compressed upload: %v", err)
		}
	}
	return n, err
}
