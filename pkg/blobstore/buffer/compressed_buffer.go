package buffer

import (
	"context"
	"io"
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
	if c, ok := b.(interface{ zstdChunkReader(int) (ChunkReader, bool) }); ok {
		return c.zstdChunkReader(maximumChunkSizeBytes)
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

func (b *casErrorHandlingBuffer) zstdChunkReader(limit int) (ChunkReader, bool) {
	if _, ok := b.errorHandler.(NonRetryingErrorHandler); !ok {
		return nil, false
	}
	r, ok := ZSTDChunkReader(b.base, limit)
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
func (r *annotatingCompressedReader) Close() { r.ChunkReader.Close(); r.handler.Done() }

func (b *casBufferWithBackgroundTask) zstdChunkReader(limit int) (ChunkReader, bool) {
	r, ok := ZSTDChunkReader(b.base, limit)
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
	ctx     context.Context
	digest  digest.Digest
	source  Source
	pool    zstd.Pool
	encoded io.ReadCloser
	stored  *storedRepresentation
}

// NewCASBufferFromZSTDReader preserves encoded bytes from a trusted backend.
// Logical consumers acquire a decoder and validate size and digest normally.
// This constructor must not be used to bypass validation of user uploads.
func NewCASBufferFromZSTDReader(ctx context.Context, d digest.Digest, r io.ReadCloser, pool zstd.Pool, source Source) Buffer {
	return &compressedBuffer{ctx: ctx, digest: d, encoded: r, pool: pool, source: source}
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
	case *compressedBuffer:
		b.ctx = ctx
	case *casBufferWithBackgroundTask:
		WithZSTDContext(b.base, ctx)
	case *casErrorHandlingBuffer:
		WithZSTDContext(b.base, ctx)
	}
	return b
}

func (b *compressedBuffer) encodedReader() io.ReadCloser {
	if b.stored == nil {
		return b.encoded
	}
	return newChunkReaderBackedReader(&storedFrameReader{ctx: b.ctx, stored: b.stored, source: b.source})
}

func (b *compressedBuffer) logical() Buffer {
	r := b.encodedReader()
	decoded, err := zstd.NewReadCloser(b.ctx, b.pool, r)
	if err != nil {
		r.Close()
		return NewBufferFromError(err)
	}
	return NewCASBufferFromReader(b.digest, decoded, b.source)
}

func (b *compressedBuffer) GetSizeBytes() (int64, error) { return b.digest.GetSizeBytes(), nil }
func (b *compressedBuffer) IntoWriter(w io.Writer) error { return b.logical().IntoWriter(w) }
func (b *compressedBuffer) ReadAt(p []byte, off int64) (int, error) {
	return b.logical().ReadAt(p, off)
}

func (b *compressedBuffer) ToProto(m proto.Message, limit int) (proto.Message, error) {
	return b.logical().ToProto(m, limit)
}

func (b *compressedBuffer) ToByteSlice(limit int) ([]byte, error) {
	return b.logical().ToByteSlice(limit)
}

func (b *compressedBuffer) ToChunkReader(off int64, limit int) ChunkReader {
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
	} else {
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

func (b *compressedBuffer) zstdChunkReader(limit int) (ChunkReader, bool) {
	return newReaderBackedChunkReader(b.encodedReader(), limit), true
}

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
	if status.Code(err) == codes.DataLoss {
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
// destination may use a different representation.
func ForLocalRefresh(b Buffer) Buffer {
	if c, ok := b.(*compressedBuffer); ok && c.stored != nil {
		return &representationCopyBuffer{Buffer: NewValidatedBufferFromReaderAt(&storedReference{c.stored}, c.stored.size), original: c}
	}
	return b
}

type storedReference struct{ *storedRepresentation }

func (r *storedReference) ReadAt(p []byte, off int64) (int, error) { return r.r.ReadAt(p, off) }
func (r *storedReference) Close() error                            { r.release(); return nil }

type representationCopyBuffer struct {
	Buffer
	original *compressedBuffer
}

func (b *representationCopyBuffer) IntoWriter(w io.Writer) error {
	defer b.Buffer.Discard()
	err := b.original.stored.index.WriteRepresentation(b.original.ctx, w)
	if status.Code(err) == codes.DataLoss {
		b.original.source.dataIntegrityCallback(false)
	}
	return err
}
