package buffer

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type sharedDeferredReader struct {
	source ReadAtCloser
	refs   atomic.Int64
}

func (r *sharedDeferredReader) ReadAt(p []byte, off int64) (int, error) {
	return r.source.ReadAt(p, off)
}

func (r *sharedDeferredReader) Close() error {
	if r.refs.Add(-1) == 0 {
		return r.source.Close()
	}
	return nil
}

type deferredCASBuffer struct {
	ctx      context.Context
	digest   digest.Digest
	source   Source
	reader   *sharedDeferredReader
	open     func(ReadAtCloser) Buffer
	resolved Buffer
}

// NewCASBufferFromDeferredReaderAt pins a random-access record without opening
// or inspecting its format. Local stores construct and clone buffers while
// holding their metadata lock, so I/O must wait until a consumer uses the data.
// open takes ownership of its reader and must return a buffer even on failure.
// Clones may invoke open independently; the underlying reader must support
// concurrent ReadAt calls and remains open until every clone is released.
func NewCASBufferFromDeferredReaderAt(d digest.Digest, r ReadAtCloser, source Source, open func(ReadAtCloser) Buffer) Buffer {
	shared := &sharedDeferredReader{source: r}
	shared.refs.Store(1)
	return &deferredCASBuffer{ctx: context.Background(), digest: d, source: source, reader: shared, open: open}
}

func (b *deferredCASBuffer) resolve() Buffer {
	if b.resolved == nil {
		if err := b.ctx.Err(); err != nil {
			b.reader.Close()
			b.resolved = NewBufferFromError(status.FromContextError(err).Err())
		} else {
			b.resolved = WithZSTDContext(b.open(b.reader), b.ctx)
		}
	}
	return b.resolved
}
func (b *deferredCASBuffer) GetSizeBytes() (int64, error) { return b.digest.GetSizeBytes(), nil }
func (b *deferredCASBuffer) IntoWriter(w io.Writer) error { return b.resolve().IntoWriter(w) }
func (b *deferredCASBuffer) ReadAt(p []byte, off int64) (int, error) {
	return b.resolve().ReadAt(p, off)
}

func (b *deferredCASBuffer) ToProto(m proto.Message, limit int) (proto.Message, error) {
	return b.resolve().ToProto(m, limit)
}

func (b *deferredCASBuffer) ToByteSlice(limit int) ([]byte, error) {
	return b.resolve().ToByteSlice(limit)
}

func (b *deferredCASBuffer) ToChunkReader(off int64, limit int) ChunkReader {
	return b.resolve().ToChunkReader(off, limit)
}
func (b *deferredCASBuffer) ToReader() io.ReadCloser { return b.resolve().ToReader() }
func (b *deferredCASBuffer) toUnvalidatedChunkReader(off int64, limit int) ChunkReader {
	return b.resolve().toUnvalidatedChunkReader(off, limit)
}

func (b *deferredCASBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return b.resolve().toUnvalidatedReader(off)
}
func (b *deferredCASBuffer) CloneCopy(limit int) (Buffer, Buffer) { return b.CloneStream() }
func (b *deferredCASBuffer) CloneStream() (Buffer, Buffer) {
	if b.resolved != nil {
		return b.resolved.CloneStream()
	}
	b.reader.refs.Add(1)
	clone := *b
	return b, &clone
}

func (b *deferredCASBuffer) WithTask(task func() error) Buffer {
	return newCASBufferWithBackgroundTask(b, b.digest, b.source, task)
}

func (b *deferredCASBuffer) applyErrorHandler(h ErrorHandler) (Buffer, bool) {
	return newCASErrorHandlingBuffer(b, h, b.digest, b.source), false
}

func (b *deferredCASBuffer) Discard() {
	if b.resolved != nil {
		b.resolved.Discard()
	} else {
		b.reader.Close()
	}
}

func (b *deferredCASBuffer) zstdChunkReaderAt(off int64, limit int) (ChunkReader, bool) {
	return ZSTDChunkReaderAt(b.resolve(), off, limit)
}
