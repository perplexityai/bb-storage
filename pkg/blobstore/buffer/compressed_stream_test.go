package buffer_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type completionBarrierReader struct {
	io.Reader
	released <-chan struct{}
}

func (r *completionBarrierReader) Close() error { <-r.released; return nil }

type uploadCompletionReader struct {
	data   io.Reader
	reads  atomic.Int64
	closes atomic.Int64
	err    error
}

func (r *uploadCompletionReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	if r.data != nil {
		return r.data.Read(p)
	}
	return 0, io.EOF
}
func (r *uploadCompletionReader) Close() error { r.closes.Add(1); return r.err }

type observedPool struct {
	bb_zstd.Pool
	encoders, decoders atomic.Int64
}

func (p *observedPool) NewEncoder(ctx context.Context, w io.Writer) (bb_zstd.Encoder, error) {
	p.encoders.Add(1)
	return p.Pool.NewEncoder(ctx, w)
}

func (p *observedPool) NewDecoder(ctx context.Context, r io.Reader) (bb_zstd.Decoder, error) {
	p.decoders.Add(1)
	return p.Pool.NewDecoder(ctx, r)
}

func pool() *observedPool {
	return &observedPool{Pool: bb_zstd.NewBoundedPool(1, 1, []zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(1 << 20)}, []zstd.DOption{zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(4 << 20)})}
}

func digestOf(p []byte) digest.Digest {
	g := digest.MustNewFunction("allowed", remoteexecution.DigestFunction_SHA256).NewGenerator(int64(len(p)))
	g.Write(p)
	return g.Sum()
}

func TestKnownReadFailureDoesNotWaitForDecoder(t *testing.T) {
	p := pool()
	held, err := p.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	defer held.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	d := digestOf([]byte("hello"))
	expected := status.Error(codes.Unavailable, "Backend read failed")
	b := buffer.NewCASBufferFromZSTDReadFactory(ctx, d, func(int64) (io.ReadCloser, error) { return nil, expected }, p, buffer.BackendProvided(buffer.Irreparable(d)))
	_, err = b.ToByteSlice(5)
	require.Equal(t, expected, err)
	require.NoError(t, ctx.Err())
	require.EqualValues(t, 1, p.decoders.Load(), "a known error must not attempt another codec acquisition")
}

func TestClonedReadersPreserveFinalCloseError(t *testing.T) {
	data := []byte("clone close")
	d := digestOf(data)
	expected := status.Error(codes.Unavailable, "Completion failed")
	source := &uploadCompletionReader{data: bytes.NewReader(data), err: expected}
	first, second := buffer.NewCASBufferFromReader(d, source, buffer.UserProvided).CloneStream()
	readers := make(chan io.ReadCloser, 2)
	go func() { readers <- first.ToReader() }()
	go func() { readers <- second.ToReader() }()
	a, b := <-readers, <-readers
	require.NoError(t, a.Close())
	require.Equal(t, expected, b.Close())
	require.EqualValues(t, 1, source.closes.Load())
}

func TestClonedStreamReportsEOFBeforeClosingSource(t *testing.T) {
	data := []byte("consumers must observe EOF before source completion")
	released := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	defer release()
	first, second := buffer.NewCASBufferFromReader(digestOf(data), &completionBarrierReader{Reader: bytes.NewReader(data), released: released}, buffer.UserProvided).CloneStream()
	var finished atomic.Int64
	done := make(chan error, 2)
	consume := func(b buffer.Buffer) {
		r := b.ToReader()
		actual, err := io.ReadAll(r)
		if err == nil && !bytes.Equal(data, actual) {
			err = io.ErrUnexpectedEOF
		}
		if finished.Add(1) == 2 {
			release()
		}
		closeErr := r.Close()
		if err == nil {
			err = closeErr
		}
		done <- err
	}
	go consume(first)
	go consume(second)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			release()
			t.Fatal("source Close ran before consumers observed EOF")
		}
	}
}
