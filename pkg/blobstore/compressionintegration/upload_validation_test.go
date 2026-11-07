package compressionintegration_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type uploadObserver struct {
	errors []error
	done   int
}

func (*uploadObserver) NonRetrying() {}
func (h *uploadObserver) OnError(err error) (buffer.Buffer, error) {
	h.errors = append(h.errors, err)
	return nil, err
}
func (h *uploadObserver) Done() { h.done++ }

func TestUploadCancellationReachesObservers(t *testing.T) {
	for _, cancelBefore := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		data := []byte("cancel upload")
		d := digestOf(data)
		source := &uploadCompletionReader{data: bytes.NewReader(data)}
		h := &uploadObserver{}
		input := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, source, buffer.UserProvided), h)
		if cancelBefore {
			cancel()
		}
		err := buffer.ConsumeCASUpload(ctx, input, pool(), func(b buffer.Buffer) error { b.Discard(); cancel(); return status.FromContextError(ctx.Err()).Err() })
		require.Equal(t, codes.Canceled, status.Code(err))
		require.Len(t, h.errors, 1)
		require.Equal(t, codes.Canceled, status.Code(h.errors[0]))
		require.Equal(t, 1, h.done)
		require.EqualValues(t, 1, source.closes.Load())
	}
}

type failingEncoderPool struct {
	bb_zstd.Pool
	err error
}

func (p failingEncoderPool) NewEncoder(context.Context, io.Writer) (bb_zstd.Encoder, error) {
	return nil, p.err
}

func TestEncoderFailureDoesNotRetryTheSource(t *testing.T) {
	data := bytes.Repeat([]byte("healthy source"), 1000)
	d := digestOf(data)
	failure := status.Error(codes.Unavailable, "local codec unavailable")
	base := &capturingStore{}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, failingEncoderPool{Pool: pool(), err: failure})
	require.NoError(t, err)
	h := &stagingRetryHandler{}
	source := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.BackendProvided(buffer.Irreparable(d))), h)
	require.ErrorIs(t, store.Put(t.Context(), d, source), failure)
	require.Zero(t, h.calls)
	require.Equal(t, 1, h.done)
	require.Zero(t, base.puts)
}

type uploadTerminalReader struct {
	*bytes.Reader
	err error
}

func (r *uploadTerminalReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.Len() == 0 {
		return n, r.err
	}
	return n, err
}
func (*uploadTerminalReader) Close() error { return nil }
func TestCompressedTransportErrorWinsOverDecoderError(t *testing.T) {
	store, _, _ := localStorage(t, pool())
	data := []byte("expected bytes")
	d := digestOf(data)
	failure := status.Error(codes.Unavailable, "transport stopped")
	r := &uploadTerminalReader{Reader: bytes.NewReader([]byte("bad!")), err: failure}
	err := store.Put(t.Context(), d, buffer.NewCASBufferFromZSTDUpload(t.Context(), d, r, pool()))
	require.ErrorIs(t, err, failure)
	missing, err := store.FindMissing(t.Context(), d.ToSingletonSet())
	require.NoError(t, err)
	require.Equal(t, d.ToSingletonSet(), missing)
}

type unavailableDecoderPool struct {
	bb_zstd.Pool
	err error
}

func (p unavailableDecoderPool) NewDecoder(context.Context, io.Reader) (bb_zstd.Decoder, error) {
	return nil, &bb_zstd.AcquisitionError{Err: p.err}
}

func TestLocalDecoderAdmissionDoesNotRetryHealthySource(t *testing.T) {
	data := bytes.Repeat([]byte("healthy encoded input"), 1000)
	d := digestOf(data)
	failure := status.Error(codes.ResourceExhausted, "upload decoder admission timed out")
	base := &capturingStore{}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, unavailableDecoderPool{Pool: pool(), err: failure})
	require.NoError(t, err)
	// The decoder is never acquired, so it must not read these source bytes.
	source := &uploadCompletionReader{data: bytes.NewReader(nil)}
	h := &stagingRetryHandler{}
	b := buffer.WithErrorHandler(encodedTestBuffer(t.Context(), d, source, pool(), buffer.BackendProvided(buffer.Irreparable(d))), h)
	require.ErrorIs(t, store.Put(t.Context(), d, b), failure)
	require.Zero(t, h.calls)
	require.Equal(t, 1, h.done)
	require.Zero(t, base.puts)
	require.Zero(t, source.reads.Load())
	require.EqualValues(t, 1, source.closes.Load())
}

func TestEncoderWaitCancellationReachesUploadObserver(t *testing.T) {
	p := pool()
	held, err := p.NewEncoder(t.Context(), io.Discard)
	require.NoError(t, err)
	defer held.Close()
	base := &capturingStore{}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, p)
	require.NoError(t, err)
	data := bytes.Repeat([]byte("encoder cancellation"), 1000)
	d := digestOf(data)
	h := &uploadObserver{}
	b := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.UserProvided), h)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err = store.Put(ctx, d, b)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Len(t, h.errors, 1)
	require.Equal(t, codes.DeadlineExceeded, status.Code(h.errors[0]))
	require.Equal(t, 1, h.done)
}

type oversizedFramePool struct{ bb_zstd.Pool }

func (oversizedFramePool) NewEncoder(context.Context, io.Writer) (bb_zstd.Encoder, error) {
	return oversizedFrameEncoder{}, nil
}

type oversizedFrameEncoder struct{}

func (oversizedFrameEncoder) EncodeAll([]byte, []byte) []byte { return make([]byte, 2<<20) }
func (oversizedFrameEncoder) Write(p []byte) (int, error)     { return len(p), nil }
func (oversizedFrameEncoder) Close() error                    { return nil }
func TestLocalFrameBudgetDoesNotRetrySource(t *testing.T) {
	base := &capturingStore{}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, oversizedFramePool{Pool: pool()})
	require.NoError(t, err)
	data := bytes.Repeat([]byte("good input"), 1000)
	d := digestOf(data)
	h := &stagingRetryHandler{}
	b := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.UserProvided), h)
	err = store.Put(t.Context(), d, b)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Zero(t, h.calls)
	require.Equal(t, 1, h.done)
	require.Zero(t, base.puts)
}

func TestNonIndexedReadersKeepValidationOutsideObservers(t *testing.T) {
	data := []byte("expected input")
	d := digestOf(data)
	h := &uploadObserver{}
	wrong := bytes.Clone(data)
	wrong[0] ^= 1
	b := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(wrong)), buffer.UserProvided), h)
	r := buffer.NewReaderFromChunkReader(b.ToChunkReader(0, 4096))
	_, err := io.ReadAll(r)
	r.Close()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, h.errors)
	require.Equal(t, 1, h.done)
}

type cancelingEncoderPool struct {
	bb_zstd.Pool
	cancel  context.CancelFunc
	failure error
}

func (p cancelingEncoderPool) NewEncoder(context.Context, io.Writer) (bb_zstd.Encoder, error) {
	return cancelingEncoder{cancel: p.cancel, failure: p.failure}, nil
}

type cancelingEncoder struct {
	cancel  context.CancelFunc
	failure error
}

func (cancelingEncoder) Write(p []byte) (int, error) { return len(p), nil }
func (e cancelingEncoder) Close() error              { e.cancel(); return e.failure }
func TestEncoderFailureSurvivesConcurrentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := status.Error(codes.Unavailable, "local encoder close failed")
	base := &capturingStore{}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, cancelingEncoderPool{Pool: pool(), cancel: cancel, failure: failure})
	require.NoError(t, err)
	data := bytes.Repeat([]byte("encoder error precedence"), 1000)
	d := digestOf(data)
	h := &stagingRetryHandler{}
	b := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.UserProvided), h)
	require.ErrorIs(t, store.Put(ctx, d, b), failure)
	require.Zero(t, h.calls)
	require.Equal(t, 1, h.done)
}
