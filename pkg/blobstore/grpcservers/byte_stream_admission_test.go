package grpcservers_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Observe entry into the real pool, without replacing its admission logic.
type observedEncoderPool struct {
	bb_zstd.Pool
	acquiring chan struct{}
}

func (p *observedEncoderPool) NewEncoder(ctx context.Context, w io.Writer) (bb_zstd.Encoder, error) {
	select {
	case p.acquiring <- struct{}{}:
	default:
	}
	return p.Pool.NewEncoder(ctx, w)
}

// The backend boundary counts requests that would otherwise start storage RPCs.
type admissionBackend struct {
	blobstore.BlobAccess
	gets   atomic.Int64
	closed atomic.Int64
	err    error
}

func (b *admissionBackend) Get(context.Context, digest.Digest) buffer.Buffer {
	b.gets.Add(1)
	if b.err != nil {
		return buffer.NewBufferFromError(b.err)
	}
	return buffer.NewValidatedBufferFromReaderAt(&admissionReader{
		Reader: bytes.NewReader([]byte("hello")),
		closed: &b.closed,
	}, 5)
}

type admissionReader struct {
	*bytes.Reader
	closed *atomic.Int64
}

func (r *admissionReader) Close() error {
	r.closed.Add(1)
	return nil
}

type admissionReadStream struct {
	bytestream.ByteStream_ReadServer
	ctx  context.Context
	data bytes.Buffer
}

func (s *admissionReadStream) Context() context.Context { return s.ctx }

func (s *admissionReadStream) Send(response *bytestream.ReadResponse) error {
	_, err := s.data.Write(response.Data)
	return err
}

func TestByteStreamEncoderWaitCancellation(t *testing.T) {
	const resourceName = "compressed-blobs/zstd/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824/5"
	for _, cancellation := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		t.Run(cancellation.String(), func(t *testing.T) {
			pool := &observedEncoderPool{
				Pool: bb_zstd.NewBoundedPool(1, 1,
					[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
					[]zstd.DOption{zstd.WithDecoderConcurrency(1)}),
				acquiring: make(chan struct{}, 1),
			}
			occupied, err := pool.Pool.NewEncoder(t.Context(), io.Discard)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, occupied.Close()) })
			backend := &admissionBackend{}
			server := grpcservers.NewByteStreamServer(backend, 64, pool)
			ctx, cancel := context.WithCancel(t.Context())
			if cancellation == codes.DeadlineExceeded {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			}
			defer cancel()
			completed := make(chan error, 1)
			go func() {
				completed <- server.Read(&bytestream.ReadRequest{ResourceName: resourceName}, &admissionReadStream{ctx: ctx})
			}()
			select {
			case <-pool.acquiring:
			case <-time.After(5 * time.Second):
				t.Fatal("read did not attempt to acquire an encoder")
			}
			if cancellation == codes.Canceled {
				cancel()
			}
			select {
			case err := <-completed:
				require.Equal(t, cancellation, status.Code(err))
				require.EqualValues(t, 1, backend.closed.Load(), "cancellation must discard the backend buffer")
			case <-time.After(5 * time.Second):
				t.Fatal("canceled read did not release its waiter")
			}

			// A canceled waiter must not consume the next available slot.
			require.NoError(t, occupied.Close())
			nextCtx, nextCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer nextCancel()
			out := &admissionReadStream{ctx: nextCtx}
			require.NoError(t, server.Read(&bytestream.ReadRequest{ResourceName: resourceName}, out))
			require.EqualValues(t, 2, backend.gets.Load())
			require.EqualValues(t, 2, backend.closed.Load())
			decoder, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer decoder.Close()
			decoded, err := decoder.DecodeAll(out.data.Bytes(), nil)
			require.NoError(t, err)
			require.Equal(t, []byte("hello"), decoded)
		})
	}

	t.Run("BackendFailureReleasesEncoder", func(t *testing.T) {
		pool := bb_zstd.NewBoundedPool(1, 1, nil, nil)
		backend := &admissionBackend{err: status.Error(codes.NotFound, "missing blob")}
		server := grpcservers.NewByteStreamServer(backend, 64, pool)
		err := server.Read(&bytestream.ReadRequest{ResourceName: resourceName}, &admissionReadStream{ctx: t.Context()})
		require.Equal(t, codes.NotFound, status.Code(err))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		encoder, err := pool.NewEncoder(ctx, io.Discard)
		require.NoError(t, err, "backend errors must return the encoder to the pool")
		require.NoError(t, encoder.Close())
	})
}
