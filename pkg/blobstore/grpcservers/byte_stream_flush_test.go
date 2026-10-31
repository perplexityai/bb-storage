package grpcservers_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failedFlushReadStream struct {
	bytestream.ByteStream_ReadServer
	ctx   context.Context
	sends int
}

func (s *failedFlushReadStream) Context() context.Context { return s.ctx }
func (s *failedFlushReadStream) Send(*bytestream.ReadResponse) error {
	s.sends++
	return status.Error(codes.Unavailable, "response transport closed")
}

func TestByteStreamCompressedReadFlushFailure(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		name := "unbounded"
		if bounded {
			name = "bounded"
		}
		t.Run(name, func(t *testing.T) {
			encoderOptions := []zstd.EOption{zstd.WithEncoderConcurrency(1)}
			decoderOptions := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
			var pool bb_zstd.Pool = bb_zstd.NewUnboundedPool(encoderOptions, decoderOptions)
			if bounded {
				pool = bb_zstd.NewBoundedPool(1, 1, encoderOptions, decoderOptions)
			}
			backend := &admissionBackend{}
			server := grpcservers.NewByteStreamServer(backend, 64, pool)
			request := &bytestream.ReadRequest{ResourceName: "compressed-blobs/zstd/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824/5"}
			// A small response remains buffered until the real encoder closes.
			stream := &failedFlushReadStream{ctx: t.Context()}
			err := server.Read(request, stream)
			require.Positive(t, stream.sends)
			require.Equal(t, codes.Unavailable, status.Code(err), "final response flush must not report success after Send fails")
			require.EqualValues(t, 1, backend.closed.Load())

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			next := &admissionReadStream{ctx: ctx}
			require.NoError(t, server.Read(request, next), "failed flush must release the encoder slot")
			decoder, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer decoder.Close()
			data, err := decoder.DecodeAll(next.data.Bytes(), nil)
			require.NoError(t, err)
			require.Equal(t, []byte("hello"), data)
			require.EqualValues(t, 2, backend.closed.Load())
		})
	}
}

func TestByteStreamCompressedReadPreservesBackendFailure(t *testing.T) {
	pool := bb_zstd.NewBoundedPool(1, 1,
		[]zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithZeroFrames(true)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1)})
	backend := &admissionBackend{err: status.Error(codes.DataLoss, "backend integrity check failed")}
	server := grpcservers.NewByteStreamServer(backend, 64, pool)
	stream := &failedFlushReadStream{ctx: t.Context()}
	err := server.Read(&bytestream.ReadRequest{ResourceName: "compressed-blobs/zstd/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824/5"}, stream)
	require.Positive(t, stream.sends, "force a second failure during encoder cleanup")
	require.Equal(t, codes.DataLoss, status.Code(err), "flush failure must not replace the backend error")
}
