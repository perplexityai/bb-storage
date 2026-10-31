package grpcservers_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func uploadDigest(data []byte) *remoteexecution.Digest {
	return &remoteexecution.Digest{Hash: fmt.Sprintf("%x", sha256.Sum256(data)), SizeBytes: int64(len(data))}
}

type uploadObservedPool struct {
	bb_zstd.Pool
	decoders int
}

func (p *uploadObservedPool) NewDecoder(ctx context.Context, r io.Reader) (bb_zstd.Decoder, error) {
	p.decoders++
	return p.Pool.NewDecoder(ctx, r)
}

func TestByteStreamUploadSizeLimit(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			size     int
			limit    int64
			rejected bool
		}{
			{"below", 4, 5, false}, {"at", 5, 5, false}, {"above", 6, 5, true}, {"disabled", 6, 0, false},
		} {
			t.Run(fmt.Sprintf("compressed=%t/%s", compressed, tc.name), func(t *testing.T) {
				data := []byte("hello!")[:tc.size]
				d := uploadDigest(data)
				resource := fmt.Sprintf("uploads/test/blobs/%s/%d", d.Hash, d.SizeBytes)
				pool := &uploadObservedPool{Pool: bb_zstd.NewBoundedPool(1, 1,
					[]zstd.EOption{zstd.WithEncoderConcurrency(1)}, []zstd.DOption{zstd.WithDecoderConcurrency(1)})}
				if compressed {
					resource = fmt.Sprintf("uploads/test/compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
					encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
					require.NoError(t, err)
					data = encoder.EncodeAll(data, nil)
					require.NoError(t, encoder.Close())
				}
				storage := &codecOrderStorage{written: make(chan []byte, 1)}
				server, err := grpcservers.NewByteStreamServerWithMaximumBlobSize(storage, 64, pool, tc.limit)
				require.NoError(t, err)
				stream := &codecOrderWriteStream{ctx: t.Context(), request: &bytestream.WriteRequest{
					ResourceName: resource, Data: data, FinishWrite: true,
				}}
				err = server.Write(stream)
				if tc.rejected {
					require.Equal(t, codes.InvalidArgument, status.Code(err))
					require.Empty(t, storage.written, "oversized upload must not enter storage")
					require.Zero(t, pool.decoders, "oversized compressed upload must not acquire a decoder")
					require.Nil(t, stream.response)
				} else {
					require.NoError(t, err)
					require.Equal(t, []byte("hello!")[:tc.size], <-storage.written)
					require.NotNil(t, stream.response)
					if compressed {
						require.Equal(t, 1, pool.decoders)
					}
				}
			})
		}
	}
}

func TestBatchUploadSizeLimit(t *testing.T) {
	for _, limit := range []int64{0, 5} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			storage := &codecOrderStorage{written: make(chan []byte, 3)}
			server, err := grpcservers.NewContentAddressableStorageServerWithMaximumBlobSize(storage, 1024, limit)
			require.NoError(t, err)
			request := &remoteexecution.BatchUpdateBlobsRequest{DigestFunction: remoteexecution.DigestFunction_SHA256}
			for _, data := range [][]byte{[]byte("hello"), []byte("hello!"), {}} {
				request.Requests = append(request.Requests, &remoteexecution.BatchUpdateBlobsRequest_Request{Digest: uploadDigest(data), Data: data})
			}
			response, err := server.BatchUpdateBlobs(t.Context(), request)
			require.NoError(t, err)
			require.Len(t, response.Responses, 3)
			for i, item := range response.Responses {
				want := codes.OK
				if limit > 0 && i == 1 {
					want = codes.InvalidArgument
				}
				require.Equal(t, want, codes.Code(item.GetStatus().GetCode()))
				require.Equal(t, request.Requests[i].Digest, item.Digest)
			}
			require.Equal(t, []byte("hello"), <-storage.written)
			if limit == 0 {
				require.Equal(t, []byte("hello!"), <-storage.written)
			}
			require.Empty(t, <-storage.written)
			require.Empty(t, storage.written)
		})
	}
}

func TestUploadSizeLimitPreservesExistingReads(t *testing.T) {
	storage := &codecOrderStorage{}
	pool := bb_zstd.NewBoundedPool(1, 1, []zstd.EOption{zstd.WithEncoderConcurrency(1)}, []zstd.DOption{zstd.WithDecoderConcurrency(1)})
	server, err := grpcservers.NewByteStreamServerWithMaximumBlobSize(storage, 64, pool, 4)
	require.NoError(t, err)
	d := uploadDigest([]byte("hello"))
	for _, compressed := range []bool{false, true} {
		resource := fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes)
		if compressed {
			resource = fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes)
		}
		stream := &admissionReadStream{ctx: t.Context()}
		require.NoError(t, server.Read(&bytestream.ReadRequest{ResourceName: resource}, stream))
		data := stream.data.Bytes()
		if compressed {
			decoder, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer decoder.Close()
			data, err = decoder.DecodeAll(data, nil)
			require.NoError(t, err)
		}
		require.Equal(t, []byte("hello"), data)
	}
	cas, err := grpcservers.NewContentAddressableStorageServerWithMaximumBlobSize(storage, 1024, 4)
	require.NoError(t, err)
	response, err := cas.BatchReadBlobs(t.Context(), &remoteexecution.BatchReadBlobsRequest{Digests: []*remoteexecution.Digest{d}})
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	require.Equal(t, codes.OK, codes.Code(response.Responses[0].GetStatus().GetCode()))
	require.Equal(t, []byte("hello"), response.Responses[0].Data)
}

func TestUploadSizeLimitRejectsNegativeConfiguration(t *testing.T) {
	_, err := grpcservers.NewByteStreamServerWithMaximumBlobSize(nil, 64, nil, -1)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = grpcservers.NewContentAddressableStorageServerWithMaximumBlobSize(nil, 1024, -1)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
