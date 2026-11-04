package compressionintegration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCompressedUploadForwarding(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	storagePool, frontendPool := pool(), pool()
	storage, _, _ := localStorage(t, storagePool)
	storageConn := serve(t, storage, storagePool)
	remote := grpcclients.NewCASBlobAccess(storageConn, uuid.NewRandom, 4096, frontendPool)
	allow := auth.NewStaticAuthorizer(func(i digest.InstanceName) bool { return i.String() == "allowed" })
	frontend := blobstore.NewAuthorizingBlobAccess(remote, allow, allow, allow)
	conn := serve(t, frontend, frontendPool)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	defer encoder.Close()
	data := bytes.Repeat([]byte("encoded upload forwarding\n"), 90000)
	d := digestOf(data)
	encoded := encoder.EncodeAll(data, nil)
	write := func(d digest.Digest, data []byte, offset int64, finish bool) (*bytestream.WriteResponse, error) {
		w, err := bytestream.NewByteStreamClient(conn).Write(ctx)
		require.NoError(t, err)
		_ = w.Send(&bytestream.WriteRequest{ResourceName: d.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD), WriteOffset: offset, Data: data, FinishWrite: finish})
		return w.CloseAndRecv()
	}
	readPoolBefore := []int64{storagePool.encoders.Load(), storagePool.decoders.Load()}
	_, err = write(d, encoded, 0, true)
	require.NoError(t, err)
	require.Zero(t, frontendPool.encoders.Load())
	require.Zero(t, frontendPool.decoders.Load())
	require.Equal(t, readPoolBefore[1], storagePool.decoders.Load(), "uploads must not take read-side decoders")
	require.Equal(t, readPoolBefore[0], storagePool.encoders.Load(), "uploads must not take read-side encoders")
	before := []int64{storagePool.encoders.Load(), storagePool.decoders.Load()}
	// A duplicate can finish before consuming any bytes, without a codec lease.
	_, err = write(d, []byte("not a zstd stream"), 0, true)
	require.NoError(t, err)
	duplicate, err := bytestream.NewByteStreamClient(storageConn).Write(ctx)
	require.NoError(t, err)
	require.NoError(t, duplicate.Send(&bytestream.WriteRequest{ResourceName: d.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD), Data: []byte("unused"), FinishWrite: true}))
	response, err := duplicate.CloseAndRecv()
	require.NoError(t, err)
	require.EqualValues(t, len("unused"), response.CommittedSize)
	require.Equal(t, before, []int64{storagePool.encoders.Load(), storagePool.decoders.Load()})
	// An early backend success must not hide a deferred local task failure.
	taskErr := status.Error(codes.Unavailable, "secondary write failed")
	b := buffer.NewCASBufferFromZSTDUpload(ctx, d, io.NopCloser(bytes.NewReader(encoded)), frontendPool).WithTask(func() error { return taskErr })
	require.Equal(t, codes.Unavailable, status.Code(remote.Put(ctx, d, b)))

	require.Equal(t, data, read(t, ctx, conn, d, remoteexecution.Compressor_IDENTITY, 0))

	// Check rejection before canceled malformed uploads can still be unwinding.
	denied, err := digest.MustNewFunction("denied", remoteexecution.DigestFunction_SHA256).NewDigest(d.GetHashString(), d.GetSizeBytes())
	require.NoError(t, err)
	before = []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()}
	_, err = write(denied, encoded, 0, true)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, before, []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()})

	for _, tc := range []struct {
		name   string
		d      digest.Digest
		data   []byte
		offset int64
		finish bool
		code   codes.Code
	}{
		{"hash", digestOf(bytes.Repeat([]byte("!"), len(data))), encoded, 0, true, codes.InvalidArgument},
		{"size", digestOf([]byte("small")), encoded, 0, true, codes.InvalidArgument},
		{"offset", d, encoded, 1, true, codes.InvalidArgument},
		{"unfinished", digestOf([]byte("unfinished")), encoder.EncodeAll([]byte("unfinished"), nil), 0, false, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := write(tc.d, tc.data, tc.offset, tc.finish)
			require.Equal(t, tc.code, status.Code(err), "%v", err)
			if tc.d != d {
				missing, err := storage.FindMissing(ctx, tc.d.ToSingletonSet())
				require.NoError(t, err)
				require.Equal(t, tc.d.ToSingletonSet(), missing)
			}
		})
	}
	changedData := []byte("resource name must remain stable")
	changedDigest := digestOf(changedData)
	changedEncoded := encoder.EncodeAll(changedData, nil)
	w, err := bytestream.NewByteStreamClient(conn).Write(ctx)
	require.NoError(t, err)
	name := changedDigest.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD)
	cut := len(changedEncoded) / 2
	require.NoError(t, w.Send(&bytestream.WriteRequest{ResourceName: name, Data: changedEncoded[:cut]}))
	_ = w.Send(&bytestream.WriteRequest{ResourceName: changedDigest.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD), WriteOffset: int64(cut), Data: changedEncoded[cut:], FinishWrite: true})
	_, err = w.CloseAndRecv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	missing, err := storage.FindMissing(ctx, changedDigest.ToSingletonSet())
	require.NoError(t, err)
	require.Equal(t, changedDigest.ToSingletonSet(), missing)
}

func TestCompressedUploadValidationAndFallback(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	defer encoder.Close()
	for _, compressedHop := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed_hop_%v", compressedHop), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			storagePool, frontendPool := pool(), pool()
			storage, _, _ := localStorageMode(t, storagePool, false, false)
			var hopPool bb_zstd.Pool
			if compressedHop {
				hopPool = frontendPool
			}
			remote := grpcclients.NewCASBlobAccess(serve(t, storage, storagePool), uuid.NewRandom, 4096, hopPool)
			conn := serveWithLimit(t, remote, frontendPool, 1024)
			write := func(ctx context.Context, d digest.Digest, data []byte) error {
				w, err := bytestream.NewByteStreamClient(conn).Write(ctx)
				if err != nil {
					return err
				}
				_ = w.Send(&bytestream.WriteRequest{ResourceName: d.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD), Data: data, FinishWrite: true})
				_, err = w.CloseAndRecv()
				return err
			}
			data := []byte("validate even when destination stores raw bytes")
			d := digestOf(data)
			encoded := encoder.EncodeAll(data, nil)
			for n := 1; n < len(encoded); n++ {
				require.Error(t, write(ctx, d, encoded[:n]), "truncated at %d", n)
				missing, err := storage.FindMissing(ctx, d.ToSingletonSet())
				require.NoError(t, err)
				require.Equal(t, d.ToSingletonSet(), missing)
			}
			require.NoError(t, write(ctx, d, encoded))
			actual, err := storage.Get(ctx, d).ToByteSlice(1024)
			require.NoError(t, err)
			require.Equal(t, data, actual)
			if compressedHop {
				require.Zero(t, frontendPool.encoders.Load())
				require.Zero(t, frontendPool.decoders.Load())
			} else {
				require.Positive(t, frontendPool.decoders.Load(), "identity hop must validate by decoding locally")
			}
			before := []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()}
			over := bytes.Repeat([]byte("x"), 1025)
			require.Equal(t, codes.InvalidArgument, status.Code(write(ctx, digestOf(over), encoder.EncodeAll(over, nil))))
			require.Equal(t, before, []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()})
			canceled, stop := context.WithCancel(ctx)
			stop()
			require.Equal(t, codes.Canceled, status.Code(write(canceled, digestOf([]byte("canceled")), encoder.EncodeAll([]byte("canceled"), nil))))
		})
	}
}

// Signal the real backend codec wait so a source failure occurs while its RPC
// is blocked. Cancellation must release that wait without a caller deadline.
type waitingDecoderPool struct {
	bb_zstd.Pool
	attempted chan struct{}
}

func (p *waitingDecoderPool) NewDecoder(ctx context.Context, r io.Reader) (bb_zstd.Decoder, error) {
	close(p.attempted)
	return p.Pool.NewDecoder(ctx, r)
}

type failingUploadReader struct {
	ctx       context.Context
	attempted <-chan struct{}
	first     bool
}

func (r *failingUploadReader) Read(p []byte) (int, error) {
	if !r.first {
		r.first = true
		return copy(p, []byte("encoded input")), nil
	}
	select {
	case <-r.attempted:
		return 0, status.Error(codes.DataLoss, "Upload source failed")
	case <-r.ctx.Done():
		return 0, status.FromContextError(r.ctx.Err()).Err()
	}
}
func (*failingUploadReader) Close() error { return nil }

func TestUploadSourceFailureCancelsRawBackendCodecWait(t *testing.T) {
	storagePool, frontendPool := pool(), pool()
	held, err := storagePool.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	defer held.Close()
	waiting := &waitingDecoderPool{Pool: storagePool, attempted: make(chan struct{})}
	storage, _, _ := localStorageMode(t, storagePool, false, false)
	client := grpcclients.NewCASBlobAccess(serve(t, storage, waiting), uuid.NewRandom, 4096, frontendPool)
	d := digestOf([]byte("hello"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b := buffer.NewCASBufferFromZSTDUpload(ctx, d, &failingUploadReader{ctx: ctx, attempted: waiting.attempted}, frontendPool)
	done := make(chan error, 1)
	go func() { done <- client.Put(ctx, d, b) }()
	select {
	case err := <-done:
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.NoError(t, ctx.Err(), "source failure must cancel the backend independently")
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("upload waited for backend codec capacity after its source failed")
	}
}

func TestIdentityDuplicateUploadToCompressedStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	storagePool := pool()
	storage, _, _ := localStorage(t, storagePool)
	remote := grpcclients.NewCASBlobAccess(serve(t, storage, storagePool), uuid.NewRandom, 4096, nil)
	conn := serve(t, remote, pool())
	data := bytes.Repeat([]byte("duplicate upload\n"), 200000)
	d := digestOf(data)
	require.NoError(t, upload(t, ctx, conn, d, data))
	before := storagePool.encoders.Load()
	require.NoError(t, upload(t, ctx, conn, d, data))
	require.Equal(t, before, storagePool.encoders.Load())
}
