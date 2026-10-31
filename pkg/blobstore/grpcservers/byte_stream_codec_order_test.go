package grpcservers_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Hold the upload's real decoder until the download reaches backend decoding.
// This forces the mixed-request schedule without relying on sleeps or load.
type heldWriteDecoderPool struct {
	bb_zstd.Pool
	acquired chan struct{}
	proceed  chan struct{}
}

func (p *heldWriteDecoderPool) NewDecoder(ctx context.Context, r io.Reader) (bb_zstd.Decoder, error) {
	decoder, err := p.Pool.NewDecoder(ctx, r)
	if err != nil {
		return nil, err
	}
	close(p.acquired)
	select {
	case <-p.proceed:
		return decoder, nil
	case <-ctx.Done():
		decoder.Close()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

type observedReadDecoderPool struct {
	bb_zstd.Pool
	attempted chan struct{}
}

func (p *observedReadDecoderPool) NewDecoder(ctx context.Context, r io.Reader) (bb_zstd.Decoder, error) {
	close(p.attempted)
	return p.Pool.NewDecoder(ctx, r)
}

type codecOrderStorage struct {
	blobstore.BlobAccess
	written chan []byte
}

func (*codecOrderStorage) Get(context.Context, digest.Digest) buffer.Buffer {
	return buffer.NewValidatedBufferFromByteSlice([]byte("hello"))
}

func (s *codecOrderStorage) Put(ctx context.Context, _ digest.Digest, b buffer.Buffer) error {
	data, err := b.ToByteSlice(1024)
	if err == nil {
		s.written <- data
	}
	return err
}

type codecOrderWriteStream struct {
	bytestream.ByteStream_WriteServer
	ctx      context.Context
	request  *bytestream.WriteRequest
	response *bytestream.WriteResponse
}

func (s *codecOrderWriteStream) Context() context.Context { return s.ctx }

func (s *codecOrderWriteStream) Recv() (*bytestream.WriteRequest, error) {
	if s.request == nil {
		return nil, io.EOF
	}
	request := s.request
	s.request = nil
	return request, nil
}

func (s *codecOrderWriteStream) SendAndClose(response *bytestream.WriteResponse) error {
	s.response = response
	return nil
}

func TestByteStreamMixedCompressedCodecOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	encoderOptions := []zstd.EOption{zstd.WithEncoderConcurrency(1)}
	decoderOptions := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
	shared := bb_zstd.NewBoundedPool(1, 1, encoderOptions, decoderOptions)
	frontendPool := &heldWriteDecoderPool{Pool: shared, acquired: make(chan struct{}), proceed: make(chan struct{})}
	backendPool := &observedReadDecoderPool{Pool: shared, attempted: make(chan struct{})}

	storage := &codecOrderStorage{written: make(chan []byte, 1)}
	listener := bufconn.Listen(1 << 20)
	storageServer := grpc.NewServer()
	bytestream.RegisterByteStreamServer(storageServer, grpcservers.NewByteStreamServer(storage, 64,
		bb_zstd.NewUnboundedPool(encoderOptions, decoderOptions)))
	remoteexecution.RegisterCapabilitiesServer(storageServer, capabilities.NewServer(capabilities.NewStaticProvider(
		&remoteexecution.ServerCapabilities{CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions:      []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256},
			SupportedCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
		}},
	)))
	go func() { _ = storageServer.Serve(listener) }()
	defer storageServer.Stop()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()
	backend := grpcclients.NewCASBlobAccess(conn, uuid.NewRandom, 64, backendPool)
	frontend, err := grpcservers.NewAdmissionControlledByteStreamServer(
		grpcservers.NewByteStreamServer(backend, 64, frontendPool), 1, 1,
	)
	require.NoError(t, err)

	blobDigest := digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", 5)
	encoder, err := zstd.NewWriter(nil, encoderOptions...)
	require.NoError(t, err)
	compressed := encoder.EncodeAll([]byte("hello"), nil)
	require.NoError(t, encoder.Close())
	writeStream := &codecOrderWriteStream{ctx: ctx, request: &bytestream.WriteRequest{
		ResourceName: blobDigest.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD),
		Data:         compressed,
		FinishWrite:  true,
	}}
	writeDone := make(chan error, 1)
	go func() { writeDone <- frontend.Write(writeStream) }()
	select {
	case <-frontendPool.acquired:
	case <-ctx.Done():
		t.Fatal("upload did not acquire its decoder")
	}

	readStream := &admissionReadStream{ctx: ctx}
	readDone := make(chan error, 1)
	go func() {
		readDone <- frontend.Read(&bytestream.ReadRequest{
			ResourceName: blobDigest.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD),
		}, readStream)
	}()
	select {
	case <-backendPool.attempted:
	case <-ctx.Done():
		t.Fatal("download did not attempt backend decoder acquisition")
	}
	close(frontendPool.proceed)

	// Both requests must finish without cancellation freeing the shared codecs.
	select {
	case err := <-writeDone:
		require.NoError(t, err, "upload stalled waiting for the download's encoder")
	case <-ctx.Done():
		t.Fatal("mixed compressed requests stalled until cancellation")
	}
	select {
	case err := <-readDone:
		require.NoError(t, err, "download stalled waiting for the upload's decoder")
	case <-ctx.Done():
		t.Fatal("mixed compressed requests stalled until cancellation")
	}
	require.NoError(t, ctx.Err())
	require.EqualValues(t, len(compressed), writeStream.response.CommittedSize)
	select {
	case written := <-storage.written:
		require.Equal(t, []byte("hello"), written)
	case <-ctx.Done():
		t.Fatal("upload did not reach storage")
	}
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(readStream.data.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), decoded)
}
