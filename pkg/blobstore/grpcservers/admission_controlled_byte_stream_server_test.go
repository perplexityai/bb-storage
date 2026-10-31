package grpcservers_test

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const admissionReadPath = "blobs/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824/5"

type admissionBlockingStorage struct {
	codecOrderStorage
	reads   atomic.Int64
	started chan struct{}
}

func (s *admissionBlockingStorage) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	if s.reads.Add(1) == 1 {
		close(s.started)
		<-ctx.Done()
		return buffer.NewBufferFromError(status.FromContextError(ctx.Err()).Err())
	}
	return s.codecOrderStorage.Get(ctx, d)
}

func newAdmissionTestConnection(t *testing.T, byteStreamServer bytestream.ByteStreamServer, options ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(options...)
	bytestream.RegisterByteStreamServer(server, byteStreamServer)
	remoteexecution.RegisterCapabilitiesServer(server, capabilities.NewServer(capabilities.NewStaticProvider(
		&remoteexecution.ServerCapabilities{CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256},
		}},
	)))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.DialContext(t.Context(), "bufnet", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func TestByteStreamReadAdmissionAcrossListeners(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		limit int64
	}{
		{name: "Bounded", limit: 1},
		{name: "Disabled", limit: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			storage := &admissionBlockingStorage{
				codecOrderStorage: codecOrderStorage{written: make(chan []byte, 1)},
				started:           make(chan struct{}),
			}
			server, err := grpcservers.NewAdmissionControlledByteStreamServer(
				grpcservers.NewByteStreamServer(storage, 64, bb_zstd.NewBoundedPool(1, 1, nil, nil)), testCase.limit, 1)
			require.NoError(t, err)
			readFinished := make(chan struct{})
			connA := newAdmissionTestConnection(t, server, grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				defer close(readFinished)
				return handler(srv, stream)
			}))
			connB := newAdmissionTestConnection(t, server)
			clientA := bytestream.NewByteStreamClient(connA)
			clientB := bytestream.NewByteStreamClient(connB)
			firstCtx, firstCancel := context.WithCancel(ctx)
			defer firstCancel()
			first, err := clientA.Read(firstCtx, &bytestream.ReadRequest{ResourceName: admissionReadPath})
			require.NoError(t, err)
			select {
			case <-storage.started:
			case <-ctx.Done():
				t.Fatal("first read did not reach storage")
			}

			second, err := clientB.Read(ctx, &bytestream.ReadRequest{ResourceName: admissionReadPath})
			require.NoError(t, err)
			response, err := second.Recv()
			if testCase.limit != 0 {
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
				// A burst must finish while the first read is still blocked:
				// queued excess requests would expire instead of being rejected.
				const burstSize = 16
				results := make(chan error, burstSize)
				for range burstSize {
					go func() {
						reader, err := clientB.Read(ctx, &bytestream.ReadRequest{ResourceName: admissionReadPath})
						if err == nil {
							_, err = reader.Recv()
						}
						results <- err
					}()
				}
				for range burstSize {
					select {
					case err := <-results:
						require.Equal(t, codes.ResourceExhausted, status.Code(err))
					case <-ctx.Done():
						t.Fatal("excess reads queued instead of failing admission")
					}
				}
				require.EqualValues(t, 1, storage.reads.Load(), "rejected reads must not reach storage")
			} else {
				require.NoError(t, err)
				require.Equal(t, []byte("hello"), response.Data)
				_, err = second.Recv()
				require.Equal(t, io.EOF, err)
			}

			// Bulk read saturation must not consume write or metadata capacity.
			caps, err := remoteexecution.NewCapabilitiesClient(connB).GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{})
			require.NoError(t, err)
			require.Equal(t, []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256}, caps.CacheCapabilities.DigestFunctions)
			writer, err := clientB.Write(ctx)
			require.NoError(t, err)
			require.NoError(t, writer.Send(&bytestream.WriteRequest{
				ResourceName: "uploads/test/" + admissionReadPath,
				Data:         []byte("hello"),
				FinishWrite:  true,
			}))
			written, err := writer.CloseAndRecv()
			require.NoError(t, err)
			require.EqualValues(t, 5, written.CommittedSize)
			select {
			case data := <-storage.written:
				require.Equal(t, []byte("hello"), data)
			case <-ctx.Done():
				t.Fatal("upload did not reach storage")
			}

			firstCancel()
			_, err = first.Recv()
			require.Equal(t, codes.Canceled, status.Code(err))
			select {
			case <-readFinished:
			case <-ctx.Done():
				t.Fatal("canceled handler did not release its slot")
			}
			third, err := clientB.Read(ctx, &bytestream.ReadRequest{ResourceName: admissionReadPath})
			require.NoError(t, err)
			response, err = third.Recv()
			require.NoError(t, err)
			require.Equal(t, []byte("hello"), response.Data)
			_, err = third.Recv()
			require.Equal(t, io.EOF, err)
		})
	}
}

type admissionWaitingWriteStream struct {
	bytestream.ByteStream_WriteServer
	ctx     context.Context
	started chan struct{}
}

func (s *admissionWaitingWriteStream) Context() context.Context { return s.ctx }

func (s *admissionWaitingWriteStream) Recv() (*bytestream.WriteRequest, error) {
	close(s.started)
	<-s.ctx.Done()
	return nil, status.FromContextError(s.ctx.Err()).Err()
}

func TestByteStreamWriteAdmissionBeforeFirstMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	storage := &codecOrderStorage{written: make(chan []byte, 1)}
	server, err := grpcservers.NewAdmissionControlledByteStreamServer(
		grpcservers.NewByteStreamServer(storage, 64, bb_zstd.NewBoundedPool(1, 1, nil, nil)), 1, 1)
	require.NoError(t, err)
	firstCtx, firstCancel := context.WithCancel(ctx)
	defer firstCancel()
	first := &admissionWaitingWriteStream{ctx: firstCtx, started: make(chan struct{})}
	finished := make(chan error, 1)
	go func() { finished <- server.Write(first) }()
	select {
	case <-first.started:
	case <-ctx.Done():
		t.Fatal("write did not start receiving its first message")
	}
	second := &codecOrderWriteStream{ctx: ctx, request: &bytestream.WriteRequest{
		ResourceName: "uploads/test/" + admissionReadPath,
		Data:         []byte("hello"),
		FinishWrite:  true,
	}}
	require.Equal(t, codes.ResourceExhausted, status.Code(server.Write(second)))
	require.NotNil(t, second.request, "rejected writes must not receive a message")
	read := &admissionReadStream{ctx: ctx}
	require.NoError(t, server.Read(&bytestream.ReadRequest{ResourceName: admissionReadPath}, read))
	require.Equal(t, "hello", read.data.String())
	firstCancel()
	select {
	case err := <-finished:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-ctx.Done():
		t.Fatal("canceled write did not release its slot")
	}
	require.NoError(t, server.Write(second))
	require.EqualValues(t, 5, second.response.CommittedSize)
	select {
	case data := <-storage.written:
		require.Equal(t, []byte("hello"), data)
	case <-ctx.Done():
		t.Fatal("replacement upload did not reach storage")
	}
}

func TestByteStreamAdmissionCanceledBeforeEntry(t *testing.T) {
	backend := &admissionBackend{}
	server, err := grpcservers.NewAdmissionControlledByteStreamServer(
		grpcservers.NewByteStreamServer(backend, 64, bb_zstd.NewBoundedPool(1, 1, nil, nil)), 1, 1)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Equal(t, codes.Canceled, status.Code(server.Read(&bytestream.ReadRequest{ResourceName: admissionReadPath}, &admissionReadStream{ctx: ctx})))
	require.Zero(t, backend.gets.Load())
	write := &codecOrderWriteStream{ctx: ctx, request: &bytestream.WriteRequest{ResourceName: "uploads/test/" + admissionReadPath}}
	require.Equal(t, codes.Canceled, status.Code(server.Write(write)))
	require.NotNil(t, write.request)
}

func TestByteStreamAdmissionInvalidLimits(t *testing.T) {
	for _, limits := range [][2]int64{{-1, 1}, {1, -1}} {
		_, err := grpcservers.NewAdmissionControlledByteStreamServer(nil, limits[0], limits[1])
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}
