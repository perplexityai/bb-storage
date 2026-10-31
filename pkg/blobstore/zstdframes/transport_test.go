package zstdframes_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// These read-only fixtures establish wire compatibility of stored frames and
// transparent forwarding. They do not replace the production ByteStream server
// or claim to exercise its authorization, replication, and retry decorators.
type frameReadService struct {
	bytestream.UnimplementedByteStreamServer
	reader *zstdframes.Reader
	whole  digest.Digest
	frames map[digest.Digest]int
}

type responseWriter struct {
	stream bytestream.ByteStream_ReadServer
}

func (w responseWriter) Write(p []byte) (int, error) {
	// Transport messages need not align with Zstandard frame boundaries.
	total := len(p)
	for len(p) > 0 {
		n := min(len(p), 257)
		if err := w.stream.Send(&bytestream.ReadResponse{Data: p[:n]}); err != nil {
			return total - len(p), err
		}
		p = p[n:]
	}
	return total, nil
}

func (s *frameReadService) Read(req *bytestream.ReadRequest, out bytestream.ByteStream_ReadServer) error {
	d, compressor, err := digest.NewDigestFromByteStreamReadPath(req.ResourceName)
	if err != nil {
		return err
	}
	if compressor != remoteexecution.Compressor_ZSTD || req.ReadOffset != 0 || req.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "Fixture only serves full compressed reads")
	}
	if d == s.whole {
		return s.reader.WriteCompressed(out.Context(), responseWriter{out})
	}
	i, ok := s.frames[d]
	if !ok {
		return status.Error(codes.NotFound, "Unknown chunk")
	}
	data, err := s.reader.ReadCompressedFrame(out.Context(), i)
	if err != nil {
		return err
	}
	_, err = (responseWriter{out}).Write(data)
	return err
}

type forwardingReadService struct {
	bytestream.UnimplementedByteStreamServer
	upstream bytestream.ByteStreamClient
}

func (s *forwardingReadService) Read(req *bytestream.ReadRequest, out bytestream.ByteStream_ReadServer) error {
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	in, err := s.upstream.Read(ctx, req)
	if err != nil {
		return err
	}
	for {
		message, err := in.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := out.Send(message); err != nil {
			return err
		}
	}
}

func connect(t *testing.T, service bytestream.ByteStreamServer) bytestream.ByteStreamClient {
	t.Helper()
	listener := bufconn.Listen(64 << 10)
	server := grpc.NewServer()
	bytestream.RegisterByteStreamServer(server, service)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	conn, err := grpc.NewClient(
		"passthrough:///frames",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return bytestream.NewByteStreamClient(conn)
}

func readWire(ctx context.Context, client bytestream.ByteStreamClient, d digest.Digest) ([]byte, error) {
	stream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: d.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD)})
	if err != nil {
		return nil, err
	}
	var data bytes.Buffer
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return data.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		data.Write(message.Data)
	}
}

func TestFramesSurviveTwoGRPCHopsAndParallelDownloads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data := make([]byte, 5*(64<<10)+11)
	// Vary data across frames so incorrect ordering cannot pass by accident.
	for i := range data {
		data[i] = byte((i / 4096) % 251)
	}
	encoded, d := encode(t, data, 64<<10, newPool())
	r, err := zstdframes.Open(bytes.NewReader(encoded), int64(len(encoded)), d)
	require.NoError(t, err)
	mapping := r.FrameDigests()
	frames := make(map[digest.Digest]int, len(mapping))
	for i, chunk := range mapping {
		frames[chunk] = i
	}
	storage := connect(t, &frameReadService{reader: r, whole: d, frames: frames})
	frontend := connect(t, &forwardingReadService{upstream: storage})

	var storedFrames bytes.Buffer
	require.NoError(t, r.WriteCompressed(ctx, &storedFrames))
	wire, err := readWire(ctx, frontend, d)
	require.NoError(t, err)
	require.Equal(t, storedFrames.Bytes(), wire, "both hops must preserve the stored encoding exactly")
	require.Equal(t, data, decode(t, wire))

	parts := make([][]byte, len(mapping))
	errs := make([]error, len(mapping))
	var wg sync.WaitGroup
	for i, chunk := range mapping {
		wg.Go(func() { parts[i], errs[i] = readWire(ctx, frontend, chunk) })
	}
	wg.Wait()
	var assembled []byte
	for i, part := range parts {
		require.NoError(t, errs[i])
		raw := decode(t, part)
		require.Equal(t, mapping[i], blobDigest(raw, remoteexecution.DigestFunction_SHA256))
		assembled = append(assembled, raw...)
	}
	require.Equal(t, d, blobDigest(assembled, remoteexecution.DigestFunction_SHA256))
	_, err = readWire(ctx, frontend, blobDigest([]byte("not present"), remoteexecution.DigestFunction_SHA256))
	require.Equal(t, codes.NotFound, status.Code(err))
}
