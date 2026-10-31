package compressionintegration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
)

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

func localStorage(t *testing.T, p *observedPool) (blobstore.BlobAccess, string, string) {
	return localStorageMode(t, p, true, false)
}

func localStorageMode(t *testing.T, p *observedPool, compressed, hierarchical bool) (blobstore.BlobAccess, string, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "blocks")
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{
	"keyLocationMap":{"inMemory":{"entries":"1024"},"maximumGetAttempts":16,"maximumPutAttempts":"64"},
	"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"spareBlocks":2},
	"oldBlocks":1,"currentBlocks":1,"newBlocks":1,
	"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"134217728","maximumConcurrentWrites":"2"}
	}}`, path, dir)), &config))
	if !compressed {
		config.GetLocal().CasCompression = nil
	}
	config.GetLocal().HierarchicalInstanceNames = hierarchical
	info, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, p))
	require.NoError(t, err)
	return info.BlobAccess, path, dir
}

func serve(t *testing.T, ba blobstore.BlobAccess, p bb_zstd.Pool) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(64 << 10)
	s := grpc.NewServer()
	bytestream.RegisterByteStreamServer(s, grpcservers.NewByteStreamServer(ba, 4096, p))
	remoteexecution.RegisterContentAddressableStorageServer(s, grpcservers.NewContentAddressableStorageServer(ba, 4<<20))
	remoteexecution.RegisterCapabilitiesServer(s, capabilities.NewServer(capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{CacheCapabilities: &remoteexecution.CacheCapabilities{
		DigestFunctions: []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256}, SupportedCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
	}})))
	go s.Serve(listener)
	t.Cleanup(func() { s.Stop(); listener.Close() })
	c, err := grpc.NewClient("passthrough:///compressed", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func read(t *testing.T, ctx context.Context, c *grpc.ClientConn, d digest.Digest, compressor remoteexecution.Compressor_Value, off int64) []byte {
	t.Helper()
	r, err := bytestream.NewByteStreamClient(c).Read(ctx, &bytestream.ReadRequest{ResourceName: d.GetByteStreamReadPath(compressor), ReadOffset: off})
	require.NoError(t, err)
	var out bytes.Buffer
	for {
		p, err := r.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		out.Write(p.Data)
	}
	return out.Bytes()
}

func upload(t *testing.T, ctx context.Context, c *grpc.ClientConn, d digest.Digest, p []byte) error {
	t.Helper()
	w, err := bytestream.NewByteStreamClient(c).Write(ctx)
	require.NoError(t, err)
	name := d.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_IDENTITY)
	var offset int64
	for len(p) > 0 {
		n := min(len(p), 64<<10)
		err = w.Send(&bytestream.WriteRequest{ResourceName: name, WriteOffset: offset, Data: p[:n]})
		if err != nil {
			break
		}
		offset += int64(n)
		p = p[n:]
		name = ""
	}
	if err == nil {
		_ = w.Send(&bytestream.WriteRequest{ResourceName: name, WriteOffset: offset, FinishWrite: true})
	}
	_, err = w.CloseAndRecv()
	return err
}

func TestCompressedPrimaryStorageAndRealFrontend(t *testing.T) {
	t.Run("flat", func(t *testing.T) { testCompressedPrimaryStorage(t, false) })
	t.Run("hierarchical", func(t *testing.T) { testCompressedPrimaryStorage(t, true) })
}

func testCompressedPrimaryStorage(t *testing.T, hierarchical bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	storagePool, frontendPool := pool(), pool()
	storage, path, dir := localStorageMode(t, storagePool, true, hierarchical)
	storageConn := serve(t, storage, storagePool)
	remote := grpcclients.NewCASBlobAccess(storageConn, uuid.NewRandom, 4096, frontendPool)
	selector, err := sharding.NewRendezvousShardSelector([]sharding.Shard{{Key: "only", Weight: 1}})
	require.NoError(t, err)
	frontend := sharding.NewShardingBlobAccess([]sharding.ShardBackend{{Backend: remote, Key: "only"}}, selector)
	frontend = blobstore.NewMetricsBlobAccess(frontend, clock.SystemClock, "cas", "compressed_integration")
	allow := auth.NewStaticAuthorizer(func(i digest.InstanceName) bool { return i.String() == "allowed" })
	frontend = blobstore.NewAuthorizingBlobAccess(frontend, allow, allow, allow)
	frontendConn := serve(t, frontend, frontendPool)
	data := bytes.Repeat([]byte("primary compressed storage\n"), 90000)
	d := digestOf(data)
	// The logical blob is much larger than an entire block. Successful storage
	// requires actual allocation by compressed size, not just wire compression.
	require.NoError(t, upload(t, ctx, frontendConn, d, data))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	end := bytes.Index(raw, []byte("BBZSTF01")) + 48
	require.Greater(t, end, 48)
	require.Less(t, end, 16<<10)
	index, err := zstdframes.Open(bytes.NewReader(raw[:end]), int64(end), d)
	require.NoError(t, err)
	var stored bytes.Buffer
	require.NoError(t, index.WriteCompressed(ctx, &stored))
	before := []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()}
	for range 2 {
		require.Equal(t, stored.Bytes(), read(t, ctx, frontendConn, d, remoteexecution.Compressor_ZSTD, 0))
	}
	require.Equal(t, before, []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()}, "full reads across actual handlers must acquire no codecs")
	require.Equal(t, data, read(t, ctx, frontendConn, d, remoteexecution.Compressor_IDENTITY, 0))
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	suffix, err := decoder.DecodeAll(read(t, ctx, frontendConn, d, remoteexecution.Compressor_ZSTD, 19), nil)
	require.NoError(t, err)
	require.Equal(t, data[19:], suffix)
	denied, err := digest.MustNewFunction("denied", remoteexecution.DigestFunction_SHA256).NewDigest(d.GetHashString(), d.GetSizeBytes())
	require.NoError(t, err)
	r, err := bytestream.NewByteStreamClient(frontendConn).Read(ctx, &bytestream.ReadRequest{ResourceName: denied.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD)})
	require.NoError(t, err)
	for err == nil {
		_, err = r.Recv()
	}
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Rotate the original record into an old block, then refresh it through the
	// normal Get path. Both the response and the refreshed record stay encoded.
	rng := rand.New(rand.NewPCG(2, 3))
	for range 3 {
		filler := make([]byte, 9000)
		for i := range filler {
			filler[i] = byte(rng.Uint32())
		}
		fd := digestOf(filler)
		require.NoError(t, storage.Put(ctx, fd, buffer.NewCASBufferFromByteSlice(fd, filler, buffer.UserProvided)))
	}
	raw, err = os.ReadFile(path)
	require.NoError(t, err)
	copiesBefore := bytes.Count(raw, stored.Bytes())
	before = []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()}
	require.Equal(t, stored.Bytes(), read(t, ctx, frontendConn, d, remoteexecution.Compressor_ZSTD, 0))
	require.Equal(t, before, []int64{storagePool.encoders.Load(), storagePool.decoders.Load(), frontendPool.encoders.Load(), frontendPool.decoders.Load()})
	raw, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, bytes.Count(raw, stored.Bytes()), copiesBefore, "Get must actually refresh the encoded record")
	require.Equal(t, data, read(t, ctx, frontendConn, d, remoteexecution.Compressor_IDENTITY, 0))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "staging files must be unlinked")
}

func TestLegacyAndCorruptStorage(t *testing.T) {
	p := pool()
	factory := blobstore.NewCompressedCASReadBufferFactory(p)
	data := bytes.Repeat([]byte("legacy raw record"), 100)
	d := digestOf(data)
	var results []bool
	callback := func(valid bool) { results = append(results, valid) }
	b := factory.NewBufferFromByteSlice(d, data, callback)
	_, encoded := buffer.ZSTDChunkReader(b, 4096)
	require.False(t, encoded)
	actual, err := b.ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
	require.Contains(t, results, true)
	var representation bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &representation, d, bytes.NewReader(data), 1<<20, p))
	for _, offset := range []int{0, representation.Len() - 1} {
		corrupted := bytes.Clone(representation.Bytes())
		corrupted[offset] ^= 0x80
		results = nil
		b := factory.NewBufferFromByteSlice(d, corrupted, callback)
		if r, ok := buffer.ZSTDChunkReader(b, 4096); ok {
			_, err = r.Read()
			r.Close()
		} else {
			_, err = b.ToByteSlice(len(data))
		}
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.Contains(t, results, false)
	}
	// Clones retain the block independently: discarding one cannot invalidate
	// the other's decoder, and background-task cloning retains the CAS digest.
	b = factory.NewBufferFromByteSlice(d, representation.Bytes(), callback).WithTask(func() error { return nil })
	a, b := b.CloneStream()
	a.Discard()
	size, err := b.GetSizeBytes()
	require.NoError(t, err)
	require.Equal(t, d.GetSizeBytes(), size)
	actual, err = b.ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
}

func TestFailedUploadsAreNotPublished(t *testing.T) {
	p := pool()
	storage, _, dir := localStorage(t, p)
	c := serve(t, storage, p)
	data := bytes.Repeat([]byte("valid logical content"), 1000)
	d := digestOf(data)
	wrong := bytes.Clone(data)
	wrong[0] ^= 1
	require.Error(t, upload(t, t.Context(), c, d, wrong))
	_, err := storage.Get(t.Context(), d).ToByteSlice(len(data))
	require.Equal(t, codes.NotFound, status.Code(err))
	// Failure releases both staging reservations and codec permits.
	require.NoError(t, upload(t, t.Context(), c, d, data))
	actual, err := storage.Get(t.Context(), d).ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	limited, err := blobstore.NewCompressedCASBlobAccess(storage, dir, 1, 1, p)
	require.NoError(t, err)
	err = limited.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided))
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = storage.Put(ctx, d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided))
	require.Equal(t, codes.Canceled, status.Code(err))
}

func TestCompressedToRawLocalCopy(t *testing.T) {
	p := pool()
	source, _, _ := localStorage(t, p)
	target, _, _ := localStorageMode(t, p, false, false)
	// The tiny blob expands when encoded; the larger one shrinks. Neither
	// physical representation may leak into a raw destination's allocation.
	for _, data := range [][]byte{[]byte("x"), bytes.Repeat([]byte("copy"), 1024)} {
		d := digestOf(data)
		require.NoError(t, source.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
		require.NoError(t, target.Put(t.Context(), d, source.Get(t.Context(), d)))
		actual, err := target.Get(t.Context(), d).ToByteSlice(len(data))
		require.NoError(t, err)
		require.Equal(t, data, actual)
	}
}

type failedSource struct{}

func (failedSource) Read([]byte) (int, error) {
	return 0, status.Error(codes.Unavailable, "source disconnected")
}

type retrySource struct {
	blobstore.BlobAccess
	pool          bb_zstd.Pool
	data, encoded []byte
	retries, done atomic.Int64
}

func (s *retrySource) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	source := io.NopCloser(io.MultiReader(bytes.NewReader(s.data[:1<<20]), failedSource{}))
	return buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, source, buffer.BackendProvided(buffer.Irreparable(d))), &replacementHandler{owner: s, ctx: ctx, d: d})
}

type replacementHandler struct {
	owner *retrySource
	ctx   context.Context
	d     digest.Digest
}

func (h *replacementHandler) OnError(err error) (buffer.Buffer, error) {
	if h.owner.retries.Add(1) != 1 {
		return nil, err
	}
	// Force the conflicting resource schedule at source replacement: another
	// transfer needs the encoder while holding the shared pool's only decoder.
	decoder, err := h.owner.pool.NewDecoder(h.ctx, bytes.NewReader(h.owner.encoded))
	if err != nil {
		return nil, err
	}
	encoder, err := h.owner.pool.NewEncoder(h.ctx, io.Discard)
	if err != nil {
		decoder.Close()
		return nil, err
	}
	err = encoder.Close()
	decoder.Close()
	if err != nil {
		return nil, err
	}
	return buffer.NewCASBufferFromZSTDReader(h.ctx, h.d, io.NopCloser(bytes.NewReader(h.owner.encoded)), h.owner.pool, buffer.BackendProvided(buffer.Irreparable(h.d))), nil
}
func (h *replacementHandler) Done() { h.owner.done.Add(1) }

func TestCompressedReadRetriesInLogicalSpaceWithoutHoldingEncoder(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte("logical retry offset"), 120000)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	require.NoError(t, err)
	encoded := encoder.EncodeAll(data, nil)
	require.NoError(t, encoder.Close())
	p := pool()
	source := &retrySource{pool: p, data: data, encoded: encoded}
	conn := serve(t, source, p)
	result := read(t, ctx, conn, digestOf(data), remoteexecution.Compressor_ZSTD, 0)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	actual, err := decoder.DecodeAll(result, nil)
	require.NoError(t, err)
	require.Equal(t, data, actual)
	require.EqualValues(t, 1, source.retries.Load())
	require.EqualValues(t, 1, source.done.Load())
}
