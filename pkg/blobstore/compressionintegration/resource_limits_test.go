package compressionintegration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type gatedUpload struct {
	io.Reader
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func (r *gatedUpload) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.Reader.Read(p)
}
func (*gatedUpload) Close() error { return nil }
func (r *gatedUpload) unblock()   { r.closeOnce.Do(func() { close(r.release) }) }

func TestStalledUploadCannotStarveReadCodecs(t *testing.T) {
	p := pool()
	store, _, dir := localStorageBudgeted(t, p, true, false, 81920, 1<<20, 1)
	data := bytes.Repeat([]byte("stored read frame"), 1000)
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	incoming := bytes.Repeat([]byte("slow input"), 1000)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	encoded := encoder.EncodeAll(incoming, nil)
	require.NoError(t, encoder.Close())
	gate := &gatedUpload{Reader: bytes.NewReader(encoded), entered: make(chan struct{}), release: make(chan struct{})}
	defer gate.unblock()
	before := []int64{p.encoders.Load(), p.decoders.Load()}
	done := make(chan error, 1)
	source := blobstore.NewMetricsBlobAccess(&encodedSourceStore{source: buffer.NewCASBufferFromZSTDReader(t.Context(), digestOf(incoming), gate, p, buffer.BackendProvided(buffer.Irreparable(digestOf(incoming))))}, clock.SystemClock, "cas", "stalled_encoded_source")
	go func() {
		done <- store.Put(t.Context(), digestOf(incoming), source.Get(t.Context(), digestOf(incoming)))
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not reach streaming decoder")
	}
	require.Equal(t, before, []int64{p.encoders.Load(), p.decoders.Load()}, "uploads must not acquire read-side codecs")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := make([]byte, 10)
	n, err := store.Get(ctx, d).ReadAt(got, 5000)
	require.NoError(t, err)
	require.Equal(t, data[5000:5000+n], got)
	other := bytes.Repeat([]byte("independent write"), 1000)
	require.NoError(t, store.Put(ctx, digestOf(other), buffer.NewCASBufferFromByteSlice(digestOf(other), other, buffer.UserProvided)))
	gate.unblock()
	require.NoError(t, <-done)
	actual, err := store.Get(t.Context(), digestOf(incoming)).ToByteSlice(len(incoming))
	require.NoError(t, err)
	require.Equal(t, incoming, actual)
	// The single representation staging file is unlinked after success.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStagingContentionWaitsAndCancellationReleasesAdmission(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("waiting bytes"), 1000)
	d := digestOf(data)
	reservation, err := zstdframes.MaximumStoredSize(int64(len(data)), 1<<20)
	require.NoError(t, err)
	store, _, _ := localStorageBudgeted(t, p, true, false, 81920, reservation, 1)
	gate := &gatedUpload{Reader: bytes.NewReader(data), entered: make(chan struct{}), release: make(chan struct{})}
	defer gate.unblock()
	first := make(chan error, 1)
	go func() {
		first <- store.Put(t.Context(), d, buffer.NewCASBufferFromReader(d, gate, buffer.UserProvided))
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	other := bytes.Repeat([]byte("other content"), 1000)
	otherDigest := digestOf(other)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	source := &uploadCompletionReader{data: bytes.NewReader(other)}
	err = store.Put(ctx, otherDigest, buffer.NewCASBufferFromReader(otherDigest, source, buffer.UserProvided))
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "contention must wait rather than immediately reject")
	require.Zero(t, source.reads.Load())
	require.EqualValues(t, 1, source.closes.Load())
	second := make(chan error, 1)
	go func() {
		second <- store.Put(t.Context(), otherDigest, buffer.NewCASBufferFromByteSlice(otherDigest, other, buffer.UserProvided))
	}()
	gate.unblock()
	require.NoError(t, <-first)
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("capacity was not released")
	}
}

func TestEncodedIngressLimitAtFrontendAndDuplicateStorage(t *testing.T) {
	storagePool, frontendPool := pool(), pool()
	storage, _, _ := localStorage(t, storagePool)
	remote := grpcclients.NewCASBlobAccess(serve(t, storage, storagePool), uuid.NewRandom, 4096, frontendPool)
	conn := serve(t, remote, frontendPool)
	data := []byte("x")
	d := digestOf(data)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	defer encoder.Close()
	payload := encoder.EncodeAll(data, nil)
	// Valid Zstandard padding is accepted only within the explicit ingress policy.
	padding := make([]byte, bb_zstd.MaximumUploadSize(1)+8)
	binary.LittleEndian.PutUint32(padding, 0x184d2a50)
	binary.LittleEndian.PutUint32(padding[4:], uint32(len(padding)-8))
	payload = append(payload, padding...)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	decoded, err := decoder.DecodeAll(payload, nil)
	decoder.Close()
	require.NoError(t, err)
	require.Equal(t, data, decoded)
	for _, duplicate := range []bool{false, true} {
		if duplicate {
			require.NoError(t, storage.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
		}
		before := []int64{frontendPool.encoders.Load(), frontendPool.decoders.Load(), storagePool.encoders.Load(), storagePool.decoders.Load()}
		stream, err := bytestream.NewByteStreamClient(conn).Write(t.Context())
		require.NoError(t, err)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{ResourceName: d.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD), Data: payload, FinishWrite: true}))
		_, err = stream.CloseAndRecv()
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Equal(t, before, []int64{frontendPool.encoders.Load(), frontendPool.decoders.Load(), storagePool.encoders.Load(), storagePool.decoders.Load()})
	}
}

func TestCompressedStorageRejectsLargeUnindexedRecords(t *testing.T) {
	data := bytes.Repeat([]byte("raw"), 2000)
	invalid := false
	b := blobstore.NewCompressedCASReadBufferFactory(pool()).NewBufferFromByteSlice(digestOf(data), data, func(valid bool) { invalid = !valid })
	_, err := b.ToByteSlice(len(data))
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.True(t, invalid)
}

type encodedSourceStore struct {
	blobstore.BlobAccess
	source buffer.Buffer
}

func (s *encodedSourceStore) Get(context.Context, digest.Digest) buffer.Buffer { return s.source }

type stagingRetryHandler struct {
	replacement buffer.Buffer
	calls       int
	done        int
}

func (h *stagingRetryHandler) OnError(err error) (buffer.Buffer, error) {
	h.calls++
	if h.replacement == nil {
		return nil, err
	}
	next := h.replacement
	h.replacement = nil
	return next, nil
}
func (h *stagingRetryHandler) Done() { h.done++ }

type errorAfterBytes struct {
	*bytes.Reader
	err error
}

func (r *errorAfterBytes) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}
func (*errorAfterBytes) Close() error { return nil }

func TestEncodedStagingRetryReplacesWholeRepresentation(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%t", malformed), func(t *testing.T) {
			p := pool()
			store, _, _ := localStorage(t, p)
			data := bytes.Repeat([]byte("replace entire encoded input"), 1000)
			d := digestOf(data)
			encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
			require.NoError(t, err)
			replacement := encoder.EncodeAll(data, nil)
			require.NoError(t, encoder.Close())
			first := make([]byte, len(replacement)+500)
			sourceErr := status.Error(codes.Unavailable, "source disconnected")
			var firstReader io.ReadCloser = &errorAfterBytes{Reader: bytes.NewReader(first), err: sourceErr}
			if malformed {
				firstReader = io.NopCloser(bytes.NewReader(first))
			}
			before := p.decoders.Load()
			h := &stagingRetryHandler{replacement: buffer.NewCASBufferFromZSTDReader(t.Context(), d, io.NopCloser(bytes.NewReader(replacement)), p, buffer.BackendProvided(buffer.Irreparable(d)))}
			input := buffer.WithErrorHandler(buffer.NewCASBufferFromZSTDReader(t.Context(), d, firstReader, p, buffer.BackendProvided(buffer.Irreparable(d))), h)
			require.NoError(t, store.Put(t.Context(), d, input))
			require.Equal(t, 1, h.calls)
			require.Equal(t, 1, h.done)
			require.EqualValues(t, 0, p.decoders.Load()-before, "replication uploads use their destination codec pool")
			actual, err := store.Get(t.Context(), d).ToByteSlice(len(data))
			require.NoError(t, err)
			require.Equal(t, data, actual)
		})
	}
}

func TestStagedBackendIntegrityCallbackRetainsSource(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := bytes.Repeat([]byte("intended bytes"), 1000)
	d := digestOf(data)
	wrong := bytes.Clone(data)
	wrong[0] ^= 1
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	encoded := encoder.EncodeAll(wrong, nil)
	require.NoError(t, encoder.Close())
	source := &uploadCompletionReader{data: bytes.NewReader(encoded)}
	invalid := 0
	b := buffer.NewCASBufferFromZSTDReader(t.Context(), d, source, p, buffer.BackendProvided(func(valid bool) {
		if !valid {
			invalid++
			require.Zero(t, source.closes.Load(), "integrity callback must retain its source reference")
		}
	}))
	err = store.Put(t.Context(), d, b)
	require.Equal(t, codes.Internal, status.Code(err), "backend corruption is not a client argument error")
	require.Equal(t, 1, invalid)
	require.EqualValues(t, 1, source.closes.Load())
	missing, err := store.FindMissing(t.Context(), d.ToSingletonSet())
	require.NoError(t, err)
	require.Equal(t, d.ToSingletonSet(), missing)
}

func TestStreamingDecodeUsesDestinationDeadline(t *testing.T) {
	p := pool()
	store, err := blobstore.NewCompressedCASBlobAccess(&repairRecordStore{lookupError: status.Error(codes.Unavailable, "missing")}, t.TempDir(), 1<<20, 1, p)
	require.NoError(t, err)
	data := bytes.Repeat([]byte("local decode deadline"), 1000)
	d := digestOf(data)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	encoded := encoder.EncodeAll(data, nil)
	require.NoError(t, encoder.Close())
	held, err := p.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	defer held.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	// The source may have a longer lifetime than this destination upload.
	source := buffer.NewCASBufferFromZSTDReader(t.Context(), d, io.NopCloser(bytes.NewReader(encoded)), p, buffer.BackendProvided(buffer.Irreparable(d)))
	go func() { done <- store.Put(ctx, d, source) }()
	select {
	case err := <-done:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(time.Second):
		held.Close()
		<-done
		t.Fatal("decoder wait ignored destination deadline")
	}
	held.Close()
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)), "canceled processing must release admission")
}

func TestStagingDestinationFailureDoesNotBlameSource(t *testing.T) {
	data := []byte("staging destination failed")
	d := digestOf(data)
	source := &uploadCompletionReader{data: bytes.NewReader(data)}
	h := &stagingRetryHandler{}
	b := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, source, buffer.BackendProvided(buffer.Irreparable(d))), h)
	failure := status.Error(codes.ResourceExhausted, "staging disk full")
	err := buffer.ConsumeCASUpload(t.Context(), b, pool(), func(input buffer.Buffer) error {
		r := input.ToReader()
		defer r.Close()
		return &buffer.UploadDestinationError{Err: failure}
	})
	require.ErrorIs(t, err, failure)
	require.Zero(t, h.calls, "destination errors must not trigger source fallback")
	require.Equal(t, 1, h.done)
	require.EqualValues(t, 1, source.closes.Load())
}

func TestDuplicateUploadsShareTheBoundedRequestQueue(t *testing.T) {
	p := pool()
	store, _, _ := localStorageBudgeted(t, p, true, false, 81920, 1<<20, 1)
	data := []byte("duplicate queue limit")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	before := []int64{p.encoders.Load(), p.decoders.Load()}
	done := make(chan error, 4)
	gates := make([]*gatedUpload, 4)
	for i := range gates {
		gate := &gatedUpload{Reader: bytes.NewReader(nil), entered: make(chan struct{}), release: make(chan struct{})}
		gates[i] = gate
		defer gate.unblock()
		go func() { done <- store.Put(t.Context(), d, buffer.NewCASBufferFromZSTDUpload(t.Context(), d, gate, p)) }()
		select {
		case <-gate.entered:
		case <-time.After(time.Second):
			t.Fatal("duplicate did not begin draining")
		}
	}
	overflow := &uploadCompletionReader{}
	err := store.Put(t.Context(), d, buffer.NewCASBufferFromZSTDUpload(t.Context(), d, overflow, p))
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Zero(t, overflow.reads.Load())
	require.EqualValues(t, 1, overflow.closes.Load())
	for _, gate := range gates {
		gate.unblock()
	}
	for range gates {
		require.NoError(t, <-done)
	}
	require.Equal(t, before, []int64{p.encoders.Load(), p.decoders.Load()})
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
}

func TestStreamingUploadEncodesBeforeInputCompletes(t *testing.T) {
	p := pool()
	base := &repairRecordStore{lookupError: status.Error(codes.Unavailable, "missing")}
	dir := t.TempDir()
	data := bytes.Repeat([]byte("s"), 2<<20)
	d := digestOf(data)
	reservation, err := zstdframes.MaximumStoredSize(int64(len(data)), 1<<20)
	require.NoError(t, err)
	// Only output space is available: reserving or spooling the input would fail.
	store, err := blobstore.NewCompressedCASBlobAccess(base, dir, reservation, 1, p)
	require.NoError(t, err)
	gate := &gatedUpload{Reader: bytes.NewReader(data[1<<20:]), entered: make(chan struct{}), release: make(chan struct{})}
	defer gate.unblock()
	source := io.NopCloser(io.MultiReader(bytes.NewReader(data[:1<<20]), gate))
	done := make(chan error, 1)
	go func() {
		done <- store.Put(t.Context(), d, buffer.NewCASBufferFromReader(d, source, buffer.UserProvided))
	}()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("upload did not stream through its first frame")
	}
	require.EqualValues(t, 1, p.encoders.Load(), "the first frame must be encoded while later input is still blocked")
	gate.unblock()
	require.NoError(t, <-done)
	r, err := zstdframes.Open(bytes.NewReader(base.record), int64(len(base.record)), d)
	require.NoError(t, err)
	var actual bytes.Buffer
	require.NoError(t, r.WriteIdentity(t.Context(), &actual, 0, p))
	require.Equal(t, data, actual.Bytes())
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

type repairFallbackHandler struct {
	ctx        context.Context
	d          digest.Digest
	replicator replication.BlobReplicator
	calls      int
}

func (*repairFallbackHandler) Done() {}
func (h *repairFallbackHandler) OnError(err error) (buffer.Buffer, error) {
	h.calls++
	if h.calls > 1 {
		return nil, err
	}
	return h.replicator.ReplicateSingle(h.ctx, h.d), nil
}

type repairSource struct {
	blobstore.BlobAccess
	data []byte
}

func (s repairSource) Get(_ context.Context, d digest.Digest) buffer.Buffer {
	return buffer.NewCASBufferFromByteSlice(d, s.data, buffer.BackendProvided(buffer.Irreparable(d)))
}

func TestStreamingFallbackCanRepairThroughSameStore(t *testing.T) {
	p := pool()
	store, _, _ := localStorageBudgeted(t, p, true, false, 81920, 1<<20, 1)
	data := bytes.Repeat([]byte("fallback repair source"), 1000)
	d := digestOf(data)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	h := &repairFallbackHandler{ctx: ctx, d: d, replicator: replication.NewLocalBlobReplicator(repairSource{data: data}, store)}
	bad := buffer.NewCASBufferFromZSTDReader(ctx, d, io.NopCloser(bytes.NewReader([]byte("malformed"))), p, buffer.BackendProvided(buffer.Irreparable(d)))
	require.NoError(t, store.Put(ctx, d, buffer.WithErrorHandler(bad, h)))
	require.Equal(t, 1, h.calls)
	actual, err := store.Get(ctx, d).ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
}

func TestIncompatibleUploadWindowsFailBeforeOpeningStorage(t *testing.T) {
	for _, selfMismatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("self=%t", selfMismatch), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "blocks")
			var config pb.BlobAccessConfiguration
			extra := ""
			readPool := bb_zstd.NewBoundedPool(1, 1, []zstd.EOption{zstd.WithWindowSize(64 << 10)}, []zstd.DOption{zstd.WithDecoderMaxWindow(64 << 10)})
			if selfMismatch {
				extra = `,"uploadZstdPool":{"decoderWindowSizeBytes":1024}`
				readPool = pool()
			}
			require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}}},"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"%s}}}`, path, dir, extra)), &config))
			_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, readPool))
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			_, err = os.Stat(path)
			require.True(t, os.IsNotExist(err), "incompatible codec settings must fail before opening the device")
		})
	}
}
