package compressionintegration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestUnsupportedRecordCanBeRepairedWithoutEvictingNeighbors(t *testing.T) {
	for _, hierarchical := range []bool{false, true} {
		t.Run(fmt.Sprint(hierarchical), func(t *testing.T) {
			store, path, _ := localStorageMode(t, pool(), true, hierarchical)
			neighbor := bytes.Repeat([]byte("keep neighboring record"), 300)
			nd := digestOf(neighbor)
			require.NoError(t, store.Put(t.Context(), nd, buffer.NewCASBufferFromByteSlice(nd, neighbor, buffer.UserProvided)))
			data := bytes.Repeat([]byte("replace unsupported record"), 300)
			d := digestOf(data)
			if hierarchical {
				d = digest.MustNewDigest("parent", d.GetDigestFunction().GetEnumValue(), d.GetHashString(), d.GetSizeBytes())
			}
			require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
			disk, err := os.ReadFile(path)
			require.NoError(t, err)
			footer := bytes.LastIndex(disk, []byte("BBZSTF01"))
			require.GreaterOrEqual(t, footer, 0)

			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = f.WriteAt([]byte("99"), int64(footer+6))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			_, err = store.Get(t.Context(), d).ToByteSlice(len(data))
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			missing, err := store.FindMissing(t.Context(), d.ToSingletonSet())
			require.NoError(t, err)
			require.Equal(t, d.ToSingletonSet(), missing)
			parent := d
			if hierarchical {
				d = digest.MustNewDigest("parent/child", d.GetDigestFunction().GetEnumValue(), d.GetHashString(), d.GetSizeBytes())
			}
			require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
			got, err := store.Get(t.Context(), d).ToByteSlice(len(data))
			require.NoError(t, err)
			require.Equal(t, data, got)
			got, err = store.Get(t.Context(), parent).ToByteSlice(len(data))
			require.NoError(t, err)
			require.Equal(t, data, got)
			got, err = store.Get(t.Context(), nd).ToByteSlice(len(neighbor))
			require.NoError(t, err)
			require.Equal(t, neighbor, got)
		})
	}
}

func TestPersistentFormatMismatchFailsBeforeDeviceOpen(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		dir := t.TempDir()
		device := filepath.Join(dir, "blocks")
		marker := "zstdframes-v1\n"
		compression := ""
		if compressed {
			marker = "raw\n"
			compression = fmt.Sprintf(`,"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}`, dir)
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "storage-format"), []byte(marker), 0o600))
		var config pb.BlobAccessConfiguration
		require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q},"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}}}%s}}`, dir, device, compression)), &config))
		_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		_, err = os.Stat(device)
		require.True(t, os.IsNotExist(err))
	}
}

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

func TestAnnotatedIndexedReaderOffsetAndClose(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("indexed observer"), 1000)
	d := digestOf(data)
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	for _, off := range []int64{-1, int64(len(data)) + 1, 1} {
		source := &countedRecordReader{Reader: bytes.NewReader(out.Bytes())}
		h := &uploadObserver{}
		b := buffer.WithErrorHandler(blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, source, int64(out.Len()), func(bool) {}), h)
		r := b.ToChunkReader(off, 4096)
		_, err := r.Read()
		if off == 1 {
			require.NoError(t, err)
		} else {
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		}
		r.Close()
		r.Close()
		require.Empty(t, h.errors)
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
	base := &repairRecordStore{lookupError: status.Error(codes.Unavailable, "lookup unavailable")}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, failingEncoderPool{Pool: pool(), err: failure})
	require.NoError(t, err)
	h := &stagingRetryHandler{}
	source := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.BackendProvided(buffer.Irreparable(d))), h)
	require.ErrorIs(t, store.Put(t.Context(), d, source), failure)
	require.Zero(t, h.calls)
	require.Equal(t, 1, h.done)
	require.Zero(t, base.puts)
}

func TestIndexedDeviceErrorsDoNotEvict(t *testing.T) {
	data := bytes.Repeat([]byte("device reads"), 1000)
	d := digestOf(data)
	p := pool()
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	for _, suffix := range []bool{false, true} {
		failure := status.Error(codes.DataLoss, "device read failed")
		source := &frameFailingReader{countedRecordReader: &countedRecordReader{Reader: bytes.NewReader(out.Bytes())}, err: failure}
		evictions := 0
		b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, source, int64(out.Len()), func(valid bool) {
			if !valid {
				evictions++
			}
		})
		var err error
		if suffix {
			r := b.ToChunkReader(1, 64)
			_, err = r.Read()
			r.Close()
		} else {
			_, err = b.ReadAt(make([]byte, 1), 1)
		}
		require.ErrorIs(t, err, failure)
		require.Zero(t, evictions)
		require.EqualValues(t, 1, source.closes.Load())
	}
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
	base := &repairRecordStore{lookupError: status.Error(codes.Unavailable, "lookup unavailable")}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, unavailableDecoderPool{Pool: pool(), err: failure})
	require.NoError(t, err)
	// The decoder is never acquired, so it must not read these source bytes.
	source := &uploadCompletionReader{data: bytes.NewReader(nil)}
	h := &stagingRetryHandler{}
	b := buffer.WithErrorHandler(buffer.NewCASBufferFromZSTDReader(t.Context(), d, source, pool(), buffer.BackendProvided(buffer.Irreparable(d))), h)
	require.ErrorIs(t, store.Put(t.Context(), d, b), failure)
	require.Zero(t, h.calls)
	require.Equal(t, 1, h.done)
	require.Zero(t, base.puts)
	require.Zero(t, source.reads.Load())
	require.EqualValues(t, 1, source.closes.Load())
}

func TestInheritedUnsupportedRecordRepair(t *testing.T) {
	store, path, _ := localStorageMode(t, pool(), true, true)
	data := bytes.Repeat([]byte("inherited unsupported record"), 300)
	original := digestOf(data)
	parent := digest.MustNewDigest("parent", original.GetDigestFunction().GetEnumValue(), original.GetHashString(), original.GetSizeBytes())
	child := digest.MustNewDigest("parent/child", original.GetDigestFunction().GetEnumValue(), original.GetHashString(), original.GetSizeBytes())
	require.NoError(t, store.Put(t.Context(), parent, buffer.NewCASBufferFromByteSlice(parent, data, buffer.UserProvided)))
	// Fill the small-record allocation block, so repair cannot rewrite the
	// original buffered sector and accidentally conceal the corrupt fixture.
	padding := make([]byte, 3000)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range padding {
		padding[i] = byte(rng.Uint32())
	}
	pd := digestOf(padding)
	require.NoError(t, store.Put(t.Context(), pd, buffer.NewCASBufferFromByteSlice(pd, padding, buffer.UserProvided)))
	disk, err := os.ReadFile(path)
	require.NoError(t, err)
	footer := bytes.LastIndex(disk, []byte("BBZSTF01"))
	require.GreaterOrEqual(t, footer, 0)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("99"), int64(footer+6))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = store.Get(t.Context(), child).ToByteSlice(len(data))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.NoError(t, store.Put(t.Context(), child, buffer.NewCASBufferFromByteSlice(child, data, buffer.UserProvided)))
	got, err := store.Get(t.Context(), child).ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestFailedStartupDoesNotPublishPersistentFormat(t *testing.T) {
	for _, invalidDevice := range []bool{false, true} {
		dir := t.TempDir()
		device := filepath.Join(dir, "blocks")
		if invalidDevice {
			device = filepath.Join(dir, "missing", "blocks")
		}
		var config pb.BlobAccessConfiguration
		require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q,"minimumEpochInterval":"1s"},"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"spareBlocks":2},"oldBlocks":1,"currentBlocks":1,"newBlocks":1,"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}}}`, dir, device, dir)), &config))
		// With a valid device, the deliberately absent key-map backend fails late,
		// after the syncer is constructed. Neither path may start persistence.
		_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
		require.Error(t, err)
		for _, name := range []string{"storage-format", "storage-format.new", "state", "state.new"} {
			_, err := os.Stat(filepath.Join(dir, name))
			require.True(t, os.IsNotExist(err), name)
		}
	}
}

func TestEncoderWaitCancellationReachesUploadObserver(t *testing.T) {
	p := pool()
	held, err := p.NewEncoder(t.Context(), io.Discard)
	require.NoError(t, err)
	defer held.Close()
	base := &repairRecordStore{lookupError: status.Error(codes.Unavailable, "lookup unavailable")}
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
	base := &repairRecordStore{lookupError: status.Error(codes.Unavailable, "lookup unavailable")}
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

func TestStoredBufferRejectsOffsetsBeforeIndexedRead(t *testing.T) {
	data := bytes.Repeat([]byte("direct bounds"), 1000)
	d := digestOf(data)
	p := pool()
	var record bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &record, d, bytes.NewReader(data), 4096, p))
	b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromByteSlice(d, record.Bytes(), func(bool) {})
	r := b.ToChunkReader(int64(len(data))+1, 4096)
	_, err := r.Read()
	r.Close()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

type probeFailureStore struct {
	repairRecordStore
	failure error
}

func (s *probeFailureStore) Get(context.Context, digest.Digest) buffer.Buffer {
	return buffer.NewBufferFromError(s.failure)
}

func TestMetadataProbePreservesTemporaryFailures(t *testing.T) {
	for _, code := range []codes.Code{codes.ResourceExhausted, codes.Unavailable, codes.PermissionDenied, codes.Canceled} {
		base := &probeFailureStore{failure: status.Error(code, "temporary probe failure")}
		store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, pool())
		require.NoError(t, err)
		data := []byte("input")
		d := digestOf(data)
		_, err = store.FindMissing(t.Context(), d.ToSingletonSet())
		require.Equal(t, code, status.Code(err))
		if code == codes.ResourceExhausted {
			err = store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided))
			require.Equal(t, code, status.Code(err))
			require.Zero(t, base.puts)
		}
	}
}

func TestMarkerPublicationFailureDoesNotStartPersistence(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "storage-format.new")
	require.NoError(t, os.Mkdir(stale, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "block-removal"), nil, 0o600))
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q,"minimumEpochInterval":"1s"},"keyLocationMap":{"inMemory":{"entries":"1024"},"maximumGetAttempts":16,"maximumPutAttempts":"64"},"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"spareBlocks":2},"oldBlocks":1,"currentBlocks":1,"newBlocks":1,"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}}}`, dir, filepath.Join(dir, "blocks"), dir)), &config))
	// No lifecycle group is supplied: starting the syncer before publication
	// succeeds would panic as well as leak its independent release goroutine.
	_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "temporary storage format marker")
	for _, name := range []string{"storage-format", "state", "state.new"} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(err), name)
	}
}

func TestMissingStateDirectoryHasStartupContext(t *testing.T) {
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q}}}`, filepath.Join(t.TempDir(), "missing"))), &config))
	_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "Failed to open persistent state directory")
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
	base := &repairRecordStore{lookupError: status.Error(codes.Unavailable, "lookup unavailable")}
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
