package compressionintegration_test

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countedRecordReader struct {
	*bytes.Reader
	reads  atomic.Int64
	closes atomic.Int64
}

func (r *countedRecordReader) ReadAt(p []byte, off int64) (int, error) {
	r.reads.Add(1)
	return r.Reader.ReadAt(p, off)
}
func (r *countedRecordReader) Close() error { r.closes.Add(1); return nil }

func TestCompressedReadFactoryDefersIOAndPinsClones(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("lazy index"), 1000)
	d := digestOf(data)
	var encoded bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &encoded, d, bytes.NewReader(data), 4096, p))
	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "compressed", true: "legacy"}[raw], func(t *testing.T) {
			record := encoded.Bytes()
			if raw {
				record = data
			}
			r := &countedRecordReader{Reader: bytes.NewReader(record)}
			b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, r, int64(len(record)), func(bool) {})
			a, b := b.CloneStream()
			size, err := a.GetSizeBytes()
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), size)
			require.Zero(t, r.reads.Load(), "construction and refresh cloning must not do I/O under the store lock")
			a.Discard()
			require.Zero(t, r.closes.Load(), "remaining clone must retain the block reference")
			actual, err := b.ToByteSlice(len(data))
			require.NoError(t, err)
			require.Equal(t, data, actual)
			require.Positive(t, r.reads.Load())
			require.EqualValues(t, 1, r.closes.Load())
		})
	}
}

func TestKnownReadFailureDoesNotWaitForDecoder(t *testing.T) {
	p := pool()
	held, err := p.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	defer held.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	d := digestOf([]byte("hello"))
	expected := status.Error(codes.Unavailable, "Backend read failed")
	b := buffer.NewCASBufferFromZSTDReadFactory(ctx, d, func(int64) (io.ReadCloser, error) { return nil, expected }, p, buffer.BackendProvided(buffer.Irreparable(d)))
	_, err = b.ToByteSlice(5)
	require.Equal(t, expected, err)
	require.NoError(t, ctx.Err())
	require.EqualValues(t, 1, p.decoders.Load(), "a known error must not attempt another codec acquisition")
}

func TestLocalDuplicatePreservesCompletionFailures(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := []byte("existing blob")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	expected := status.Error(codes.Unavailable, "Replication failed")
	source := buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.UserProvided).WithTask(func() error { return expected })
	require.Equal(t, expected, store.Put(t.Context(), d, source))
	actual, err := store.Get(t.Context(), d).ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
}

type delayedRecordDevice struct {
	*os.File
	enabled atomic.Bool
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (d *delayedRecordDevice) ReadAt(p []byte, off int64) (int, error) {
	if d.enabled.Load() {
		d.once.Do(func() { close(d.started); <-d.release })
	}
	return d.File.ReadAt(p, off)
}

func TestBlockedIndexReadDoesNotBlockOtherWrites(t *testing.T) {
	p := pool()
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "blocks")
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, f.Truncate(5*16384))
	device := &delayedRecordDevice{File: f, started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(device.release) }) }
	defer release()
	allocator := local.NewBlockDeviceBackedBlockAllocator(device, blobstore.NewCompressedCASReadBufferFactory(p), 1, 16384, 5, "index_lock_regression")
	locations := local.NewOldCurrentNewLocationBlobMap(local.NewVolatileBlockList(allocator), local.NewImmutableBlockListGrowthPolicy(1, 1), util.DefaultErrorLogger, "index_lock_regression", 16384, 1, 1, 0)
	keys := lossymap.NewHashMap(local.NewInMemoryLocationRecordArray(1024), func(k *local.LocationRecordKey) uint64 {
		h := uint64(k.Attempt)
		for _, c := range k.Key {
			h = h*1099511628211 ^ uint64(c)
		}
		return h
	}, 1024, func(a, b *local.Location) int {
		if c := cmp.Compare(a.BlockIndex, b.BlockIndex); c != 0 {
			return c
		}
		return cmp.Compare(a.OffsetBytes, b.OffsetBytes)
	}, 16, 64, "index_lock_regression")
	base := local.NewFlatBlobAccess(keys, locations, locations, digest.KeyWithoutInstance, &sync.RWMutex{}, "index_lock_regression", nil)
	store, err := blobstore.NewCompressedCASBlobAccess(base, dir, 128<<20, 2, p)
	require.NoError(t, err)
	data := []byte("record with a slow index read")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	device.enabled.Store(true)
	readDone := make(chan error, 1)
	go func() {
		actual, err := store.Get(t.Context(), d).ToByteSlice(len(data))
		if err == nil && !bytes.Equal(actual, data) {
			err = fmt.Errorf("read returned incorrect bytes")
		}
		readDone <- err
	}()
	select {
	case <-device.started:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not reach storage")
	}
	writeDone := make(chan error, 1)
	other := []byte("unrelated record")
	otherDigest := digestOf(other)
	go func() {
		writeDone <- store.Put(t.Context(), otherDigest, buffer.NewCASBufferFromByteSlice(otherDigest, other, buffer.UserProvided))
	}()
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		release()
		<-writeDone
		<-readDone
		t.Fatal("index I/O held the store lock and blocked an unrelated write")
	}
	release()
	require.NoError(t, <-readDone)
}

type uploadCompletionReader struct {
	data   io.Reader
	reads  atomic.Int64
	closes atomic.Int64
	err    error
}

func (r *uploadCompletionReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	if r.data != nil {
		return r.data.Read(p)
	}
	return 0, io.EOF
}
func (r *uploadCompletionReader) Close() error { r.closes.Add(1); return r.err }

func TestLocalDuplicateClosesEncodedUploadWithoutCodecs(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := []byte("already present")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	before := []int64{p.encoders.Load(), p.decoders.Load()}
	closeErr := status.Error(codes.DataLoss, "Upload close failed")
	taskErr := status.Error(codes.Unavailable, "Background completion failed")
	for _, tc := range []struct {
		name                    string
		closeErr, taskErr, want error
	}{
		{"success", nil, nil, nil},
		{"task", nil, taskErr, taskErr},
		{"close", closeErr, nil, closeErr},
		{"both", closeErr, taskErr, closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &uploadCompletionReader{err: tc.closeErr}
			var tasks atomic.Int64
			b := buffer.NewCASBufferFromZSTDUpload(t.Context(), d, r, p).WithTask(func() error { tasks.Add(1); return tc.taskErr }).WithTask(func() error { tasks.Add(1); return nil })
			require.Equal(t, tc.want, store.Put(t.Context(), d, b))
			require.Zero(t, r.reads.Load())
			require.EqualValues(t, 1, r.closes.Load())
			require.EqualValues(t, 2, tasks.Load())
			require.Equal(t, before, []int64{p.encoders.Load(), p.decoders.Load()})
		})
	}
}

func TestCanceledLazyReadReleasesRecordWithoutIO(t *testing.T) {
	p := pool()
	data := []byte("unused record")
	d := digestOf(data)
	r := &countedRecordReader{Reader: bytes.NewReader(data)}
	b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, r, int64(len(data)), func(bool) {})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := buffer.WithZSTDContext(b, ctx).ToByteSlice(len(data))
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Zero(t, r.reads.Load())
	require.EqualValues(t, 1, r.closes.Load())
	require.Zero(t, p.decoders.Load())
}

type completionErrorHandler struct{ done bool }

func (*completionErrorHandler) OnError(err error) (buffer.Buffer, error) { return nil, err }
func (h *completionErrorHandler) Done()                                  { h.done = true }

func TestDuplicateFallbackPreservesReaderCloseError(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := []byte("existing wrapped input")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	expected := status.Error(codes.Unavailable, "Source completion failed")
	r := &uploadCompletionReader{data: bytes.NewReader(data), err: expected}
	h := &completionErrorHandler{}
	source := buffer.WithErrorHandler(buffer.NewCASBufferFromReader(d, r, buffer.UserProvided), h)
	require.Equal(t, expected, store.Put(t.Context(), d, source))
	require.True(t, h.done)
	require.EqualValues(t, 1, r.closes.Load())
}
