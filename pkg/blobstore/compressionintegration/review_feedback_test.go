package compressionintegration_test

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Legacy clients returned Send's EOF without first obtaining the successful
// response. Preserve that behavior in the compatibility fixture, rather than
// testing only the current client's explicit early-completion support.
type legacyUploadWriter struct {
	stream bytestream.ByteStream_WriteClient
	name   string
	offset int64
}

func (w *legacyUploadWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&bytestream.WriteRequest{ResourceName: w.name, WriteOffset: w.offset, Data: p}); err != nil {
		return 0, err
	}
	w.offset += int64(len(p))
	w.name = ""
	return len(p), nil
}

func TestLegacyCompressedClientDuplicateCompatibility(t *testing.T) {
	p := pool()
	store, _, _ := localStorageSized(t, p, true, false, 5*(8<<20))
	conn := serve(t, store, p)
	random := rand.New(rand.NewPCG(19, 21))
	for _, size := range []int{1 << 20, 4 << 20} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(random.Uint32())
		}
		d := digestOf(data)
		require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
		before := []int64{p.encoders.Load(), p.decoders.Load()}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		stream, err := bytestream.NewByteStreamClient(conn).Write(ctx)
		require.NoError(t, err)
		w := &legacyUploadWriter{stream: stream, name: d.GetByteStreamWritePath(uuid.New(), remoteexecution.Compressor_ZSTD)}
		encoder, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		_, copyErr := io.Copy(encoder, bytes.NewReader(data))
		closeErr := encoder.Close()
		require.NoError(t, copyErr)
		require.NoError(t, closeErr)
		require.NoError(t, stream.Send(&bytestream.WriteRequest{WriteOffset: w.offset, FinishWrite: true}))
		response, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, w.offset, response.CommittedSize)
		require.Equal(t, before, []int64{p.encoders.Load(), p.decoders.Load()}, "duplicates drain bytes without codecs")
		cancel()
	}
}

type recordingStore struct {
	blobstore.BlobAccess
	data   []byte
	p      *observedPool
	reader *countedRecordReader
}

func (s *recordingStore) Get(_ context.Context, d digest.Digest) buffer.Buffer {
	s.reader = &countedRecordReader{Reader: bytes.NewReader(s.data)}
	return blobstore.NewCompressedCASReadBufferFactory(s.p).NewBufferFromReaderAt(d, s.reader, int64(len(s.data)), func(bool) {})
}

func TestIndexedReadsThroughMetrics(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("random access frame"), 170000)
	d := digestOf(data)
	var encoded bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &encoded, d, bytes.NewReader(data), 1<<20, p))
	base := &recordingStore{data: encoded.Bytes(), p: p}
	store := blobstore.NewMetricsBlobAccess(base, clock.SystemClock, "cas", "indexed_read_feedback")
	var got [7]byte
	n, err := store.Get(t.Context(), d).ReadAt(got[:], 2<<20)
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	require.Equal(t, data[2<<20:(2<<20)+len(got)], got[:])
	require.EqualValues(t, 3, base.reader.reads.Load(), "only footer, index, and requested frame should be read")
	require.EqualValues(t, 1, base.reader.closes.Load())
	r := store.Get(t.Context(), d).ToChunkReader(3<<20, 4096)
	var suffix bytes.Buffer
	for {
		chunk, err := r.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		suffix.Write(chunk)
	}
	r.Close()
	require.Equal(t, data[3<<20:], suffix.Bytes())
	require.EqualValues(t, 3, base.reader.reads.Load(), "metrics must preserve indexed suffix reads")
}

type frameFailingReader struct {
	*countedRecordReader
	err error
}

func (r *frameFailingReader) ReadAt(p []byte, off int64) (int, error) {
	if off == 0 {
		return 0, r.err
	}
	return r.countedRecordReader.ReadAt(p, off)
}

func TestUnsupportedVersionAndFrameIOErrorsDoNotEvict(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("frame"), 1000)
	d := digestOf(data)
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	future := bytes.Clone(out.Bytes())
	copy(future[len(future)-48:], "BBZSTF02")
	invalid := 0
	b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromByteSlice(d, future, func(valid bool) {
		if !valid {
			invalid++
		}
	})
	_, err := b.ToByteSlice(len(data))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, invalid)
	for _, sourceErr := range []error{status.Error(codes.DataLoss, "Device failed"), io.ErrUnexpectedEOF} {
		r := &frameFailingReader{countedRecordReader: &countedRecordReader{Reader: bytes.NewReader(out.Bytes())}, err: sourceErr}
		b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, r, int64(out.Len()), func(valid bool) {
			if !valid {
				invalid++
			}
		})
		chunks, ok := buffer.ZSTDChunkReader(b, 4096)
		require.True(t, ok)
		_, err := chunks.Read()
		chunks.Close()
		require.Equal(t, sourceErr, err)
		require.Zero(t, invalid)
		require.EqualValues(t, 1, r.closes.Load())
	}
}

func TestRefreshRejectsAlternateConsumers(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("refresh"), 1000)
	d := digestOf(data)
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	factory := blobstore.NewCompressedCASReadBufferFactory(p)
	r := &countedRecordReader{Reader: bytes.NewReader(out.Bytes())}
	b := buffer.ForLocalRefresh(factory.NewBufferFromReaderAt(d, r, int64(out.Len()), func(bool) {}))
	_, err := b.ToByteSlice(out.Len())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.EqualValues(t, 1, r.closes.Load())
	var copied bytes.Buffer
	require.NoError(t, buffer.ForLocalRefresh(factory.NewBufferFromByteSlice(d, out.Bytes(), func(bool) {})).IntoWriter(&copied))
	require.Equal(t, out.Bytes(), copied.Bytes())
}

func TestResolvedTinyRawCloneCopyIsIndependent(t *testing.T) {
	data := []byte("tiny raw record")
	d := digestOf(data)
	b := blobstore.NewCompressedCASReadBufferFactory(pool()).NewBufferFromByteSlice(d, data, func(bool) {})
	_, encoded := buffer.ZSTDChunkReader(b, 4096)
	require.False(t, encoded)
	first, second := b.CloneCopy(len(data))
	done := make(chan error, 1)
	go func() {
		actual, err := first.ToByteSlice(len(data))
		if err == nil && !bytes.Equal(data, actual) {
			err = io.ErrUnexpectedEOF
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		second.Discard()
		<-done
		t.Fatal("CloneCopy requires concurrent consumers")
	}
	actual, err := second.ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
}

func TestClonedReadersPreserveFinalCloseError(t *testing.T) {
	data := []byte("clone close")
	d := digestOf(data)
	expected := status.Error(codes.Unavailable, "Completion failed")
	source := &uploadCompletionReader{data: bytes.NewReader(data), err: expected}
	first, second := buffer.NewCASBufferFromReader(d, source, buffer.UserProvided).CloneStream()
	readers := make(chan io.ReadCloser, 2)
	go func() { readers <- first.ToReader() }()
	go func() { readers <- second.ToReader() }()
	a, b := <-readers, <-readers
	require.NoError(t, a.Close())
	require.Equal(t, expected, b.Close())
	require.EqualValues(t, 1, source.closes.Load())
}

func TestTinyRecordsStayRawAndMalformedUploadsHaveStatus(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := []byte("x")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	b := store.Get(t.Context(), d)
	_, encoded := buffer.ZSTDChunkReader(b, 4096)
	require.False(t, encoded, "expanded records must stay raw")
	actual, err := b.ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
	bad := buffer.NewCASBufferFromZSTDUpload(t.Context(), d, io.NopCloser(bytes.NewReader([]byte("invalid zstd"))), p)
	_, err = bad.ToByteSlice(1)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestIndexedReadAtBoundaries(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("boundary"), 1000)
	d := digestOf(data)
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	for _, tc := range []struct {
		offset  int64
		size, n int
		err     error
	}{
		{int64(len(data)) + 1, 1, 0, io.EOF},
		{int64(len(data)) - 1, 4, 1, io.EOF},
		{int64(len(data)), 0, 0, nil},
		{0, 0, 0, nil},
	} {
		b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromByteSlice(d, out.Bytes(), func(bool) {})
		got := make([]byte, tc.size)
		n, err := b.ReadAt(got, tc.offset)
		require.Equal(t, tc.n, n)
		require.Equal(t, tc.err, err)
		if n > 0 {
			require.Equal(t, data[tc.offset:tc.offset+int64(n)], got[:n])
		}
	}
}

type repairRecordStore struct {
	blobstore.BlobAccess
	lookupError error
	record      []byte
	puts        int
}

func (s *repairRecordStore) FindMissing(context.Context, digest.Set) (digest.Set, error) {
	return digest.EmptySet, s.lookupError
}

func (s *repairRecordStore) Put(_ context.Context, _ digest.Digest, b buffer.Buffer) error {
	s.puts++
	var err error
	s.record, err = b.ToByteSlice(1 << 20)
	return err
}

func TestRepairUploadSurvivesLookupIOFailure(t *testing.T) {
	data := bytes.Repeat([]byte("repair input"), 1000)
	d := digestOf(data)
	for _, code := range []codes.Code{codes.Unavailable, codes.Internal, codes.DataLoss, codes.Unknown, codes.PermissionDenied, codes.Canceled} {
		t.Run(code.String(), func(t *testing.T) {
			p := pool()
			base := &repairRecordStore{lookupError: status.Error(code, "Existing record lookup failed")}
			store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, p)
			require.NoError(t, err)
			err = store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided))
			if code == codes.PermissionDenied || code == codes.Canceled {
				require.Equal(t, code, status.Code(err))
				require.Zero(t, base.puts)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, base.puts)
			index, err := zstdframes.Open(bytes.NewReader(base.record), int64(len(base.record)), d)
			require.NoError(t, err)
			var restored bytes.Buffer
			require.NoError(t, index.WriteIdentity(t.Context(), &restored, 0, p))
			require.Equal(t, data, restored.Bytes())
		})
	}
}

func TestRawRecordWithFutureFormatLikeTail(t *testing.T) {
	data := bytes.Repeat([]byte("raw"), 1000)
	copy(data[len(data)-48:], "BBZSTF02")
	d := digestOf(data)
	invalid := 0
	b := blobstore.NewCompressedCASReadBufferFactory(pool()).NewBufferFromByteSlice(d, data, func(valid bool) {
		if !valid {
			invalid++
		}
	})
	actual, err := b.ToByteSlice(len(data))
	require.NoError(t, err)
	require.Equal(t, data, actual)
	require.Zero(t, invalid)
}

type completionBarrierReader struct {
	io.Reader
	released <-chan struct{}
}

func (r *completionBarrierReader) Close() error { <-r.released; return nil }
func TestClonedStreamReportsEOFBeforeClosingSource(t *testing.T) {
	data := []byte("consumers must observe EOF before source completion")
	released := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	defer release()
	first, second := buffer.NewCASBufferFromReader(digestOf(data), &completionBarrierReader{Reader: bytes.NewReader(data), released: released}, buffer.UserProvided).CloneStream()
	var finished atomic.Int64
	done := make(chan error, 2)
	consume := func(b buffer.Buffer) {
		r := b.ToReader()
		actual, err := io.ReadAll(r)
		if err == nil && !bytes.Equal(data, actual) {
			err = io.ErrUnexpectedEOF
		}
		if finished.Add(1) == 2 {
			release()
		}
		closeErr := r.Close()
		if err == nil {
			err = closeErr
		}
		done <- err
	}
	go consume(first)
	go consume(second)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			release()
			t.Fatal("source Close ran before consumers observed EOF")
		}
	}
}
