package compressionintegration_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	_, encoded := buffer.ZSTDChunkReaderAt(b, 0, 4096)
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

func TestTinyRecordsStayRawAndMalformedUploadsHaveStatus(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := []byte("x")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	b := store.Get(t.Context(), d)
	_, encoded := buffer.ZSTDChunkReaderAt(b, 0, 4096)
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

type capturingStore struct {
	blobstore.BlobAccess
	record []byte
	puts   int
}

func (*capturingStore) FindMissing(_ context.Context, digests digest.Set) (digest.Set, error) {
	return digests, nil
}

func (s *capturingStore) Put(_ context.Context, _ digest.Digest, b buffer.Buffer) error {
	s.puts++
	var err error
	s.record, err = b.ToByteSlice(1 << 20)
	return err
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

func TestStoredReadFailuresDoNotEvict(t *testing.T) {
	data := bytes.Repeat([]byte("device reads"), 1000)
	d := digestOf(data)
	p := pool()
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	for _, path := range []string{"encoded", "read_at", "suffix"} {
		for _, failure := range []error{status.Error(codes.DataLoss, "device failed"), io.ErrUnexpectedEOF} {
			t.Run(path+"/"+failure.Error(), func(t *testing.T) {
				source := &frameFailingReader{countedRecordReader: &countedRecordReader{Reader: bytes.NewReader(out.Bytes())}, err: failure}
				evictions := 0
				b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, source, int64(out.Len()), func(valid bool) {
					if !valid {
						evictions++
					}
				})
				var err error
				switch path {
				case "read_at":
					_, err = b.ReadAt(make([]byte, 1), 1)
				case "encoded":
					r, ok := buffer.ZSTDChunkReaderAt(b, 0, 4096)
					require.True(t, ok)
					_, err = r.Read()
					r.Close()
				case "suffix":
					r := b.ToChunkReader(1, 64)
					_, err = r.Read()
					r.Close()
				}
				require.ErrorIs(t, err, failure)
				require.Zero(t, evictions)
				require.EqualValues(t, 1, source.closes.Load())
			})
		}
	}
	future := bytes.Clone(out.Bytes())
	copy(future[len(future)-48:], "BBZSTF02")
	evictions := 0
	b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromByteSlice(d, future, func(valid bool) {
		if !valid {
			evictions++
		}
	})
	_, err := b.ToByteSlice(len(data))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, evictions)
}
