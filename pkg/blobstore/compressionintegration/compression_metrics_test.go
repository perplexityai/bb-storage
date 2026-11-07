package compressionintegration_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func metricValue(t *testing.T, name string, labels ...string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.Metric {
			matches := true
			for i := 0; i < len(labels); i += 2 {
				found := false
				for _, l := range m.Label {
					if l.GetName() == labels[i] && l.GetValue() == labels[i+1] {
						found = true
					}
				}
				matches = matches && found
			}
			if matches {
				if m.Counter != nil {
					return m.Counter.GetValue()
				}
				if m.Gauge != nil {
					return m.Gauge.GetValue()
				}
				if m.Histogram != nil {
					return float64(m.Histogram.GetSampleCount())
				}
			}
		}
	}
	return 0
}

func TestCompressionPutAndTransferMetrics(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	conn := serve(t, store, p)
	remote := grpcclients.NewCASBlobAccess(conn, uuid.NewRandom, 4096, p)
	for _, tc := range []struct {
		name string
		data []byte
	}{{"indexed", bytes.Repeat([]byte("compressed metrics"), 1000)}, {"tiny_raw", []byte("tiny")}} {
		d := digestOf(tc.data)
		puts := metricValue(t, "buildbarn_cas_compression_puts_total", "outcome", tc.name)
		logical := metricValue(t, "buildbarn_cas_compression_logical_bytes_total", "outcome", tc.name)
		physical := metricValue(t, "buildbarn_cas_compression_physical_bytes_total", "outcome", tc.name)
		fallback := metricValue(t, "buildbarn_bytestream_compression_transfers_total", "direction", "upload", "path", "logical_fallback")
		require.NoError(t, remote.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, tc.data, buffer.UserProvided)))
		require.Equal(t, fallback+1, metricValue(t, "buildbarn_bytestream_compression_transfers_total", "direction", "upload", "path", "logical_fallback"))
		require.Equal(t, puts+1, metricValue(t, "buildbarn_cas_compression_puts_total", "outcome", tc.name))
		require.Equal(t, logical+float64(len(tc.data)), metricValue(t, "buildbarn_cas_compression_logical_bytes_total", "outcome", tc.name))
		written := metricValue(t, "buildbarn_cas_compression_physical_bytes_total", "outcome", tc.name) - physical
		if tc.name == "tiny_raw" {
			require.Equal(t, float64(len(tc.data)), written)
		} else {
			require.Positive(t, written)
			require.Less(t, written, float64(len(tc.data)))
		}
		duplicate := metricValue(t, "buildbarn_cas_compression_puts_total", "outcome", "duplicate")
		require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, tc.data, buffer.UserProvided)))
		require.Equal(t, duplicate+1, metricValue(t, "buildbarn_cas_compression_puts_total", "outcome", "duplicate"))
		require.Equal(t, physical+written, metricValue(t, "buildbarn_cas_compression_physical_bytes_total", "outcome", tc.name))
		readPath := "compressed_passthrough"
		if tc.name == "tiny_raw" {
			readPath = "logical_fallback"
		}
		reads := metricValue(t, "buildbarn_bytestream_compression_transfers_total", "direction", "read", "path", readPath)
		_ = read(t, t.Context(), conn, d, remoteexecution.Compressor_ZSTD, 0)
		require.Equal(t, reads+1, metricValue(t, "buildbarn_bytestream_compression_transfers_total", "direction", "read", "path", readPath))
		uploads := metricValue(t, "buildbarn_bytestream_compression_transfers_total", "direction", "upload", "path", "compressed_passthrough")
		require.NoError(t, remote.Put(t.Context(), d, remote.Get(t.Context(), d)))
		require.Equal(t, uploads+1, metricValue(t, "buildbarn_bytestream_compression_transfers_total", "direction", "upload", "path", "compressed_passthrough"))
	}
	require.Zero(t, metricValue(t, "buildbarn_cas_compression_active_requests"))
	require.Zero(t, metricValue(t, "buildbarn_cas_compression_staging_reserved_bytes"))
}

func TestReadIntegrityMetricsExcludeDeviceErrors(t *testing.T) {
	data := bytes.Repeat([]byte("integrity"), 1000)
	d := digestOf(data)
	p := pool()
	var out bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data), 4096, p))
	for _, reason := range []string{"invalid_index", "unsupported_version", "corrupt_frame", "device"} {
		before := map[string]float64{}
		for _, label := range []string{"invalid_index", "unsupported_version", "corrupt_frame"} {
			before[label] = metricValue(t, "buildbarn_cas_compression_read_integrity_errors_total", "reason", label)
		}
		record := bytes.Clone(out.Bytes())
		switch reason {
		case "invalid_index":
			record[len(record)-1] ^= 1
		case "unsupported_version":
			record[len(record)-48+7] = '2'
		case "corrupt_frame":
			record[0] ^= 1
		}
		var source io.ReaderAt = bytes.NewReader(record)
		if reason == "device" {
			source = &frameFailingReader{countedRecordReader: &countedRecordReader{Reader: bytes.NewReader(record)}, err: status.Error(codes.DataLoss, "device failure")}
		}
		index, err := zstdframes.Open(source, int64(len(record)), d)
		if err == nil {
			_, err = index.ReadCompressedFrame(t.Context(), 0)
		}
		require.Error(t, err)
		for label, value := range before {
			if label == reason {
				value++
			}
			require.Equal(t, value, metricValue(t, "buildbarn_cas_compression_read_integrity_errors_total", "reason", label), reason+":"+label)
		}
	}
}

type metadataOnlyStore struct {
	blobstore.BlobAccess
	calls int
}

func (s *metadataOnlyStore) FindMissing(_ context.Context, ds digest.Set) (digest.Set, error) {
	s.calls++
	return digest.EmptySet, nil
}

func (*metadataOnlyStore) Get(context.Context, digest.Digest) buffer.Buffer {
	panic("presence checks must not read records")
}

func TestCompressedPresenceAndDuplicateUseMetadataOnly(t *testing.T) {
	base := &metadataOnlyStore{}
	store, err := blobstore.NewCompressedCASBlobAccess(base, t.TempDir(), 1<<20, 1, pool())
	require.NoError(t, err)
	d := digestOf([]byte("present"))
	missing, err := store.FindMissing(t.Context(), d.ToSingletonSet())
	require.NoError(t, err)
	require.True(t, missing.Empty())
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, []byte("present"), buffer.UserProvided)))
	require.Equal(t, 2, base.calls)
}

func TestDeadlineDecoratorPreservesEncodedRead(t *testing.T) {
	p := pool()
	store, _, _ := localStorage(t, p)
	data := bytes.Repeat([]byte("deadline fast path"), 1000)
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	r, ok := buffer.ZSTDChunkReaderAt(blobstore.NewDeadlineEnforcingBlobAccess(store, time.Minute).Get(t.Context(), d), 0, 4096)
	require.True(t, ok)
	before := p.decoders.Load()
	got, err := io.ReadAll(buffer.NewReaderFromChunkReader(r))
	r.Close()
	require.NoError(t, err)
	require.NotEmpty(t, got)
	require.Equal(t, before, p.decoders.Load())
}
