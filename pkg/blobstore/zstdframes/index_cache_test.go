package zstdframes_test

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

type indexReadCounter struct {
	*bytes.Reader
	reads int
}

func (r *indexReadCounter) ReadAt(p []byte, off int64) (int, error) {
	r.reads++
	return r.Reader.ReadAt(p, off)
}

func TestIndexCacheUsesCurrentSourceAndDigest(t *testing.T) {
	data := bytes.Repeat([]byte("cached immutable index"), 1000)
	record, d := encode(t, data, 4096, newPool())
	cache := zstdframes.NewIndexCache(1 << 20)
	first := &indexReadCounter{Reader: bytes.NewReader(record)}
	_, err := cache.Open(first, int64(len(record)), d)
	require.NoError(t, err)
	require.Equal(t, 2, first.reads)
	// No old source may be retained by a cache hit. A replacement block with a
	// damaged frame must fail its checksum, even when its metadata is identical.
	damaged := bytes.Clone(record)
	damaged[0] ^= 1
	second := &indexReadCounter{Reader: bytes.NewReader(damaged)}
	r, err := cache.Open(second, int64(len(record)), d)
	require.NoError(t, err)
	require.Equal(t, 1, second.reads, "a hit reads only the footer")
	_, err = r.ReadCompressedFrame(t.Context(), 0)
	require.ErrorIs(t, err, zstdframes.ErrCorruptFrame)
	require.Equal(t, 2, first.reads, "cache must not retain the first source")
	wrong := bytes.Clone(data)
	wrong[0] ^= 1
	_, err = cache.Open(bytes.NewReader(record), int64(len(record)), blobDigest(wrong, d.GetDigestFunction().GetEnumValue()))
	require.ErrorIs(t, err, zstdframes.ErrInvalidRepresentation)
	// A new valid encoding of the same digest must use its own frame mapping.
	replacement, _ := encode(t, data, 8192, newPool())
	r, err = cache.Open(bytes.NewReader(replacement), int64(len(replacement)), d)
	require.NoError(t, err)
	require.Equal(t, (len(data)+8191)/8192, r.FrameCount())
	var actual bytes.Buffer
	require.NoError(t, r.WriteIdentity(t.Context(), &actual, 0, newPool()))
	require.Equal(t, data, actual.Bytes())
}

func TestIndexCacheEvictsAndSupportsConcurrentReaders(t *testing.T) {
	p := newPool()
	a, da := encode(t, bytes.Repeat([]byte("a"), 5000), 4096, p)
	b, db := encode(t, bytes.Repeat([]byte("b"), 5000), 4096, p)
	// Each two-frame entry fits separately, but both exceed this budget.
	cache := zstdframes.NewIndexCache(2500)
	source := func(data []byte) *indexReadCounter { return &indexReadCounter{Reader: bytes.NewReader(data)} }
	a1 := source(a)
	_, err := cache.Open(a1, int64(len(a)), da)
	require.NoError(t, err)
	a2 := source(a)
	_, err = cache.Open(a2, int64(len(a)), da)
	require.NoError(t, err)
	require.Equal(t, 1, a2.reads)
	_, err = cache.Open(source(b), int64(len(b)), db)
	require.NoError(t, err)
	a3 := source(a)
	_, err = cache.Open(a3, int64(len(a)), da)
	require.NoError(t, err)
	require.Equal(t, 2, a3.reads, "least-recently-used metadata must be evicted")
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Go(func() {
			r, err := cache.Open(bytes.NewReader(a), int64(len(a)), da)
			if err != nil {
				t.Error(err)
				return
			}
			if err := r.WriteCompressed(t.Context(), io.Discard); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
}

func BenchmarkIndexOpen(b *testing.B) {
	data := bytes.Repeat([]byte("index lookup benchmark"), 1<<20)
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	var out bytes.Buffer
	if err := zstdframes.Encode(b.Context(), &out, d, bytes.NewReader(data), 4096, newPool()); err != nil {
		b.Fatal(err)
	}
	record := out.Bytes()
	for _, cached := range []bool{false, true} {
		name := "uncached"
		if cached {
			name = "cached"
		}
		b.Run(name, func(b *testing.B) {
			cache := zstdframes.NewIndexCache(64 << 20)
			if _, err := cache.Open(bytes.NewReader(record), int64(len(record)), d); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				if cached {
					_, err = cache.Open(bytes.NewReader(record), int64(len(record)), d)
				} else {
					_, err = zstdframes.Open(bytes.NewReader(record), int64(len(record)), d)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestIndexCacheDoesNotRetainInstanceIdentity(t *testing.T) {
	data := bytes.Repeat([]byte("instance-independent metadata"), 1000)
	record, d := encode(t, data, 4096, newPool())
	cache := zstdframes.NewIndexCache(1 << 20)
	for _, instance := range []string{strings.Repeat("a", 1<<20), strings.Repeat("b", 1<<20)} {
		requested := digest.MustNewDigest(instance, d.GetDigestFunction().GetEnumValue(), d.GetHashString(), d.GetSizeBytes())
		source := &indexReadCounter{Reader: bytes.NewReader(record)}
		r, err := cache.Open(source, int64(len(record)), requested)
		require.NoError(t, err)
		if instance[0] == 'b' {
			require.Equal(t, 1, source.reads, "metadata is shared across instances")
		}
		require.Equal(t, instance, r.FrameDigests()[0].GetInstanceName().String(), "returned readers retain the current caller identity")
	}
}

func TestIndexCacheFooterSeparatesSameSizeRecords(t *testing.T) {
	data := bytes.Repeat([]byte("f"), 4096)
	record, d := encode(t, data, 4096, newPool())
	cache := zstdframes.NewIndexCache(1 << 20)
	_, err := cache.Open(bytes.NewReader(record), int64(len(record)), d)
	require.NoError(t, err)
	other := rewriteIndex(t, record, func(m map[string]any) {
		m["frames"].([]any)[0].(map[string]any)["hash"] = strings.Repeat("0", 64)
	})
	require.Len(t, other, len(record), "cache keys must collide if footer is omitted")
	source := &indexReadCounter{Reader: bytes.NewReader(other)}
	index, err := cache.Open(source, int64(len(other)), d)
	require.NoError(t, err)
	require.Equal(t, 2, source.reads, "changed footer must force index parsing")
	_, _, err = index.ReadFrameAt(t.Context(), 0, false, newPool())
	require.ErrorIs(t, err, zstdframes.ErrCorruptFrame)
}

func TestIndexCacheAccessUpdatesRecency(t *testing.T) {
	a, da := encode(t, bytes.Repeat([]byte("a"), 5000), 4096, newPool())
	b, db := encode(t, bytes.Repeat([]byte("b"), 5000), 4096, newPool())
	c, dc := encode(t, bytes.Repeat([]byte("c"), 5000), 4096, newPool())
	cache := zstdframes.NewIndexCache(5000)
	open := func(data []byte, d digest.Digest) int {
		r := &indexReadCounter{Reader: bytes.NewReader(data)}
		_, err := cache.Open(r, int64(len(data)), d)
		require.NoError(t, err)
		return r.reads
	}
	require.Equal(t, 2, open(a, da))
	require.Equal(t, 2, open(b, db))
	require.Equal(t, 1, open(a, da)) // Touch A, making B oldest.
	require.Equal(t, 2, open(c, dc))
	require.Equal(t, 1, open(a, da), "recently accessed A must survive")
	require.Equal(t, 2, open(b, db), "B must have been evicted")
}
