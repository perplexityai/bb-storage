package zstdframes_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countingPool struct {
	bb_zstd.Pool
	encoders atomic.Int64
	decoders atomic.Int64
}

func (p *countingPool) NewEncoder(ctx context.Context, w io.Writer) (bb_zstd.Encoder, error) {
	p.encoders.Add(1)
	return p.Pool.NewEncoder(ctx, w)
}

func (p *countingPool) NewDecoder(ctx context.Context, r io.Reader) (bb_zstd.Decoder, error) {
	p.decoders.Add(1)
	return p.Pool.NewDecoder(ctx, r)
}

func newPool() *countingPool {
	return &countingPool{Pool: bb_zstd.NewBoundedPool(
		1, 1,
		[]zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(1 << 20)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(4 << 20)},
	)}
}

func blobDigest(data []byte, function remoteexecution.DigestFunction_Value) digest.Digest {
	g := digest.MustNewFunction("test", function).NewGenerator(int64(len(data)))
	g.Write(data)
	return g.Sum()
}

func encode(t *testing.T, data []byte, frameSize int, pool bb_zstd.Pool) ([]byte, digest.Digest) {
	t.Helper()
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	var b bytes.Buffer
	require.NoError(t, zstdframes.Encode(context.Background(), &b, d, bytes.NewReader(data), frameSize, pool))
	return b.Bytes(), d
}

func decode(t *testing.T, data []byte) []byte {
	t.Helper()
	r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	require.NoError(t, err)
	defer r.Close()
	decoded, err := r.DecodeAll(data, nil)
	require.NoError(t, err)
	return decoded
}

func TestDiskRoundTripAndParallelFrames(t *testing.T) {
	ctx := context.Background()
	const frameSize = 64 << 10
	data := bytes.Repeat([]byte("independent compressed frames\n"), 12000)
	pool := newPool()
	encoded, d := encode(t, data, frameSize, pool)
	require.Less(t, len(encoded), len(data)/10)
	path := filepath.Join(t.TempDir(), "blob")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	r, err := zstdframes.Open(f, int64(len(encoded)), d)
	require.NoError(t, err)
	encoders, decoders := pool.encoders.Load(), pool.decoders.Load()

	// Read compressed bytes from a reopened file; no encoder or decoder is
	// available to the serving path. A normal decoder accepts their concatenation.
	var compressed bytes.Buffer
	require.NoError(t, r.WriteCompressed(ctx, &compressed))
	require.Equal(t, data, decode(t, compressed.Bytes()))
	var again bytes.Buffer
	require.NoError(t, r.WriteCompressed(ctx, &again))
	require.Equal(t, compressed.Bytes(), again.Bytes())
	require.Equal(t, encoders, pool.encoders.Load())
	require.Equal(t, decoders, pool.decoders.Load())

	digests := r.FrameDigests()
	parts := make([][]byte, len(digests))
	errs := make([]error, len(digests))
	var wg sync.WaitGroup
	for i := range parts {
		wg.Go(func() { parts[i], errs[i] = r.ReadCompressedFrame(ctx, i) })
	}
	wg.Wait()
	var reconstructed []byte
	for i, p := range parts {
		require.NoError(t, errs[i])
		raw := decode(t, p)
		require.Equal(t, digests[i], blobDigest(raw, remoteexecution.DigestFunction_SHA256))
		reconstructed = append(reconstructed, raw...)
	}
	require.Equal(t, d, blobDigest(reconstructed, remoteexecution.DigestFunction_SHA256))

	for _, off := range []int64{0, 1, frameSize, frameSize + 17, int64(len(data)) - 1, int64(len(data))} {
		before := pool.decoders.Load()
		var raw bytes.Buffer
		require.NoError(t, r.WriteIdentity(ctx, &raw, off, pool))
		require.True(t, bytes.Equal(data[off:], raw.Bytes()))
		want := int64(len(digests)) - off/frameSize
		if off == int64(len(data)) {
			want = 0
		}
		require.Equal(t, want, pool.decoders.Load()-before)
	}
}

func TestIncompressibleAndEmptyBlobs(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	random := make([]byte, 3*(64<<10)+7)
	for i := range random {
		random[i] = byte(rng.Uint32())
	}
	for _, data := range [][]byte{nil, {42}, random} {
		for _, function := range []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256, remoteexecution.DigestFunction_GITSHA1} {
			pool := newPool()
			d := blobDigest(data, function)
			var stored bytes.Buffer
			require.NoError(t, zstdframes.Encode(context.Background(), &stored, d, bytes.NewReader(data), 64<<10, pool))
			r, err := zstdframes.Open(bytes.NewReader(stored.Bytes()), int64(stored.Len()), d)
			require.NoError(t, err)
			var compressed, identity bytes.Buffer
			require.NoError(t, r.WriteCompressed(context.Background(), &compressed))
			require.Equal(t, data, decode(t, compressed.Bytes()))
			require.NoError(t, r.WriteIdentity(context.Background(), &identity, 0, pool))
			require.Equal(t, data, identity.Bytes())
		}
	}
}

type failWriter struct{ err error }

func (w failWriter) Write([]byte) (int, error) { return 0, w.err }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestFailedUploadsNeverProduceReadableRepresentations(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 10000)
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	errSource := errors.New("source failed after payload")
	for name, src := range map[string]io.Reader{
		"wrong hash":     bytes.NewReader(bytes.Repeat([]byte("y"), len(data))),
		"truncated":      bytes.NewReader(data[:len(data)-1]),
		"oversized":      io.MultiReader(bytes.NewReader(data), bytes.NewReader([]byte{0})),
		"deferred error": io.MultiReader(bytes.NewReader(data), errorReader{errSource}),
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			err := zstdframes.Encode(context.Background(), &out, d, src, 4096, newPool())
			require.Error(t, err)
			if name == "deferred error" {
				require.ErrorIs(t, err, errSource)
			}
			_, err = zstdframes.Open(bytes.NewReader(out.Bytes()), int64(out.Len()), d)
			require.Error(t, err)
		})
	}
	errDisk := errors.New("disk full")
	require.ErrorIs(t, zstdframes.Encode(context.Background(), failWriter{errDisk}, d, bytes.NewReader(data), 4096, newPool()), errDisk)
	require.ErrorIs(t, zstdframes.Encode(context.Background(), shortWriter{}, d, bytes.NewReader(data), 4096, newPool()), io.ErrShortWrite)
}

func TestCorruptionAndCancellation(t *testing.T) {
	pool := newPool()
	data := bytes.Repeat([]byte("frame"), 3000)
	encoded, d := encode(t, data, 4096, pool)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	require.Equal(t, codes.Canceled, status.Code(zstdframes.Encode(ctx, &out, d, bytes.NewReader(data), 4096, pool)))
	require.Zero(t, out.Len())
	r, err := zstdframes.Open(bytes.NewReader(encoded), int64(len(encoded)), d)
	require.NoError(t, err)
	require.Equal(t, codes.Canceled, status.Code(r.WriteCompressed(ctx, &out)))
	require.Equal(t, codes.Canceled, status.Code(r.WriteIdentity(ctx, &out, 0, pool)))
	require.Zero(t, out.Len())

	corrupt := bytes.Clone(encoded)
	corrupt[0] ^= 1
	r, err = zstdframes.Open(bytes.NewReader(corrupt), int64(len(corrupt)), d)
	require.NoError(t, err)
	require.Equal(t, codes.DataLoss, status.Code(r.WriteCompressed(context.Background(), &out)))
	require.Zero(t, out.Len(), "a corrupt frame must not be partially sent")
	before := pool.decoders.Load()
	require.Equal(t, codes.DataLoss, status.Code(r.WriteIdentity(context.Background(), &out, 0, pool)))
	require.Equal(t, before, pool.decoders.Load())

	corrupt = bytes.Clone(encoded)
	corrupt[len(corrupt)-1] ^= 1
	_, err = zstdframes.Open(bytes.NewReader(corrupt), int64(len(corrupt)), d)
	require.Equal(t, codes.DataLoss, status.Code(err))
	_, err = zstdframes.Open(bytes.NewReader(encoded), int64(len(encoded)), blobDigest([]byte("other blob"), remoteexecution.DigestFunction_SHA256))
	require.Equal(t, codes.DataLoss, status.Code(err))
	r, err = zstdframes.Open(bytes.NewReader(encoded), int64(len(encoded)), d)
	require.NoError(t, err)
	errNetwork := errors.New("client disconnected")
	require.ErrorIs(t, r.WriteCompressed(context.Background(), failWriter{errNetwork}), errNetwork)
	require.ErrorIs(t, r.WriteIdentity(context.Background(), failWriter{errNetwork}, 0, pool), errNetwork)
	require.ErrorIs(t, r.WriteCompressed(context.Background(), shortWriter{}), io.ErrShortWrite)
	for _, off := range []int64{-1, int64(len(data)) + 1} {
		require.Equal(t, codes.InvalidArgument, status.Code(r.WriteIdentity(context.Background(), &out, off, pool)))
	}
	for _, i := range []int{-1, len(r.FrameDigests())} {
		_, err := r.ReadCompressedFrame(context.Background(), i)
		require.Equal(t, codes.OutOfRange, status.Code(err))
	}
}

// Recompute the storage checksum to exercise structural validation separately
// from bit-rot detection. This does not model an authenticated upload path.
func rewriteIndex(t *testing.T, encoded []byte, modify func(map[string]any)) []byte {
	t.Helper()
	footer := bytes.Clone(encoded[len(encoded)-48:])
	length := int(binary.LittleEndian.Uint64(footer[8:16]))
	start := len(encoded) - 48 - length
	var idx map[string]any
	require.NoError(t, json.Unmarshal(encoded[start:start+length], &idx))
	modify(idx)
	metadata, err := json.Marshal(idx)
	require.NoError(t, err)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(metadata)))
	h := sha256.Sum256(metadata)
	copy(footer[16:], h[:])
	return append(append(bytes.Clone(encoded[:start]), metadata...), footer...)
}

func TestIndexRejectsUnsafeRanges(t *testing.T) {
	encoded, d := encode(t, bytes.Repeat([]byte("x"), 10000), 4096, newPool())
	for name, modify := range map[string]func(map[string]any){
		"oversized frame":        func(m map[string]any) { m["frameSize"] = float64(1 << 30) },
		"missing frame":          func(m map[string]any) { m["frames"] = m["frames"].([]any)[1:] },
		"overlap":                func(m map[string]any) { m["frames"].([]any)[1].(map[string]any)["offset"] = float64(0) },
		"negative encoded size":  func(m map[string]any) { m["frames"].([]any)[0].(map[string]any)["encodedSize"] = float64(-1) },
		"oversized encoded size": func(m map[string]any) { m["frames"].([]any)[0].(map[string]any)["encodedSize"] = float64(1 << 40) },
		"wrong logical size":     func(m map[string]any) { m["frames"].([]any)[0].(map[string]any)["size"] = float64(1) },
		"invalid chunk hash":     func(m map[string]any) { m["frames"].([]any)[0].(map[string]any)["hash"] = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := rewriteIndex(t, encoded, modify)
			_, err := zstdframes.Open(bytes.NewReader(bad), int64(len(bad)), d)
			require.Equal(t, codes.DataLoss, status.Code(err))
		})
	}
	bad := bytes.Clone(encoded)
	binary.LittleEndian.PutUint64(bad[len(bad)-40:len(bad)-32], ^uint64(0))
	_, err := zstdframes.Open(bytes.NewReader(bad), int64(len(bad)), d)
	require.Equal(t, codes.DataLoss, status.Code(err))
}

func TestEncodingBudgetsAndIndexEntryLimit(t *testing.T) {
	pool := newPool()
	d := blobDigest(nil, remoteexecution.DigestFunction_SHA256)
	tooLarge, err := d.GetDigestFunction().NewDigest(d.GetHashString(), (4096<<16)+1)
	require.NoError(t, err)
	errSource := errors.New("input must not be read")
	for _, size := range []int{0, 4095, (4 << 20) + 1} {
		require.Equal(t, codes.InvalidArgument, status.Code(zstdframes.Encode(context.Background(), io.Discard, d, errorReader{errSource}, size, pool)))
	}
	require.Equal(t, codes.ResourceExhausted, status.Code(zstdframes.Encode(context.Background(), io.Discard, tooLarge, errorReader{errSource}, 4096, pool)))
	require.Zero(t, pool.encoders.Load())
	encoded, d := encode(t, []byte("small"), 4096, pool)
	bad := rewriteIndex(t, encoded, func(m map[string]any) {
		frames := make([]any, (1<<16)+1)
		for i := range frames {
			frames[i] = map[string]any{}
		}
		m["frames"] = frames
	})
	_, err = zstdframes.Open(bytes.NewReader(bad), int64(len(bad)), d)
	require.Equal(t, codes.DataLoss, status.Code(err))
}

func FuzzOpen(f *testing.F) {
	data := []byte("frame index fuzz seed")
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	var encoded bytes.Buffer
	if err := zstdframes.Encode(context.Background(), &encoded, d, bytes.NewReader(data), 4096, newPool()); err != nil {
		f.Fatal(err)
	}
	f.Add(encoded.Bytes())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		r, err := zstdframes.Open(bytes.NewReader(data), int64(len(data)), d)
		if err == nil {
			// A parser success must never turn into a panic or unbounded range
			// when consuming the representation.
			_ = r.WriteCompressed(context.Background(), io.Discard)
			_ = r.WriteIdentity(context.Background(), io.Discard, 0, newPool())
		}
	})
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

type closeErrorEncoder struct {
	bb_zstd.Encoder
	err error
}

func (e closeErrorEncoder) Close() error {
	if err := e.Encoder.Close(); err != nil {
		return err
	}
	return e.err
}

type closeErrorPool struct {
	bb_zstd.Pool
	err error
}

func (p closeErrorPool) NewEncoder(ctx context.Context, w io.Writer) (bb_zstd.Encoder, error) {
	e, err := p.Pool.NewEncoder(ctx, w)
	if err != nil {
		return nil, err
	}
	return closeErrorEncoder{e, p.err}, nil
}

func TestCodecReleaseAndMidstreamCancellation(t *testing.T) {
	data := bytes.Repeat([]byte("frame"), 3000)
	pool := newPool()
	encoded, d := encode(t, data, 4096, pool)
	r, err := zstdframes.Open(bytes.NewReader(encoded), int64(len(encoded)), d)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	part, err := r.ReadCompressedFrame(ctx, 0)
	require.NoError(t, err)
	var result bytes.Buffer
	err = r.WriteIdentity(ctx, writerFunc(func(p []byte) (int, error) {
		// The one-slot pool must already be available when a consumer writes.
		decoder, err := pool.NewDecoder(ctx, bytes.NewReader(part))
		if err != nil {
			return 0, err
		}
		decoder.Close()
		return result.Write(p)
	}), 0, pool)
	require.NoError(t, err)
	require.Equal(t, data, result.Bytes())

	ctx2, cancel2 := context.WithCancel(context.Background())
	writes := 0
	err = r.WriteCompressed(ctx2, writerFunc(func(p []byte) (int, error) {
		writes++
		cancel2()
		return len(p), nil
	}))
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Equal(t, 1, writes)
	flushError := errors.New("encoder final flush failed")
	var out bytes.Buffer
	require.ErrorIs(t, zstdframes.Encode(context.Background(), &out, d, bytes.NewReader(data), 4096, closeErrorPool{pool, flushError}), flushError)
	require.Zero(t, out.Len())
	// Failed closure also returns the encoder to the bounded pool.
	require.NoError(t, zstdframes.Encode(ctx, &out, d, bytes.NewReader(data), 4096, pool))
}

func BenchmarkRepresentationReads(b *testing.B) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(3, 4))
	block := make([]byte, 64<<10)
	for i := range block {
		block[i] = byte(rng.Uint32())
	}
	data := bytes.Repeat(block, 512)
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	pool := newPool()
	var stored bytes.Buffer
	if err := zstdframes.Encode(ctx, &stored, d, bytes.NewReader(data), 1<<20, pool); err != nil {
		b.Fatal(err)
	}
	r, err := zstdframes.Open(bytes.NewReader(stored.Bytes()), int64(stored.Len()), d)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("compressed", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			if err := r.WriteCompressed(ctx, io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("identity", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			if err := r.WriteIdentity(ctx, io.Discard, 0, pool); err != nil {
				b.Fatal(err)
			}
		}
	})
}
