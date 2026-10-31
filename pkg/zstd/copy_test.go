package zstd_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

type retryingCodecReader struct {
	ctx   context.Context
	pool  bb_zstd.Pool
	data  *bytes.Reader
	reads int
}

func (r *retryingCodecReader) Read(p []byte) (int, error) {
	// Model source replacement: a competing operation needs the encoder while
	// it owns the decoder. No output encoder may remain held across this call.
	d, err := r.pool.NewDecoder(r.ctx, bytes.NewReader(nil))
	if err != nil {
		return 0, err
	}
	defer d.Close()
	e, err := r.pool.NewEncoder(r.ctx, io.Discard)
	if err != nil {
		return 0, err
	}
	if err = e.Close(); err != nil {
		return 0, err
	}
	r.reads++
	return r.data.Read(p)
}

func TestCopyReleasesEncoderBeforeFurtherReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	pool := bb_zstd.NewBoundedPool(1, 1, []zstd.EOption{zstd.WithEncoderConcurrency(1)}, []zstd.DOption{zstd.WithDecoderConcurrency(1)})
	data := bytes.Repeat([]byte("multiple independent frames"), 100000)
	source := &retryingCodecReader{ctx: ctx, pool: pool, data: bytes.NewReader(data)}
	var compressed bytes.Buffer
	require.NoError(t, bb_zstd.Copy(ctx, pool, &compressed, source))
	require.Greater(t, source.reads, 2)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	actual, err := decoder.DecodeAll(compressed.Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, data, actual)
	compressed.Reset()
	require.NoError(t, bb_zstd.Copy(ctx, pool, &compressed, bytes.NewReader(nil)))
	actual, err = decoder.DecodeAll(compressed.Bytes(), nil)
	require.NoError(t, err)
	require.Empty(t, actual)
}

type finalErrorReader struct{ err error }

func (r finalErrorReader) Read(p []byte) (int, error) { clear(p); return len(p), r.err }
func TestCopyPreservesFullFrameSourceError(t *testing.T) {
	failure := errors.New("source failed with final bytes")
	var out bytes.Buffer
	// No encoder is needed: invalid input must not reach it.
	require.ErrorIs(t, bb_zstd.Copy(t.Context(), nil, &out, finalErrorReader{failure}), failure)
	require.Empty(t, out.Bytes())
}
