package zstd

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodecAcquisitionTimeoutDoesNotLimitCodecLifetime(t *testing.T) {
	p := NewBoundedPool(1, 1, []zstd.EOption{zstd.WithEncoderConcurrency(1)}, []zstd.DOption{zstd.WithDecoderConcurrency(1)}).(*boundedPool)
	p.acquisitionTimeout = 10 * time.Millisecond
	encoder, err := p.NewEncoder(t.Context(), io.Discard)
	require.NoError(t, err)
	_, err = p.NewEncoder(t.Context(), io.Discard)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	var acquisition *AcquisitionError
	require.ErrorAs(t, err, &acquisition)
	require.NoError(t, encoder.Close())
	decoder, err := p.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	_, err = p.NewDecoder(t.Context(), nil)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = p.NewDecoder(ctx, nil)
	require.Equal(t, codes.Canceled, status.Code(err))
	decoder.Close()
	// Acquiring cancels its timer immediately; the actual codec must remain usable.
	var encoded bytes.Buffer
	encoder, err = p.NewEncoder(t.Context(), &encoded)
	require.NoError(t, err)
	_, err = encoder.Write([]byte("after acquisition"))
	require.NoError(t, err)
	require.NoError(t, encoder.Close())
	decoder, err = p.NewDecoder(t.Context(), bytes.NewReader(encoded.Bytes()))
	require.NoError(t, err)
	actual, err := io.ReadAll(decoder)
	decoder.Close()
	require.NoError(t, err)
	require.Equal(t, []byte("after acquisition"), actual)
}
