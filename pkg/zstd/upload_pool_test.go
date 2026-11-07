package zstd_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/zstd"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUploadPoolIsIndependentAndBounded(t *testing.T) {
	first, err := bb_zstd.NewUploadPoolFromConfiguration(nil, 1)
	require.NoError(t, err)
	second, err := bb_zstd.NewUploadPoolFromConfiguration(nil, 1)
	require.NoError(t, err)
	held, err := first.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	defer held.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = first.NewDecoder(ctx, nil)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	other, err := second.NewDecoder(t.Context(), nil)
	require.NoError(t, err)
	other.Close()
	held.Close()
	data := bytes.Repeat([]byte("configured upload codecs"), 1000)
	encoded, err := bb_zstd.EncodeFrame(t.Context(), first, data)
	require.NoError(t, err)
	decoder, err := first.NewDecoder(t.Context(), bytes.NewReader(encoded))
	require.NoError(t, err)
	actual, err := io.ReadAll(decoder)
	decoder.Close()
	require.NoError(t, err)
	require.Equal(t, data, actual)
}

func TestUploadPoolRejectsInvalidConfiguration(t *testing.T) {
	for _, config := range []*configuration.PoolConfiguration{
		{MaximumDecoders: -1},
		{MaximumEncoders: -1},
		{MaximumDecoders: 3},
		{EncoderWindowSizeBytes: 3},
		{DecoderWindowSizeBytes: -1},
		{DecoderWindowSizeBytes: 1},
		{EncoderLevel: 5},
	} {
		_, err := bb_zstd.NewUploadPoolFromConfiguration(config, 2)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	_, err := bb_zstd.NewUploadPoolFromConfiguration(nil, 0)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestUploadFramingCompatibility(t *testing.T) {
	small, err := bb_zstd.NewUploadPoolFromConfiguration(&configuration.PoolConfiguration{EncoderWindowSizeBytes: 64 << 10, DecoderWindowSizeBytes: 64 << 10}, 1)
	require.NoError(t, err)
	require.NoError(t, bb_zstd.ValidatePoolCompatibility(small, small, 1<<20))
	defaults, err := bb_zstd.NewUploadPoolFromConfiguration(nil, 1)
	require.NoError(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(bb_zstd.ValidatePoolCompatibility(defaults, small, 1<<20)))
}
