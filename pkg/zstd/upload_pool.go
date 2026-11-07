package zstd

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/zstd"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewUploadPoolFromConfiguration creates codecs independent of read-side pools.
// Zero settings use bounded defaults; slow receivers may hold only these codecs.
func NewUploadPoolFromConfiguration(config *configuration.PoolConfiguration, maximumWrites int64) (Pool, error) {
	encoders, decoders := config.GetMaximumEncoders(), config.GetMaximumDecoders()
	window, decodeWindow := config.GetEncoderWindowSizeBytes(), config.GetDecoderWindowSizeBytes()
	level := config.GetEncoderLevel()
	if encoders == 0 {
		encoders = maximumWrites
	}
	if decoders == 0 {
		decoders = maximumWrites
	}
	if window == 0 {
		window = 1 << 20
	}
	if decodeWindow == 0 {
		decodeWindow = 8 << 20
	}
	if level == 0 {
		level = int32(zstd.SpeedFastest)
	}
	if maximumWrites <= 0 || encoders <= 0 || decoders <= 0 || encoders > maximumWrites || decoders > maximumWrites || level < int32(zstd.SpeedFastest) || level > int32(zstd.SpeedBestCompression) {
		return nil, status.Error(codes.InvalidArgument, "Upload codec counts must be positive and no greater than concurrent writes; encoder level must be between 1 and 4")
	}
	encoderOptions := []zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.EncoderLevel(level)), zstd.WithWindowSize(int(window))}
	decoderOptions := []zstd.DOption{zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(uint64(decodeWindow))}
	// Validate options before opening the storage device; pool construction is lazy.
	encoder, err := zstd.NewWriter(nil, encoderOptions...)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid upload encoder options: %v", err)
	}
	encoder.Close()
	if decodeWindow < max(1024, min(window, 1<<20)) {
		return nil, status.Error(codes.InvalidArgument, "Upload decoder window must cover stored frame windows")
	}
	decoder, err := zstd.NewReader(nil, decoderOptions...)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid upload decoder options: %v", err)
	}
	decoder.Close()
	pool := NewBoundedPool(encoders, decoders, encoderOptions, decoderOptions).(*boundedPool)
	pool.acquisitionTimeout = 30 * time.Second
	return NewMetricsPool(pool, clock.SystemClock, "ZstdUploadPool"), nil
}
