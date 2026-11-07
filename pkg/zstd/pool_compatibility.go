package zstd

import (
	"bytes"
	"context"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidatePoolCompatibility checks actual frame settings, including custom
// pools, before opening a storage device. Encoding releases its lease before
// decoding. A bounded context prevents occupied pools from hanging startup.
func ValidatePoolCompatibility(encoderPool, decoderPool Pool, frameSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	encoded, err := EncodeFrame(ctx, encoderPool, make([]byte, frameSize))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Cannot validate storage encoder: %v", err)
	}
	decoder, err := decoderPool.NewDecoder(ctx, bytes.NewReader(encoded))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Cannot validate storage decoder: %v", err)
	}
	defer decoder.Close()
	n, err := io.Copy(io.Discard, decoder)
	if err != nil || n != int64(frameSize) {
		return status.Errorf(codes.InvalidArgument, "Read decoder cannot decode upload storage frames: %v (decoded %d of %d bytes)", err, n, frameSize)
	}
	return nil
}
