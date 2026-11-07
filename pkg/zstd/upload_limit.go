package zstd

import (
	"io"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MaximumUploadSize is an ingress policy, not a bound on all legal Zstandard
// streams: redundant empty and skippable frames may exceed it. It allows 12.5%
// expansion, 1 KiB per MiB of logical data, and 1 KiB of fixed framing overhead.
func MaximumUploadSize(logicalSize int64) int64 {
	if logicalSize < 0 || logicalSize > math.MaxInt64/2 {
		return math.MaxInt64
	}
	frames := max(int64(1), (logicalSize+(1<<20)-1)/(1<<20))
	return logicalSize + logicalSize/8 + frames*1024 + 1024
}

// LimitUpload bounds encoded ingress without decoding. The extra byte detects
// overflow instead of presenting a truncated stream as successful completion.
func LimitUpload(r io.ReadCloser, logicalSize int64) io.ReadCloser {
	return &limitedUpload{ReadCloser: r, remaining: MaximumUploadSize(logicalSize)}
}

type limitedUpload struct {
	io.ReadCloser
	remaining int64
}

func (r *limitedUpload) Read(p []byte) (int, error) {
	if r.remaining < 0 {
		return 0, status.Error(codes.InvalidArgument, "Encoded upload exceeds its byte budget")
	}
	if int64(len(p)) > r.remaining && r.remaining < math.MaxInt64 {
		p = p[:r.remaining+1]
	}
	n, err := r.ReadCloser.Read(p)
	r.remaining -= int64(n)
	if r.remaining < 0 {
		return 0, status.Error(codes.InvalidArgument, "Encoded upload exceeds its byte budget")
	}
	return n, err
}

func (r *limitedUpload) ReadInterrupt() func() {
	if source, ok := r.ReadCloser.(interface{ ReadInterrupt() func() }); ok {
		return source.ReadInterrupt()
	}
	return nil
}
