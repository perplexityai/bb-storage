package zstd_test

import (
	"bytes"
	"io"
	"math"
	"testing"

	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUploadLimitDistinguishesEOFAndOverflow(t *testing.T) {
	limit := bb_zstd.MaximumUploadSize(10)
	for _, extra := range []int64{0, 1} {
		source := io.NopCloser(bytes.NewReader(make([]byte, limit+extra)))
		r := bb_zstd.LimitUpload(source, 10)
		_, err := io.Copy(io.Discard, r)
		require.NoError(t, r.Close())
		if extra == 0 {
			require.NoError(t, err)
		} else {
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		}
	}
	require.EqualValues(t, math.MaxInt64, bb_zstd.MaximumUploadSize(math.MaxInt64))
}

type limitedSource struct {
	err        error
	closes     int
	interrupts int
}

func (s *limitedSource) Read([]byte) (int, error) { return 0, s.err }
func (s *limitedSource) Close() error             { s.closes++; return s.err }
func (s *limitedSource) ReadInterrupt() func()    { return func() { s.interrupts++ } }

func TestUploadLimitPreservesCompletionAndInterruption(t *testing.T) {
	expected := status.Error(codes.Unavailable, "source failed")
	source := &limitedSource{err: expected}
	r := bb_zstd.LimitUpload(source, 0)
	_, err := io.ReadAll(r)
	require.ErrorIs(t, err, expected)
	interrupt := r.(interface{ ReadInterrupt() func() }).ReadInterrupt()
	require.NotNil(t, interrupt)
	interrupt()
	require.Equal(t, 1, source.interrupts)
	require.ErrorIs(t, r.Close(), expected)
	require.Equal(t, 1, source.closes)
}
