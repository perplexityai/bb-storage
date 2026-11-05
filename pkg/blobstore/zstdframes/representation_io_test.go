package zstdframes_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type payloadErrorReader struct {
	data []byte
	err  error
}

func (r *payloadErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

func TestSourceErrorsReturnedWithFinalBytes(t *testing.T) {
	errSource := errors.New("source checksum failure")
	for _, size := range []int{17, 4096, 4097, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), size)
			d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
			for _, terminal := range []error{errSource, io.EOF} {
				var out bytes.Buffer
				err := zstdframes.Encode(context.Background(), &out, d, &payloadErrorReader{data, terminal}, 4096, newPool())
				if terminal == io.EOF {
					require.NoError(t, err)
					_, err = zstdframes.Open(bytes.NewReader(out.Bytes()), int64(out.Len()), d)
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, errSource)
					_, err = zstdframes.Open(bytes.NewReader(out.Bytes()), int64(out.Len()), d)
					require.Error(t, err)
				}
			}
		})
	}
}

func TestEmptyBlobHasACompleteZstandardFrame(t *testing.T) {
	encoded, d := encode(t, nil, 4096, newPool())
	r, err := zstdframes.Open(bytes.NewReader(encoded), int64(len(encoded)), d)
	require.NoError(t, err)
	var compressed bytes.Buffer
	require.NoError(t, r.WriteCompressed(context.Background(), &compressed))
	var header zstd.Header
	require.NoError(t, header.Decode(compressed.Bytes()), "empty input to DecodeAll is not evidence of a valid frame")
	require.False(t, header.Skippable)
	require.True(t, header.HasFCS)
	require.Zero(t, header.FrameContentSize)
	require.Empty(t, decode(t, compressed.Bytes()))
	mapping := r.FrameDigests()
	require.Len(t, mapping, 1)
	require.Equal(t, d, mapping[0])
	frame, err := r.ReadCompressedFrame(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, compressed.Bytes(), frame)
}

type readerAtFunc func([]byte, int64) (int, error)

func (f readerAtFunc) ReadAt(p []byte, offset int64) (int, error) { return f(p, offset) }

func TestReaderAtTerminalErrors(t *testing.T) {
	data := bytes.Repeat([]byte("input"), 1000)
	encoded, d := encode(t, data, 4096, newPool())
	source := bytes.NewReader(encoded)
	fullEOF := readerAtFunc(func(p []byte, offset int64) (int, error) {
		n, err := source.ReadAt(p, offset)
		if n == len(p) {
			return n, io.EOF
		}
		return n, err
	})
	r, err := zstdframes.Open(fullEOF, int64(len(encoded)), d)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, r.WriteCompressed(context.Background(), &out))
	require.Equal(t, data, decode(t, out.Bytes()))

	errStorage := errors.New("storage read failed")
	_, err = zstdframes.Open(readerAtFunc(func(p []byte, offset int64) (int, error) {
		n, _ := source.ReadAt(p, offset)
		return n, errStorage
	}), int64(len(encoded)), d)
	require.ErrorIs(t, err, errStorage)
	_, err = zstdframes.Open(readerAtFunc(func(p []byte, offset int64) (int, error) {
		n, _ := source.ReadAt(p[:len(p)-1], offset)
		return n, io.EOF
	}), int64(len(encoded)), d)
	require.Error(t, err)
}

func TestEncodeCancellationDuringCommit(t *testing.T) {
	data := []byte("one frame")
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	for _, phase := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var out bytes.Buffer
			writes := 0
			err := zstdframes.Encode(ctx, writerFunc(func(p []byte) (int, error) {
				writes++
				n, err := out.Write(p)
				if writes == phase {
					cancel()
				}
				return n, err
			}), d, bytes.NewReader(data), 4096, newPool())
			require.Equal(t, codes.Canceled, status.Code(err))
			require.Equal(t, phase, writes, "no writes may start after cancellation is observed")
			// A footer write may have completed before cancellation was observed.
			// Publication must always be conditional on Encode succeeding.
			if phase < 3 {
				_, err = zstdframes.Open(bytes.NewReader(out.Bytes()), int64(out.Len()), d)
				require.Error(t, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errDisk := errors.New("destination failed during cancellation")
	err := zstdframes.Encode(ctx, writerFunc(func(p []byte) (int, error) {
		cancel()
		return len(p), errDisk
	}), d, bytes.NewReader(data), 4096, newPool())
	require.ErrorIs(t, err, errDisk)
}

func TestShortInputHasNonRetryableStatus(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 4097)
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	for _, size := range []int{0, 17, 4096} {
		var out bytes.Buffer
		err := zstdframes.Encode(t.Context(), &out, d, bytes.NewReader(data[:size]), 4096, newPool())
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

type injectedDecoder struct{ io.Reader }

func (injectedDecoder) Close() {}

type decoderResultPool struct {
	bb_zstd.Pool
	reader io.Reader
}

func (p decoderResultPool) NewDecoder(context.Context, io.Reader) (bb_zstd.Decoder, error) {
	return injectedDecoder{p.reader}, nil
}

func TestFrameDecodeLengthAndTerminalErrors(t *testing.T) {
	data := bytes.Repeat([]byte("d"), 4096)
	record, d := encode(t, data, 4096, newPool())
	index, err := zstdframes.Open(bytes.NewReader(record), int64(len(record)), d)
	require.NoError(t, err)
	for _, tc := range []struct {
		name                    string
		data                    []byte
		terminal, errorExpected error
	}{
		{"exact", data, io.EOF, nil},
		{"short", data[:4095], io.EOF, zstdframes.ErrCorruptFrame},
		{"long", append(bytes.Clone(data), 'd'), io.EOF, zstdframes.ErrCorruptFrame},
		{"truncated-compressed", data, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF},
		{"full-buffer-terminal", append(bytes.Clone(data), 'd'), io.ErrUnexpectedEOF, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := decoderResultPool{Pool: newPool(), reader: &payloadErrorReader{data: tc.data, err: tc.terminal}}
			got, _, err := index.ReadFrameAt(t.Context(), 0, false, p)
			require.ErrorIs(t, err, tc.errorExpected)
			if err == nil {
				require.Equal(t, data, got)
			}
		})
	}
}

func BenchmarkReadOneFrame(b *testing.B) {
	data := bytes.Repeat([]byte("d"), 1<<20)
	d := blobDigest(data, remoteexecution.DigestFunction_SHA256)
	p := newPool()
	var out bytes.Buffer
	if err := zstdframes.Encode(b.Context(), &out, d, bytes.NewReader(data), 1<<20, p); err != nil {
		b.Fatal(err)
	}
	index, err := zstdframes.Open(bytes.NewReader(out.Bytes()), int64(out.Len()), d)
	if err != nil {
		b.Fatal(err)
	}
	if _, _, err := index.ReadFrameAt(b.Context(), 0, false, p); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := index.ReadFrameAt(b.Context(), 0, false, p); err != nil {
			b.Fatal(err)
		}
	}
}
