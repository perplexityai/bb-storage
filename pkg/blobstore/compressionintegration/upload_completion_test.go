package compressionintegration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/zstdframes"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackendCompletionInterruptsPausedUpload(t *testing.T) {
	for _, incoming := range []remoteexecution.Compressor_Value{remoteexecution.Compressor_IDENTITY, remoteexecution.Compressor_ZSTD} {
		for _, compressedHop := range []bool{false, true} {
			for _, reject := range []bool{false, true} {
				// Identity storage consumes the upload to preserve arbitrary reader
				// completion errors. Compressed duplicates can be accepted immediately.
				if !compressedHop && !reject {
					continue
				}
				for _, metadataOnly := range []bool{false, true} {
					t.Run(fmt.Sprintf("incoming=%s/hop=%t/reject=%t/metadataOnly=%t", incoming, compressedHop, reject, metadataOnly), func(t *testing.T) {
						storagePool, frontendPool := pool(), pool()
						store, _, _ := localStorage(t, storagePool)
						data := bytes.Repeat([]byte("paused upload"), 1000)
						d := digestOf(data)
						var limit int64
						if reject {
							limit = 1
						} else {
							require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
						}
						var hopPool bb_zstd.Pool
						if compressedHop {
							hopPool = frontendPool
						}
						remote := grpcclients.NewCASBlobAccess(serveWithLimit(t, store, storagePool, limit), uuid.NewRandom, 4096, hopPool)
						conn := serve(t, remote, frontendPool)
						// Early completion must also interrupt fallback waiting for a codec.
						encoder, err := frontendPool.NewEncoder(t.Context(), io.Discard)
						require.NoError(t, err)
						defer encoder.Close()
						decoder, err := frontendPool.NewDecoder(t.Context(), nil)
						require.NoError(t, err)
						defer decoder.Close()
						ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
						defer cancel()
						w, err := bytestream.NewByteStreamClient(conn).Write(ctx)
						require.NoError(t, err)
						var first []byte
						if !metadataOnly {
							first = data[:1]
						}
						require.NoError(t, w.Send(&bytestream.WriteRequest{ResourceName: d.GetByteStreamWritePath(uuid.New(), incoming), Data: first}))
						// Leave the send side open: success must not depend on FinishWrite,
						// CloseSend, or the client's deadline releasing a blocked Recv.
						var response bytestream.WriteResponse
						err = w.RecvMsg(&response)
						if reject {
							require.Equal(t, codes.InvalidArgument, status.Code(err))
						} else {
							require.NoError(t, err)
							if incoming == remoteexecution.Compressor_ZSTD {
								require.EqualValues(t, -1, response.CommittedSize)
							} else {
								require.Equal(t, d.GetSizeBytes(), response.CommittedSize)
							}
						}
						require.NoError(t, ctx.Err())
					})
				}
			}
		}
	}
}

func TestForwardedUploadsPreserveCompletionErrors(t *testing.T) {
	for _, compressedHop := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressedHop), func(t *testing.T) {
			storagePool, frontendPool := pool(), pool()
			store, _, _ := localStorage(t, storagePool)
			// Larger than gRPC's default message limit, to also check that readers
			// with WriterTo cannot bypass the client's transport chunk bound.
			data := bytes.Repeat([]byte("source completion"), 300000)
			d := digestOf(data)
			var hopPool bb_zstd.Pool
			if compressedHop {
				hopPool = frontendPool
			}
			remote := grpcclients.NewCASBlobAccess(serve(t, store, storagePool), uuid.NewRandom, 4096, hopPool)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			require.NoError(t, remote.Put(ctx, d, buffer.NewValidatedBufferFromByteSlice(data)))
			expected := status.Error(codes.Unavailable, "Source completion failed")
			source := buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.UserProvided).WithTask(func() error { return expected })
			require.Equal(t, expected, remote.Put(ctx, d, source))
			actual, err := store.Get(ctx, d).ToByteSlice(len(data))
			require.NoError(t, err)
			require.Equal(t, data, actual)
		})
	}
}

func TestDuplicatePreservesChunkAdapterCompletionFailure(t *testing.T) {
	store, _, _ := localStorage(t, pool())
	data := []byte("wrapped duplicate")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	expected := status.Error(codes.Unavailable, "Background completion failed")
	inner := buffer.NewCASBufferFromReader(d, io.NopCloser(bytes.NewReader(data)), buffer.UserProvided).WithTask(func() error { return expected })
	source := buffer.NewCASBufferFromReader(d, buffer.NewReaderFromChunkReader(inner.ToChunkReader(0, 4096)), buffer.UserProvided)
	require.Equal(t, expected, store.Put(t.Context(), d, source))
}

type failingIndexReader struct {
	*countedRecordReader
	err    error
	failAt int64
}

func (r *failingIndexReader) ReadAt(p []byte, off int64) (int, error) {
	if off == r.failAt && r.err != nil {
		err := r.err
		r.err = nil
		r.reads.Add(1)
		return 0, err
	}
	return r.countedRecordReader.ReadAt(p, off)
}

func TestIndexIOFailureDoesNotInvalidateOrFallBack(t *testing.T) {
	p := pool()
	data := bytes.Repeat([]byte("index read failure"), 1000)
	d := digestOf(data)
	var encoded bytes.Buffer
	require.NoError(t, zstdframes.Encode(t.Context(), &encoded, d, bytes.NewReader(data), 4096, p))
	for _, legacy := range []bool{false, true} {
		for _, code := range []codes.Code{codes.Unavailable, codes.Canceled, codes.DataLoss} {
			t.Run(fmt.Sprintf("legacy=%t/code=%s", legacy, code), func(t *testing.T) {
				record := encoded.Bytes()
				if legacy {
					record = data
				}
				expected := status.Error(code, "Storage read failed")
				r := &failingIndexReader{countedRecordReader: &countedRecordReader{Reader: bytes.NewReader(record)}, err: expected, failAt: int64(len(record) - 48)}
				callbacks := 0
				b := blobstore.NewCompressedCASReadBufferFactory(p).NewBufferFromReaderAt(d, r, int64(len(record)), func(bool) { callbacks++ })
				_, err := b.ToByteSlice(len(data))
				require.Equal(t, expected, err)
				require.Zero(t, callbacks, "a read failure does not establish corrupt bytes")
				require.EqualValues(t, 1, r.reads.Load(), "a source error must not be retried as a raw record")
				require.EqualValues(t, 1, r.closes.Load())
			})
		}
	}
}

type completionAnnotationHandler struct{ completionErrorHandler }

func (*completionAnnotationHandler) NonRetrying() {}

func TestAnnotatedCompressedReaderPreservesCompletionFailure(t *testing.T) {
	store, _, _ := localStorage(t, pool())
	data := []byte("annotated compressed completion")
	d := digestOf(data)
	require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
	expected := status.Error(codes.Unavailable, "Background completion failed")
	handler := &completionAnnotationHandler{}
	source := buffer.WithErrorHandler(store.Get(t.Context(), d).WithTask(func() error { return expected }), handler)
	chunks, ok := buffer.ZSTDChunkReader(source, 4096)
	require.True(t, ok)
	require.Equal(t, expected, buffer.NewReaderFromChunkReader(chunks).Close())
	require.True(t, handler.done)
}
