package grpcclients

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type casBlobAccess struct {
	byteStreamClient                bytestream.ByteStreamClient
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	uuidGenerator                   util.UUIDGenerator
	readChunkSize                   int
	supportedCompressors            atomic.Pointer[[]remoteexecution.Compressor_Value]
	zstdPool                        bb_zstd.Pool
}

// NewCASBlobAccess creates a BlobAccess handle that relays any requests
// to a gRPC service that implements the bytestream.ByteStream and
// remoteexecution.ContentAddressableStorage services. Those are the
// services that Bazel uses to access blobs stored in the Content
// Addressable Storage.
//
// If zstdPool is non-nil, the client will use ZSTD compression for
// ByteStream operations if the server supports it.
func NewCASBlobAccess(client grpc.ClientConnInterface, uuidGenerator util.UUIDGenerator, readChunkSize int, zstdPool bb_zstd.Pool) blobstore.BlobAccess {
	return &casBlobAccess{
		byteStreamClient:                bytestream.NewByteStreamClient(client),
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		uuidGenerator:                   uuidGenerator,
		readChunkSize:                   readChunkSize,
		zstdPool:                        zstdPool,
	}
}

type byteStreamChunkReader struct {
	client bytestream.ByteStream_ReadClient
	cancel context.CancelFunc
}

func (r *byteStreamChunkReader) Read() ([]byte, error) {
	chunk, err := r.client.Recv()
	if err != nil {
		return nil, err
	}
	return chunk.Data, nil
}

func (r *byteStreamChunkReader) Close() {
	r.cancel()
	for {
		if _, err := r.client.Recv(); err != nil {
			break
		}
	}
}

type byteStreamWriter struct {
	client       bytestream.ByteStream_WriteClient
	resourceName string
	writeOffset  int64
	sendError    error
}

func (w *byteStreamWriter) Write(p []byte) (int, error) {
	if err := w.client.Send(&bytestream.WriteRequest{
		ResourceName: w.resourceName,
		WriteOffset:  w.writeOffset,
		Data:         p,
	}); err != nil {
		w.sendError = err
		return 0, err
	}
	w.writeOffset += int64(len(p))
	w.resourceName = ""
	return len(p), nil
}

const resourceNameHeader = "build.bazel.remote.execution.v2.resource-name"

// shouldUseZSTDCompression checks if ZSTD compression should be used.
// It ensures GetCapabilities has been called to negotiate compression support.
func (ba *casBlobAccess) shouldUseZSTDCompression(ctx context.Context, digest digest.Digest) (bool, error) {
	if ba.zstdPool == nil {
		return false, nil
	}

	supportedCompressors := ba.supportedCompressors.Load()
	if supportedCompressors == nil {
		// Call GetCapabilities to check server support.
		if _, err := ba.GetCapabilities(ctx, digest.GetDigestFunction().GetInstanceName()); err != nil {
			return false, err
		}
		supportedCompressors = ba.supportedCompressors.Load()
	}

	return slices.Contains(*supportedCompressors, remoteexecution.Compressor_ZSTD), nil
}

func (ba *casBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	if useCompression {
		return buffer.NewCASBufferFromZSTDReadFactory(ctx, digest, func(offset int64) (io.ReadCloser, error) {
			ctxWithCancel, cancel := context.WithCancel(ctx)
			resourceName := digest.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD)
			client, err := ba.byteStreamClient.Read(metadata.AppendToOutgoingContext(ctxWithCancel, resourceNameHeader, resourceName),
				&bytestream.ReadRequest{ResourceName: resourceName, ReadOffset: offset})
			if err != nil {
				cancel()
				return nil, err
			}
			return buffer.NewReaderFromChunkReader(&byteStreamChunkReader{client: client, cancel: cancel}), nil
		}, ba.zstdPool, buffer.BackendProvided(buffer.Irreparable(digest)))
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	resourceName := digest.GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY)
	client, err := ba.byteStreamClient.Read(
		metadata.AppendToOutgoingContext(ctxWithCancel, resourceNameHeader, resourceName),
		&bytestream.ReadRequest{
			ResourceName: resourceName,
		},
	)
	if err != nil {
		cancel()
		return buffer.NewBufferFromError(err)
	}

	return buffer.NewCASBufferFromChunkReader(digest, &byteStreamChunkReader{
		client: client,
		cancel: cancel,
	}, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *casBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *casBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		b.Discard()
		return err
	}

	compressor := remoteexecution.Compressor_IDENTITY
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	resourceName := digest.GetByteStreamWritePath(uuid.Must(ba.uuidGenerator()), compressor)
	client, err := ba.byteStreamClient.Write(
		metadata.AppendToOutgoingContext(ctxWithCancel, resourceNameHeader, resourceName),
	)
	if err != nil {
		cancel()
		b.Discard()
		return err
	}

	defer cancel()
	copyCtx, stopCopy := context.WithCancelCause(ctx)
	defer stopCopy(nil)
	w := &byteStreamWriter{client: client, resourceName: resourceName}
	var result chan error
	if interrupt := buffer.UploadReadInterrupt(b); interrupt != nil {
		result = make(chan error, 1)
		go func() {
			var response bytestream.WriteResponse
			err := client.RecvMsg(&response)
			stopCopy(buffer.ErrUploadInterrupted)
			interrupt()
			result <- err
		}()
		// Let admission checks and duplicate detection run even if the source
		// has not supplied bytes yet. Only one goroutine sends on this RPC.
		w.Write(nil)
	}
	receive := func() error {
		if result != nil {
			client.CloseSend()
			return <-result
		}
		_, err := client.CloseAndRecv()
		return err
	}
	b = buffer.WithZSTDContext(b, copyCtx)
	var r io.ReadCloser
	var encoded bool
	if useCompression {
		r, encoded = buffer.ZSTDUploadReader(b)
	}
	if !encoded {
		r = b.ToReader()
	}
	copyErr := w.sendError
	if copyErr == nil {
		if useCompression && !encoded {
			copyErr = bb_zstd.Copy(copyCtx, ba.zstdPool, w, r)
		} else {
			// Hide WriterTo so every transport message respects readChunkSize.
			_, copyErr = io.CopyBuffer(w, struct{ io.Reader }{r}, make([]byte, ba.readChunkSize))
		}
	}
	closeErr := r.Close()
	interrupted := errors.Is(copyErr, buffer.ErrUploadInterrupted)
	if w.sendError != nil || interrupted {
		remoteErr := receive()
		if closeErr != nil {
			return closeErr
		}
		return remoteErr
	}
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		// Closing only the send side cannot release a backend codec wait.
		cancel()
		receive()
		return copyErr
	}
	client.Send(&bytestream.WriteRequest{ResourceName: w.resourceName, WriteOffset: w.writeOffset, FinishWrite: true})
	return receive()
}

func (ba *casBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return findMissingBlobsInternal(ctx, digests, ba.contentAddressableStorageClient)
}

func findMissingBlobsInternal(ctx context.Context, digests digest.Set, cas remoteexecution.ContentAddressableStorageClient) (digest.Set, error) {
	// Partition all digests by digest function, as the
	// FindMissingBlobs() RPC can only process digests for a single
	// instance name and digest function.
	perFunctionDigests := map[digest.Function][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		digestFunction := digest.GetDigestFunction()
		perFunctionDigests[digestFunction] = append(perFunctionDigests[digestFunction], digest.GetProto())
	}

	missingDigests := digest.NewSetBuilder(0)
	for digestFunction, blobDigests := range perFunctionDigests {
		// Call FindMissingBlobs() for each digest function.
		request := remoteexecution.FindMissingBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			BlobDigests:    blobDigests,
			DigestFunction: digestFunction.GetEnumValue(),
		}
		response, err := cas.FindMissingBlobs(ctx, &request)
		if err != nil {
			return digest.EmptySet, err
		}

		// Convert results back.
		for _, proto := range response.MissingBlobDigests {
			blobDigest, err := digestFunction.NewDigestFromProto(proto)
			if err != nil {
				return digest.EmptySet, err
			}
			missingDigests.Add(blobDigest)
		}
	}
	return missingDigests.Build(), nil
}

func (ba *casBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := getServerCapabilitiesWithCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}

	cacheCapabilities := serverCapabilities.CacheCapabilities

	// Store supported compressors for compression negotiation.
	ba.supportedCompressors.Store(&cacheCapabilities.SupportedCompressors)

	// Only return fields that pertain to the Content Addressable
	// Storage. Don't set 'max_batch_total_size_bytes', as we don't
	// issue batch operations.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions:     digest.RemoveUnsupportedDigestFunctions(cacheCapabilities.DigestFunctions),
			MaxCasBlobSizeBytes: cacheCapabilities.MaxCasBlobSizeBytes,
		},
		DeprecatedApiVersion: serverCapabilities.DeprecatedApiVersion,
		LowApiVersion:        serverCapabilities.LowApiVersion,
		HighApiVersion:       serverCapabilities.HighApiVersion,
	}, nil
}
