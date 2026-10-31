package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type casBlobSizeLimitingProvider struct {
	base             Provider
	maximumSizeBytes int64
}

// NewCASBlobSizeLimitingProvider advertises the smaller of a local upload
// limit and a backend's limit. Zero leaves the backend's capabilities unchanged.
func NewCASBlobSizeLimitingProvider(base Provider, maximumSizeBytes int64) (Provider, error) {
	if maximumSizeBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "Maximum CAS blob size cannot be negative")
	}
	if maximumSizeBytes == 0 {
		return base, nil
	}
	return &casBlobSizeLimitingProvider{base: base, maximumSizeBytes: maximumSizeBytes}, nil
}

func (p *casBlobSizeLimitingProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	caps, err := p.base.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	cache := caps.GetCacheCapabilities()
	if cache == nil || (cache.MaxCasBlobSizeBytes > 0 && cache.MaxCasBlobSizeBytes <= p.maximumSizeBytes) {
		return caps, nil
	}
	// Providers may return shared immutable messages.
	copied := proto.Clone(caps).(*remoteexecution.ServerCapabilities)
	copied.CacheCapabilities.MaxCasBlobSizeBytes = p.maximumSizeBytes
	return copied, nil
}
