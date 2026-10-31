package capabilities_test

import (
	"fmt"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestCASBlobSizeLimitingProvider(t *testing.T) {
	for _, tc := range []struct{ backend, local, want int64 }{
		{0, 0, 0}, {0, 5, 5}, {3, 5, 3}, {5, 5, 5}, {8, 5, 5}, {8, 0, 8},
	} {
		t.Run(fmt.Sprintf("backend=%d/local=%d", tc.backend, tc.local), func(t *testing.T) {
			original := &remoteexecution.ServerCapabilities{CacheCapabilities: &remoteexecution.CacheCapabilities{
				MaxCasBlobSizeBytes: tc.backend, DigestFunctions: []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256},
			}}
			provider, err := capabilities.NewCASBlobSizeLimitingProvider(capabilities.NewStaticProvider(original), tc.local)
			require.NoError(t, err)
			result, err := provider.GetCapabilities(t.Context(), digest.EmptyInstanceName)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.CacheCapabilities.MaxCasBlobSizeBytes)
			require.Equal(t, original.CacheCapabilities.DigestFunctions, result.CacheCapabilities.DigestFunctions)
			require.Equal(t, tc.backend, original.CacheCapabilities.MaxCasBlobSizeBytes, "must not mutate shared capabilities")
		})
	}
	// An execution-only service must not acquire cache capabilities.
	provider, err := capabilities.NewCASBlobSizeLimitingProvider(capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{}), 5)
	require.NoError(t, err)
	result, err := provider.GetCapabilities(t.Context(), digest.EmptyInstanceName)
	require.NoError(t, err)
	require.Nil(t, result.CacheCapabilities)
	_, err = capabilities.NewCASBlobSizeLimitingProvider(nil, -1)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
