package compressionintegration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestPersistentFormatMismatchFailsBeforeDeviceOpen(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		dir := t.TempDir()
		device := filepath.Join(dir, "blocks")
		marker := "zstdframes-v1\n"
		compression := ""
		if compressed {
			marker = "raw\n"
			compression = fmt.Sprintf(`,"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}`, dir)
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "storage-format"), []byte(marker), 0o600))
		var config pb.BlobAccessConfiguration
		require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q},"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}}}%s}}`, dir, device, compression)), &config))
		_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		_, err = os.Stat(device)
		require.True(t, os.IsNotExist(err))
	}
}

func TestFailedStartupDoesNotPublishPersistentFormat(t *testing.T) {
	for _, invalidDevice := range []bool{false, true} {
		dir := t.TempDir()
		device := filepath.Join(dir, "blocks")
		if invalidDevice {
			device = filepath.Join(dir, "missing", "blocks")
		}
		var config pb.BlobAccessConfiguration
		require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q,"minimumEpochInterval":"1s"},"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"spareBlocks":2},"oldBlocks":1,"currentBlocks":1,"newBlocks":1,"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}}}`, dir, device, dir)), &config))
		// With a valid device, the deliberately absent key-map backend fails late,
		// after the syncer is constructed. Neither path may start persistence.
		_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
		require.Error(t, err)
		for _, name := range []string{"storage-format", "storage-format.new", "state", "state.new"} {
			_, err := os.Stat(filepath.Join(dir, name))
			require.True(t, os.IsNotExist(err), name)
		}
	}
}

func TestMarkerPublicationFailureDoesNotStartPersistence(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "storage-format.new")
	require.NoError(t, os.Mkdir(stale, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "block-removal"), nil, 0o600))
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q,"minimumEpochInterval":"1s"},"keyLocationMap":{"inMemory":{"entries":"1024"},"maximumGetAttempts":16,"maximumPutAttempts":"64"},"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"spareBlocks":2},"oldBlocks":1,"currentBlocks":1,"newBlocks":1,"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}}}`, dir, filepath.Join(dir, "blocks"), dir)), &config))
	// No lifecycle group is supplied: starting the syncer before publication
	// succeeds would panic as well as leak its independent release goroutine.
	_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "temporary storage format marker")
	for _, name := range []string{"storage-format", "state", "state.new"} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(err), name)
	}
}

func TestMissingStateDirectoryHasStartupContext(t *testing.T) {
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"persistent":{"stateDirectoryPath":%q}}}`, filepath.Join(t.TempDir(), "missing"))), &config))
	_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "Failed to open persistent state directory")
	require.Contains(t, err.Error(), config.GetLocal().Persistent.StateDirectoryPath)
}

func TestCompressionRejectsIntegrityCacheBeforeOpeningDevice(t *testing.T) {
	dir := t.TempDir()
	device := filepath.Join(dir, "blocks")
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{"blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"dataIntegrityValidationCache":{}},"casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"1048576","maximumConcurrentWrites":"1"}}}`, device, dir)), &config))
	_, err := configuration.NewBlobAccessFromConfiguration(nil, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, pool()))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = os.Stat(device)
	require.True(t, os.IsNotExist(err))
}
