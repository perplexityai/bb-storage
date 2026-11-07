package compressionintegration_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

// Separate processes close mappings and discard all in-memory indexes between
// phases. Shutdown runs the real syncer before the next process opens the files.
func TestPersistentCompressedRestart(t *testing.T) {
	dir := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	for _, phase := range []string{"write", "refresh", "reopen"} {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		command := exec.CommandContext(ctx, executable, "-test.run=^TestPersistentCompressedRestartChild$", "-test.v")
		command.Env = append(os.Environ(), "BB_STORAGE_RESTART_DIR="+dir, "BB_STORAGE_RESTART_PHASE="+phase)
		output, err := command.CombinedOutput()
		cancel()
		require.NoError(t, err, "%s: %s", phase, output)
	}
}

type persistenceGroup struct {
	ctx context.Context
	wg  sync.WaitGroup
	t   *testing.T
}

func (g *persistenceGroup) Go(r program.Routine) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := r(g.ctx, g, g); err != nil {
			g.t.Error(err)
		}
	}()
}

func TestPersistentCompressedRestartChild(t *testing.T) {
	dir := os.Getenv("BB_STORAGE_RESTART_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	phase := os.Getenv("BB_STORAGE_RESTART_PHASE")
	state := filepath.Join(dir, "state-directory")
	require.NoError(t, os.MkdirAll(state, 0o700))
	device := filepath.Join(dir, "blocks")
	var config pb.BlobAccessConfiguration
	require.NoError(t, protojson.Unmarshal([]byte(fmt.Sprintf(`{"local":{
 "persistent":{"stateDirectoryPath":%q,"minimumEpochInterval":"0.001s"},
 "keyLocationMap":{"onBlockDevice":{"file":{"path":%q,"sizeBytes":"131072"}},"maximumGetAttempts":16,"maximumPutAttempts":"64"},
 "blocksOnBlockDevice":{"source":{"file":{"path":%q,"sizeBytes":"81920"}},"spareBlocks":2},
 "oldBlocks":1,"currentBlocks":1,"newBlocks":1,
 "casCompression":{"stagingDirectory":%q,"maximumStagedBytes":"16777216","maximumConcurrentWrites":"2"}
 }}`, state, filepath.Join(dir, "keys"), device, dir)), &config))
	ctx, cancel := context.WithCancel(t.Context())
	group := &persistenceGroup{ctx: ctx, t: t}
	defer func() { cancel(); group.wg.Wait() }()
	p := pool()
	info, err := configuration.NewBlobAccessFromConfiguration(group, &config, configuration.NewCASBlobAccessCreator(nil, 4<<20, p))
	require.NoError(t, err)
	store := info.BlobAccess
	data := bytes.Repeat([]byte("persistent compressed record"), 100000)
	d := digestOf(data)
	tiny := []byte("tiny persistent raw record")
	td := digestOf(tiny)
	if phase == "write" {
		require.NoError(t, store.Put(t.Context(), d, buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)))
		require.NoError(t, store.Put(t.Context(), td, buffer.NewCASBufferFromByteSlice(td, tiny, buffer.UserProvided)))
	} else {
		missing, err := store.FindMissing(t.Context(), d.ToSingletonSet())
		require.NoError(t, err)
		require.True(t, missing.Empty())
	}
	conn := serve(t, store, p)
	before := p.decoders.Load()
	encoded := read(t, t.Context(), conn, d, remoteexecution.Compressor_ZSTD, 0)
	require.Equal(t, before, p.decoders.Load(), "reopening must preserve encoded reads")
	require.Less(t, len(encoded), len(data))
	require.Equal(t, data, read(t, t.Context(), conn, d, remoteexecution.Compressor_IDENTITY, 0))
	got, err := store.Get(t.Context(), td).ToByteSlice(len(tiny))
	require.NoError(t, err)
	require.Equal(t, tiny, got)
	if phase == "refresh" {
		random := rand.New(rand.NewPCG(2, 3))
		for range 2 {
			filler := make([]byte, 9000)
			for i := range filler {
				filler[i] = byte(random.Uint32())
			}
			fd := digestOf(filler)
			require.NoError(t, store.Put(t.Context(), fd, buffer.NewCASBufferFromByteSlice(fd, filler, buffer.UserProvided)))
		}
		raw, err := os.ReadFile(device)
		require.NoError(t, err)
		copies := bytes.Count(raw, encoded)
		before := p.decoders.Load()
		require.Equal(t, encoded, read(t, t.Context(), conn, d, remoteexecution.Compressor_ZSTD, 0))
		require.Equal(t, before, p.decoders.Load(), "refresh must copy the encoded representation")
		raw, err = os.ReadFile(device)
		require.NoError(t, err)
		require.Greater(t, bytes.Count(raw, encoded), copies, "refresh must publish another physical record")
		// Keep the tiny record live across the same rotation.
		got, err = store.Get(t.Context(), td).ToByteSlice(len(tiny))
		require.NoError(t, err)
		require.Equal(t, tiny, got)
	}
}
