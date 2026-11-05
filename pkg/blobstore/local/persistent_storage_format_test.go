package local_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistentStorageFormat(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		dir := t.TempDir()
		require.NoError(t, local.CheckPersistentStorageFormat(dir, compressed))
		require.NoError(t, local.CheckPersistentStorageFormat(dir, compressed), "same-format restart")
		require.Equal(t, codes.FailedPrecondition, status.Code(local.CheckPersistentStorageFormat(dir, !compressed)))
		require.NoError(t, local.CheckPersistentStorageFormat(dir, compressed), "failed switch must preserve marker")
	}
	for _, name := range []string{"state", "state.new"} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("old persistent state"), 0o600))
		require.Equal(t, codes.FailedPrecondition, status.Code(local.CheckPersistentStorageFormat(dir, true)))
		require.NoError(t, local.CheckPersistentStorageFormat(dir, false))
	}
	for _, contents := range []string{"", "zstdframes", "zstdframes-v2\n"} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "storage-format"), []byte(contents), 0o600))
		require.Equal(t, codes.FailedPrecondition, status.Code(local.CheckPersistentStorageFormat(dir, true)))
		require.Equal(t, codes.FailedPrecondition, status.Code(local.CheckPersistentStorageFormat(dir, false)))
	}
}
