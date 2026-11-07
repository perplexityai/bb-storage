package local_test

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistentStorageFormat(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		dir := t.TempDir()
		require.NoError(t, checkStorageFormat(dir, compressed))
		require.NoError(t, checkStorageFormat(dir, compressed), "same-format restart")
		require.Equal(t, codes.FailedPrecondition, status.Code(checkStorageFormat(dir, !compressed)))
		require.NoError(t, checkStorageFormat(dir, compressed), "failed switch must preserve marker")
	}
	for _, name := range []string{"state", "state.new"} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("old persistent state"), 0o600))
		require.Equal(t, codes.FailedPrecondition, status.Code(checkStorageFormat(dir, true)))
		require.NoError(t, checkStorageFormat(dir, false))
	}
	for _, contents := range []string{"", "zstdframes", "zstdframes-v2\n"} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "storage-format"), []byte(contents), 0o600))
		require.Equal(t, codes.FailedPrecondition, status.Code(checkStorageFormat(dir, true)))
		require.Equal(t, codes.FailedPrecondition, status.Code(checkStorageFormat(dir, false)))
	}
}

func checkStorageFormat(directory string, compressed bool) error {
	dir, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directory))
	if err != nil {
		return err
	}
	defer dir.Close()
	publish, err := local.PreparePersistentStorageFormat(dir, compressed)
	if err != nil {
		return err
	}
	return publish()
}

type markerFaultDirectory struct {
	filesystem.Directory
	failure string
}

func (d *markerFaultDirectory) OpenAppend(name path.Component, mode filesystem.CreationMode) (filesystem.FileAppender, error) {
	f, err := d.Directory.OpenAppend(name, mode)
	if err != nil {
		return nil, err
	}
	return &markerFaultFile{FileAppender: f, failure: d.failure}, nil
}

func (d *markerFaultDirectory) Rename(old path.Component, target filesystem.Directory, newName path.Component) error {
	if d.failure == "rename" {
		return syscall.EIO
	}
	if target == d {
		target = d.Directory
	}
	return d.Directory.Rename(old, target, newName)
}

func (d *markerFaultDirectory) Sync() error {
	if d.failure == "directory-sync" {
		return syscall.EIO
	}
	return d.Directory.Sync()
}

type markerFaultFile struct {
	filesystem.FileAppender
	failure string
}

func (f *markerFaultFile) Write(p []byte) (int, error) {
	if f.failure == "write" {
		n, _ := f.FileAppender.Write(p[:1])
		return n, syscall.ENOSPC
	}
	if f.failure == "short-write" {
		return f.FileAppender.Write(p[:1])
	}
	return f.FileAppender.Write(p)
}

func (f *markerFaultFile) Sync() error {
	if f.failure == "file-sync" {
		return syscall.EIO
	}
	return f.FileAppender.Sync()
}

func (f *markerFaultFile) Close() error {
	err := f.FileAppender.Close()
	if f.failure == "close" {
		return syscall.EIO
	}
	return err
}

func TestStorageFormatPublicationSurvivesInterruptedWrites(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, failure := range []string{"write", "short-write", "file-sync", "close", "rename", "directory-sync"} {
			t.Run(fmt.Sprintf("compressed=%t/%s", compressed, failure), func(t *testing.T) {
				dir := t.TempDir()
				actual, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(dir))
				require.NoError(t, err)
				defer actual.Close()
				faulty := &markerFaultDirectory{Directory: actual, failure: failure}
				publish, err := local.PreparePersistentStorageFormat(faulty, compressed)
				require.NoError(t, err)
				_, err = os.Stat(filepath.Join(dir, "storage-format"))
				require.True(t, os.IsNotExist(err), "validation must not publish")
				err = publish()
				require.Equal(t, codes.Internal, status.Code(err))
				require.Contains(t, err.Error(), "storage format")
				contents, err := os.ReadFile(filepath.Join(dir, "storage-format"))
				if failure == "directory-sync" {
					require.NoError(t, err)
					expected := "raw\n"
					if compressed {
						expected = "zstdframes-v1\n"
					}
					require.Equal(t, expected, string(contents))
				} else {
					require.True(t, os.IsNotExist(err), "failed publication must leave final marker absent")
				}
				faulty.failure = ""
				publish, err = local.PreparePersistentStorageFormat(faulty, compressed)
				require.NoError(t, err)
				require.NoError(t, publish(), "retry must tolerate abandoned temporary marker")
				require.NoError(t, checkStorageFormat(dir, compressed))
			})
		}
	}
}

func TestPrepareStorageFormatDoesNotBindAnUnusedDirectory(t *testing.T) {
	dir := t.TempDir()
	actual, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(dir))
	require.NoError(t, err)
	defer actual.Close()
	_, err = local.PreparePersistentStorageFormat(actual, true)
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, checkStorageFormat(dir, false))
}
