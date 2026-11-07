package local

import (
	"io"
	"os"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	componentStorageFormat    = path.MustNewComponent("storage-format")
	componentStorageFormatNew = path.MustNewComponent("storage-format.new")
)

// PreparePersistentStorageFormat validates the record format without changing
// persistent state. Call the returned publisher after startup checks succeed,
// before starting state syncers or admitting writes. The caller owns directory.
func PreparePersistentStorageFormat(directory filesystem.Directory, compressed bool) (func() error, error) {
	mode := "raw\n"
	if compressed {
		mode = "zstdframes-v1\n"
	}
	f, err := directory.OpenRead(componentStorageFormat)
	if err == nil {
		data, readErr := io.ReadAll(io.NewSectionReader(f, 0, 64))
		closeErr := f.Close()
		if readErr != nil {
			return nil, util.StatusWrapWithCode(readErr, codes.Internal, "Failed to read storage format marker")
		}
		if closeErr != nil {
			return nil, util.StatusWrapWithCode(closeErr, codes.Internal, "Failed to close storage format marker")
		}
		if string(data) != mode {
			return nil, status.Error(codes.FailedPrecondition, "Persistent storage format mismatch or damaged marker; verify the recorded format before restarting, or provision a fresh cache when changing formats")
		}
		return func() error {
			if err := directory.Sync(); err != nil {
				return util.StatusWrapWithCode(err, codes.Internal, "Failed to synchronize storage format directory")
			}
			return nil
		}, nil
	}
	if !os.IsNotExist(err) {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to open storage format marker")
	}
	if compressed {
		for _, name := range []path.Component{componentState, componentStateNew} {
			if _, err := directory.Lstat(name); err == nil {
				return nil, status.Error(codes.FailedPrecondition, "Unmarked persistent state belongs to raw storage; CAS compression requires a fresh state directory and key-location map")
			} else if !os.IsNotExist(err) {
				return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to inspect legacy storage state")
			}
		}
	}
	return func() error {
		if err := directory.Remove(componentStorageFormatNew); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to remove temporary storage format marker")
		}
		f, err := directory.OpenAppend(componentStorageFormatNew, filesystem.CreateExcl(0o600))
		if err != nil {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to create temporary storage format marker")
		}
		if n, err := f.Write([]byte(mode)); err != nil || n != len(mode) {
			f.Close()
			if err == nil {
				err = io.ErrShortWrite
			}
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to write temporary storage format marker")
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to synchronize temporary storage format marker")
		}
		if err := f.Close(); err != nil {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to close temporary storage format marker")
		}
		if err := directory.Rename(componentStorageFormatNew, directory, componentStorageFormat); err != nil {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to publish storage format marker")
		}
		if err := directory.Sync(); err != nil {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to synchronize storage format directory")
		}
		return nil
	}, nil
}
