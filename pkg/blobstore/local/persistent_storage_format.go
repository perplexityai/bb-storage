package local

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckPersistentStorageFormat binds a state directory to its record format
// before any block device is opened. Unmarked state belongs to the raw format.
// A partial marker is rejected, so a crash cannot silently change formats.
func CheckPersistentStorageFormat(directory string, compressed bool) error {
	mode := "raw\n"
	if compressed {
		mode = "zstdframes-v1\n"
	}
	marker := filepath.Join(directory, "storage-format")
	f, err := os.Open(marker)
	if err == nil {
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, 64))
		if err != nil {
			return err
		}
		if string(data) != mode {
			return status.Error(codes.FailedPrecondition, "Persistent storage format mismatch; use a fresh state directory and key-location map when changing CAS compression")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if compressed {
		for _, name := range []string{"state", "state.new"} {
			if _, err := os.Lstat(filepath.Join(directory, name)); err == nil {
				return status.Error(codes.FailedPrecondition, "Unmarked persistent state belongs to raw storage; CAS compression requires a fresh state directory and key-location map")
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	f, err = os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.WriteString(f, mode); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
