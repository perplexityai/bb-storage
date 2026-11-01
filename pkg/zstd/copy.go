package zstd

import (
	"context"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Copy writes independent Zstandard frames, reading at most one MiB before
// acquiring an encoder and releasing it before reading more input. In
// particular, source retries may acquire decoders without holding an encoder
// from the same bounded pool. The caller owns both I/O handles.
func Copy(ctx context.Context, pool Pool, dst io.Writer, src io.Reader) error {
	data := make([]byte, 1<<20)
	first := true
	for {
		size, emptyReads := 0, 0
		var readErr error
		for size < len(data) && readErr == nil {
			if err := ctx.Err(); err != nil {
				return status.FromContextError(err).Err()
			}
			n, err := src.Read(data[size:])
			size += n
			readErr = err
			if n == 0 && err == nil {
				emptyReads++
				if emptyReads == 100 {
					return io.ErrNoProgress
				}
			} else {
				emptyReads = 0
			}
		}
		// Do not erase errors returned alongside a complete frame of bytes.
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if size == 0 {
			if first {
				// A valid empty frame even when pool options suppress them.
				empty := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00}
				n, err := dst.Write(empty)
				if err == nil && n != len(empty) {
					err = io.ErrShortWrite
				}
				if err != nil {
					return err
				}
				return status.FromContextError(ctx.Err()).Err()
			}
			return nil
		}
		first = false
		encoder, err := pool.NewEncoder(ctx, dst)
		if err != nil {
			if code := status.Code(err); code == codes.Canceled || code == codes.DeadlineExceeded {
				return err
			}
			return status.Errorf(codes.ResourceExhausted, "Failed to acquire ZSTD encoder: %v", err)
		}
		n, writeErr := encoder.Write(data[:size])
		closeErr := encoder.Close()
		if writeErr != nil {
			return writeErr
		}
		if n != size {
			return io.ErrShortWrite
		}
		if closeErr != nil {
			return closeErr
		}
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if readErr == io.EOF {
			return nil
		}
	}
}
