package zstd

import (
	"context"
	"io"
)

// Copy writes independent Zstandard frames, reading at most one MiB before
// acquiring an encoder and releasing it before reading more input. In
// particular, source retries may acquire decoders without holding an encoder
// from the same bounded pool. The caller owns both I/O handles.
func Copy(ctx context.Context, pool Pool, dst io.Writer, src io.Reader) error {
	return CopySized(ctx, pool, dst, src, 1<<20)
}

// CopySized bounds the input frame buffer using the expected logical size.
// The size is a buffer hint; callers retain responsibility for size validation.
func CopySized(ctx context.Context, pool Pool, dst io.Writer, src io.Reader, sizeHint int64) error {
	data := make([]byte, min(int64(1<<20), max(int64(1), sizeHint)))
	first := true
	for {
		size, emptyReads := 0, 0
		var readErr error
		for size < len(data) && readErr == nil {
			if err := ctx.Err(); err != nil {
				return contextError(ctx)
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
			return contextError(ctx)
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
				return contextError(ctx)
			}
			return nil
		}
		first = false
		encoded, err := EncodeFrame(ctx, pool, data[:size])
		if err != nil {
			return err
		}
		n, err := dst.Write(encoded)
		if err != nil {
			return err
		}
		if n != len(encoded) {
			return io.ErrShortWrite
		}
		if err := ctx.Err(); err != nil {
			return contextError(ctx)
		}
		if readErr == io.EOF {
			return nil
		}
	}
}
