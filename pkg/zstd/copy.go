package zstd

import (
	"context"
	"io"
)

// CopySized bounds the input frame buffer using the expected logical size.
// The size is a buffer hint; callers retain responsibility for size validation.
// Each independent frame releases its encoder before writing to the destination
// or reading more input. The caller owns both I/O handles.
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
				empty := []byte(emptyFrame)
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
