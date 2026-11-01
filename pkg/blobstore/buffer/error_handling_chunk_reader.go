package buffer

import (
	"io"
)

type errorHandlingChunkReader struct {
	r                     ChunkReader
	errorHandler          ErrorHandler
	off                   int64
	maximumChunkSizeBytes int
	err                   error
}

// newErrorHandlingChunkReader returns a ChunkReader that forwards calls
// to a reader obtained from a Buffer. Upon I/O failure, it calls into
// an ErrorHandler to request a new Buffer to continue the transfer.
func newErrorHandlingChunkReader(b Buffer, errorHandler ErrorHandler, off int64, maximumChunkSizeBytes int) ChunkReader {
	return &errorHandlingChunkReader{
		r:                     b.toUnvalidatedChunkReader(off, maximumChunkSizeBytes),
		errorHandler:          errorHandler,
		off:                   off,
		maximumChunkSizeBytes: maximumChunkSizeBytes,
	}
}

func (r *errorHandlingChunkReader) Read() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	for {
		chunk, originalErr := r.r.Read()
		if originalErr == nil {
			r.off += int64(len(chunk))
			return chunk, nil
		} else if originalErr == io.EOF {
			r.err = io.EOF
			return nil, io.EOF
		}
		// Replacement callbacks may acquire the failed source's decoder slot.
		r.r.Close()
		r.r = nil
		b, translatedErr := r.errorHandler.OnError(originalErr)
		if translatedErr != nil {
			r.err = translatedErr
			return nil, translatedErr
		}
		r.r = b.toUnvalidatedChunkReader(r.off, r.maximumChunkSizeBytes)
	}
}

func (r *errorHandlingChunkReader) Close() {
	if r.errorHandler != nil {
		r.errorHandler.Done()
		r.errorHandler = nil
	}
	r.err = io.EOF
	if r.r != nil {
		r.r.Close()
		r.r = nil
	}
}
