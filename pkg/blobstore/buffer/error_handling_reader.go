package buffer

import (
	"io"
)

type errorHandlingReader struct {
	r            io.ReadCloser
	errorHandler ErrorHandler
	off          int64
	err          error
}

// newErrorHandlingReader returns an io.ReadCloser that forwards calls
// to a reader obtained from a Buffer. Upon I/O failure, it calls into
// an ErrorHandler to request a new Buffer to continue the transfer.
func newErrorHandlingReader(b Buffer, errorHandler ErrorHandler, off int64) io.ReadCloser {
	return &errorHandlingReader{
		r:            b.toUnvalidatedReader(off),
		errorHandler: errorHandler,
		off:          off,
	}
}

func (r *errorHandlingReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, originalErr := r.r.Read(p)
	r.off += int64(n)
	if originalErr == nil || originalErr == io.EOF {
		r.err = originalErr
		return n, originalErr
	}
	// A replacement may need the same bounded decoder or block resources.
	// Release the failed source before invoking code that obtains another.
	r.r.Close()
	r.r = nil
	b, translatedErr := r.errorHandler.OnError(originalErr)
	if translatedErr != nil {
		r.err = translatedErr
		return n, translatedErr
	}
	r.r = b.toUnvalidatedReader(r.off)
	return n, nil
}

func (r *errorHandlingReader) Close() error {
	if r.errorHandler != nil {
		r.errorHandler.Done()
		r.errorHandler = nil
	}
	r.err = io.EOF
	if r.r != nil {
		reader := r.r
		r.r = nil
		return reader.Close()
	}
	return nil
}
