package buffer

import (
	"io"
)

type readerBackedChunkReader struct {
	r                     io.ReadCloser
	maximumChunkSizeBytes int

	err error
}

// newReaderBackedChunkReader creates a ChunkReader based on an existing
// ReadCloser. It attempts to read data from the ReadCloser, turning it
// into chunks of the maximum permitted size.
func newReaderBackedChunkReader(r io.ReadCloser, maximumChunkSizeBytes int) ChunkReader {
	return &readerBackedChunkReader{
		r:                     r,
		maximumChunkSizeBytes: maximumChunkSizeBytes,
	}
}

func (r *readerBackedChunkReader) Read() ([]byte, error) {
	if r.err == nil {
		b := make([]byte, r.maximumChunkSizeBytes)
		source := terminalErrorReader{Reader: r.r}
		n, err := io.ReadFull(&source, b[:])
		// ReadFull may synthesize UnexpectedEOF from a short clean read, or
		// suppress an error returned alongside a full chunk. Preserve actual
		// source failures, including truncated compressed streams.
		if source.err != nil {
			r.err = source.err
		} else {
			r.err = err
		}
		if n > 0 {
			return b[:n], nil
		}
	}
	return nil, r.err
}

type terminalErrorReader struct {
	io.Reader
	err error
}

func (r *terminalErrorReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.err = err
	return n, err
}

func (r *readerBackedChunkReader) Close() {
	r.r.Close()
}
