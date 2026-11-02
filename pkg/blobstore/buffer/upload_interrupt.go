package buffer

import "errors"

// ErrUploadInterrupted is returned by an interruptible upload source when its
// destination has completed early. It must not hide an independent read error.
var ErrUploadInterrupted = errors.New("Upload destination completed")

// UploadReadInterrupt returns a concurrency-safe way to stop an upload read, if
// the source explicitly provides one. The caller must still close the reader
// and preserve its completion errors. Unknown wrappers are not interruptible.
func UploadReadInterrupt(b Buffer) func() {
	switch b := b.(type) {
	case *compressedBuffer:
		return uploadReadInterrupt(b.encoded)
	case *casReaderBuffer:
		return uploadReadInterrupt(b.r)
	case *casChunkReaderBuffer:
		return uploadReadInterrupt(b.r)
	case *casBufferWithBackgroundTask:
		return UploadReadInterrupt(b.base)
	}
	return nil
}

func uploadReadInterrupt(r any) func() {
	if r, ok := r.(interface{ ReadInterrupt() func() }); ok {
		return r.ReadInterrupt()
	}
	return nil
}
