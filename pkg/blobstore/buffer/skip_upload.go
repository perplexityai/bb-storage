package buffer

import "io"

// SkipUpload completes an upload whose destination already has the blob.
// Explicitly marked encoded uploads can be closed without reading or acquiring
// codecs, but their close and background-task errors still matter.
// Other buffers are consumed without storing them: Discard alone cannot expose
// completion errors from arbitrary decorators or synchronized cloned streams.
func SkipUpload(b Buffer) error {
	switch b := b.(type) {
	case *compressedBuffer:
		if b.upload {
			return b.encoded.Close()
		}
	case *casBufferWithBackgroundTask:
		err := SkipUpload(b.base)
		<-b.task.completion
		if err != nil {
			return err
		}
		return b.task.err
	}
	r := b.ToReader()
	_, readErr := io.Copy(io.Discard, r)
	closeErr := r.Close()
	if readErr != nil {
		return readErr
	}
	return closeErr
}
