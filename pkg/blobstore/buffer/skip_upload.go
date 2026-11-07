package buffer

import "io"

// SkipUpload completes an upload whose destination already has the blob.
// Encoded uploads are drained without acquiring codecs. Older clients treat
// an early successful server response as a failed send, so duplicates must
// still consume the encoded stream through FinishWrite.
// Other buffers are consumed without storing them: Discard alone cannot expose
// completion errors from arbitrary decorators or synchronized cloned streams.
func SkipUpload(b Buffer) error {
	switch b := b.(type) {
	case *compressedBuffer:
		if b.upload {
			_, readErr := io.Copy(io.Discard, b.encoded)
			closeErr := b.encoded.Close()
			if readErr != nil {
				return readErr
			}
			return closeErr
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
