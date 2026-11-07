package zstd

import (
	"bytes"
	"context"
	"io"
)

const emptyFrame = "\x28\xb5\x2f\xfd\x20\x00\x01\x00\x00"

// EncodeFrame releases the encoder before returning bytes to a potentially
// slow destination. Built-in encoders use known-size encoding while respecting
// configured window limits. Custom streaming
// encoders retain their existing framing but also release before transmission.
func EncodeFrame(ctx context.Context, pool Pool, data []byte) (encoded []byte, err error) {
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []byte(emptyFrame), nil
	}
	var output bytes.Buffer
	encoder, err := pool.NewEncoder(ctx, &output)
	if err != nil {
		return nil, err
	}
	framed := encoder
	for {
		m, ok := framed.(*metricsEncoder)
		if !ok {
			break
		}
		framed = m.Encoder
	}
	blockEncoder, hasBlocks := framed.(interface{ EncodeAll([]byte, []byte) []byte })
	defer func() {
		closeErr := encoder.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = contextError(ctx)
		}
		if !hasBlocks {
			encoded = output.Bytes()
		}
	}()
	if hasBlocks {
		encoded = blockEncoder.EncodeAll(data, nil)
	} else {
		var n int
		n, err = encoder.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	return encoded, err
}
