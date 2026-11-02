package grpcservers

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/status"
)

type writeReceiveResult struct {
	request *bytestream.WriteRequest
	err     error
}

type interruptibleWriteStream struct {
	bytestream.ByteStream_WriteServer
	ctx     context.Context
	cancel  context.CancelCauseFunc
	once    sync.Once
	results chan writeReceiveResult
	demand  chan struct{}
}

func newInterruptibleWriteStream(stream bytestream.ByteStream_WriteServer) *interruptibleWriteStream {
	ctx, cancel := context.WithCancelCause(stream.Context())
	return &interruptibleWriteStream{ByteStream_WriteServer: stream, ctx: ctx, cancel: cancel, results: make(chan writeReceiveResult), demand: make(chan struct{})}
}

func (s *interruptibleWriteStream) ReadInterrupt() func() {
	return func() { s.cancel(buffer.ErrUploadInterrupted) }
}

func (s *interruptibleWriteStream) Recv() (*bytestream.WriteRequest, error) {
	s.once.Do(func() { go s.receive() })
	select {
	case s.demand <- struct{}{}:
	case <-s.ctx.Done():
		return nil, s.interruptionError()
	}
	select {
	case <-s.ctx.Done():
		return nil, s.interruptionError()
	case result := <-s.results:
		return result.request, result.err
	}
}

func (s *interruptibleWriteStream) interruptionError() error {
	if context.Cause(s.ctx) == buffer.ErrUploadInterrupted {
		return buffer.ErrUploadInterrupted
	}
	return status.FromContextError(s.ctx.Err()).Err()
}

func (s *interruptibleWriteStream) receive() {
	var terminal error
	for {
		// Receive only on demand: a paused consumer must not retain another
		// potentially large message. Returning from the upload handler releases
		// the one transport Recv that may remain blocked after interruption.
		select {
		case <-s.demand:
		case <-s.ctx.Done():
			return
		}
		var request *bytestream.WriteRequest
		err := terminal
		if err == nil {
			request, err = s.ByteStream_WriteServer.Recv()
			terminal = err
		}
		select {
		case s.results <- writeReceiveResult{request: request, err: err}:
		case <-s.ctx.Done():
			return
		}
	}
}
