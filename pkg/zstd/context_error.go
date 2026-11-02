package zstd

import (
	"context"

	"google.golang.org/grpc/status"
)

// Preserve the cause only at context checks and canceled codec acquisitions.
// Callers can then distinguish their own interruption from an independent
// source error carrying the same gRPC status code.
func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		s := status.FromContextError(err)
		if cause := context.Cause(ctx); cause != err {
			return &contextCauseError{status: s, cause: cause}
		}
		return s.Err()
	}
	return nil
}

type contextCauseError struct {
	status *status.Status
	cause  error
}

func (e *contextCauseError) Error() string              { return e.status.Err().Error() }
func (e *contextCauseError) GRPCStatus() *status.Status { return e.status }
func (e *contextCauseError) Unwrap() error              { return e.cause }
