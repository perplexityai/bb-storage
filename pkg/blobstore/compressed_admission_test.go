package blobstore

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *admissionWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestStagingBudgetAllowsFittingUploadPastLargeWaiter(t *testing.T) {
	budget := &stagingBudget{available: 100, changed: make(chan struct{})}
	require.NoError(t, budget.Acquire(t.Context(), 75))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := &admissionWaitContext{Context: ctx, waiting: make(chan struct{})}
	large := make(chan error, 1)
	go func() { large <- budget.Acquire(waiting, 50) }()
	<-waiting.waiting
	smallCtx, smallCancel := context.WithTimeout(t.Context(), time.Second)
	defer smallCancel()
	require.NoError(t, budget.Acquire(smallCtx, 25), "small upload must pass blocked large upload")
	budget.Release(25)
	cancel()
	require.ErrorIs(t, <-large, context.Canceled)
	budget.Release(75)
	require.NoError(t, budget.Acquire(t.Context(), 100), "cancellation must not leak reserved bytes")
	budget.Release(100)
}

func TestStagingFilesystemErrorCodes(t *testing.T) {
	for _, err := range []error{syscall.ENOSPC, syscall.EDQUOT} {
		require.Equal(t, codes.ResourceExhausted, status.Code(stagingError(errors.Join(errors.New("write"), err))))
	}
	require.Equal(t, codes.Internal, status.Code(stagingError(syscall.EIO)))
}
