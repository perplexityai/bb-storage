package blobstore

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

func TestStagingAdmissionDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := &stagingBudget{changed: make(chan struct{})}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		before := stagingTimeouts(t)
		start := time.Now()
		require.Equal(t, codes.ResourceExhausted, status.Code(acquireUploadCapacity(ctx, budget, 1)))
		require.Equal(t, 30*time.Second, time.Since(start))
		require.Equal(t, before+1, stagingTimeouts(t))
		require.NoError(t, ctx.Err(), "staging must enforce its own timeout")
	})
}

func stagingTimeouts(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "buildbarn_cas_compression_rejected_requests_total" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "reason" && label.GetValue() == "staging_timeout" {
						return metric.Counter.GetValue()
					}
				}
			}
		}
	}
	return 0
}
