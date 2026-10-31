package grpcservers

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	byteStreamAdmissionMetrics sync.Once
	byteStreamAdmissionActive  = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "buildbarn",
		Subsystem: "bytestream_admission",
		Name:      "active_requests",
		Help:      "Number of ByteStream requests holding an admission slot, including requests waiting for codecs or clients.",
	}, []string{"method"})
	byteStreamAdmissionRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "bytestream_admission",
		Name:      "rejected_requests_total",
		Help:      "Number of ByteStream requests rejected because all admission slots were occupied.",
	}, []string{"method"})
)

type byteStreamAdmissionLimiter struct {
	slots    *semaphore.Weighted
	method   string
	active   prometheus.Gauge
	rejected prometheus.Counter
}

func newByteStreamAdmissionLimiter(maximum int64, method string) *byteStreamAdmissionLimiter {
	if maximum == 0 {
		return nil
	}
	return &byteStreamAdmissionLimiter{
		slots:    semaphore.NewWeighted(maximum),
		method:   method,
		active:   byteStreamAdmissionActive.WithLabelValues(method),
		rejected: byteStreamAdmissionRejected.WithLabelValues(method),
	}
}

func (l *byteStreamAdmissionLimiter) acquire(ctx context.Context) error {
	if ctx.Err() != nil {
		return util.StatusFromContext(ctx)
	}
	if l != nil {
		if !l.slots.TryAcquire(1) {
			l.rejected.Inc()
			return status.Errorf(codes.ResourceExhausted, "ByteStream.%s concurrent request limit reached", l.method)
		}
		l.active.Inc()
	}
	return nil
}

func (l *byteStreamAdmissionLimiter) release() {
	if l != nil {
		l.active.Dec()
		l.slots.Release(1)
	}
}

type admissionControlledByteStreamServer struct {
	bytestream.ByteStreamServer
	reads  *byteStreamAdmissionLimiter
	writes *byteStreamAdmissionLimiter
}

// NewAdmissionControlledByteStreamServer bounds active reads and writes
// independently, rejecting excess requests without queueing. Slots remain
// occupied until the underlying handler returns, including time spent
// waiting for clients, codecs, and storage. Zero disables a method's limit.
// Share the returned server between listeners to enforce process-wide limits.
func NewAdmissionControlledByteStreamServer(base bytestream.ByteStreamServer, maximumReads, maximumWrites int64) (bytestream.ByteStreamServer, error) {
	if maximumReads < 0 || maximumWrites < 0 {
		return nil, status.Error(codes.InvalidArgument, "ByteStream concurrent request limits cannot be negative")
	}
	if maximumReads == 0 && maximumWrites == 0 {
		return base, nil
	}
	byteStreamAdmissionMetrics.Do(func() {
		prometheus.MustRegister(byteStreamAdmissionActive, byteStreamAdmissionRejected)
	})
	return &admissionControlledByteStreamServer{
		ByteStreamServer: base,
		reads:            newByteStreamAdmissionLimiter(maximumReads, "Read"),
		writes:           newByteStreamAdmissionLimiter(maximumWrites, "Write"),
	}, nil
}

func (s *admissionControlledByteStreamServer) Read(request *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	if err := s.reads.acquire(stream.Context()); err != nil {
		return err
	}
	defer s.reads.release()
	return s.ByteStreamServer.Read(request, stream)
}

func (s *admissionControlledByteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	// Reserve before Recv so clients that have not sent the first message
	// still count towards the write limit without allocating a decoder.
	if err := s.writes.acquire(stream.Context()); err != nil {
		return err
	}
	defer s.writes.release()
	return s.ByteStreamServer.Write(stream)
}
