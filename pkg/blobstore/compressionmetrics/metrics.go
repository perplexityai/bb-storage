// Package compressionmetrics exposes bounded-label metrics for compressed CAS.
package compressionmetrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	register  sync.Once
	reserved  = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "staging_reserved_bytes", Help: "Bytes reserved by active compressed CAS uploads, including waits for upload codecs."})
	active    = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "active_requests", Help: "Compressed CAS Put requests holding admission slots, including duplicates and staging waiters."})
	rejected  = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "rejected_requests_total", Help: "Compressed CAS requests rejected by queue or staging admission."}, []string{"reason"})
	waits     = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "staging_wait_duration_seconds", Help: "Time spent acquiring staging bytes, including immediately admitted requests.", Buckets: prometheus.ExponentialBuckets(0.001, 4, 9)}, []string{"outcome"})
	puts      = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "puts_total", Help: "Successful compressed CAS Put paths; duplicate means detected before encoding."}, []string{"outcome"})
	logical   = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "logical_bytes_total", Help: "Logical bytes of successful Put outcomes; duplicate bytes are not written again."}, []string{"outcome"})
	physical  = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "physical_bytes_total", Help: "Representation bytes accepted by successful indexed or tiny-raw Put calls; excludes detected duplicates, but includes races and downstream canonical deduplication."}, []string{"outcome"})
	transfers = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "buildbarn", Subsystem: "bytestream_compression", Name: "transfers_total", Help: "Selected ByteStream transfer paths, including attempts that later fail. Reads are counted at the server; uploads at the forwarding client."}, []string{"direction", "path"})
	integrity = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "buildbarn", Subsystem: "cas_compression", Name: "read_integrity_errors_total", Help: "Verified corrupt frames, invalid indexes, and unsupported versions; excludes device I/O failures."}, []string{"reason"})
)

func initialize() {
	register.Do(func() {
		prometheus.MustRegister(reserved, active, rejected, waits, puts, logical, physical, transfers, integrity)
	})
}
func RequestStarted()      { initialize(); active.Inc() }
func RequestFinished()     { active.Dec() }
func Reject(reason string) { initialize(); rejected.WithLabelValues(reason).Inc() }
func Reserve(bytes int64)  { initialize(); reserved.Add(float64(bytes)) }
func Release(bytes int64)  { reserved.Sub(float64(bytes)) }
func StagingWait(start time.Time, outcome string) {
	initialize()
	waits.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}

func Put(outcome string, logicalBytes, physicalBytes int64) {
	initialize()
	puts.WithLabelValues(outcome).Inc()
	logical.WithLabelValues(outcome).Add(float64(logicalBytes))
	if outcome != "duplicate" {
		physical.WithLabelValues(outcome).Add(float64(physicalBytes))
	}
}

func Transfer(direction string, encoded bool) {
	initialize()
	path := "logical_fallback"
	if encoded {
		path = "compressed_passthrough"
	}
	transfers.WithLabelValues(direction, path).Inc()
}
func Integrity(reason string) { initialize(); integrity.WithLabelValues(reason).Inc() }
