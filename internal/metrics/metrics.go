package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "omni_joiner"

var (
	// JoinSuccessTotal counts successfully completed joins (published to egress).
	JoinSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "join_success_total",
			Help:      "Total number of successful N-way joins published",
		},
		[]string{"config"},
	)
	// JoinLatencySeconds is the histogram of join-to-egress latency (time from last participant arrival to publish).
	JoinLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "join_latency_seconds",
			Help:      "Join-to-egress latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms to ~8s
		},
		[]string{"config"},
	)
	// StateAgeSeconds is the age of the oldest participant in state when join completes (for monitoring).
	StateAgeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "state_age_seconds",
			Help:      "Age of join state in seconds when join completes",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1s to ~12 days
		},
		[]string{"config"},
	)
	// EventsProcessedTotal counts events processed (upserts).
	EventsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_processed_total",
			Help:      "Total events processed (upserts into state)",
		},
		[]string{"config", "stream"},
	)
)

func init() {
	prometheus.MustRegister(JoinSuccessTotal, JoinLatencySeconds, StateAgeSeconds, EventsProcessedTotal)
}
