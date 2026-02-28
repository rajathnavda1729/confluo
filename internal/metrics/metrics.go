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
	// HandleErrorsTotal counts record handle failures (e.g. bad payload, store error).
	HandleErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "handle_errors_total",
			Help:      "Total record handle failures",
		},
		[]string{"config"},
	)
	// CommitErrorsTotal counts Kafka commit failures after a successful handle.
	CommitErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "commit_errors_total",
			Help:      "Total Kafka commit failures",
		},
		[]string{"config"},
	)
	// EgressFailuresTotal counts produce failures to egress or corrections topic (consumer, delay queue, timeout manager).
	EgressFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "egress_failures_total",
			Help:      "Total egress produce failures",
		},
		[]string{"config", "phase"}, // phase: main, delay, timeout, correction
	)
)

func init() {
	prometheus.MustRegister(
		JoinSuccessTotal,
		JoinLatencySeconds,
		StateAgeSeconds,
		EventsProcessedTotal,
		HandleErrorsTotal,
		CommitErrorsTotal,
		EgressFailuresTotal,
	)
}
