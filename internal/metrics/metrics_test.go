package metrics

import (
	"testing"
)

func TestMetrics_RegisterAndObserve(t *testing.T) {
	// Ensure we can label and increment without panic (already registered in init)
	JoinSuccessTotal.WithLabelValues("test-config").Inc()
	EventsProcessedTotal.WithLabelValues("test-config", "streamA").Inc()
	JoinLatencySeconds.WithLabelValues("test-config").Observe(0.001)
	StateAgeSeconds.WithLabelValues("test-config").Observe(1.0)
	HandleErrorsTotal.WithLabelValues("test-config").Inc()
	CommitErrorsTotal.WithLabelValues("test-config").Inc()
	EgressFailuresTotal.WithLabelValues("test-config", "main").Inc()
	EgressFailuresTotal.WithLabelValues("test-config", "delay").Inc()
	EgressFailuresTotal.WithLabelValues("test-config", "timeout").Inc()
	EgressFailuresTotal.WithLabelValues("test-config", "correction").Inc()
}
