// Package health provides HTTP handlers for liveness and readiness probes.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// StorePinger can run a lightweight check against the state store (e.g. ScyllaDB).
type StorePinger interface {
	Ping(ctx context.Context) error
}

// KafkaPinger can run a lightweight check against Kafka (e.g. metadata request).
type KafkaPinger interface {
	Ping(ctx context.Context) error
}

// RedisPinger can run a ping against Redis (e.g. PING command).
type RedisPinger interface {
	Ping(ctx context.Context) error
}

// Checker holds dependencies for readiness checks. Any field may be nil; nil is skipped.
type Checker struct {
	Store  StorePinger
	Kafka  KafkaPinger
	Redis  RedisPinger
	Timeout time.Duration
}

// NewChecker returns a Checker with default timeout (3s) for each dependency check.
func NewChecker(store StorePinger, kafka KafkaPinger, redis RedisPinger) *Checker {
	return &Checker{
		Store:   store,
		Kafka:   kafka,
		Redis:   redis,
		Timeout: 3 * time.Second,
	}
}

// ServeHTTP serves GET /health (liveness) and GET /ready (readiness). Other methods/paths get 404.
func (c *Checker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	switch r.URL.Path {
	case "/health", "/live":
		c.liveness(w)
	case "/ready":
		c.readiness(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (c *Checker) liveness(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (c *Checker) readiness(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	var failures []string
	if c.Store != nil {
		if err := c.Store.Ping(ctx); err != nil {
			failures = append(failures, "store: "+err.Error())
		}
	}
	if c.Kafka != nil {
		if err := c.Kafka.Ping(ctx); err != nil {
			failures = append(failures, "kafka: "+err.Error())
		}
	}
	if c.Redis != nil {
		if err := c.Redis.Ping(ctx); err != nil {
			failures = append(failures, "redis: "+err.Error())
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if len(failures) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "not ready",
			"failures": failures,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}
