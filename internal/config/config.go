package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

// EgressPolicy defines when to publish joined or partial results.
type EgressPolicy string

const (
	EgressInner   EgressPolicy = "inner"   // Publish only when all N participants arrive
	EgressPartial EgressPolicy = "partial" // Publish available data after timeout
)

// Duration is a time.Duration that unmarshals from JSON as nanoseconds (int) or string (e.g. "10m").
type Duration time.Duration

func (d Duration) ToDuration() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case float64:
		*d = Duration(int64(x))
		return nil
	case string:
		t, err := time.ParseDuration(x)
		if err != nil {
			return err
		}
		*d = Duration(t)
		return nil
	default:
		return nil
	}
}

// KeyDef describes the join key: single field or composite.
type KeyDef struct {
	// Single key: one field name from the event payload.
	Field string `json:"field,omitempty"`
	// Composite: ordered list of field names; all must be present.
	Fields []string `json:"fields,omitempty"`
}

// ProjectionField maps one output field to a stream and source field.
type ProjectionField struct {
	Output string `json:"output"` // e.g. "price"
	Stream string `json:"stream"` // e.g. "A"
	Field  string `json:"field"`  // e.g. "price"
}

// JoinConfig defines one N-way join: streams, key, TTL, projection, egress.
type JoinConfig struct {
	ConfigID      uuid.UUID         `json:"config_id"`
	Name          string            `json:"name"`
	StreamIDs     []string          `json:"stream_ids"` // N streams, e.g. ["StreamA", "StreamB"]
	Key           KeyDef            `json:"key"`
	TTL           Duration          `json:"ttl"`                       // Per join-group TTL (nanoseconds or string e.g. "10m")
	Projection    []ProjectionField `json:"projection"`                // Output schema
	Egress        EgressPolicy      `json:"egress"`                    // inner | partial
	PostJoinDelay Duration          `json:"post_join_delay,omitempty"` // Optional settle time before publish
}

// ProcessorConfig is the top-level config for the processor (brokers, store, etc.).
type ProcessorConfig struct {
	KafkaBrokers     []string    `json:"kafka_brokers"`
	InputTopic       string      `json:"input_topic"`
	EgressTopic      string      `json:"egress_topic"`
	ConfigPath       string      `json:"config_path"`           // Path to join config file or dir
	JoinConfig       *JoinConfig `json:"join_config,omitempty"` // Or inline
	ScyllaHosts      []string    `json:"scylla_hosts"`
	ScyllaKeyspace   string      `json:"scylla_keyspace"`
	RedisAddr        string      `json:"redis_addr"`
	RedisBloomKey    string      `json:"redis_bloom_key"`   // Key for the Bloom filter
	CorrectionsTopic string      `json:"corrections_topic"` // Optional; for late-arrival correction events
}

// LoadJoinConfig loads a JoinConfig from a JSON file.
func LoadJoinConfig(path string) (*JoinConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load join config from %s: %w", path, err)
	}
	var c JoinConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("load join config from %s: %w", path, err)
	}
	return &c, nil
}

// N returns the number of streams (join width).
func (c *JoinConfig) N() int { return len(c.StreamIDs) }
