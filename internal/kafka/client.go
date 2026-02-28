// Package kafka provides a Kafka producer abstraction using franz-go (Redpanda-validated client).
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Producer sends messages to Kafka. Implementations are safe for concurrent use.
type Producer interface {
	ProduceSync(ctx context.Context, topic string, key, value []byte) error
}

// Client wraps a kgo.Client to implement Producer and to run the consumer group loop.
type Client struct {
	*kgo.Client
}

// NewClient creates a Kafka client that can produce and consume. Uses franz-go so Redpanda v24+ works.
func NewClient(brokers []string, groupID, consumeTopic string) (*Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(consumeTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Client{Client: cl}, nil
}

// ProduceSync sends a single message and blocks until acknowledged.
func (c *Client) ProduceSync(ctx context.Context, topic string, key, value []byte) error {
	err := c.Client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}).FirstErr()
	if err != nil {
		return fmt.Errorf("produce %s: %w", topic, err)
	}
	return nil
}

// Close closes the client. Best-effort; call defer c.Close() for cleanup.
func (c *Client) Close() {
	c.Client.Close()
}

// Ping verifies connectivity to the broker. Use for readiness probes.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.Client.Request(ctx, &kmsg.MetadataRequest{})
	if err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}
	return nil
}
