// Package consumer runs the join processor: consume from input topic, upsert state,
// publish on completion. For strict per-key FIFO ordering, the input topic must be
// produced with Kafka message key = join key (or join_key_hash) so that all events
// for the same key land in the same partition and are processed in order.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluo/omni-joiner/internal/config"
	"github.com/confluo/omni-joiner/internal/egress"
	"github.com/confluo/omni-joiner/internal/engine"
	"github.com/confluo/omni-joiner/internal/keys"
	"github.com/confluo/omni-joiner/internal/metrics"
	"github.com/confluo/omni-joiner/internal/projection"
	"github.com/confluo/omni-joiner/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

const streamIDHeader = "x-stream-id"

// Producer sends messages to Kafka. Implemented by internal/kafka.Client.
// Defined here so delay and timeout packages can use it without depending on kafka.
type Producer interface {
	ProduceSync(ctx context.Context, topic string, key, value []byte) error
}

// Record is a single Kafka message (topic, key, value, headers).
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []struct{ Key, Value string }
}

// Handler processes consumed messages.
type Handler struct {
	store             *store.Store
	joinCfg           *config.JoinConfig
	engine            *engine.Engine
	producer          Producer
	egressTopic       string
	bloom             BloomChecker
	timeoutSched      TimeoutScheduler
	delayQueue        DelayPublisher
	lateTracker       LateArrivalTracker
	correctionsTopic  string
}

// LateArrivalTracker indicates whether a key had partial egress (so we can send correction on late arrival).
type LateArrivalTracker interface {
	Add(ctx context.Context, joinKeyHash []byte) error
	Contains(ctx context.Context, joinKeyHash []byte) (bool, error)
	Remove(ctx context.Context, joinKeyHash []byte) error
}

// DelayPublisher schedules a completed join to be published after a settle delay.
type DelayPublisher interface {
	Schedule(joinKeyHash, joinedBytes, msgKey []byte, publishAt time.Time)
}

// BloomChecker is the subset of bloom operations the handler needs.
type BloomChecker interface {
	Exists(ctx context.Context, keyHash []byte) (bool, error)
	Add(ctx context.Context, keyHash []byte) error
}

// TimeoutScheduler schedules a timeout for a join key.
type TimeoutScheduler interface {
	ScheduleTimeout(ctx context.Context, joinKeyHash []byte, deadline time.Time) error
}

// NewHandler builds a handler that uses the store, join config, and optional producer for egress.
func NewHandler(store *store.Store, joinCfg *config.JoinConfig, producer Producer, egressTopic string, bloom BloomChecker, timeoutSched TimeoutScheduler, delayQueue DelayPublisher, lateTracker LateArrivalTracker, correctionsTopic string) *Handler {
	return &Handler{
		store:            store,
		joinCfg:          joinCfg,
		engine:           engine.New(joinCfg),
		producer:         producer,
		egressTopic:      egressTopic,
		bloom:            bloom,
		timeoutSched:     timeoutSched,
		delayQueue:       delayQueue,
		lateTracker:      lateTracker,
		correctionsTopic: correctionsTopic,
	}
}

// Handle processes one Kafka message.
func (h *Handler) Handle(ctx context.Context, rec *Record) error {
	streamID, err := getStreamID(rec)
	if err != nil {
		return err
	}
	joinKeyHash, joinKeyRaw, err := h.extractKey(rec.Value)
	if err != nil {
		return err
	}
	payload := rec.Value
	if payload == nil {
		payload = []byte("{}")
	}

	metrics.EventsProcessedTotal.WithLabelValues(h.joinCfg.Name, streamID).Inc()

	var state *store.JoinState
	if h.bloom != nil {
		exists, err := h.bloom.Exists(ctx, joinKeyHash)
		if err != nil {
			return fmt.Errorf("bloom exists: %w", err)
		}
		if !exists {
			if err := h.store.UpsertParticipantOnly(ctx, joinKeyHash, joinKeyRaw, h.joinCfg.ConfigID, streamID, payload); err != nil {
				return fmt.Errorf("upsert participant only: %w", err)
			}
			if err := h.bloom.Add(ctx, joinKeyHash); err != nil {
				return fmt.Errorf("bloom add: %w", err)
			}
			if h.timeoutSched != nil {
				ttl := h.joinCfg.TTL.ToDuration()
				if ttl > 0 {
					_ = h.timeoutSched.ScheduleTimeout(ctx, joinKeyHash, time.Now().Add(ttl))
				}
			}
			return nil
		}
	}

	state, err = h.store.UpsertParticipant(ctx, joinKeyHash, joinKeyRaw, h.joinCfg.ConfigID, streamID, payload)
	if err != nil {
		return fmt.Errorf("upsert participant: %w", err)
	}

	if state != nil && len(state.ParticipantData) < h.joinCfg.N() && h.lateTracker != nil && h.correctionsTopic != "" {
		ok, _ := h.lateTracker.Contains(ctx, joinKeyHash)
		if ok {
			partial, err := projection.Apply(state.ParticipantData, h.joinCfg.Projection)
			if err != nil {
				return err
			}
			corr := &egress.CorrectionEvent{
				Kind:      egress.KindLate,
				JoinKey:   state.JoinKeyRaw,
				Timestamp: time.Now(),
				Payload:   json.RawMessage(partial),
			}
			corrBytes, _ := corr.Marshal()
			_ = h.publishTo(ctx, h.correctionsTopic, corrBytes, joinKeyHash)
			_ = h.lateTracker.Remove(ctx, joinKeyHash)
			_ = h.store.DeleteState(ctx, joinKeyHash)
			return nil
		}
	}

	joinStart := time.Now()
	res, err := h.engine.Process(ctx, state)
	if err != nil {
		return err
	}

	if len(res.JoinedBytes) > 0 {
		delay := h.joinCfg.PostJoinDelay.ToDuration()
		if h.delayQueue != nil && delay > 0 {
			h.delayQueue.Schedule(joinKeyHash, res.JoinedBytes, rec.Key, time.Now().Add(delay))
			return nil
		}
		if err := h.publish(ctx, res.JoinedBytes, rec.Key); err != nil {
			return err
		}
		metrics.JoinSuccessTotal.WithLabelValues(h.joinCfg.Name).Inc()
		metrics.JoinLatencySeconds.WithLabelValues(h.joinCfg.Name).Observe(time.Since(joinStart).Seconds())
		if state != nil && len(state.ArrivalTimestamps) > 0 {
			var oldest time.Time
			first := true
			for _, t := range state.ArrivalTimestamps {
				if first || t.Before(oldest) {
					oldest = t
					first = false
				}
			}
			if !first {
				metrics.StateAgeSeconds.WithLabelValues(h.joinCfg.Name).Observe(time.Since(oldest).Seconds())
			}
		}
		if err := h.store.DeleteState(ctx, joinKeyHash); err != nil {
			return fmt.Errorf("delete state after egress: %w", err)
		}
	}
	return nil
}

func getStreamID(rec *Record) (string, error) {
	for _, h := range rec.Headers {
		if h.Key == streamIDHeader {
			return h.Value, nil
		}
	}
	return "", fmt.Errorf("missing header %q", streamIDHeader)
}

func (h *Handler) extractKey(payload []byte) (hash []byte, raw string, err error) {
	var m map[string]interface{}
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, "", err
	}
	if h.joinCfg.Key.Field != "" {
		v, _ := m[h.joinCfg.Key.Field].(string)
		hash, raw = keys.CompositeKey(v, nil)
		return hash, raw, nil
	}
	hash, raw = keys.KeyFromFields(h.joinCfg.Key.Fields, m)
	return hash, raw, nil
}

func (h *Handler) publish(ctx context.Context, value []byte, key []byte) error {
	return h.publishTo(ctx, h.egressTopic, value, key)
}

func (h *Handler) publishTo(ctx context.Context, topic string, value []byte, key []byte) error {
	if h.producer == nil {
		return nil
	}
	return h.producer.ProduceSync(ctx, topic, key, value)
}

// Run consumes from the topic using the kgo client, processes each record with the handler, and commits.
// The client must have been created with ConsumerGroup and ConsumeTopics for the given topic.
func Run(ctx context.Context, client *kgo.Client, joinCfg *config.JoinConfig, store *store.Store, producer Producer, egressTopic string, bloom BloomChecker, timeoutSched TimeoutScheduler, delayQueue DelayPublisher, lateTracker LateArrivalTracker, correctionsTopic string) error {
	handler := NewHandler(store, joinCfg, producer, egressTopic, bloom, timeoutSched, delayQueue, lateTracker, correctionsTopic)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != nil {
					return e.Err
				}
			}
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, rec := range p.Records {
				r := recordFromKgo(rec)
				if err := handler.Handle(ctx, r); err != nil {
					continue
				}
				_ = client.CommitRecords(ctx, rec)
			}
		})
		client.AllowRebalance()
	}
}

func recordFromKgo(rec *kgo.Record) *Record {
	r := &Record{
		Topic: rec.Topic,
		Key:   rec.Key,
		Value: rec.Value,
	}
	for _, h := range rec.Headers {
		r.Headers = append(r.Headers, struct{ Key, Value string }{Key: string(h.Key), Value: string(h.Value)})
	}
	return r
}
