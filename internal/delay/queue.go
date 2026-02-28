package delay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/confluo/omni-joiner/internal/consumer"
	"github.com/confluo/omni-joiner/internal/logger"
	"github.com/confluo/omni-joiner/internal/metrics"
	"github.com/confluo/omni-joiner/internal/store"
)

const defaultBatchSize = 100

// Pending is a join result waiting for the settle delay to pass.
type pending struct {
	joinKeyHash []byte
	joinedBytes []byte
	msgKey      []byte
	publishAt   time.Time
}

// delayedItem is the Redis ZSET member format (JSON with base64-encoded bytes).
type delayedItem struct {
	H string `json:"h"` // join_key_hash base64
	J string `json:"j"` // joined_bytes base64
	K string `json:"k"` // msg_key base64
}

// Queue holds completed joins that should be published after a settle delay.
// When redis and redisKey are set, pending items are stored in a Redis ZSET (score = publishAt)
// so they survive process restarts. Otherwise only in-memory (existing behavior).
type Queue struct {
	store      store.StateStore
	producer   consumer.Producer
	topic      string
	configName string
	log        logger.Logger
	redis      redis.UniversalClient
	redisKey   string
	mu         sync.Mutex
	pending    []pending // used only when redis == nil
	tick       time.Duration
	stop       chan struct{}
}

// NewQueue creates a delay queue that publishes to the given topic after each item's publishAt.
// stateStore can be *store.Store (single-config) or *store.ScopedStore (multi-config).
// If redis is not nil and redisKey is non-empty, pending items are persisted in the Redis ZSET.
func NewQueue(stateStore store.StateStore, producer consumer.Producer, topic string, configName string, log logger.Logger, redis redis.UniversalClient, redisKey string) *Queue {
	if log == nil {
		log = logger.Global
	}
	return &Queue{
		store:      stateStore,
		producer:   producer,
		topic:      topic,
		configName: configName,
		log:        log,
		redis:      redis,
		redisKey:   redisKey,
		tick:       50 * time.Millisecond,
		stop:       make(chan struct{}),
	}
}

// Schedule adds a completed join to be published at publishAt (typically now + post_join_delay).
func (q *Queue) Schedule(joinKeyHash, joinedBytes, msgKey []byte, publishAt time.Time) {
	if q.redis != nil && q.redisKey != "" {
		q.scheduleRedis(context.Background(), joinKeyHash, joinedBytes, msgKey, publishAt)
		return
	}
	item := pending{
		joinKeyHash: copyBytes(joinKeyHash),
		joinedBytes: copyBytes(joinedBytes),
		msgKey:      copyBytes(msgKey),
		publishAt:   publishAt,
	}
	q.mu.Lock()
	q.pending = append(q.pending, item)
	q.mu.Unlock()
}

func (q *Queue) scheduleRedis(ctx context.Context, joinKeyHash, joinedBytes, msgKey []byte, publishAt time.Time) {
	payload := delayedItem{
		H: base64.StdEncoding.EncodeToString(joinKeyHash),
		J: base64.StdEncoding.EncodeToString(joinedBytes),
		K: base64.StdEncoding.EncodeToString(msgKey),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		q.log.Error("delay queue marshal failed", "error", err)
		metrics.EgressFailuresTotal.WithLabelValues(q.configName, "delay").Inc()
		return
	}
	score := float64(publishAt.UnixNano())
	if err := q.redis.ZAdd(ctx, q.redisKey, redis.Z{Score: score, Member: string(b)}).Err(); err != nil {
		q.log.Error("delay queue ZADD failed", "error", err)
		metrics.EgressFailuresTotal.WithLabelValues(q.configName, "delay").Inc()
	}
}

// Run processes due items until ctx is cancelled. Call in a goroutine.
func (q *Queue) Run(ctx context.Context) {
	ticker := time.NewTicker(q.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.flushDue(ctx)
		}
	}
}

func (q *Queue) flushDue(ctx context.Context) {
	if q.redis != nil && q.redisKey != "" {
		q.flushDueRedis(ctx)
		return
	}
	q.flushDueMemory(ctx)
}

func (q *Queue) flushDueMemory(ctx context.Context) {
	now := time.Now()
	q.mu.Lock()
	var due []pending
	var keep []pending
	for _, p := range q.pending {
		if !p.publishAt.After(now) {
			due = append(due, p)
		} else {
			keep = append(keep, p)
		}
	}
	q.pending = keep
	q.mu.Unlock()

	for _, p := range due {
		q.publishAndCleanup(ctx, p.joinKeyHash, p.joinedBytes, p.msgKey)
	}
}

func (q *Queue) flushDueRedis(ctx context.Context) {
	now := time.Now().UnixNano()
	max := fmt.Sprintf("%d", now)
	members, err := q.redis.ZRangeByScore(ctx, q.redisKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   max,
		Count: int64(defaultBatchSize),
	}).Result()
	if err != nil {
		q.log.Warn("delay queue ZRANGEBYSCORE failed", "error", err)
		return
	}
	for _, member := range members {
		var payload delayedItem
		if err := json.Unmarshal([]byte(member), &payload); err != nil {
			q.log.Warn("delay queue unmarshal failed", "error", err)
			q.redis.ZRem(ctx, q.redisKey, member)
			continue
		}
		joinKeyHash, _ := base64.StdEncoding.DecodeString(payload.H)
		joinedBytes, _ := base64.StdEncoding.DecodeString(payload.J)
		msgKey, _ := base64.StdEncoding.DecodeString(payload.K)
		q.publishAndCleanup(ctx, joinKeyHash, joinedBytes, msgKey)
		if err := q.redis.ZRem(ctx, q.redisKey, member).Err(); err != nil {
			q.log.Warn("delay queue ZREM failed", "error", err)
		}
	}
}

func (q *Queue) publishAndCleanup(ctx context.Context, joinKeyHash, joinedBytes, msgKey []byte) {
	if q.producer != nil {
		if err := q.producer.ProduceSync(ctx, q.topic, msgKey, joinedBytes); err != nil {
			metrics.EgressFailuresTotal.WithLabelValues(q.configName, "delay").Inc()
			q.log.Warn("delay queue produce failed", "key", msgKey, "error", err)
			return
		}
	}
	if q.store != nil {
		//nolint:errcheck // best-effort cleanup after successful produce
		_ = q.store.DeleteState(ctx, joinKeyHash)
	}
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
