package delay

import (
	"context"
	"sync"
	"time"

	"github.com/confluo/omni-joiner/internal/consumer"
	"github.com/confluo/omni-joiner/internal/store"
)

// Pending is a join result waiting for the settle delay to pass.
type pending struct {
	joinKeyHash []byte
	joinedBytes []byte
	msgKey      []byte
	publishAt   time.Time
}

// Queue holds completed joins that should be published after a settle delay.
type Queue struct {
	store    *store.Store
	producer consumer.Producer
	topic    string
	mu       sync.Mutex
	pending  []pending
	tick     time.Duration
	stop     chan struct{}
}

// NewQueue creates a delay queue that publishes to the given topic after each item's publishAt.
func NewQueue(store *store.Store, producer consumer.Producer, topic string) *Queue {
	return &Queue{
		store:    store,
		producer: producer,
		topic:    topic,
		tick:     50 * time.Millisecond,
		stop:     make(chan struct{}),
	}
}

// Schedule adds a completed join to be published at publishAt (typically now + post_join_delay).
func (q *Queue) Schedule(joinKeyHash, joinedBytes, msgKey []byte, publishAt time.Time) {
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
		if q.producer != nil {
			_ = q.producer.ProduceSync(ctx, q.topic, p.msgKey, p.joinedBytes)
		}
		if q.store != nil {
			_ = q.store.DeleteState(ctx, p.joinKeyHash)
		}
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
