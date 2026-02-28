package delay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestQueue_ScheduleAndFlushDue(t *testing.T) {
	// Use nil store and producer; we only test that Schedule enqueues and flushDue drains due items
	q := NewQueue(nil, nil, "test-topic", "test-config", nil, nil, "")

	hash := []byte("hash123")
	payload := []byte(`{"a":1}`)
	key := []byte("key")

	now := time.Now()
	past := now.Add(-time.Second)
	future := now.Add(time.Hour)

	q.Schedule(hash, payload, key, past)
	q.Schedule([]byte("other"), payload, key, future)

	// flushDue should process the past item only
	ctx := context.Background()
	q.flushDue(ctx)

	// We can't easily assert store.DeleteState without a mock; just ensure no panic and pending is reduced
	// After flushDue, only the future item should remain (pending slice has 1 element)
	q.mu.Lock()
	left := len(q.pending)
	q.mu.Unlock()
	if left != 1 {
		t.Errorf("expected 1 pending after flush, got %d", left)
	}
}

func TestQueue_ScheduleCopiesBytes(t *testing.T) {
	q := NewQueue(nil, nil, "topic", "test-config", nil, nil, "")
	hash := []byte("hash")
	payload := []byte("payload")
	key := []byte("key")
	q.Schedule(hash, payload, key, time.Now().Add(time.Minute))
	// Mutate originals; queue should have copies
	hash[0] = 'x'
	payload[0] = 'y'
	q.mu.Lock()
	p := q.pending[0]
	q.mu.Unlock()
	if string(p.joinKeyHash) == "xash" || string(p.joinedBytes) == "yayload" {
		t.Error("Schedule should copy bytes")
	}
}

func TestQueue_RunStopsOnContextCancel(t *testing.T) {
	q := NewQueue(nil, nil, "topic", "test-config", nil, nil, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestQueue_RedisDurable_ScheduleAndFlush(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available:", err)
	}
	key := "test:delay:durable:" + t.Name()
	defer rdb.Del(ctx, key)

	q := NewQueue(nil, nil, "egress", "test-config", nil, rdb, key)
	joinKeyHash := []byte("hash123")
	joinedBytes := []byte(`{"a":1}`)
	msgKey := []byte("key1")
	publishAt := time.Now().Add(-time.Second) // already due
	q.Schedule(joinKeyHash, joinedBytes, msgKey, publishAt)

	n, err := rdb.ZCard(ctx, key).Result()
	if err != nil || n != 1 {
		t.Fatalf("expected 1 member in ZSET: err=%v n=%d", err, n)
	}
	q.flushDue(ctx)
	n, _ = rdb.ZCard(ctx, key).Result()
	if n != 0 {
		t.Errorf("expected 0 members after flush, got %d", n)
	}
}

func TestDelayedItem_SerializeRoundTrip(t *testing.T) {
	payload := delayedItem{
		H: base64.StdEncoding.EncodeToString([]byte("hash")),
		J: base64.StdEncoding.EncodeToString([]byte(`{"x":1}`)),
		K: base64.StdEncoding.EncodeToString([]byte("key")),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded delayedItem
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.H != payload.H || decoded.J != payload.J || decoded.K != payload.K {
		t.Error("round trip mismatch")
	}
}
