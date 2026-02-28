package delay

import (
	"context"
	"testing"
	"time"

	"github.com/confluo/omni-joiner/internal/store"
)

func TestQueue_ScheduleAndFlushDue(t *testing.T) {
	// Use nil store and producer; we only test that Schedule enqueues and flushDue drains due items
	q := NewQueue((*store.Store)(nil), nil, "test-topic", "test-config")

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
	q := NewQueue((*store.Store)(nil), nil, "topic", "test-config")
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
	q := NewQueue((*store.Store)(nil), nil, "topic", "test-config")
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
