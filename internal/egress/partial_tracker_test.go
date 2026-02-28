package egress

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisPartialTracker_AddContainsRemove(t *testing.T) {
	// Use miniredis or skip if Redis not available
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available:", err)
	}
	key := "test:partial:" + t.Name()
	defer rdb.Del(ctx, key)

	tracker := NewRedisPartialTracker(rdb, key)
	hash := []byte("join-key-hash-123")

	if err := tracker.Add(ctx, hash); err != nil {
		t.Fatal(err)
	}
	ok, err := tracker.Contains(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected Contains true after Add")
	}
	if err := tracker.Remove(ctx, hash); err != nil {
		t.Fatal(err)
	}
	ok, err = tracker.Contains(ctx, hash)
	if err != nil || ok {
		t.Errorf("expected Contains false after Remove: ok=%v err=%v", ok, err)
	}
}
