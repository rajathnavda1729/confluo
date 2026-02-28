package timeout

import (
	"context"
	"testing"
	"time"

	"github.com/confluo/omni-joiner/internal/config"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestManager_ScheduleTimeout_RequiresRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available:", err)
	}
	setKey := "test:timeout:" + t.Name()
	defer rdb.Del(ctx, setKey)

	m := New(Config{
		Store:      nil,
		JoinConfig: &config.JoinConfig{StreamIDs: []string{"A", "B"}},
		Producer:   nil,
		Redis:      rdb,
		SetKey:     setKey,
	})

	hash := []byte("key-hash-123")
	deadline := time.Now().Add(time.Minute)
	if err := m.ScheduleTimeout(ctx, hash, deadline); err != nil {
		t.Fatal(err)
	}
	// Verify ZADD
	members, err := rdb.ZRangeByScore(ctx, setKey, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("expected 1 member: err=%v len=%d", err, len(members))
	}
}

func TestNew_SetsDefaults(t *testing.T) {
	m := New(Config{
		Store:      nil,
		JoinConfig: &config.JoinConfig{ConfigID: uuid.Nil, StreamIDs: []string{"A"}},
		SetKey:     "",
	})
	if m.setKey != "omni_joiner:timeouts" {
		t.Errorf("default setKey: got %q", m.setKey)
	}
	if m.pollInterval != defaultPollInterval || m.batchSize != defaultBatchSize {
		t.Errorf("defaults: poll=%v batch=%d", m.pollInterval, m.batchSize)
	}
}
