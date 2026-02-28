package egress

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const defaultPartialSetKey = "omni_joiner:partial"

// PartialTracker records which join keys have had a partial egress published,
// so late arrivals can be detected and correction events sent.
type PartialTracker interface {
	Add(ctx context.Context, joinKeyHash []byte) error
	Contains(ctx context.Context, joinKeyHash []byte) (bool, error)
	Remove(ctx context.Context, joinKeyHash []byte) error
}

// RedisPartialTracker uses a Redis set to track keys that were partially published.
type RedisPartialTracker struct {
	redis redis.UniversalClient
	key   string
}

// NewRedisPartialTracker creates a PartialTracker backed by Redis.
func NewRedisPartialTracker(redis redis.UniversalClient, setKey string) *RedisPartialTracker {
	if setKey == "" {
		setKey = defaultPartialSetKey
	}
	return &RedisPartialTracker{redis: redis, key: setKey}
}

// Add marks the join key as having had a partial published.
func (r *RedisPartialTracker) Add(ctx context.Context, joinKeyHash []byte) error {
	member := hex.EncodeToString(joinKeyHash)
	_, err := r.redis.SAdd(ctx, r.key, member).Result()
	if err != nil {
		return fmt.Errorf("partial tracker add: %w", err)
	}
	return nil
}

// Contains returns true if this key was partially published (late arrival possible).
func (r *RedisPartialTracker) Contains(ctx context.Context, joinKeyHash []byte) (bool, error) {
	member := hex.EncodeToString(joinKeyHash)
	ok, err := r.redis.SIsMember(ctx, r.key, member).Result()
	if err != nil {
		return false, fmt.Errorf("partial tracker contains: %w", err)
	}
	return ok, nil
}

// Remove removes the key from the set after sending a correction.
func (r *RedisPartialTracker) Remove(ctx context.Context, joinKeyHash []byte) error {
	member := hex.EncodeToString(joinKeyHash)
	_, err := r.redis.SRem(ctx, r.key, member).Result()
	if err != nil {
		return fmt.Errorf("partial tracker remove: %w", err)
	}
	return nil
}
