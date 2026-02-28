package bloom

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Filter provides a Redis-backed Bloom filter for "maybe seen" join keys.
type Filter struct {
	client redis.UniversalClient
	key    string
}

// New creates a Bloom filter using the given Redis client and filter key name.
func New(client redis.UniversalClient, filterKey string) *Filter {
	return &Filter{client: client, key: filterKey}
}

// Exists returns true if the key might have been seen (or definitely not if false).
// keyHash is the join_key_hash blob; we store it as hex for readability in Redis.
func (f *Filter) Exists(ctx context.Context, keyHash []byte) (bool, error) {
	item := hex.EncodeToString(keyHash)
	v, err := f.client.Do(ctx, "BF.EXISTS", f.key, item).Bool()
	if err != nil {
		return false, fmt.Errorf("bloom exists: %w", err)
	}
	return v, nil
}

// Add adds the key to the filter. Call after first write to state.
func (f *Filter) Add(ctx context.Context, keyHash []byte) error {
	item := hex.EncodeToString(keyHash)
	_, err := f.client.Do(ctx, "BF.ADD", f.key, item).Result()
	if err != nil {
		return fmt.Errorf("bloom add: %w", err)
	}
	return nil
}

// Reserve creates the filter with desired capacity and error rate if it doesn't exist.
// Optional; Redis creates with defaults if not called. errorRate should be < 0.01 for <1% false positives.
func (f *Filter) Reserve(ctx context.Context, capacity uint64, errorRate float64) error {
	_, err := f.client.Do(ctx, "BF.RESERVE", f.key, errorRate, capacity).Result()
	if err != nil && err.Error() != "ERR item exists" {
		return fmt.Errorf("bloom reserve: %w", err)
	}
	return nil
}
