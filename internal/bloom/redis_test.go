package bloom

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestFilter_ExistsAdd_RequiresRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available:", err)
	}
	key := "test:bloom:" + t.Name()
	defer rdb.Del(ctx, key)

	f := New(rdb, key)
	hash := []byte("join-key-hash")

	ok, err := f.Exists(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected Exists false before Add")
	}
	if err := f.Add(ctx, hash); err != nil {
		t.Fatal(err)
	}
	ok, err = f.Exists(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected Exists true after Add")
	}
}
