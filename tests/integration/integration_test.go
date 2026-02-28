// Package integration holds tests that require external services (ScyllaDB, Kafka/Redpanda, Redis).
// Run with: go test -v ./tests/integration/...
// Use -short to skip integration tests: go test -short ./...
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/confluo/omni-joiner/internal/keys"
	"github.com/confluo/omni-joiner/internal/store"
)

// TestStore_JoinFlow_Integration requires ScyllaDB at 127.0.0.1. Skip with -short.
func TestStore_JoinFlow_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cfg := store.Config{Hosts: []string{"127.0.0.1"}, Keyspace: "omni_joiner", Timeout: 5 * time.Second}
	ctx := context.Background()
	if err := store.EnsureKeyspaceAndTable(ctx, cfg); err != nil {
		t.Skip("ScyllaDB not available:", err)
	}
	st, err := store.New(cfg)
	if err != nil {
		t.Skip("ScyllaDB not available:", err)
	}
	defer st.Close()

	joinKey := "order-123"
	hash := keys.HashRaw(joinKey)
	configID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	// First participant
	state1, err := st.UpsertParticipant(ctx, hash, joinKey, configID, "orders", []byte(`{"order_id":"order-123","price":99}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(state1.ParticipantData) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(state1.ParticipantData))
	}

	// Second participant - join complete
	state2, err := st.UpsertParticipant(ctx, hash, joinKey, configID, "shipments", []byte(`{"order_id":"order-123","status":"shipped"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(state2.ParticipantData) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(state2.ParticipantData))
	}

	if err := st.DeleteState(ctx, hash); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetState(ctx, hash)
	if err != nil || got != nil {
		t.Errorf("after delete: expected nil state, got %v err=%v", got, err)
	}
}
