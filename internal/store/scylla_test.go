package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestJoinState_ZeroValue tests that we don't panic on nil maps when building state in tests.
func TestJoinState_ZeroValue(t *testing.T) {
	var state JoinState
	if state.ParticipantData != nil {
		t.Error("zero value ParticipantData should be nil")
	}
	if state.ArrivalTimestamps != nil {
		t.Error("zero value ArrivalTimestamps should be nil")
	}
}

// Integration test: requires ScyllaDB. Run with: go test -tags=integration ./internal/store/...
func TestStore_UpsertAndGetState_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cfg := Config{Hosts: []string{"127.0.0.1"}, Keyspace: "omni_joiner", Timeout: 5 * time.Second}
	ctx := context.Background()
	if err := EnsureKeyspaceAndTable(ctx, cfg); err != nil {
		t.Skip("ScyllaDB not available:", err)
	}
	st, err := New(cfg)
	if err != nil {
		t.Skip("ScyllaDB not available:", err)
	}
	defer st.Close()

	hash := []byte("test-key-hash-12345678") // at least 8 bytes for any hash use
	raw := "test-key-raw"
	configID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	streamID := "StreamA"
	payload := []byte(`{"order_id":"o1"}`)

	state, err := st.UpsertParticipant(ctx, hash, raw, configID, streamID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || len(state.ParticipantData) != 1 || state.ParticipantData[streamID] == nil {
		t.Errorf("unexpected state: %+v", state)
	}

	// Cleanup
	//nolint:errcheck // test teardown best-effort
	_ = st.DeleteState(ctx, hash)
}
