package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/confluo/omni-joiner/internal/config"
	"github.com/confluo/omni-joiner/internal/store"
)

func TestEngine_IsComplete(t *testing.T) {
	joinCfg := &config.JoinConfig{StreamIDs: []string{"A", "B"}}
	e := New(joinCfg)

	if e.IsComplete(nil) {
		t.Error("nil state should not be complete")
	}
	if e.IsComplete(&store.JoinState{}) {
		t.Error("empty state should not be complete")
	}
	if e.IsComplete(&store.JoinState{ParticipantData: map[string][]byte{"A": {}}}) {
		t.Error("one participant should not be complete")
	}
	if !e.IsComplete(&store.JoinState{ParticipantData: map[string][]byte{"A": {}, "B": {}}}) {
		t.Error("two participants should be complete")
	}
}

func TestEngine_Project(t *testing.T) {
	joinCfg := &config.JoinConfig{
		StreamIDs: []string{"A", "B"},
		Projection: []config.ProjectionField{
			{Output: "x", Stream: "A", Field: "x"},
			{Output: "y", Stream: "B", Field: "y"},
		},
	}
	e := New(joinCfg)

	out, err := e.Project(nil)
	if err != nil || out != nil {
		t.Fatalf("Project(nil): err=%v out=%v", err, out)
	}

	state := &store.JoinState{
		ParticipantData: map[string][]byte{
			"A": []byte(`{"x":1}`),
			"B": []byte(`{"y":2}`),
		},
	}
	out, err = e.Project(state)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["x"] != float64(1) || m["y"] != float64(2) {
		t.Errorf("unexpected projection: %v", m)
	}
}

func TestEngine_Process(t *testing.T) {
	joinCfg := &config.JoinConfig{
		ConfigID:  uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		StreamIDs: []string{"A", "B"},
		Projection: []config.ProjectionField{
			{Output: "k", Stream: "A", Field: "k"},
		},
	}
	e := New(joinCfg)
	ctx := context.Background()

	// Incomplete state
	state := &store.JoinState{ParticipantData: map[string][]byte{"A": []byte(`{"k":"v"}`)}}
	res, err := e.Process(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.JoinedBytes) != 0 {
		t.Errorf("expected no joined bytes, got %d", len(res.JoinedBytes))
	}

	// Complete state
	state.ParticipantData["B"] = []byte(`{}`)
	res, err = e.Process(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.JoinedBytes) == 0 {
		t.Error("expected joined bytes")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(res.JoinedBytes, &m); err != nil {
		t.Fatal(err)
	}
	if m["k"] != "v" {
		t.Errorf("unexpected joined: %v", m)
	}
}
