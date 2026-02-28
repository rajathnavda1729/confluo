package consumer

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/confluo/omni-joiner/internal/config"
)

func TestHandler_ExtractKey_SingleField(t *testing.T) {
	joinCfg := &config.JoinConfig{
		Key: config.KeyDef{Field: "order_id"},
	}
	h := &Handler{joinCfg: joinCfg}
	payload := []byte(`{"order_id":"ord-123","price":99}`)
	hash, raw, err := h.extractKey(payload)
	if err != nil {
		t.Fatal(err)
	}
	if raw != "ord-123" {
		t.Errorf("raw: got %q", raw)
	}
	if len(hash) != 32 {
		t.Errorf("hash length: got %d", len(hash))
	}
}

func TestHandler_ExtractKey_Composite(t *testing.T) {
	joinCfg := &config.JoinConfig{
		Key: config.KeyDef{Fields: []string{"tenant", "id"}},
	}
	h := &Handler{joinCfg: joinCfg}
	payload := []byte(`{"tenant":"acme","id":"o1","extra":1}`)
	hash, raw, err := h.extractKey(payload)
	if err != nil {
		t.Fatal(err)
	}
	if raw == "" || len(hash) != 32 {
		t.Errorf("hash=%d raw=%q", len(hash), raw)
	}
}

func TestHandler_ExtractKey_InvalidJSON(t *testing.T) {
	h := &Handler{joinCfg: &config.JoinConfig{}}
	_, _, err := h.extractKey([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestHandler_ExtractKey_EmptyPayload(t *testing.T) {
	joinCfg := &config.JoinConfig{Key: config.KeyDef{Field: "x"}}
	h := &Handler{joinCfg: joinCfg}
	hash, raw, err := h.extractKey([]byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if raw != "" {
		t.Errorf("raw: got %q", raw)
	}
	// Hash of empty string
	if len(hash) != 32 {
		t.Errorf("hash length: got %d", len(hash))
	}
}

// Test config_id JSON unmarshal for join config (used by consumer indirectly)
func TestJoinConfig_ConfigID_Unmarshal(t *testing.T) {
	data := []byte(`{"config_id":"550e8400-e29b-41d4-a716-446655440000","name":"test","stream_ids":["A","B"],"key":{},"ttl":600,"projection":[],"egress":"inner"}`)
	var c config.JoinConfig
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	if c.ConfigID != uuid.MustParse("550e8400-e29b-41d4-a716-446655440000") {
		t.Errorf("config_id: %v", c.ConfigID)
	}
}
