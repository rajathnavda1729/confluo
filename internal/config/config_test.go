package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestDuration_UnmarshalJSON_Number(t *testing.T) {
	var d Duration
	err := json.Unmarshal([]byte("600000000000"), &d)
	if err != nil {
		t.Fatal(err)
	}
	if d.ToDuration().Seconds() != 600 {
		t.Errorf("expected 600s, got %v", d.ToDuration())
	}
}

func TestDuration_UnmarshalJSON_String(t *testing.T) {
	var d Duration
	err := json.Unmarshal([]byte(`"10m"`), &d)
	if err != nil {
		t.Fatal(err)
	}
	if d.ToDuration().Minutes() != 10 {
		t.Errorf("expected 10m, got %v", d.ToDuration())
	}
}

func TestJoinConfig_N(t *testing.T) {
	c := &JoinConfig{StreamIDs: []string{"A", "B", "C"}}
	if c.N() != 3 {
		t.Errorf("N(): got %d", c.N())
	}
}

func TestLoadJoinConfig(t *testing.T) {
	path := filepath.Join("..", "..", "config", "join_example.json")
	if _, err := os.Stat(path); err != nil {
		path = "config/join_example.json"
		if _, err := os.Stat(path); err != nil {
			t.Skip("config/join_example.json not found")
		}
	}
	c, err := LoadJoinConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name == "" || c.N() == 0 || c.ConfigID == uuid.Nil {
		t.Errorf("incomplete config: %+v", c)
	}
	if err := ValidateJoinConfig(c); err != nil {
		t.Errorf("expected valid config: %v", err)
	}
}

func TestValidateJoinConfig_Valid(t *testing.T) {
	valid := &JoinConfig{
		Name:      "test",
		StreamIDs: []string{"orders", "shipments"},
		Key:       KeyDef{Field: "order_id"},
		TTL:       Duration(600000000000),
		Projection: []ProjectionField{
			{Output: "order_id", Stream: "orders", Field: "order_id"},
			{Output: "status", Stream: "shipments", Field: "status"},
		},
		Egress: EgressInner,
	}
	if err := ValidateJoinConfig(valid); err != nil {
		t.Errorf("expected valid: %v", err)
	}
	validPartial := &JoinConfig{
		Name:      "partial",
		StreamIDs: []string{"A", "B"},
		Key:       KeyDef{Field: "id"},
		TTL:       Duration(60000000000), // 60s
		Egress:    EgressPartial,
	}
	if err := ValidateJoinConfig(validPartial); err != nil {
		t.Errorf("expected valid partial: %v", err)
	}
	validComposite := &JoinConfig{
		Name:      "composite",
		StreamIDs: []string{"stream_a", "stream_b"},
		Key:       KeyDef{Fields: []string{"tenant_id", "order_id"}},
		TTL:       Duration(600000000000),
		Egress:    EgressInner,
	}
	if err := ValidateJoinConfig(validComposite); err != nil {
		t.Errorf("expected valid composite: %v", err)
	}
}

func TestValidateJoinConfig_Nil(t *testing.T) {
	if err := ValidateJoinConfig(nil); err == nil {
		t.Error("expected error for nil config")
	}
}

func TestValidateJoinConfig_EmptyStreamIDs(t *testing.T) {
	c := &JoinConfig{Name: "bad", StreamIDs: nil, Key: KeyDef{Field: "id"}}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for empty stream_ids")
	}
	c.StreamIDs = []string{}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for empty stream_ids")
	}
}

func TestValidateJoinConfig_ProjectionStreamNotInStreamIDs(t *testing.T) {
	c := &JoinConfig{
		Name:      "bad",
		StreamIDs: []string{"orders"},
		Key:       KeyDef{Field: "order_id"},
		Projection: []ProjectionField{
			{Output: "x", Stream: "shipments", Field: "x"},
		},
		Egress: EgressInner,
	}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for projection stream not in stream_ids")
	}
}

func TestValidateJoinConfig_ProjectionEmptyStream(t *testing.T) {
	c := &JoinConfig{
		Name:      "bad",
		StreamIDs: []string{"orders"},
		Key:       KeyDef{Field: "order_id"},
		Projection: []ProjectionField{{Output: "x", Stream: "", Field: "x"}},
		Egress:    EgressInner,
	}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for empty projection stream")
	}
}

func TestValidateJoinConfig_KeyMissing(t *testing.T) {
	c := &JoinConfig{
		Name:      "bad",
		StreamIDs: []string{"A", "B"},
		Key:       KeyDef{},
		Egress:    EgressInner,
	}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for missing key")
	}
}

func TestValidateJoinConfig_KeyFieldsEmptyElement(t *testing.T) {
	c := &JoinConfig{
		Name:      "bad",
		StreamIDs: []string{"A"},
		Key:       KeyDef{Fields: []string{"a", "", "c"}},
		Egress:    EgressInner,
	}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for empty key.fields element")
	}
}

func TestValidateJoinConfig_PartialRequiresTTL(t *testing.T) {
	c := &JoinConfig{
		Name:      "bad",
		StreamIDs: []string{"A", "B"},
		Key:       KeyDef{Field: "id"},
		TTL:       0,
		Egress:    EgressPartial,
	}
	if err := ValidateJoinConfig(c); err == nil {
		t.Error("expected error for partial egress with zero TTL")
	}
}
