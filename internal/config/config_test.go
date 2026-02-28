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
}
