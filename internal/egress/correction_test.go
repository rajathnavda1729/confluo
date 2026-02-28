package egress

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCorrectionEvent_Marshal(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	e := &CorrectionEvent{
		Kind:      KindLate,
		JoinKey:   "o1",
		Timestamp: ts,
		Payload:   json.RawMessage(`{"order_id":"o1","status":"shipped"}`),
	}
	b, err := e.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m["kind"] != "late" || m["join_key"] != "o1" {
		t.Errorf("unexpected marshal: %v", m)
	}
}

func TestCorrectionKind_Constants(t *testing.T) {
	if KindPartial != "partial" || KindFull != "full" || KindLate != "late" {
		t.Errorf("kind constants wrong")
	}
}
