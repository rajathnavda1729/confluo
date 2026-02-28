package egress

import (
	"encoding/json"
	"time"
)

// CorrectionKind indicates whether the event is a partial (timeout) or full (late arrival update).
type CorrectionKind string

const (
	KindPartial CorrectionKind = "partial"
	KindFull    CorrectionKind = "full"
	KindLate    CorrectionKind = "late"
)

// CorrectionEvent is emitted to the corrections topic when partial egress was already sent
// and later data arrived (late arrival), so downstream can upsert.
type CorrectionEvent struct {
	Kind      CorrectionKind `json:"kind"`
	JoinKey   string         `json:"join_key"`   // join_key_raw for downstream idempotent upsert
	Timestamp time.Time      `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`   // projected document
}

// Marshal returns the JSON bytes for the correction event.
func (c *CorrectionEvent) Marshal() ([]byte, error) {
	return json.Marshal(c)
}
