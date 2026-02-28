package projection

import (
	"encoding/json"
	"testing"

	"github.com/confluo/omni-joiner/internal/config"
)

func TestApply(t *testing.T) {
	participantData := map[string][]byte{
		"orders":    []byte(`{"order_id":"o1","price":99.99}`),
		"shipments": []byte(`{"order_id":"o1","status":"shipped"}`),
	}
	proj := []config.ProjectionField{
		{Output: "order_id", Stream: "orders", Field: "order_id"},
		{Output: "price", Stream: "orders", Field: "price"},
		{Output: "status", Stream: "shipments", Field: "status"},
	}
	out, err := Apply(participantData, proj)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["order_id"] != "o1" || m["price"] != 99.99 || m["status"] != "shipped" {
		t.Errorf("unexpected output: %v", m)
	}
}

func BenchmarkApply(b *testing.B) {
	participantData := map[string][]byte{
		"orders":    []byte(`{"order_id":"o1","price":99.99,"quantity":2}`),
		"shipments": []byte(`{"order_id":"o1","status":"shipped","timestamp":"2024-01-15T10:00:00Z"}`),
	}
	proj := []config.ProjectionField{
		{Output: "order_id", Stream: "orders", Field: "order_id"},
		{Output: "price", Stream: "orders", Field: "price"},
		{Output: "status", Stream: "shipments", Field: "status"},
		{Output: "timestamp", Stream: "shipments", Field: "timestamp"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Apply(participantData, proj) //nolint:errcheck // benchmark ignores output
	}
}
