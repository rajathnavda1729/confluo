package keys

import (
	"testing"
)

func TestCompositeKey_Single(t *testing.T) {
	hash, raw := CompositeKey("order-123", nil)
	if raw != "order-123" {
		t.Errorf("raw: got %q", raw)
	}
	if len(hash) != 32 {
		t.Errorf("hash length: got %d", len(hash))
	}
}

func TestCompositeKey_CompositeDeterministic(t *testing.T) {
	m1 := map[string]interface{}{"a": "1", "b": "2"}
	m2 := map[string]interface{}{"b": "2", "a": "1"}
	hash1, raw1 := CompositeKey("", m1)
	hash2, raw2 := CompositeKey("", m2)
	if raw1 != raw2 {
		t.Errorf("raw not deterministic: %q vs %q", raw1, raw2)
	}
	if len(hash1) != 32 || string(hash1) != string(hash2) {
		t.Errorf("hash not deterministic")
	}
}

func TestKeyFromFields(t *testing.T) {
	fields := []string{"tenant", "id"}
	values := map[string]interface{}{"tenant": "acme", "id": "o1", "extra": "ignored"}
	hash, raw := KeyFromFields(fields, values)
	if raw == "" || len(hash) != 32 {
		t.Errorf("unexpected hash=%d raw=%q", len(hash), raw)
	}
}

func TestHashToPartition(t *testing.T) {
	hash := HashRaw("key1") // 32 bytes
	p := HashToPartition(hash, 10)
	if p < 0 || p >= 10 {
		t.Errorf("partition out of range: %d", p)
	}
	if HashToPartition(hash, 0) != 0 {
		t.Errorf("expected 0 for numPartitions 0")
	}
}

func BenchmarkCompositeKey_Single(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CompositeKey("order-12345", nil)
	}
}

func BenchmarkCompositeKey_Composite(b *testing.B) {
	m := map[string]interface{}{
		"order_id": "ord-999",
		"region":   "us-east-1",
		"tenant":   "acme",
	}
	for i := 0; i < b.N; i++ {
		CompositeKey("", m)
	}
}

func BenchmarkHashRaw(b *testing.B) {
	raw := "order-12345-tenant-acme"
	for i := 0; i < b.N; i++ {
		HashRaw(raw)
	}
}

func BenchmarkKeyFromFields(b *testing.B) {
	fields := []string{"tenant", "order_id"}
	values := map[string]interface{}{
		"tenant":   "acme",
		"order_id": "ord-999",
	}
	for i := 0; i < b.N; i++ {
		KeyFromFields(fields, values)
	}
}
