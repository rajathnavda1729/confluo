package keys

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sort"
)

// CompositeKey builds a deterministic blob from a single field or multiple fields.
// For composite keys, field names are sorted so that {"b": "x", "a": "y"} and {"a": "y", "b": "x"} produce the same hash.
func CompositeKey(single string, composite map[string]interface{}) (hash []byte, raw string) {
	if single != "" {
		return HashRaw(single), single
	}
	raw = serializeComposite(composite)
	return HashRaw(raw), raw
}

// HashRaw returns SHA-256 hash of the input string as the blob for join_key_hash.
func HashRaw(raw string) []byte {
	h := sha256.Sum256([]byte(raw))
	return h[:]
}

// serializeComposite produces a deterministic string from a map (e.g. for composite keys).
func serializeComposite(m map[string]interface{}) string {
	if len(m) == 0 {
		return ""
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make(map[string]interface{}, len(m))
	for _, k := range names {
		out[k] = m[k]
	}
	b, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(b)
}

// KeyFromFields builds composite key from ordered field names and values (e.g. from config KeyDef.Fields).
func KeyFromFields(fields []string, values map[string]interface{}) (hash []byte, raw string) {
	m := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		if v, ok := values[f]; ok {
			m[f] = v
		}
	}
	return CompositeKey("", m)
}

// HashToPartition returns a partition index in [0, numPartitions) for Kafka partitioning.
func HashToPartition(hash []byte, numPartitions int) int32 {
	if numPartitions <= 0 {
		return 0
	}
	u := binary.BigEndian.Uint64(hash[:8])
	return int32(u % uint64(numPartitions))
}
