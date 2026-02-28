package projection

import (
	"encoding/json"

	"github.com/confluo/omni-joiner/internal/config"
)

// Apply builds the output document from join state using the config's projection.
// participantData is streamID -> raw payload (JSON bytes). Returns JSON bytes for the projected document.
func Apply(participantData map[string][]byte, proj []config.ProjectionField) ([]byte, error) {
	streamDecoded := make(map[string]map[string]interface{})
	for streamID, raw := range participantData {
		var m map[string]interface{}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		streamDecoded[streamID] = m
	}

	out := make(map[string]interface{})
	for _, p := range proj {
		streamData, ok := streamDecoded[p.Stream]
		if !ok {
			continue
		}
		if v, ok := streamData[p.Field]; ok {
			out[p.Output] = v
		}
	}
	return json.Marshal(out)
}
