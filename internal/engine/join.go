package engine

import (
	"context"

	"github.com/confluo/omni-joiner/internal/config"
	"github.com/confluo/omni-joiner/internal/projection"
	"github.com/confluo/omni-joiner/internal/store"
)

// Engine performs the join completion check and projection.
type Engine struct {
	joinConfig *config.JoinConfig
}

// New creates an engine for the given join config.
func New(joinConfig *config.JoinConfig) *Engine {
	return &Engine{joinConfig: joinConfig}
}

// IsComplete returns true if state has all N participants.
func (e *Engine) IsComplete(state *store.JoinState) bool {
	if state == nil {
		return false
	}
	return len(state.ParticipantData) >= e.joinConfig.N()
}

// Project builds the egress payload from the join state using the config projection.
func (e *Engine) Project(state *store.JoinState) ([]byte, error) {
	if state == nil {
		return nil, nil
	}
	return projection.Apply(state.ParticipantData, e.joinConfig.Projection)
}

// ProcessResult is the outcome of processing one event: either updated state or a joined payload to publish.
type ProcessResult struct {
	State       *store.JoinState
	JoinedBytes []byte // Non-nil when join is complete and we should publish then delete state
}

// Process runs the join step: the caller has already called UpsertParticipant and got state.
// It checks completion and if so returns the projected payload.
func (e *Engine) Process(ctx context.Context, state *store.JoinState) (*ProcessResult, error) {
	if state == nil {
		return &ProcessResult{}, nil
	}
	if !e.IsComplete(state) {
		return &ProcessResult{State: state}, nil
	}
	joined, err := e.Project(state)
	if err != nil {
		return nil, err
	}
	return &ProcessResult{JoinedBytes: joined}, nil
}
