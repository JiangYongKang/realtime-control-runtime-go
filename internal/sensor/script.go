package sensor

import (
	"context"
	"sync"
)

// ScriptStep is one scripted Read outcome: exactly one of Sample/Err is used.
type ScriptStep struct {
	Sample Sample
	Err    error
}

// ScriptSource replays a fixed list of outcomes, then returns ErrUnavailable
// for any further reads. It is safe for concurrent use.
type ScriptSource struct {
	mu    sync.Mutex
	id    string
	steps []ScriptStep
	pos   int
}

// NewScriptSource builds a source replaying the given steps.
func NewScriptSource(id string, steps ...ScriptStep) *ScriptSource {
	return &ScriptSource{id: id, steps: steps}
}

// ID returns the channel identifier.
func (s *ScriptSource) ID() string { return s.id }

// Read returns the next scripted outcome.
func (s *ScriptSource) Read(ctx context.Context) (Sample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos >= len(s.steps) {
		return Sample{}, ErrUnavailable
	}
	st := s.steps[s.pos]
	s.pos++
	if st.Err != nil {
		return Sample{}, st.Err
	}
	return st.Sample, nil
}
