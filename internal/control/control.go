// Package control implements deadline-aware control evaluation and the
// deterministic safety state machine (normal -> degraded -> safe -> recovery).
package control

import (
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// SamplerStats is the stats view the controller needs (decouples control
// from the sampler package).
type SamplerStats struct {
	Faults  int64
	Dropped int64
}

// Input is one control evaluation request.
type Input struct {
	// Now is the unified runtime-clock time of the evaluation.
	Now time.Time
	// Snapshot is the latest classified envelope per channel.
	Snapshot map[model.ChannelID]model.Envelope
	// NewEnvelopes are envelopes observed since the previous evaluation.
	NewEnvelopes []model.Envelope
	// Stats are cumulative sampler counters.
	Stats SamplerStats
	// DeadlineExceeded is set by the scheduling loop when a decision could
	// not be completed within safety.decisionDeadline.
	DeadlineExceeded bool
	// Generation is the scheduling-loop tick number. An evaluation whose
	// generation is older than the last deadline-driven ForceSafe is stale
	// and must be discarded without mutating state.
	Generation int64
}

// Controller is the control/safety component.
type Controller interface {
	// Evaluate performs one decision step deterministically from the input
	// and the configuration version currently in the store.
	Evaluate(in Input) model.Decision
	// ForceSafe immediately commands the safe state for generation and
	// invalidates any in-flight evaluation at that or an older generation.
	ForceSafe(now time.Time, generation int64, reason string) model.Decision
	// State returns the current safety state.
	State() model.SafetyState
	// Reset clears fault history and returns to the normal state.
	Reset()
}
