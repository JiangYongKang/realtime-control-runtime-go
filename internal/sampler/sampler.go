// Package sampler periodically collects samples from injectable sources,
// classifies them deterministically and keeps bounded per-channel state.
//
// The sampler is a passive, tick-driven poller: the runtime owns the single
// scheduler tick and calls Poll once per period. This gives the whole system
// one time base and a deterministic within-tick order (sample, then decide).
package sampler

import (
	"context"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// Stats summarizes sampler operation since startup.
type Stats struct {
	Accepted   int64
	Duplicates int64
	OutOfOrder int64
	Stale      int64
	Faults     int64
	Dropped    int64
}

// Sampler is the sampling component.
type Sampler interface {
	// Poll performs exactly one bounded read from every source and
	// classifies each obtained sample. It never blocks longer than the
	// configured read budget and never blocks on a slow downstream: if the
	// bounded output channel is full, extra envelopes are counted dropped.
	Poll(ctx context.Context)
	// Envelopes returns the bounded stream of classified samples.
	Envelopes() <-chan model.Envelope
	// SetPeriod atomically retunes the per-read budget baseline.
	SetPeriod(d time.Duration)
	// Snapshot returns a consistent copy of the latest envelope per channel.
	Snapshot() map[model.ChannelID]model.Envelope
	// Stats returns the cumulative classification counters.
	Stats() Stats
}
