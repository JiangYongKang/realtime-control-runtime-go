// Package sensor defines the sensor sample model and the injectable data
// source abstraction. No real devices are involved: sources are either
// deterministic scripts (tests) or formula-driven simulations (local runs).
package sensor

import (
	"context"
	"errors"
	"time"
)

// Sample is a single sensor reading carrying the device-side timestamp.
type Sample struct {
	// ChannelID identifies the source channel.
	ChannelID string
	// Value is the engineering-unit measurement.
	Value float64
	// Timestamp is the time the reading was produced (unified time base).
	Timestamp time.Time
	// Seq is a monotonically increasing per-channel sequence number.
	Seq uint64
}

// ErrUnavailable is returned by a source that fails to produce a sample.
var ErrUnavailable = errors.New("sensor: sample unavailable")

// Source produces one sample per Read call. Implementations must honor
// context cancellation.
type Source interface {
	Read(ctx context.Context) (Sample, error)
}
