// Package model defines the shared domain types exchanged between the
// sampling, control and configuration components.
package model

import "time"

// ChannelID identifies a single sensor channel.
type ChannelID string

// Verdict is the deterministic classification of an incoming sample.
type Verdict int

const (
	// VerdictAccepted means the sample is fresh, in-order and was stored.
	VerdictAccepted Verdict = iota
	// VerdictDuplicate means a sample with the same (channel, sequence)
	// pair was already accepted.
	VerdictDuplicate
	// VerdictOutOfOrder means the sequence number is lower than the last
	// accepted sequence for the channel.
	VerdictOutOfOrder
	// VerdictStale means the sample is newer in sequence but its timestamp
	// is older than the configured staleness horizon.
	VerdictStale
	// VerdictFault means the source produced an explicit read error.
	VerdictFault
	// VerdictDropped means the bounded per-channel buffer was full and the
	// sample had to be discarded to protect the sampler (backpressure).
	VerdictDropped
)

// String returns a stable human-readable name for the verdict.
func (v Verdict) String() string {
	return verdictNames[v]
}

var verdictNames = [...]string{
	VerdictAccepted:   "accepted",
	VerdictDuplicate:  "duplicate",
	VerdictOutOfOrder: "out_of_order",
	VerdictStale:      "stale",
	VerdictFault:      "fault",
	VerdictDropped:    "dropped",
}

// Sample is one raw reading produced by a sensor source.
type Sample struct {
	// Channel is the originating channel identifier.
	Channel ChannelID
	// Seq is a monotonically increasing sequence number assigned by the
	// source. It is the primary ordering key.
	Seq int64
	// Time is the source-side timestamp of the reading.
	Time time.Time
	// Value is the measured value; meaningless when Err is set.
	Value float64
	// Err is non-nil when the source failed to produce a reading.
	Err error
}

// Envelope wraps a sample with runtime bookkeeping.
type Envelope struct {
	// Sample is the original sample.
	Sample Sample
	// RecvTime is the unified runtime-clock time at which the sampler
	// received and classified the sample.
	RecvTime time.Time
	// Verdict is the deterministic handling conclusion.
	Verdict Verdict
}

// SafetyState is the operational state the controller can be in.
type SafetyState int

const (
	// StateNormal: all channels healthy, control output is authoritative.
	StateNormal SafetyState = iota
	// StateDegraded: a soft anomaly (e.g. one channel stale) was detected;
	// output is held and reduced authority is applied.
	StateDegraded
	// StateSafe: hard fault condition; actuators are commanded to the
	// configured safe output.
	StateSafe
)

// String returns a stable human-readable name for the state.
func (s SafetyState) String() string {
	switch s {
	case StateNormal:
		return "normal"
	case StateDegraded:
		return "degraded"
	case StateSafe:
		return "safe"
	default:
		return "unknown"
	}
}

// TriggerReason explains why a state transition or a decision was taken.
type TriggerReason string

const (
	ReasonTickOK             TriggerReason = "tick_ok"
	ReasonChannelFault       TriggerReason = "channel_fault"
	ReasonDataTimeout        TriggerReason = "data_timeout"
	ReasonDecisionDeadline   TriggerReason = "decision_deadline_exceeded"
	ReasonConsecutiveErrors  TriggerReason = "consecutive_errors"
	ReasonThresholdViolation TriggerReason = "threshold_violation"
	ReasonRecoveryProgress   TriggerReason = "recovery_progress"
	ReasonBackpressure       TriggerReason = "backpressure_drop"
)

// Decision is one control evaluation emitted by the controller.
type Decision struct {
	// Time is the unified runtime-clock time of the evaluation.
	Time time.Time
	// State is the safety state after evaluation.
	State SafetyState
	// Previous is the safety state before evaluation.
	Previous SafetyState
	// Reasons are the human-readable facts that drove the evaluation.
	Reasons []string
	// Values are the latest accepted values per channel at evaluation time.
	Values map[ChannelID]float64
	// Output is the commanded actuator output.
	Output float64
	// Version is the configuration version that was in effect.
	Version int64
}

// Transitioned reports whether the decision changed the safety state.
func (d *Decision) Transitioned() bool { return d.State != d.Previous }
