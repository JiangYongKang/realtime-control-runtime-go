// Package config provides atomic, versioned runtime configuration. Readers
// always observe a complete, immutable snapshot; updates are validated before
// becoming visible and illegal or conflicting updates leave the old behaviour
// untouched.
package config

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ChannelCfg configures one sampled channel.
type ChannelCfg struct {
	// ID uniquely identifies the channel.
	ID string `json:"id"`
	// Period is the sampling period; must be > 0.
	Period time.Duration `json:"period"`
	// MaxLag is how late a device timestamp may be relative to the
	// unified clock before the sample is classified stale; must be >= 0.
	MaxLag time.Duration `json:"max_lag"`
	// MinValue/MaxValue bound valid readings; MinValue <= MaxValue.
	MinValue float64 `json:"min_value"`
	MaxValue float64 `json:"max_value"`
	// Sim selects the simulated device formula and fault injection.
	Sim SimSpec `json:"sim"`
}

// SimSpec describes the simulated data source for a channel.
type SimSpec struct {
	Base         float64 `json:"base"`
	Amplitude    float64 `json:"amplitude"`
	PeriodS      float64 `json:"period_s"`
	FailEveryN   int     `json:"fail_every_n"`
	FailFromRead int     `json:"fail_from_read"`
}

// ThresholdCfg holds control thresholds with WarnLow <= WarnHigh.
type ThresholdCfg struct {
	WarnLow  float64 `json:"warn_low"`
	WarnHigh float64 `json:"warn_high"`
}

// SafetyCfg configures deadline monitoring and safe-state transitions.
type SafetyCfg struct {
	// ControlPeriod is the period of the control decision loop; > 0.
	ControlPeriod time.Duration `json:"control_period"`
	// DecisionDeadline bounds the age of the data a decision may use.
	DecisionDeadline time.Duration `json:"decision_deadline"`
	// MaxConsecutiveErr tolerates transient source errors before
	// entering the safe state; must be >= 1.
	MaxConsecutiveErr int `json:"max_consecutive_err"`
	// RecoverGoodTicks is the number of consecutive healthy control
	// ticks required before leaving the safe state; must be >= 1 and is
	// required to exceed MaxConsecutiveErr so a single jitter cannot
	// cause flapping.
	RecoverGoodTicks int `json:"recover_good_ticks"`
	// JudgeTimeout bounds a single evaluation of a locally registered
	// custom decision function. Zero means "no custom judge configured";
	// when a judge is registered the runtime substitutes ControlPeriod at
	// startup. When set it must be > 0 and no larger than ControlPeriod so
	// one slow evaluation can never hold up the control loop.
	JudgeTimeout time.Duration `json:"judge_timeout"`
	// WarmupGrace is the grace window after startup or a configuration
	// reload during which a "no data yet" verdict is reported as the
	// distinguishable warming state instead of immediately entering the
	// safe state. Zero disables warm-up (the legacy behaviour: missing
	// data enters SAFE at once). It must be <= MaxWarmupGrace; real
	// anomalies and threshold violations are never covered by the grace.
	WarmupGrace time.Duration `json:"warmup_grace"`
}

// MaxWarmupGrace caps the configurable warm-up window so misconfiguration
// can never postpone safe-state entry indefinitely.
const MaxWarmupGrace = 30 * time.Second

// Config is one immutable configuration version.
type Config struct {
	// Version is monotonically increasing; 0 marks the initial config.
	Version int64 `json:"version"`
	// Channels must contain at least one entry and unique IDs.
	Channels  []ChannelCfg `json:"channels"`
	Threshold ThresholdCfg `json:"threshold"`
	Safety    SafetyCfg    `json:"safety"`
}

// Channel returns the config for a channel and whether it exists.
func (c *Config) Channel(id string) (ChannelCfg, bool) {
	for i := range c.Channels {
		if c.Channels[i].ID == id {
			return c.Channels[i], true
		}
	}
	return ChannelCfg{}, false
}

// Errors returned by validation and updates.
var (
	ErrNoChannels      = errors.New("config: at least one channel required")
	ErrDuplicateID     = errors.New("config: duplicate channel id")
	ErrBadPeriod       = errors.New("config: period must be > 0")
	ErrNegativeLag     = errors.New("config: max_lag must be >= 0")
	ErrBadValueRange   = errors.New("config: min_value must be <= max_value")
	ErrBadThresholds   = errors.New("config: warn_low must be <= warn_high")
	ErrBadDeadline     = errors.New("config: decision_deadline must be > 0")
	ErrBadErrorCount   = errors.New("config: max_consecutive_err must be >= 1")
	ErrBadRecoverTicks = errors.New("config: recover_good_ticks must be >= 1")
	ErrFlapRisk        = errors.New("config: recover_good_ticks must be > max_consecutive_err to prevent flapping")
	ErrBadJudgeTimeout = errors.New("config: judge_timeout must be > 0 and <= control_period when set")
	ErrBadWarmupGrace  = errors.New("config: warmup_grace must be >= 0 and <= 30s")
	ErrBadSim          = errors.New("config: sim fault parameters must be >= 0, and period_s > 0 when amplitude != 0")
	ErrVersionRewind   = errors.New("config: new version must be greater than current version")
)

// Validate checks a config for illegal or conflicting settings.
func Validate(c *Config) error {
	if c == nil {
		return errors.New("config: nil config")
	}
	if len(c.Channels) == 0 {
		return ErrNoChannels
	}
	seen := make(map[string]struct{}, len(c.Channels))
	for i := range c.Channels {
		ch := &c.Channels[i]
		if ch.ID == "" {
			return fmt.Errorf("config: channel %d has empty id", i)
		}
		if _, dup := seen[ch.ID]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateID, ch.ID)
		}
		seen[ch.ID] = struct{}{}
		if ch.Period <= 0 {
			return fmt.Errorf("%w: channel %q", ErrBadPeriod, ch.ID)
		}
		if ch.MaxLag < 0 {
			return fmt.Errorf("%w: channel %q", ErrNegativeLag, ch.ID)
		}
		if ch.MinValue > ch.MaxValue {
			return fmt.Errorf("%w: channel %q", ErrBadValueRange, ch.ID)
		}
		s := ch.Sim
		if s.FailEveryN < 0 || s.FailFromRead < 0 || (s.Amplitude != 0 && s.PeriodS <= 0) {
			return fmt.Errorf("%w: channel %q", ErrBadSim, ch.ID)
		}
	}
	if c.Threshold.WarnLow > c.Threshold.WarnHigh {
		return ErrBadThresholds
	}
	if c.Safety.ControlPeriod <= 0 {
		return fmt.Errorf("%w: control_period", ErrBadDeadline)
	}
	if c.Safety.DecisionDeadline <= 0 {
		return ErrBadDeadline
	}
	if c.Safety.MaxConsecutiveErr < 1 {
		return ErrBadErrorCount
	}
	if c.Safety.RecoverGoodTicks < 1 {
		return ErrBadRecoverTicks
	}
	if c.Safety.RecoverGoodTicks <= c.Safety.MaxConsecutiveErr {
		return ErrFlapRisk
	}
	// judge_timeout is optional (0 = unset); a set value must fit in one
	// control period so the loop deadline is always honoured.
	if c.Safety.JudgeTimeout < 0 ||
		(c.Safety.JudgeTimeout > 0 && c.Safety.JudgeTimeout > c.Safety.ControlPeriod) {
		return ErrBadJudgeTimeout
	}
	if c.Safety.WarmupGrace < 0 || c.Safety.WarmupGrace > MaxWarmupGrace {
		return ErrBadWarmupGrace
	}
	return nil
}

// Manager stores the active config and applies validated updates atomically.
type Manager struct {
	v  atomic.Pointer[Config]
	mu sync.Mutex // serialises writers so version checks are authoritative
}

// NewManager builds a manager holding the validated initial config.
func NewManager(initial *Config) (*Manager, error) {
	if err := Validate(initial); err != nil {
		return nil, err
	}
	cp := clone(initial)
	m := &Manager{}
	m.v.Store(cp)
	return m, nil
}

// Current returns an immutable snapshot. It is a defensive copy, so callers
// may freely modify it without affecting the active configuration; under
// concurrent reads this never exposes a partially updated state.
func (m *Manager) Current() *Config { return clone(m.v.Load()) }

// Update validates next, enforces a strictly increasing version and installs
// it atomically. On any failure the previous config stays active unchanged.
func (m *Manager) Update(next *Config) (*Config, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.v.Load()
	if next == nil {
		return cur, errors.New("config: nil config")
	}
	if next.Version <= cur.Version {
		return cur, fmt.Errorf("%w: got %d, current %d", ErrVersionRewind, next.Version, cur.Version)
	}
	if err := Validate(next); err != nil {
		return cur, err
	}
	installed := clone(next)
	m.v.Store(installed)
	return installed, nil
}

// clone produces a detached copy so callers cannot mutate active snapshots.
func clone(c *Config) *Config {
	cp := *c
	cp.Channels = append([]ChannelCfg(nil), c.Channels...)
	return &cp
}
