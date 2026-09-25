// Package config holds the runtime configuration schema, validation and the
// concurrency-safe, versioned configuration store.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Duration wraps time.Duration so it can be expressed as e.g. "100ms" in JSON.
type Duration struct{ time.Duration }

// MarshalJSON renders the duration in its canonical string form.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// UnmarshalJSON accepts either a duration string ("100ms") or a JSON number
// interpreted as nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case string:
		parsed, err := time.ParseDuration(x)
		if err != nil {
			return fmt.Errorf("%w: bad duration %q: %v", ErrInvalid, x, err)
		}
		d.Duration = parsed
	case float64:
		d.Duration = time.Duration(x)
	default:
		return fmt.Errorf("%w: duration must be string or number", ErrInvalid)
	}
	return nil
}

// Channel is the configuration of one sensor channel.
type Channel struct {
	ID       string  `json:"id"`
	MinValue float64 `json:"minValue"`
	MaxValue float64 `json:"maxValue"`
	Required bool    `json:"required"`
}

// Safety describes fault detection and safe-state policy.
type Safety struct {
	// DataTimeout: a required channel with no accepted sample for longer
	// than this triggers degradation/safe state.
	DataTimeout Duration `json:"dataTimeout"`
	// ConsecutiveFaults: accepted fault samples in a row (per any required
	// channel) that force the safe state.
	ConsecutiveFaults int `json:"consecutiveFaults"`
	// DecisionDeadline: a control tick must complete within this budget;
	// overruns force the safe state.
	DecisionDeadline Duration `json:"decisionDeadline"`
	// RecoveryStableRuns: number of consecutive healthy ticks required to
	// leave a non-normal state. Prevents flapping on single jitters.
	RecoveryStableRuns int `json:"recoveryStableRuns"`
	// SafeOutput is the actuator command while in the safe state.
	SafeOutput float64 `json:"safeOutput"`
}

// Config is a complete, versioned runtime configuration.
type Config struct {
	Version   int64              `json:"version"`
	Period    Duration           `json:"period"`
	Channels  []Channel          `json:"channels"`
	Threshold map[string]float64 `json:"threshold"`
	Safety    Safety             `json:"safety"`
}

// ErrInvalid is returned when a candidate configuration is rejected.
var ErrInvalid = errors.New("invalid configuration")

// Validate checks structural and semantic validity of c. A rejected config
// leaves the previously active configuration untouched at the store level.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: nil config", ErrInvalid)
	}
	if c.Version <= 0 {
		return fmt.Errorf("%w: version must be positive", ErrInvalid)
	}
	if c.Period.Duration <= 0 {
		return fmt.Errorf("%w: period must be positive", ErrInvalid)
	}
	if len(c.Channels) == 0 {
		return fmt.Errorf("%w: at least one channel required", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(c.Channels))
	required := 0
	for _, ch := range c.Channels {
		if ch.ID == "" {
			return fmt.Errorf("%w: empty channel id", ErrInvalid)
		}
		if _, dup := seen[ch.ID]; dup {
			return fmt.Errorf("%w: duplicate channel id %q", ErrInvalid, ch.ID)
		}
		seen[ch.ID] = struct{}{}
		if ch.MinValue > ch.MaxValue {
			return fmt.Errorf("%w: channel %q minValue > maxValue", ErrInvalid, ch.ID)
		}
		if ch.Required {
			required++
		}
	}
	if required == 0 {
		return fmt.Errorf("%w: at least one required channel needed", ErrInvalid)
	}
	if c.Safety.DataTimeout.Duration <= 0 {
		return fmt.Errorf("%w: safety.dataTimeout must be positive", ErrInvalid)
	}
	if c.Safety.DecisionDeadline.Duration <= 0 {
		return fmt.Errorf("%w: safety.decisionDeadline must be positive", ErrInvalid)
	}
	if c.Safety.ConsecutiveFaults <= 0 {
		return fmt.Errorf("%w: safety.consecutiveFaults must be positive", ErrInvalid)
	}
	if c.Safety.RecoveryStableRuns <= 0 {
		return fmt.Errorf("%w: safety.recoveryStableRuns must be positive", ErrInvalid)
	}
	// Every threshold must refer to a known channel.
	for id := range c.Threshold {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("%w: threshold references unknown channel %q", ErrInvalid, id)
		}
	}
	return nil
}

// Clone returns a deep copy of c so callers cannot mutate an installed
// configuration through the value they handed in.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	cp := *c
	cp.Channels = append([]Channel(nil), c.Channels...)
	if c.Threshold != nil {
		cp.Threshold = make(map[string]float64, len(c.Threshold))
		for k, v := range c.Threshold {
			cp.Threshold[k] = v
		}
	}
	return &cp
}
