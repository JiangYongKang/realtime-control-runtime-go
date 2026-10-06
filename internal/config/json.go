package config

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// jsonChannel mirrors ChannelCfg with durations as human-readable strings
// (e.g. "50ms"); the zero string decodes as 0.
type jsonChannel struct {
	ID       string  `json:"id"`
	Period   string  `json:"period"`
	MaxLag   string  `json:"max_lag"`
	MinValue float64 `json:"min_value"`
	MaxValue float64 `json:"max_value"`
	Sim      SimSpec `json:"sim"`
}

type jsonConfig struct {
	Version   int64         `json:"version"`
	Channels  []jsonChannel `json:"channels"`
	Threshold ThresholdCfg  `json:"threshold"`
	Safety    struct {
		ControlPeriod     string `json:"control_period"`
		DecisionDeadline  string `json:"decision_deadline"`
		JudgeTimeout      string `json:"judge_timeout"`
		WarmupGrace       string `json:"warmup_grace"`
		MaxConsecutiveErr int    `json:"max_consecutive_err"`
		RecoverGoodTicks  int    `json:"recover_good_ticks"`
	} `json:"safety"`
}

func parseDur(name, s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("config: %s must not be empty", name)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s %q: %w", name, s, err)
	}
	return d, nil
}

// parseDurOpt parses an optional duration: empty decodes as 0 so legacy
// configurations that predate the field keep their default behaviour.
func parseDurOpt(name, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return parseDur(name, s)
}

// DecodeJSON reads and validates a JSON configuration stream.
func DecodeJSON(r io.Reader) (*Config, error) {
	var jc jsonConfig
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&jc); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}
	cfg := &Config{
		Version:   jc.Version,
		Threshold: jc.Threshold,
	}
	for _, jch := range jc.Channels {
		period, err := parseDur("period", jch.Period)
		if err != nil {
			return nil, err
		}
		lag, err := parseDur("max_lag", jch.MaxLag)
		if err != nil {
			return nil, err
		}
		cfg.Channels = append(cfg.Channels, ChannelCfg{
			ID:       jch.ID,
			Period:   period,
			MaxLag:   lag,
			MinValue: jch.MinValue,
			MaxValue: jch.MaxValue,
			Sim:      jch.Sim,
		})
	}
	var err error
	if cfg.Safety.ControlPeriod, err = parseDur("control_period", jc.Safety.ControlPeriod); err != nil {
		return nil, err
	}
	if cfg.Safety.DecisionDeadline, err = parseDur("decision_deadline", jc.Safety.DecisionDeadline); err != nil {
		return nil, err
	}
	if cfg.Safety.JudgeTimeout, err = parseDurOpt("judge_timeout", jc.Safety.JudgeTimeout); err != nil {
		return nil, err
	}
	if cfg.Safety.WarmupGrace, err = parseDurOpt("warmup_grace", jc.Safety.WarmupGrace); err != nil {
		return nil, err
	}
	cfg.Safety.MaxConsecutiveErr = jc.Safety.MaxConsecutiveErr
	cfg.Safety.RecoverGoodTicks = jc.Safety.RecoverGoodTicks
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// EncodeJSON writes cfg as pretty JSON with string-encoded durations.
func EncodeJSON(w io.Writer, cfg *Config) error {
	jc := jsonConfig{
		Version:   cfg.Version,
		Threshold: cfg.Threshold,
	}
	jc.Safety.ControlPeriod = cfg.Safety.ControlPeriod.String()
	jc.Safety.DecisionDeadline = cfg.Safety.DecisionDeadline.String()
	jc.Safety.JudgeTimeout = cfg.Safety.JudgeTimeout.String()
	jc.Safety.WarmupGrace = cfg.Safety.WarmupGrace.String()
	jc.Safety.MaxConsecutiveErr = cfg.Safety.MaxConsecutiveErr
	jc.Safety.RecoverGoodTicks = cfg.Safety.RecoverGoodTicks
	for _, ch := range cfg.Channels {
		jc.Channels = append(jc.Channels, jsonChannel{
			ID:       ch.ID,
			Period:   ch.Period.String(),
			MaxLag:   ch.MaxLag.String(),
			MinValue: ch.MinValue,
			MaxValue: ch.MaxValue,
			Sim:      ch.Sim,
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(&jc)
}
