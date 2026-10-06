package config

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestValidateJudgeTimeout(t *testing.T) {
	c := baseConfig()
	// Zero is allowed (unset).
	c.Safety.JudgeTimeout = 0
	if err := Validate(c); err != nil {
		t.Fatalf("zero judge_timeout should be allowed: %v", err)
	}
	// Positive, within control period allowed.
	c.Safety.JudgeTimeout = 10 * time.Millisecond
	if err := Validate(c); err != nil {
		t.Fatalf("judge_timeout == control_period should pass: %v", err)
	}
	// Larger than control period rejected.
	c.Safety.JudgeTimeout = 11 * time.Millisecond
	if err := Validate(c); err != ErrBadJudgeTimeout {
		t.Fatalf("want ErrBadJudgeTimeout, got %v", err)
	}
	// Negative rejected.
	c.Safety.JudgeTimeout = -1
	if err := Validate(c); err == nil {
		t.Fatal("negative judge_timeout accepted")
	}
}

func TestValidateWarmupGrace(t *testing.T) {
	c := baseConfig()
	// Zero disables warm-up (legacy).
	c.Safety.WarmupGrace = 0
	if err := Validate(c); err != nil {
		t.Fatalf("zero warmup_grace allowed: %v", err)
	}
	// Boundary value allowed.
	c.Safety.WarmupGrace = MaxWarmupGrace
	if err := Validate(c); err != nil {
		t.Fatalf("warmup at cap should pass: %v", err)
	}
	// Over the cap rejected.
	c.Safety.WarmupGrace = MaxWarmupGrace + time.Nanosecond
	if err := Validate(c); err != ErrBadWarmupGrace {
		t.Fatalf("want ErrBadWarmupGrace, got %v", err)
	}
	// Negative rejected.
	c.Safety.WarmupGrace = -1
	if err := Validate(c); err == nil {
		t.Fatal("negative warmup_grace accepted")
	}
}

// TestRejectNewFieldsKeepsOldBehaviour: an update carrying invalid new
// parameters is rejected and the installed snapshot stays unchanged.
func TestRejectNewFieldsKeepsOldBehaviour(t *testing.T) {
	c := baseConfig()
	c.Safety.WarmupGrace = 100 * time.Millisecond
	c.Safety.JudgeTimeout = 5 * time.Millisecond
	m, err := NewManager(c)
	if err != nil {
		t.Fatal(err)
	}

	bad := m.Current()
	bad.Version = 2
	bad.Safety.JudgeTimeout = time.Hour // exceeds control_period
	if _, err := m.Update(bad); err == nil {
		t.Fatal("update with judge_timeout > control_period accepted")
	}
	if got := m.Current(); got.Version != 1 || got.Safety.WarmupGrace != 100*time.Millisecond ||
		got.Safety.JudgeTimeout != 5*time.Millisecond {
		t.Fatalf("rejected update changed active snapshot: %+v", got.Safety)
	}

	bad2 := m.Current()
	bad2.Version = 3
	bad2.Safety.WarmupGrace = MaxWarmupGrace + time.Second
	if _, err := m.Update(bad2); err == nil {
		t.Fatal("over-cap warmup accepted")
	}
	if m.Current().Version != 1 {
		t.Fatal("active version changed after rejected update")
	}

	// Valid new values install atomically.
	good := m.Current()
	good.Version = 4
	good.Safety.WarmupGrace = 250 * time.Millisecond
	good.Safety.JudgeTimeout = 8 * time.Millisecond
	installed, err := m.Update(good)
	if err != nil {
		t.Fatal(err)
	}
	if installed.Safety.WarmupGrace != 250*time.Millisecond ||
		installed.Safety.JudgeTimeout != 8*time.Millisecond {
		t.Fatalf("new fields not installed: %+v", installed.Safety)
	}
}

// TestJSONNewFieldsOptional: old config files without the new fields decode
// to zero (legacy defaults); explicit values round-trip.
func TestJSONNewFieldsOptional(t *testing.T) {
	old := `{
      "version": 1,
      "channels": [{"id":"c1","period":"10ms","max_lag":"20ms","min_value":0,"max_value":10}],
      "threshold": {"warn_low":1,"warn_high":9},
      "safety": {"control_period":"10ms","decision_deadline":"50ms",
                 "max_consecutive_err":2,"recover_good_ticks":3}
    }`
	cfg, err := DecodeJSON(strings.NewReader(old))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Safety.JudgeTimeout != 0 || cfg.Safety.WarmupGrace != 0 {
		t.Fatalf("missing fields must default to zero, got %+v", cfg.Safety)
	}

	full := baseConfig()
	full.Safety.JudgeTimeout = 7 * time.Millisecond
	full.Safety.WarmupGrace = 400 * time.Millisecond
	var buf bytes.Buffer
	if err := EncodeJSON(&buf, full); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"judge_timeout": "7ms"`) ||
		!strings.Contains(buf.String(), `"warmup_grace": "400ms"`) {
		t.Fatalf("new fields not string-encoded:\n%s", buf.String())
	}
	dec, err := DecodeJSON(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Safety.JudgeTimeout != 7*time.Millisecond ||
		dec.Safety.WarmupGrace != 400*time.Millisecond {
		t.Fatalf("round-trip mismatch: %+v", dec.Safety)
	}

	// Garbage duration rejected.
	bad := strings.NewReader(`{
      "version": 1,
      "channels": [{"id":"c1","period":"10ms","max_lag":"20ms","min_value":0,"max_value":10}],
      "threshold": {"warn_low":1,"warn_high":9},
      "safety": {"control_period":"10ms","decision_deadline":"50ms",
                 "max_consecutive_err":2,"recover_good_ticks":3,"warmup_grace":"not-a-duration"}
    }`)
	if _, err := DecodeJSON(bad); err == nil {
		t.Fatal("invalid warmup_grace string accepted")
	}
}
