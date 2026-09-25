package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

type discardLog struct{}

func (discardLog) Printf(string, ...any) {}

func ctrlCfg() *config.Config {
	return &config.Config{
		Version: 1,
		Channels: []config.ChannelCfg{
			{ID: "a", Period: 10 * time.Millisecond, MaxLag: 100 * time.Millisecond, MinValue: 0, MaxValue: 100},
			{ID: "b", Period: 10 * time.Millisecond, MaxLag: 100 * time.Millisecond, MinValue: 0, MaxValue: 100},
		},
		Threshold: config.ThresholdCfg{WarnLow: 0, WarnHigh: 50},
		Safety: config.SafetyCfg{
			ControlPeriod:     10 * time.Millisecond,
			DecisionDeadline:  50 * time.Millisecond,
			MaxConsecutiveErr: 2,
			RecoverGoodTicks:  3,
		},
	}
}

func event(ch string, oc sampler.Outcome, seq uint64, ts time.Time, v float64) sampler.Event {
	return sampler.Event{
		ChannelID: ch,
		Outcome:   oc,
		Sample:    sensor.Sample{ChannelID: ch, Value: v, Timestamp: ts, Seq: seq},
	}
}

func healthyAt(c *Controller, cfg *config.Config, now time.Time) {
	c.applyEvent(cfg, event("a", sampler.OutcomeAccepted, 1, now, 10))
	c.applyEvent(cfg, event("b", sampler.OutcomeAccepted, 1, now, 10))
}

func newController(cfg *config.Config) (*Controller, *clock.FakeClock) {
	fc := clock.NewFake(time.Unix(0, 0))
	c := New(func() *config.Config { return cfg }, fc, discardLog{})
	return c, fc
}

func TestEntersSafeOnNoData(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	now := time.Unix(0, 0)
	d := c.evaluate(cfg, now)
	if d.State != StateSafe || d.Reason != ReasonNoData {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
	if d.Previous != StateNormal {
		t.Fatalf("previous=%s", d.Previous)
	}
}

func TestStaysNormalWithHealthyFreshData(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	now := time.Unix(0, 0)
	healthyAt(c, cfg, now)
	d := c.evaluate(cfg, now.Add(10*time.Millisecond))
	if d.State != StateNormal || d.Reason != ReasonHealthy {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
}

func TestEntersSafeOnDeadlineMiss(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)
	healthyAt(c, cfg, t0)
	d := c.evaluate(cfg, t0.Add(10*time.Millisecond))
	if d.State != StateNormal {
		t.Fatalf("fresh tick state=%s", d.State)
	}
	// 60ms later: age 60ms > 50ms deadline.
	d = c.evaluate(cfg, t0.Add(60*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonDeadlineMissed {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
}

func TestConsecutiveErrorsRequireThreshold(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)
	healthyAt(c, cfg, t0)

	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	d := c.evaluate(cfg, t0.Add(10*time.Millisecond))
	if d.State != StateNormal {
		t.Fatalf("single anomaly must stay normal, got %s", d.State)
	}
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	d = c.evaluate(cfg, t0.Add(20*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonConsecutiveErr {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
}

func TestThresholdViolationEntersSafe(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	now := time.Unix(0, 0)
	c.applyEvent(cfg, event("a", sampler.OutcomeAccepted, 1, now, 10))
	c.applyEvent(cfg, event("b", sampler.OutcomeAccepted, 1, now, 60)) // > warn_high 50
	d := c.evaluate(cfg, now.Add(5*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonThreshold {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
}

func TestDuplicateAndStaleDoNotChangeState(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	now := time.Unix(0, 0)
	healthyAt(c, cfg, now)
	c.evaluate(cfg, now.Add(5*time.Millisecond))
	c.applyEvent(cfg, event("a", sampler.OutcomeDuplicate, 1, now, 10))
	c.applyEvent(cfg, event("b", sampler.OutcomeStale, 1, now, 10))
	d := c.evaluate(cfg, now.Add(10*time.Millisecond))
	if d.State != StateNormal {
		t.Fatalf("duplicate/stale changed state to %s", d.State)
	}
}

func TestRecoveryRequiresConsecutiveHealthyTicks(t *testing.T) {
	cfg := ctrlCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)

	// Force SAFE through missing data.
	d := c.evaluate(cfg, t0)
	if d.State != StateSafe {
		t.Fatalf("expected initial safe, got %s", d.State)
	}

	// Two healthy ticks are not enough: still SAFE/recovering.
	for i := 1; i <= 2; i++ {
		now := t0.Add(time.Duration(i) * 10 * time.Millisecond)
		healthyAt(c, cfg, now)
		d = c.evaluate(cfg, now.Add(5*time.Millisecond))
		if d.State != StateSafe {
			t.Fatalf("tick %d recovered early: %s", i, d.State)
		}
	}

	// Two consecutive source anomalies reset the stable-run counter; a
	// single jitter would not (MaxConsecutiveErr == 2).
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	bad := t0.Add(30 * time.Millisecond)
	d = c.evaluate(cfg, bad.Add(5*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonConsecutiveErr || d.HealthyRun != 0 {
		t.Fatalf("expected reset healthy run, got state=%s reason=%s run=%d", d.State, d.Reason, d.HealthyRun)
	}

	// Restart the healthy streak; recovery must again need 3 ticks.
	for i := 4; i <= 6; i++ {
		now := t0.Add(time.Duration(i) * 10 * time.Millisecond)
		healthyAt(c, cfg, now)
		d = c.evaluate(cfg, now.Add(5*time.Millisecond))
	}
	if d.State != StateNormal {
		t.Fatalf("expected recovery after 3 consecutive healthy ticks, got %s (run=%d)", d.State, d.HealthyRun)
	}
}

// TestControlLoopReconfigureAndDecisions drives the actual Run loop.
func TestControlLoopReconfigureAndDecisions(t *testing.T) {
	cfg := ctrlCfg()
	c, fc := newController(cfg)
	in := make(chan sampler.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	advancing := make(chan struct{})
	go func() {
		defer close(advancing)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fc.Advance(10 * time.Millisecond)
				now := fc.Now()
				in <- event("a", sampler.OutcomeAccepted, uint64(now.UnixMilli())+1, now, 10)
				in <- event("b", sampler.OutcomeAccepted, uint64(now.UnixMilli())+1, now, 10)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Eventually reach NORMAL after 3 consecutive healthy ticks.
	gotNormal := atomic.Bool{}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case d := <-c.Decisions():
			if d.State == StateNormal && d.Reason == ReasonHealthy {
				gotNormal.Store(true)
			}
		case <-time.After(50 * time.Millisecond):
		}
		if gotNormal.Load() {
			break
		}
	}
	if !gotNormal.Load() {
		t.Fatal("never reached NORMAL with healthy data")
	}

	// Reconfigure to a new version; loop must keep running and stamp it.
	next := ctrlCfg()
	next.Version = 7
	next.Safety.ControlPeriod = 20 * time.Millisecond
	c.Reconfigure(next)
	sawV7 := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !sawV7 {
		select {
		case d := <-c.Decisions():
			if d.ConfigVersion == 7 {
				sawV7 = true
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	<-runDone
	<-advancing
	if !sawV7 {
		t.Fatal("decisions never carried new config version")
	}
}
