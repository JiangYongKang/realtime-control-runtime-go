package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
)

func warmCfg(grace time.Duration) *config.Config {
	cfg := ctrlCfg()
	cfg.Safety.ControlPeriod = 10 * time.Millisecond
	cfg.Safety.WarmupGrace = grace
	return cfg
}

// TestWarmupReportsWarmingNotSafe: while no data exists inside the grace
// window, the verdict is the distinguishable WARMING state, not SAFE.
func TestWarmupReportsWarmingNotSafe(t *testing.T) {
	cfg := warmCfg(100 * time.Millisecond)
	c, fc := newController(cfg)
	t0 := fc.Now()

	// Simulate a cold start opening the window, as Run does.
	c.Reconfigure(cfg)

	d := c.evaluate(cfg, t0.Add(10*time.Millisecond))
	if d.State != StateWarming || d.Reason != ReasonWarming || !d.Warming {
		t.Fatalf("want WARMING/warming, got %s/%s warming=%t", d.State, d.Reason, d.Warming)
	}
	d = c.evaluate(cfg, t0.Add(50*time.Millisecond))
	if d.State != StateWarming {
		t.Fatalf("still within grace, got %s", d.State)
	}
}

// TestWarmupExpiresIntoSafe: once the grace elapses with no data, normal
// no-data handling enters SAFE.
func TestWarmupExpiresIntoSafe(t *testing.T) {
	cfg := warmCfg(100 * time.Millisecond)
	c, fc := newController(cfg)
	t0 := fc.Now()
	c.Reconfigure(cfg)

	if d := c.evaluate(cfg, t0.Add(50*time.Millisecond)); d.State != StateWarming {
		t.Fatalf("want WARMING at 50ms, got %s", d.State)
	}
	d := c.evaluate(cfg, t0.Add(101*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonNoData {
		t.Fatalf("want SAFE/no_data after grace, got %s/%s", d.State, d.Reason)
	}
}

// TestWarmupDoesNotMaskAnomalies: a real threshold violation during the
// grace window still enters SAFE; grace only covers "no data yet".
func TestWarmupDoesNotMaskThresholdViolation(t *testing.T) {
	cfg := warmCfg(100 * time.Millisecond)
	c, fc := newController(cfg)
	now := fc.Now()
	c.Reconfigure(cfg)

	// Channel a healthy, channel b over warn_high (50): hard evidence.
	c.applyEvent(cfg, event("a", sampler.OutcomeAccepted, 1, now, 10))
	c.applyEvent(cfg, event("b", sampler.OutcomeAccepted, 1, now, 60))

	d := c.evaluate(cfg, now.Add(5*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonThreshold {
		t.Fatalf("threshold violation must enter SAFE during warm-up, got %s/%s", d.State, d.Reason)
	}
	if d.Warming {
		t.Fatal("real violation must not be reported as warming")
	}
}

// TestWarmupDoesNotMaskConsecutiveErrors: enough consecutive anomalies during
// the grace window enter SAFE the same way as without grace.
func TestWarmupDoesNotMaskConsecutiveErrors(t *testing.T) {
	cfg := warmCfg(100 * time.Millisecond)
	c, fc := newController(cfg)
	now := fc.Now()
	c.Reconfigure(cfg)

	// Give one channel a prior accepted sample so it is not "no data",
	// then hit MaxConsecutiveErr read errors.
	c.applyEvent(cfg, event("a", sampler.OutcomeAccepted, 1, now, 10))
	c.applyEvent(cfg, event("b", sampler.OutcomeAccepted, 1, now, 10))
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))

	d := c.evaluate(cfg, now.Add(5*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonConsecutiveErr {
		t.Fatalf("anomalies must enter SAFE during warm-up, got %s/%s", d.State, d.Reason)
	}
}

// TestWarmupClearsOnceDataArrives: healthy data during the window leads
// straight to NORMAL; the window does not need to elapse first.
func TestWarmupClearsOnceDataArrives(t *testing.T) {
	cfg := warmCfg(100 * time.Millisecond)
	c, fc := newController(cfg)
	now := fc.Now()
	c.Reconfigure(cfg)

	if d := c.evaluate(cfg, now); d.State != StateWarming {
		t.Fatalf("first tick want WARMING, got %s", d.State)
	}
	healthyAt(c, cfg, now.Add(10*time.Millisecond))
	d := c.evaluate(cfg, now.Add(15*time.Millisecond))
	if d.State != StateNormal || d.Reason != ReasonHealthy {
		t.Fatalf("healthy data must leave WARMING for NORMAL, got %s/%s", d.State, d.Reason)
	}
}

// TestWarmupReopensOnReconfigure: a reload resets the grace window.
func TestWarmupReopensOnReconfigure(t *testing.T) {
	cfg := warmCfg(100 * time.Millisecond)
	c, fc := newController(cfg)
	t0 := fc.Now()
	c.Reconfigure(cfg)
	healthyAt(c, cfg, t0)
	if d := c.evaluate(cfg, t0.Add(5*time.Millisecond)); d.State != StateNormal {
		t.Fatalf("want NORMAL with data, got %s", d.State)
	}

	// Reload with a new generation; advance the clock well past the
	// original window, then reopen it via Reconfigure.
	fc.Advance(500 * time.Millisecond)
	next := warmCfg(100 * time.Millisecond)
	next.Version = 2
	// Drop channel evidence by using fresh ids so "no data" holds.
	next.Channels = []config.ChannelCfg{
		{ID: "x", Period: 10 * time.Millisecond, MaxLag: 100 * time.Millisecond, MinValue: 0, MaxValue: 100},
		{ID: "y", Period: 10 * time.Millisecond, MaxLag: 100 * time.Millisecond, MinValue: 0, MaxValue: 100},
	}
	c.Reconfigure(next)
	d := c.evaluate(next, fc.Now())
	if d.State != StateWarming {
		t.Fatalf("reload must reopen warm-up, got %s", d.State)
	}
}

// TestWarmupDisabledByDefault: zero grace preserves the legacy behaviour of
// entering SAFE immediately on missing data.
func TestWarmupDisabledByDefault(t *testing.T) {
	cfg := warmCfg(0)
	c, fc := newController(cfg)
	d := c.evaluate(cfg, fc.Now())
	if d.State != StateSafe || d.Reason != ReasonNoData || d.Warming {
		t.Fatalf("legacy no-data behaviour broken: state=%s reason=%s warming=%t", d.State, d.Reason, d.Warming)
	}
}

// TestWarmupLoopEmitsWarming drives the actual Run loop and confirms
// WARMING decisions come out before data, and SAFE after the window expires.
func TestWarmupLoopEmitsWarming(t *testing.T) {
	cfg := warmCfg(60 * time.Millisecond)
	fc := clock.NewFake(time.Unix(0, 0))
	c := New(func() *config.Config { return cfg }, fc, discardLog{})

	in := make(chan sampler.Event, 16) // never fed: no data source
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	var warming, safe atomic.Int32
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-c.Decisions():
				if d.Warming {
					warming.Add(1)
				}
				if d.State == StateSafe {
					safe.Add(1)
				}
			}
		}
	}()

	// Advance past grace with no samples.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fc.Advance(10 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
		if warming.Load() > 0 && safe.Load() > 0 {
			break
		}
	}
	cancel()
	<-runDone
	if warming.Load() == 0 {
		t.Fatal("expected WARMING decisions before grace expiry")
	}
	if safe.Load() == 0 {
		t.Fatal("expected SAFE after grace expiry with no data")
	}
}
