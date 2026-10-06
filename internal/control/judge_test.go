package control

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
)

// judgeCfg returns a two-channel config with an explicit judge timeout and
// no warm-up grace.
func judgeCfg() *config.Config {
	cfg := ctrlCfg()
	cfg.Safety.JudgeTimeout = 10 * time.Millisecond
	return cfg
}

// scriptedJudge returns verdicts from a function; calls and started signal
// allow tests to synchronise with invocations.
type scriptedJudge struct {
	calls   atomic.Int64
	started chan struct{}
	fn      func(in JudgeInput, call int64) Verdict
}

func (j *scriptedJudge) Judge(in JudgeInput) Verdict {
	n := j.calls.Add(1)
	if j.started != nil {
		select {
		case j.started <- struct{}{}:
		default:
		}
	}
	return j.fn(in, n)
}

// runDecideAsync runs decide in the background so the test can advance the
// fake clock while the judge timeout timer is pending.
func runDecideAsync(c *Controller, cfg *config.Config, now time.Time) <-chan Decision {
	ch := make(chan Decision, 1)
	go func() { ch <- c.decide(cfg, now) }()
	return ch
}

func TestCustomJudgeHealthyVerdict(t *testing.T) {
	cfg := judgeCfg()
	c, _ := newController(cfg)
	c.SetJudge(&scriptedJudge{fn: func(in JudgeInput, _ int64) Verdict {
		if len(in.Channels) != 2 {
			t.Errorf("judge saw %d channels, want 2", len(in.Channels))
		}
		if in.Config.Version != cfg.Version {
			t.Errorf("judge saw config version %d, want %d", in.Config.Version, cfg.Version)
		}
		return Verdict{Healthy: true, Detail: "all good"}
	}})

	now := time.Unix(0, 0)
	healthyAt(c, cfg, now)
	d := c.decide(cfg, now.Add(5*time.Millisecond))
	if d.State != StateNormal || d.Reason != ReasonCustom {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
	if d.ForcedSafe || d.Warmup {
		t.Fatalf("unexpected flags forced=%t warmup=%t", d.ForcedSafe, d.Warmup)
	}
}

func TestCustomJudgeUnhealthyVerdictEntersSafe(t *testing.T) {
	cfg := judgeCfg()
	c, _ := newController(cfg)
	c.SetJudge(&scriptedJudge{fn: func(JudgeInput, int64) Verdict {
		return Verdict{Healthy: false, Reason: Reason("model_rejected"), Detail: "local model veto"}
	}})

	now := time.Unix(0, 0)
	healthyAt(c, cfg, now)
	d := c.decide(cfg, now.Add(5*time.Millisecond))
	if d.State != StateSafe || d.Reason != Reason("model_rejected") {
		t.Fatalf("state=%s reason=%s", d.State, d.Reason)
	}
	if !strings.Contains(d.Detail, "local model veto") {
		t.Fatalf("verdict detail not propagated: %q", d.Detail)
	}
}

// TestJudgeTimeoutForcesSafeAndLateVerdictDiscarded blocks the judge past
// its budget: the runtime must force SAFE within judge_timeout, and the
// late verdict must be discarded without flipping the conclusion or causing
// an extra transition.
func TestJudgeTimeoutForcesSafeAndLateVerdictDiscarded(t *testing.T) {
	cfg := judgeCfg()
	c, fc := newController(cfg)
	release := make(chan struct{})
	j := &scriptedJudge{
		started: make(chan struct{}, 8),
		fn: func(JudgeInput, int64) Verdict {
			<-release // stuck until the test releases it
			return Verdict{Healthy: true, Detail: "late healthy"}
		},
	}
	c.SetJudge(j)

	t0 := time.Unix(0, 0)
	healthyAt(c, cfg, t0)

	// Tick 1: judge blocks; advancing the clock past judge_timeout must
	// produce a forced-safe decision bounded by the timeout.
	res := runDecideAsync(c, cfg, t0.Add(10*time.Millisecond))
	<-j.started
	fc.Advance(10 * time.Millisecond) // reaches judge_timeout
	d := <-res
	if d.State != StateSafe || d.Reason != ReasonJudgeTimeout || !d.ForcedSafe {
		t.Fatalf("state=%s reason=%s forced=%t", d.State, d.Reason, d.ForcedSafe)
	}
	if c.JudgeTimeoutCount() != 1 {
		t.Fatalf("timeout count=%d", c.JudgeTimeoutCount())
	}

	// Tick 2: the abandoned judgment is still running; no new call may be
	// spawned and SAFE is held immediately.
	res = runDecideAsync(c, cfg, t0.Add(20*time.Millisecond))
	d = <-res
	if d.Reason != ReasonJudgeBusy || !d.ForcedSafe || d.State != StateSafe {
		t.Fatalf("state=%s reason=%s forced=%t", d.State, d.Reason, d.ForcedSafe)
	}
	if n := j.calls.Load(); n != 1 {
		t.Fatalf("judge invoked %d times while one was still stuck", n)
	}

	// Release the stuck judge: its late verdict must be reaped and
	// discarded. The reaping tick spawns a fresh judgment (release is
	// already closed, so it answers healthy immediately); depending on
	// goroutine scheduling the first attempt may still observe the
	// in-flight call, so retry until the reaping tick happens.
	close(release)
	for i := 0; i < 100; i++ {
		d = <-runDecideAsync(c, cfg, t0.Add(30*time.Millisecond))
		if d.Reason != ReasonJudgeBusy {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if c.LateVerdictCount() != 1 {
		t.Fatalf("late verdicts=%d, want 1", c.LateVerdictCount())
	}
	// The late "healthy" verdict never participated: still SAFE, and the
	// only transition so far was the forced entry into SAFE.
	if d.State != StateSafe || d.Previous != StateSafe {
		t.Fatalf("late verdict flipped state: prev=%s state=%s", d.Previous, d.State)
	}
}

// TestJudgeWithinBudgetUsesVerdict: a slow judge that answers inside its
// budget has its verdict applied, with the actual duration recorded.
func TestJudgeWithinBudgetUsesVerdict(t *testing.T) {
	cfg := judgeCfg()
	c, fc := newController(cfg)
	gate := make(chan struct{})
	j := &scriptedJudge{
		started: make(chan struct{}, 1),
		fn: func(JudgeInput, int64) Verdict {
			<-gate // "computes" until the test advances the clock
			return Verdict{Healthy: true}
		},
	}
	c.SetJudge(j)

	t0 := time.Unix(0, 0)
	healthyAt(c, cfg, t0)
	res := runDecideAsync(c, cfg, t0.Add(10*time.Millisecond))
	<-j.started
	fc.Advance(5 * time.Millisecond) // judge "works" for 5ms < 10ms budget
	close(gate)
	d := <-res
	if d.State != StateNormal || d.Reason != ReasonCustom || d.ForcedSafe {
		t.Fatalf("state=%s reason=%s forced=%t", d.State, d.Reason, d.ForcedSafe)
	}
	if d.JudgeDuration != 5*time.Millisecond {
		t.Fatalf("judge duration=%s, want 5ms", d.JudgeDuration)
	}
	if c.JudgeTimeoutCount() != 0 {
		t.Fatalf("unexpected timeouts=%d", c.JudgeTimeoutCount())
	}
}

// --- Warm-up grace ---

func warmupCfg() *config.Config {
	cfg := ctrlCfg()
	cfg.Safety.WarmupGrace = 30 * time.Millisecond
	return cfg
}

func TestWarmupMasksMissingFirstData(t *testing.T) {
	cfg := warmupCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)
	c.Reconfigure(cfg) // arms the grace window at the fake now (t0)

	d := c.evaluate(cfg, t0.Add(10*time.Millisecond))
	if !d.Warmup || d.Reason != ReasonWarmup {
		t.Fatalf("expected warmup decision, got reason=%s warmup=%t", d.Reason, d.Warmup)
	}
	if d.State != StateNormal {
		t.Fatalf("warmup must not command SAFE, got %s", d.State)
	}

	// Grace expired with still no data: normal safety applies.
	d = c.evaluate(cfg, t0.Add(31*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonNoData || d.Warmup {
		t.Fatalf("after grace: state=%s reason=%s warmup=%t", d.State, d.Reason, d.Warmup)
	}
}

func TestWarmupEndsEarlyWhenFirstBatchArrives(t *testing.T) {
	cfg := warmupCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)
	c.Reconfigure(cfg)

	healthyAt(c, cfg, t0.Add(5*time.Millisecond))
	d := c.evaluate(cfg, t0.Add(10*time.Millisecond))
	if d.Warmup || d.State != StateNormal || d.Reason != ReasonHealthy {
		t.Fatalf("state=%s reason=%s warmup=%t", d.State, d.Reason, d.Warmup)
	}
}

func TestWarmupDoesNotMaskRealAnomalies(t *testing.T) {
	cfg := warmupCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)
	c.Reconfigure(cfg)

	// Threshold violation during the grace window: must go SAFE at once.
	c.applyEvent(cfg, event("a", sampler.OutcomeAccepted, 1, t0.Add(time.Millisecond), 10))
	c.applyEvent(cfg, event("b", sampler.OutcomeAccepted, 1, t0.Add(time.Millisecond), 60))
	d := c.evaluate(cfg, t0.Add(5*time.Millisecond))
	if d.State != StateSafe || d.Reason != ReasonThreshold || d.Warmup {
		t.Fatalf("violation masked: state=%s reason=%s warmup=%t", d.State, d.Reason, d.Warmup)
	}
}

func TestWarmupDoesNotMaskConsecutiveErrors(t *testing.T) {
	cfg := warmupCfg()
	c, _ := newController(cfg)
	t0 := time.Unix(0, 0)
	c.Reconfigure(cfg)

	// Source errors from the very first read: a genuine anomaly that the
	// grace window must not cover.
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	c.applyEvent(cfg, event("a", sampler.OutcomeReadError, 0, time.Time{}, 0))
	d := c.evaluate(cfg, t0.Add(5*time.Millisecond))
	if d.Warmup {
		t.Fatalf("consecutive anomalies masked by warmup: %+v", d)
	}
	if d.State != StateSafe {
		t.Fatalf("expected SAFE on real anomaly during warmup, got %s (%s)", d.State, d.Reason)
	}
}

func TestWarmupDisabledByDefault(t *testing.T) {
	cfg := ctrlCfg() // WarmupGrace == 0
	c, _ := newController(cfg)
	c.Reconfigure(cfg)
	d := c.evaluate(cfg, time.Unix(0, 0))
	if d.Warmup || d.State != StateSafe || d.Reason != ReasonNoData {
		t.Fatalf("legacy behaviour changed: state=%s reason=%s warmup=%t", d.State, d.Reason, d.Warmup)
	}
}
