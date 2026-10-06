package runtime

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/control"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

// blockingJudge blocks until released, then answers healthy.
type blockingJudge struct {
	release chan struct{}
	calls   atomic.Int64
}

func (j *blockingJudge) Judge(control.JudgeInput) control.Verdict {
	j.calls.Add(1)
	<-j.release
	return control.Verdict{Healthy: true}
}

// TestRuntimeJudgeTimeoutForcesSafe: a stuck custom judge must not stall
// the control loop — the runtime forces SAFE within the judge budget, holds
// it while the judgment is stuck, and discards the late verdict once the
// judge finally returns.
func TestRuntimeJudgeTimeoutForcesSafe(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	judge := &blockingJudge{release: make(chan struct{})}
	rt, err := New(rtConfig(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
		Judge:   judge,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()

	// The judge never answers: the runtime must force SAFE on its own.
	waitForState(t, rt, control.StateSafe, 3*time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rt.Controller.JudgeTimeoutCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if rt.Controller.JudgeTimeoutCount() == 0 {
		t.Fatal("judge timeout never forced the safe state")
	}
	// At most one abandoned call may be in flight while it stays stuck.
	if n := judge.calls.Load(); n != 1 {
		t.Fatalf("judge calls=%d, want exactly 1 outstanding", n)
	}

	// Unblock the judge: the late verdict is discarded; fresh judgments
	// are healthy and the runtime recovers through the normal debounce.
	close(judge.release)
	waitForState(t, rt, control.StateNormal, 3*time.Second)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rt.Controller.LateVerdictCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if rt.Controller.LateVerdictCount() == 0 {
		t.Fatal("late verdict was never reaped")
	}
}

// gatedSource blocks every read until the test opens the gate, then
// delivers healthy samples — simulating a device whose first batch arrives
// late, with the arrival moment fully controlled by the test.
type gatedSource struct {
	gate <-chan struct{}
	clk  *clock.FakeClock
	seq  atomic.Uint64
}

func (g *gatedSource) Read(ctx context.Context) (sensor.Sample, error) {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return sensor.Sample{}, ctx.Err()
	}
	now := g.clk.Now()
	return sensor.Sample{ChannelID: "c1", Value: 10, Timestamp: now, Seq: g.seq.Add(1)}, nil
}

// TestRuntimeWarmupGraceCoversLateFirstBatch: with warm-up grace configured,
// a late first batch must yield distinguishable warm-up decisions instead of
// an immediate SAFE, and the system then reaches NORMAL. The fake clock is
// stepped manually so the observation is fully deterministic.
func TestRuntimeWarmupGraceCoversLateFirstBatch(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	cfg := rtConfig(1)
	cfg.Safety.WarmupGrace = time.Second
	cfg.Threshold = config.ThresholdCfg{WarnLow: 0, WarnHigh: 50}

	gate := make(chan struct{})
	decisions := make(chan control.Decision, 256)
	rt, err := New(cfg, Options{
		Clock:         fc,
		Log:           discardLog{},
		Sources:       map[string]sensor.Source{"c1": &gatedSource{gate: gate, clk: fc}},
		DecisionDrain: func(d control.Decision) { decisions <- d },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = rt.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Step the clock through the whole grace window with the gate closed:
	// every decision must be a warm-up decision, never SAFE. Early
	// iterations may see no decision yet (controller still starting), so a
	// missed receive simply continues to the next step.
	var sawWarmup bool
	for i := 0; i < 15; i++ { // 150ms, well inside the 1s grace
		fc.Advance(10 * time.Millisecond)
		select {
		case d := <-decisions:
			if d.State == control.StateSafe {
				t.Fatalf("SAFE commanded during warm-up grace: %+v", d)
			}
			if d.Warmup {
				sawWarmup = true
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !sawWarmup {
		t.Fatal("no warm-up decision observed for late first batch")
	}

	// Open the gate: the first batch arrives and the system must reach
	// NORMAL without ever having commanded SAFE.
	close(gate)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fc.Advance(10 * time.Millisecond)
		select {
		case d := <-decisions:
			if d.State == control.StateSafe {
				t.Fatalf("SAFE commanded after first batch: %+v", d)
			}
			if d.State == control.StateNormal && d.Reason == control.ReasonHealthy {
				return
			}
		case <-time.After(100 * time.Millisecond):
			// Runtime not scheduled yet; keep stepping.
		}
	}
	t.Fatal("never reached NORMAL after first batch arrived")
}

// neverSource blocks until the runtime shuts down: no sample ever arrives.
type neverSource struct{}

func (neverSource) Read(ctx context.Context) (sensor.Sample, error) {
	<-ctx.Done()
	return sensor.Sample{}, ctx.Err()
}

// TestRuntimeWarmupExpiryEntersSafe: when the grace window expires and data
// is still missing, the normal safety path takes over.
func TestRuntimeWarmupExpiryEntersSafe(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	cfg := rtConfig(1)
	cfg.Safety.WarmupGrace = 50 * time.Millisecond

	rt, err := New(cfg, Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": neverSource{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()

	waitForState(t, rt, control.StateSafe, 3*time.Second)
}

// TestRuntimeConcurrentReloadsWithJudgeAndWarmup hammers hot reloads mixing
// valid and conflicting new safety parameters while a custom judge runs.
func TestRuntimeConcurrentReloadsWithJudgeAndWarmup(t *testing.T) {
	for round := 0; round < 3; round++ {
		fc := clock.NewFake(time.Unix(0, 0))
		judge := &blockingJudge{release: make(chan struct{})}
		close(judge.release) // answers immediately
		rt, err := New(rtConfig(1), Options{
			Clock:   fc,
			Log:     discardLog{},
			Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
			Judge:   judge,
		})
		if err != nil {
			t.Fatal(err)
		}
		cancel, done, stopAdv := startRuntime(t, rt, fc)

		var wg sync.WaitGroup
		for g := 0; g < 6; g++ {
			wg.Add(1)
			go func(base int64) {
				defer wg.Done()
				for i := int64(0); i < 30; i++ {
					c := rtConfig(base*100 + i + 2)
					switch i % 4 {
					case 0: // valid: both new params
						c.Safety.JudgeTimeout = 5 * time.Millisecond
						c.Safety.WarmupGrace = 30 * time.Millisecond
					case 1: // invalid: judge timeout exceeds control period
						c.Safety.JudgeTimeout = time.Second
					case 2: // invalid: warm-up grace above the cap
						c.Safety.WarmupGrace = config.MaxWarmupGrace + time.Second
					}
					if _, err := rt.Reload(c); err != nil {
						// Rejected updates must leave a valid config active.
						if verr := config.Validate(rt.CfgMgr.Current()); verr != nil {
							t.Errorf("active config invalid after rejection: %v", verr)
							return
						}
						continue
					}
					cur := rt.CfgMgr.Current()
					if err := config.Validate(cur); err != nil {
						t.Errorf("observed invalid active config: %v", err)
						return
					}
					if cur.Safety.JudgeTimeout > cur.Safety.ControlPeriod {
						t.Errorf("conflicting judge timeout installed: %s > %s",
							cur.Safety.JudgeTimeout, cur.Safety.ControlPeriod)
						return
					}
				}
			}(int64(g))
		}
		wg.Wait()
		stopAdv()
		cancel()
		<-done
		if v := rt.CfgMgr.Current().Version; v < 2 {
			t.Fatalf("round %d: no reload installed, version=%d", round, v)
		}
	}
}

// TestRuntimeWithJudgeStopsAllGoroutines repeats start/stop with a custom
// judge registered and asserts no runtime or judge goroutines survive.
func TestRuntimeWithJudgeStopsAllGoroutines(t *testing.T) {
	for i := 0; i < 5; i++ {
		fc := clock.NewFake(time.Unix(0, 0))
		judge := &blockingJudge{release: make(chan struct{})}
		close(judge.release)
		rt, err := New(rtConfig(int64(i+1)), Options{
			Clock:   fc,
			Log:     discardLog{},
			Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
			Judge:   judge,
		})
		if err != nil {
			t.Fatal(err)
		}
		cancel, done, stopAdv := startRuntime(t, rt, fc)
		time.Sleep(30 * time.Millisecond)
		stopAdv()
		cancel()
		<-done
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		stacks := string(buf[:n])
		if !strings.Contains(stacks, "internal/sampler.(*Sampler).runChannel") &&
			!strings.Contains(stacks, "internal/control.(*Controller).Run") &&
			!strings.Contains(stacks, "internal/control.(*Controller).judgeDecision") &&
			!strings.Contains(stacks, "internal/runtime.drain") {
			return
		}
	}
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Fatalf("runtime goroutine leak detected:\n%s", string(buf[:n]))
}
