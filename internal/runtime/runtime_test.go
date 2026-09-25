package runtime

import (
	"bytes"
	"context"
	runtime2 "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/control"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

func e2eConfig(v int64) *config.Config {
	return &config.Config{
		Version: v,
		Period:  config.Duration{Duration: 10 * time.Millisecond},
		Channels: []config.Channel{
			{ID: "a", MinValue: -100, MaxValue: 100, Required: true},
		},
		Threshold: map[string]float64{"a": 5},
		Safety: config.Safety{
			DataTimeout:        config.Duration{Duration: 100 * time.Millisecond},
			ConsecutiveFaults:  3,
			DecisionDeadline:   config.Duration{Duration: 10 * time.Millisecond},
			RecoveryStableRuns: 1,
			SafeOutput:         -7,
		},
	}
}

func waitDecision(t *testing.T, ch <-chan model.Decision) model.Decision {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for decision")
		return model.Decision{}
	}
}

// TestEndToEndNormalToSafeAndRecovery drives a full runtime with the fake
// clock: missing data trips safe, fresh data recovers.
func TestEndToEndNormalToSafeAndRecovery(t *testing.T) {
	t0 := time.Unix(5000, 0)
	f := clock.NewFake(t0)
	mock := sensor.NewMockSource([]model.ChannelID{"a"})
	rt, err := New(Options{
		Clock: f, Initial: e2eConfig(1), Sources: []sensor.Sensor{mock},
		Log: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- rt.Run(ctx) }()
	<-rt.Ready()

	// first tick with fresh data -> normal
	mock.Push(model.Sample{Channel: "a", Seq: 1, Time: t0, Value: 1})
	f.Advance(10 * time.Millisecond)
	d := waitDecision(t, rt.Decisions())
	if d.State != model.StateNormal || d.Version != 1 {
		t.Fatalf("want normal v1, got %s v%d reasons=%v", d.State, d.Version, d.Reasons)
	}

	// no fresh data beyond 100ms -> safe
	f.Advance(101 * time.Millisecond)
	d = waitDecision(t, rt.Decisions())
	if d.State != model.StateSafe || d.Output != -7 {
		t.Fatalf("want safe -7, got %s %v reasons=%v", d.State, d.Output, d.Reasons)
	}

	// fresh data for recoveryStableRuns=1 tick -> normal
	now := f.Now()
	mock.Push(model.Sample{Channel: "a", Seq: 2, Time: now, Value: 2})
	f.Advance(10 * time.Millisecond)
	d = waitDecision(t, rt.Decisions())
	if d.State != model.StateNormal {
		t.Fatalf("want recovery to normal, got %s reasons=%v", d.State, d.Reasons)
	}

	cancel()
	mock.Close()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop")
	}
	if nt, ntk := f.Waiters(); nt != 0 || ntk != 0 {
		t.Fatalf("clock leak timers=%d tickers=%d", nt, ntk)
	}
}

// slowController blocks its first evaluation until released, then delegates.
type slowController struct {
	inner   control.Controller
	calls   atomic.Int64
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func newSlowController(inner control.Controller) *slowController {
	return &slowController{
		inner:   inner,
		blocked: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *slowController) Evaluate(in control.Input) model.Decision {
	if s.calls.Add(1) == 1 {
		// Block BEFORE taking the inner controller's lock, modeling a
		// decision that is stuck in pre-processing. ForceSafe must still be
		// able to enter and command safety concurrently.
		s.blocked <- struct{}{}
		<-s.release
		// On release, this generation was already superseded by ForceSafe;
		// the controller discards it without mutating state.
	}
	return s.inner.Evaluate(in)
}

func (s *slowController) State() model.SafetyState { return s.inner.State() }
func (s *slowController) Reset()                   { s.inner.Reset() }
func (s *slowController) ForceSafe(now time.Time, generation int64, reason string) model.Decision {
	return s.inner.ForceSafe(now, generation, reason)
}

// TestDecisionDeadlineForcesSafe verifies the deadline monitor: a decision
// that cannot complete within budget forces the safe state.
func TestDecisionDeadlineForcesSafe(t *testing.T) {
	t0 := time.Unix(5000, 0)
	f := clock.NewFake(t0)
	mock := sensor.NewMockSource([]model.ChannelID{"a"})

	st, err := config.NewStore(e2eConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	inner, err := control.New(control.Options{Store: st, Log: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	slow := newSlowController(inner)

	rt, err := New(Options{
		Clock: f, Initial: e2eConfig(1), Sources: []sensor.Sensor{mock},
		Controller: slow, Log: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- rt.Run(ctx) }()
	<-rt.Ready()

	mock.Push(model.Sample{Channel: "a", Seq: 1, Time: t0, Value: 1})
	f.Advance(10 * time.Millisecond)
	select {
	case <-slow.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("first evaluation never started")
	}
	// Advance past the decision deadline while the evaluation is stuck.
	f.Advance(15 * time.Millisecond)
	d := waitDecision(t, rt.Decisions())
	if d.State != model.StateSafe {
		t.Fatalf("deadline must force safe, got %s reasons=%v", d.State, d.Reasons)
	}

	cancel()
	mock.Close()
	close(slow.release) // unblock the abandoned evaluation
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop")
	}
}

// TestConcurrentConfigReload hammers reloads and decisions concurrently and
// asserts decisions always reference internally consistent config versions.
func TestConcurrentConfigReload(t *testing.T) {
	t0 := time.Unix(5000, 0)
	f := clock.NewFake(t0)
	mock := sensor.NewMockSource([]model.ChannelID{"a"})
	rt, err := New(Options{
		Clock: f, Initial: e2eConfig(1), Sources: []sensor.Sensor{mock},
		Log: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- rt.Run(ctx) }()
	<-rt.Ready()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // monotonic reloads
		defer wg.Done()
		for v := int64(2); v <= 60; v++ {
			c := e2eConfig(v)
			// alternate period and safe output
			if v%2 == 0 {
				c.Period = config.Duration{Duration: 20 * time.Millisecond}
			}
			c.Safety.SafeOutput = float64(v)
			if _, err := rt.Reload(ctx, c); err != nil {
				return
			}
			f.Advance(20 * time.Millisecond)
			time.Sleep(time.Millisecond)
		}
	}()
	go func() { // decision observer: every emitted decision must be coherent
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-rt.Decisions():
				if !ok {
					return
				}
				if d.Version < 1 {
					t.Errorf("decision with invalid version %d", d.Version)
					return
				}
				if d.State == model.StateSafe && d.Output < -100 {
					t.Errorf("incoherent safe output %v", d.Output)
					return
				}
			}
		}
	}()

	// feed data concurrently
	feedDone := make(chan struct{})
	go func() {
		seq := int64(1)
		for {
			select {
			case <-ctx.Done():
				close(feedDone)
				return
			default:
				mock.Push(model.Sample{Channel: "a", Seq: seq, Time: f.Now(), Value: 1})
				seq++
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	mock.Close()
	<-feedDone
	wg.Wait()
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop")
	}
}

// TestNoGoroutineOrClockLeak verifies that after a full start/stop cycle no
// runtime goroutines remain and the fake clock has no scheduled timers.
func TestNoGoroutineOrClockLeak(t *testing.T) {
	// Warm up so background test-infrastructure goroutines are accounted for.
	runtime2.Stack(make([]byte, 1<<16), true)
	base := goroutineCount()

	t0 := time.Unix(7000, 0)
	for iter := 0; iter < 3; iter++ {
		f := clock.NewFake(t0)
		mock := sensor.NewMockSource([]model.ChannelID{"a"})
		rt, err := New(Options{
			Clock: f, Initial: e2eConfig(1), Sources: []sensor.Sensor{mock},
			Log: func(string, ...any) {},
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- rt.Run(ctx) }()
		<-rt.Ready()
		mock.Push(model.Sample{Channel: "a", Seq: 1, Time: f.Now(), Value: 1})
		f.Advance(10 * time.Millisecond)
		select {
		case <-rt.Decisions():
		case <-time.After(2 * time.Second):
			t.Fatal("no decision")
		}
		cancel()
		mock.Close()
		<-runErr
		if nt, ntk := f.Waiters(); nt != 0 || ntk != 0 {
			t.Fatalf("iter %d clock leak timers=%d tickers=%d", iter, nt, ntk)
		}
	}

	// After several cycles and forced GC, goroutine count must not retain
	// runtime workers (allow a small margin for test harness goroutines).
	deadline := time.Now().Add(2 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		runtime2.GC()
		n = goroutineCount()
		if n <= base+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: base=%d now=%d", base, n)
}

func goroutineCount() int {
	// Drain and count goroutines via a stack dump.
	buf := make([]byte, 1<<16)
	for {
		n := runtime2.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte("\ngoroutine "))
		}
		buf = make([]byte, 2*len(buf))
	}
}
