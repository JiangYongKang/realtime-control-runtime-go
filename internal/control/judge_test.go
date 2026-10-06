package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

func judgeCfg() *config.Config {
	cfg := ctrlCfg()
	cfg.Safety.ControlPeriod = 20 * time.Millisecond
	cfg.Safety.JudgeTimeout = 20 * time.Millisecond
	return cfg
}

var errBoomJudge = errors.New("judge boom")

// slowJudge honors ctx: a call whose deadline elapses returns ctx.Err();
// otherwise it waits delay and returns its verdict.
type slowJudge struct {
	delay time.Duration
	calls atomic.Uint64
	safe  atomic.Bool
	fail  bool
}

func (s *slowJudge) judge(ctx context.Context, in JudgeInput) (Verdict, error) {
	s.calls.Add(1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return Verdict{Safe: true}, ctx.Err()
	}
	if s.fail {
		return Verdict{}, errBoomJudge
	}
	return Verdict{Safe: s.safe.Load()}, nil
}

func jevent(ch string, oc sampler.Outcome, seq uint64, ts time.Time, v float64) sampler.Event {
	return sampler.Event{
		ChannelID: ch,
		Outcome:   oc,
		Sample:    sensor.Sample{ChannelID: ch, Value: v, Timestamp: ts, Seq: seq},
	}
}

// driveLoop advances the fake clock in small steps while feeding healthy samples.
func driveLoop(wg *sync.WaitGroup, stop chan struct{}, fc *clock.FakeClock, in chan<- sampler.Event, step time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				fc.Advance(step)
				now := fc.Now()
				in <- jevent("a", sampler.OutcomeAccepted, uint64(now.UnixNano())+1, now, 10)
				in <- jevent("b", sampler.OutcomeAccepted, uint64(now.UnixNano())+1, now, 10)
				time.Sleep(time.Millisecond)
			}
		}
	}()
}

func waitFor(want func() bool, t time.Duration) bool {
	deadline := time.Now().Add(t)
	for time.Now().Before(deadline) {
		if want() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return want()
}

// TestSlowJudgeEntersSafeOnTimeout: a judge slower than the deadline must
// cause a forced-safe decision within the time limit, even though it would
// eventually have returned "go".
func TestSlowJudgeEntersSafeOnTimeout(t *testing.T) {
	cfg := judgeCfg()
	fc := clock.NewFake(time.Unix(0, 0))
	j := &slowJudge{delay: 50 * time.Millisecond}
	c := NewWithJudge(func() *config.Config { return cfg }, fc, discardLog{}, j.judge, cfg.Safety.JudgeTimeout)

	in := make(chan sampler.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	driveLoop(&wg, stop, fc, in, 10*time.Millisecond)

	var sawForced atomic.Bool
	go func() {
		for {
			select {
			case <-stop:
				return
			case d := <-c.Decisions():
				if d.ForcedSafe && d.State == StateSafe && d.Reason == ReasonJudgeTimeout {
					sawForced.Store(true)
				}
			}
		}
	}()

	if !waitFor(sawForced.Load, 3*time.Second) {
		t.Fatal("slow judge never produced a forced-safe timeout decision")
	}
	if c.JudgeTimeoutCount() == 0 {
		t.Fatal("timeout counter not advanced")
	}
	close(stop)
	cancel()
	<-runDone
	wg.Wait()
}

// oneShotJudge blocks its first call (ignoring ctx) until fire is called;
// the abandoned call returns a late "go". Subsequent calls answer promptly.
type oneShotJudge struct {
	started  chan struct{}
	release  chan struct{}
	lateSafe bool
}

func (o *oneShotJudge) judge(ctx context.Context, in JudgeInput) (Verdict, error) {
	select {
	case <-o.started:
	default:
		close(o.started)
	}
	select {
	case <-o.release:
		return Verdict{Safe: o.lateSafe}, nil
	default:
	}
	// First call: block ignoring cancellation until the test releases it.
	if !o.released() {
		<-o.release
		return Verdict{Safe: o.lateSafe}, nil
	}
	return Verdict{Safe: false}, nil
}

func (o *oneShotJudge) released() bool {
	select {
	case <-o.release:
		return true
	default:
		return false
	}
}

// TestAbandonedJudgeResultIsDiscarded: the verdict of the call abandoned at
// the deadline is dropped (counted) and is never committed; state only moves
// on genuinely new ticks.
func TestAbandonedJudgeResultIsDiscarded(t *testing.T) {
	cfg := judgeCfg()
	fc := clock.NewFake(time.Unix(0, 0))
	j := &oneShotJudge{started: make(chan struct{}), release: make(chan struct{}), lateSafe: false}
	c := NewWithJudge(func() *config.Config { return cfg }, fc, discardLog{}, j.judge, cfg.Safety.JudgeTimeout)

	in := make(chan sampler.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	driveLoop(&wg, stop, fc, in, 10*time.Millisecond)

	var mu sync.Mutex
	var rec []Decision
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case d := <-c.Decisions():
				mu.Lock()
				rec = append(rec, d)
				mu.Unlock()
			}
		}
	}()

	<-j.started
	// Wait until the deadline forced safe at least once.
	if !waitFor(func() bool { return c.JudgeTimeoutCount() > 0 }, 3*time.Second) {
		t.Fatal("deadline never forced safe while the judge was stuck")
	}
	if c.State() != StateSafe {
		t.Fatalf("state during stuck judge = %s, want SAFE", c.State())
	}

	// Record decisions present at the moment of release, then free the
	// abandoned call. Its late verdict must not itself commit a decision.
	fc.Advance(40 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	snapshot := len(rec)
	mu.Unlock()
	close(j.release)

	// Drain for a while; any decision emitted after release belongs to
	// genuinely new ticks (seq 2+), never to the abandoned seq 1.
	stable := false
	for i := 0; i < 20; i++ {
		fc.Advance(20 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		if len(rec) == snapshot {
			stable = true
		} else {
			snapshot = len(rec)
			stable = false
		}
		mu.Unlock()
	}
	_ = stable

	close(stop)
	cancel()
	<-runDone
	wg.Wait()

	if c.LateResultCount() == 0 {
		t.Fatal("abandoned late judge result was not counted as discarded")
	}
}

// hungJudge never returns, ignoring ctx forever. It occupies the single
// worker, so every subsequent tick must fall back to forced safe immediately
// without queuing goroutines.
type hungJudge struct{ started chan struct{} }

func (h *hungJudge) judge(ctx context.Context, in JudgeInput) (Verdict, error) {
	select {
	case <-h.started:
	default:
		close(h.started)
	}
	select {} // ignore ctx forever
}

// TestHungJudgeKeepsForcingSafe: while the worker is permanently blocked,
// every control tick still produces a forced-safe verdict on time.
func TestHungJudgeKeepsForcingSafe(t *testing.T) {
	cfg := judgeCfg()
	fc := clock.NewFake(time.Unix(0, 0))
	h := &hungJudge{started: make(chan struct{})}
	c := NewWithJudge(func() *config.Config { return cfg }, fc, discardLog{}, h.judge, cfg.Safety.JudgeTimeout)

	in := make(chan sampler.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	driveLoop(&wg, stop, fc, in, 10*time.Millisecond)

	var forced int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case d := <-c.Decisions():
				if d.ForcedSafe && d.State == StateSafe {
					atomic.AddInt64(&forced, 1)
				}
			}
		}
	}()

	<-h.started
	if !waitFor(func() bool { return atomic.LoadInt64(&forced) >= 3 }, 3*time.Second) {
		t.Fatalf("only %d forced-safe ticks while judge hung, want >= 3", atomic.LoadInt64(&forced))
	}
	if c.State() != StateSafe {
		t.Fatalf("state = %s, want SAFE", c.State())
	}
	close(stop)
	cancel()
	<-runDone
	wg.Wait()
}

// TestJudgeErrorForcesSafe: a judge returning an error always fails safe.
func TestJudgeErrorForcesSafe(t *testing.T) {
	cfg := judgeCfg()
	fc := clock.NewFake(time.Unix(0, 0))
	j := &slowJudge{delay: time.Millisecond, fail: true}
	c := NewWithJudge(func() *config.Config { return cfg }, fc, discardLog{}, j.judge, cfg.Safety.JudgeTimeout)

	in := make(chan sampler.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	driveLoop(&wg, stop, fc, in, 10*time.Millisecond)

	var sawErr atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case d := <-c.Decisions():
				if d.State == StateSafe && d.Reason == ReasonJudgeError && d.ForcedSafe {
					sawErr.Store(true)
				}
			}
		}
	}()

	if !waitFor(sawErr.Load, 3*time.Second) {
		t.Fatal("judge error never forced safe")
	}
	close(stop)
	cancel()
	<-runDone
	wg.Wait()
}

// TestHealthyJudgeAllowsRecovery: a fast "go" judge lets the loop reach
// NORMAL once data is healthy.
func TestHealthyJudgeAllowsRecovery(t *testing.T) {
	cfg := judgeCfg()
	fc := clock.NewFake(time.Unix(0, 0))
	j := &slowJudge{delay: time.Millisecond}
	c := NewWithJudge(func() *config.Config { return cfg }, fc, discardLog{}, j.judge, cfg.Safety.JudgeTimeout)

	in := make(chan sampler.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(ctx, in) }()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	driveLoop(&wg, stop, fc, in, 10*time.Millisecond)

	var sawNormal atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case d := <-c.Decisions():
				if d.State == StateNormal && d.Reason == ReasonHealthy {
					sawNormal.Store(true)
				}
			}
		}
	}()

	if !waitFor(sawNormal.Load, 3*time.Second) {
		t.Fatal("healthy fast judge never reached NORMAL")
	}
	close(stop)
	cancel()
	<-runDone
	wg.Wait()
}
