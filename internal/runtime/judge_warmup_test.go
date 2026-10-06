package runtime

import (
	"context"
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

func rtCfgNew(v int64) *config.Config {
	c := rtConfig(v)
	c.Safety.ControlPeriod = 10 * time.Millisecond
	c.Safety.DecisionDeadline = 60 * time.Millisecond
	c.Safety.JudgeTimeout = 10 * time.Millisecond
	c.Safety.WarmupGrace = 200 * time.Millisecond
	return c
}

// recordingJudge simulates a slow local algorithm that honors ctx; a
// deadline cancellation fails the call.
type recordingJudge struct {
	delay time.Duration
	hung  chan struct{} // non-nil: block forever ignoring ctx
}

func (j *recordingJudge) Judge(ctx context.Context, in control.JudgeInput) (control.Verdict, error) {
	if j.hung != nil {
		<-j.hung
	}
	select {
	case <-time.After(j.delay):
		return control.Verdict{Safe: false}, nil
	case <-ctx.Done():
		return control.Verdict{Safe: true}, ctx.Err()
	}
}

func TestRuntimeSlowJudgeFallsBackToSafe(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtCfgNew(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
		Judge:   (&recordingJudge{delay: 80 * time.Millisecond}).Judge,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()

	var forced atomic.Int64
	collect := make(chan struct{})
	go func() {
		for {
			select {
			case <-collect:
				return
			case d := <-rt.Controller.Decisions():
				if d.ForcedSafe {
					forced.Add(1)
				}
			}
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && forced.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	close(collect)
	if forced.Load() == 0 {
		t.Fatal("runtime never forced safe despite slow judge")
	}
	if rt.Controller.JudgeTimeoutCount() == 0 {
		t.Fatal("judge timeout count stayed zero")
	}
}

// TestRuntimeHungJudgeLoopSurvives: a judge that never returns must not kill
// the control loop; safe ticks keep being produced and shutdown completes
// without waiting on the stuck call. The judge is released at cleanup so no
// goroutine survives the test.
func TestRuntimeHungJudgeLoopSurvives(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	hungRelease := make(chan struct{})
	rt, err := New(rtCfgNew(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
		Judge:   (&recordingJudge{hung: hungRelease}).Judge,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, c := context.WithCancel(context.Background())
	rtDone := make(chan struct{})
	go func() { defer close(rtDone); _ = rt.Run(ctx) }()

	advStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-advStop:
				return
			default:
				fc.Advance(10 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	waitForState(t, rt, control.StateSafe, 3*time.Second)
	if n := rt.Controller.JudgeTimeoutCount(); n == 0 {
		t.Fatal("expected forced-safe ticks while hung, count=0")
	}
	c()
	select {
	case <-rtDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return even with a stuck judge")
	}
	close(advStop)
	// Free the abandoned worker so the goroutine exits after the loop has
	// already returned (proving shutdown never waits on user code).
	close(hungRelease)
}

// noDataSource never returns a sample within the test window.
type noDataSource struct{}

func (noDataSource) Read(ctx context.Context) (sensor.Sample, error) {
	<-ctx.Done()
	return sensor.Sample{}, ctx.Err()
}

func TestRuntimeWarmupThenSafeWithoutData(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtCfgNew(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": noDataSource{}},
		Judge:   (&recordingJudge{delay: time.Millisecond}).Judge,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, c := context.WithCancel(context.Background())
	rtDone := make(chan struct{})
	go func() { defer close(rtDone); _ = rt.Run(ctx) }()

	var sawWarming, sawSafe atomic.Bool
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-rt.Controller.Decisions():
				if d.Warming && d.State == control.StateWarming {
					sawWarming.Store(true)
				}
				if d.State == control.StateSafe {
					sawSafe.Store(true)
				}
			}
		}
	}()

	advStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-advStop:
				return
			default:
				fc.Advance(10 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !(sawWarming.Load() && sawSafe.Load()) {
		time.Sleep(10 * time.Millisecond)
	}
	close(advStop)
	c()
	<-rtDone
	<-collectDone
	if !sawWarming.Load() {
		t.Fatal("never observed WARMING during grace window")
	}
	if !sawSafe.Load() {
		t.Fatal("never observed SAFE after grace expired without data")
	}
}

func TestRuntimeWarmupWithHealthyDataReachesNormal(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtCfgNew(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
		Judge:   (&recordingJudge{delay: time.Millisecond}).Judge,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()
	waitForState(t, rt, control.StateNormal, 3*time.Second)
}

// TestRuntimeConcurrentReloadsWithNewFields hammers reload including the new
// judge_timeout/warmup_grace fields under -race over repeated rounds.
func TestRuntimeConcurrentReloadsWithNewFields(t *testing.T) {
	for round := 0; round < 5; round++ {
		fc := clock.NewFake(time.Unix(0, 0))
		rt, err := New(rtCfgNew(1), Options{
			Clock:   fc,
			Log:     discardLog{},
			Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
			Judge:   (&recordingJudge{delay: time.Millisecond}).Judge,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, c := context.WithCancel(context.Background())
		rtDone := make(chan struct{})
		go func() { defer close(rtDone); _ = rt.Run(ctx) }()

		advStop := make(chan struct{})
		go func() {
			for {
				select {
				case <-advStop:
					return
				default:
					fc.Advance(10 * time.Millisecond)
					time.Sleep(time.Millisecond)
				}
			}
		}()

		var wg sync.WaitGroup
		for g := 0; g < 6; g++ {
			wg.Add(1)
			go func(base int64) {
				defer wg.Done()
				for i := int64(0); i < 20; i++ {
					cfg := rtCfgNew(base*100 + i + 2)
					switch i % 4 {
					case 0:
						cfg.Safety.JudgeTimeout = cfg.Safety.ControlPeriod // at boundary
					case 1:
						cfg.Safety.WarmupGrace = 0 // disabled
					case 2:
						cfg.Safety.JudgeTimeout = 0 // unset -> control period
					case 3:
						cfg.Safety.WarmupGrace = time.Second
					}
					if i%7 == 0 {
						cfg.Safety.JudgeTimeout = time.Hour // invalid
					}
					if _, err := rt.Reload(cfg); err != nil {
						// rejected updates must leave a valid snapshot
						if verr := config.Validate(rt.CfgMgr.Current()); verr != nil {
							t.Errorf("invalid active snapshot after rejection: %v", verr)
						}
					}
				}
			}(int64(g))
		}
		wg.Wait()
		close(advStop)
		c()
		<-rtDone
	}
}

// TestRuntimeJudgeNoGoroutineLeak: with a well-behaved (ctx-honoring) judge,
// repeated start/stop leaves no control or judge-worker goroutines behind.
func TestRuntimeJudgeNoGoroutineLeak(t *testing.T) {
	for i := 0; i < 5; i++ {
		fc := clock.NewFake(time.Unix(0, 0))
		rt, err := New(rtCfgNew(int64(i+1)), Options{
			Clock:   fc,
			Log:     discardLog{},
			Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
			Judge:   (&recordingJudge{delay: time.Millisecond}).Judge,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, c := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); _ = rt.Run(ctx) }()
		advStop := make(chan struct{})
		go func() {
			for {
				select {
				case <-advStop:
					return
				default:
					fc.Advance(10 * time.Millisecond)
					time.Sleep(time.Millisecond)
				}
			}
		}()
		time.Sleep(30 * time.Millisecond)
		c()
		<-done
		close(advStop)
	}
	leakMarkers := []string{
		"internal/control.(*Controller).Run",
		"internal/sampler.(*Sampler).runChannel",
		"internal/runtime.drain",
		"internal/control.(*Controller).runJudgeWorker",
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := stackLen(buf)
		stacks := string(buf[:n])
		leaked := false
		for _, m := range leakMarkers {
			if strings.Contains(stacks, m) {
				leaked = true
			}
		}
		if !leaked {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	buf := make([]byte, 1<<20)
	n := stackLen(buf)
	t.Fatalf("runtime goroutine leak:\n%s", string(buf[:n]))
}

func stackLen(buf []byte) int {
	return stackCapture(buf)
}
