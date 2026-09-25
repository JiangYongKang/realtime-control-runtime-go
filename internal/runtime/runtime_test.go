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

type discardLog struct{}

func (discardLog) Printf(string, ...any) {}

func rtConfig(v int64) *config.Config {
	return &config.Config{
		Version: v,
		Channels: []config.ChannelCfg{{
			ID:       "c1",
			Period:   10 * time.Millisecond,
			MaxLag:   100 * time.Millisecond,
			MinValue: 0,
			MaxValue: 100,
		}},
		Safety: config.SafetyCfg{
			ControlPeriod:     10 * time.Millisecond,
			DecisionDeadline:  50 * time.Millisecond,
			MaxConsecutiveErr: 2,
			RecoverGoodTicks:  3,
		},
	}
}

// healthySource produces fresh in-range samples keyed to the fake clock.
type healthySource struct {
	clk *clock.FakeClock
	seq atomic.Uint64
}

func (h *healthySource) Read(ctx context.Context) (sensor.Sample, error) {
	now := h.clk.Now()
	return sensor.Sample{ChannelID: "c1", Value: 10, Timestamp: now, Seq: h.seq.Add(1)}, nil
}

func waitForState(t *testing.T, rt *Runtime, want control.State, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if rt.Controller.State() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("state did not reach %s, is %s", want, rt.Controller.State())
}

// startRuntime advances a fake clock in the background until stopped.
func startRuntime(t *testing.T, rt *Runtime, fc *clock.FakeClock) (cancel func(), done chan struct{}, stopAdv func()) {
	ctx, c := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() { defer close(done); _ = rt.Run(ctx) }()

	advCtx, stopAdvancer := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-advCtx.Done():
				return
			default:
				fc.Advance(10 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	return c, done, stopAdvancer
}

func TestRuntimeHealthyReachesNormal(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtConfig(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()

	waitForState(t, rt, control.StateNormal, 3*time.Second)
}

// failingSource returns 4 healthy samples then fails permanently,
// simulating a dead device; the runtime must enter SAFE.
type failingSource struct {
	clk *clock.FakeClock
	seq atomic.Uint64
}

func (f *failingSource) Read(ctx context.Context) (sensor.Sample, error) {
	now := f.clk.Now()
	if now.UnixMilli() >= 60 {
		return sensor.Sample{}, sensor.ErrUnavailable
	}
	return sensor.Sample{ChannelID: "c1", Value: 10, Timestamp: now, Seq: f.seq.Add(1)}, nil
}

func TestRuntimeDeviceFaultEntersSafe(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtConfig(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &failingSource{clk: fc}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()

	waitForState(t, rt, control.StateNormal, 3*time.Second)
	waitForState(t, rt, control.StateSafe, 3*time.Second)
	// Once failing, read errors keep accumulating; allow the polling
	// loop a moment to record them after the transition.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rt.Sampler.ReadErrorCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if rt.Sampler.ReadErrorCount() == 0 {
		t.Fatal("expected sampler to record read errors")
	}
}

func TestRuntimeReloadRejectsAndInstallsAtomically(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtConfig(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)
	defer func() { stopAdv(); cancel(); <-done }()

	waitForState(t, rt, control.StateNormal, 3*time.Second)

	// Invalid config: conflicting thresholds; must be rejected, version stays.
	bad := rtConfig(2)
	bad.Threshold = config.ThresholdCfg{WarnLow: 9, WarnHigh: 0}
	if _, err := rt.Reload(bad); err == nil {
		t.Fatal("invalid reload accepted")
	}
	if rt.CfgMgr.Current().Version != 1 {
		t.Fatalf("rejected reload changed version to %d", rt.CfgMgr.Current().Version)
	}

	// Version rewind rejected.
	rewind := rtConfig(1)
	if _, err := rt.Reload(rewind); err == nil {
		t.Fatal("version rewind accepted")
	}

	// Valid reload installs and components stamp the new version.
	good := rtConfig(5)
	good.Safety.MaxConsecutiveErr = 3
	good.Safety.RecoverGoodTicks = 4
	installed, err := rt.Reload(good)
	if err != nil {
		t.Fatalf("valid reload: %v", err)
	}
	if installed.Version != 5 {
		t.Fatalf("installed version=%d", installed.Version)
	}
	waitForState(t, rt, control.StateNormal, 3*time.Second)
}

func TestRuntimeConcurrentReloads(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	rt, err := New(rtConfig(1), Options{
		Clock:   fc,
		Log:     discardLog{},
		Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done, stopAdv := startRuntime(t, rt, fc)

	var rejected atomic.Int64
	var wg sync.WaitGroup
	// Hammer updates concurrently with mixed versions and contents.
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < 30; i++ {
				c := rtConfig(base*100 + i + 2)
				if i%5 == 0 {
					// Deliberately invalid every fifth attempt.
					c.Safety.RecoverGoodTicks = 1
				}
				if _, err := rt.Reload(c); err != nil {
					rejected.Add(1)
				}
				// Active snapshot must always be internally valid.
				if err := config.Validate(rt.CfgMgr.Current()); err != nil {
					t.Errorf("observed invalid active config: %v", err)
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
		t.Fatalf("expected some reload to install, version=%d", v)
	}
}

func TestRuntimeStopsAllGoroutines(t *testing.T) {
	// Repeated start/stop must not leak sampling or control goroutines.
	for i := 0; i < 5; i++ {
		fc := clock.NewFake(time.Unix(0, 0))
		rt, err := New(rtConfig(int64(i+1)), Options{
			Clock:   fc,
			Log:     discardLog{},
			Sources: map[string]sensor.Source{"c1": &healthySource{clk: fc}},
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

	// Assert via stack inspection that no runtime goroutines survive.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		stacks := string(buf[:n])
		if !strings.Contains(stacks, "internal/sampler.(*Sampler).runChannel") &&
			!strings.Contains(stacks, "internal/control.(*Controller).Run") &&
			!strings.Contains(stacks, "internal/runtime.drain") {
			return
		}
	}
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Fatalf("runtime goroutine leak detected:\n%s", string(buf[:n]))
}
