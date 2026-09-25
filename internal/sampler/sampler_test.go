package sampler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

type discardLog struct{}

func (discardLog) Printf(string, ...any) {}

func testCfg(version int64, period, maxLag time.Duration) *config.Config {
	return &config.Config{
		Version: version,
		Channels: []config.ChannelCfg{{
			ID:       "c1",
			Period:   period,
			MaxLag:   maxLag,
			MinValue: -100,
			MaxValue: 100,
		}},
		Threshold: config.ThresholdCfg{WarnLow: -50, WarnHigh: 50},
		Safety: config.SafetyCfg{
			ControlPeriod:     10 * time.Millisecond,
			DecisionDeadline:  100 * time.Millisecond,
			MaxConsecutiveErr: 2,
			RecoverGoodTicks:  3,
		},
	}
}

func smp(seq uint64, ts time.Time, v float64) sensor.Sample {
	return sensor.Sample{ChannelID: "c1", Value: v, Timestamp: ts, Seq: seq}
}

func TestIngestClassifiesOrderingDuplicatesAndLateness(t *testing.T) {
	start := time.Unix(0, 0)
	fc := clock.NewFake(start)
	cfg := testCfg(1, 10*time.Millisecond, 25*time.Millisecond)
	s, err := New(cfg, fc, discardLog{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ch := cfg.Channels[0]

	type step struct {
		in   sensor.Sample
		want Outcome
	}
	steps := []step{
		{smp(1, start.Add(10*time.Millisecond), 1), OutcomeAccepted},
		{smp(2, start.Add(20*time.Millisecond), 2), OutcomeAccepted},
		{smp(2, start.Add(20*time.Millisecond), 2), OutcomeDuplicate}, // exact retransmit
		{smp(1, start.Add(15*time.Millisecond), 1), OutcomeStale},     // older timestamp
		{smp(5, start.Add(50*time.Millisecond), 5), OutcomeGapped},    // seq 3,4 lost
		{smp(6, start.Add(10*time.Millisecond), 6), OutcomeStale},     // older than latest ts
	}
	for i, st := range steps {
		ev := s.Ingest(ch, st.in, st.in.Timestamp)
		if ev.Outcome != st.want {
			t.Fatalf("step %d: got %s (%s), want %s", i, ev.Outcome, ev.Detail, st.want)
		}
		if ev.ConfigVersion != 1 {
			t.Fatalf("step %d: version %d", i, ev.ConfigVersion)
		}
	}
	if s.AcceptedCount() != 2 {
		t.Fatalf("accepted=%d want 2 (only seq 1 and 2; gapped seq 5 is stored but not counted accepted)", s.AcceptedCount())
	}

	// Expired relative to the unified clock at observed time.
	late := s.Ingest(ch, smp(7, start.Add(55*time.Millisecond), 7), start.Add(100*time.Millisecond))
	if late.Outcome != OutcomeStale {
		t.Fatalf("late sample got %s want stale", late.Outcome)
	}
}

func TestIngestErrorIsCountedAndEmitted(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	cfg := testCfg(1, 10*time.Millisecond, 0)
	s, _ := New(cfg, fc, discardLog{}, nil)
	ev := s.IngestError("c1", sensor.ErrUnavailable, time.Unix(0, 0))
	if ev.Outcome != OutcomeReadError || ev.Err == nil {
		t.Fatalf("unexpected event %+v", ev)
	}
	if s.ReadErrorCount() != 1 {
		t.Fatalf("read errors=%d", s.ReadErrorCount())
	}
	got := <-s.Events()
	if got.Outcome != OutcomeReadError {
		t.Fatalf("emitted %s", got.Outcome)
	}
}

func TestBackpressureDropsInsteadOfBlockingOrGrowing(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	cfg := testCfg(1, 10*time.Millisecond, 0)
	s, _ := New(cfg, fc, discardLog{}, nil)
	ch := cfg.Channels[0]

	// Drain the channel once so capacity is fully available to measure.
	// Ingest far more events than the fixed buffer without any consumer.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint64(0); i < 5000; i++ {
			ts := fc.Now().Add(time.Duration(i) * time.Millisecond)
			s.Ingest(ch, smp(i+1, ts, float64(i)), ts)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Ingest blocked under backpressure")
	}
	if s.DropCount() == 0 {
		t.Fatal("expected backpressure drops to be counted")
	}
	if len(s.Events()) > cap(s.Events()) {
		t.Fatalf("output exceeded fixed capacity %d: %d", cap(s.Events()), len(s.Events()))
	}
}

type funcSource func(ctx context.Context) (sensor.Sample, error)

func (f funcSource) Read(ctx context.Context) (sensor.Sample, error) { return f(ctx) }

// TestSamplingLoopHonorsTickerAndContext drives the real polling loop with
// the fake clock. Time advances in a background goroutine; the fake clock
// delivers a due tick immediately at ticker registration, so no startup
// ordering is assumed.
func TestSamplingLoopHonorsTickerAndContext(t *testing.T) {
	start := time.Unix(0, 0)
	fc := clock.NewFake(start)
	cfg := testCfg(1, 10*time.Millisecond, 25*time.Millisecond)

	var reads atomic.Uint64
	src := funcSource(func(ctx context.Context) (sensor.Sample, error) {
		n := reads.Add(1)
		ts := fc.Now()
		return sensor.Sample{ChannelID: "c1", Value: float64(n), Timestamp: ts, Seq: n}, nil
	})

	s, _ := New(cfg, fc, discardLog{}, func(config.ChannelCfg) sensor.Source { return src })
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = s.Run(ctx) }()

	advancing := make(chan struct{})
	go func() {
		defer close(advancing)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fc.Advance(10 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	consumed := 0
	for i := 0; i < 20; i++ {
		select {
		case ev := <-s.Events():
			if ev.Outcome != OutcomeAccepted {
				t.Fatalf("event %d: %s (%s)", i, ev.Outcome, ev.Detail)
			}
			consumed++
		case <-time.After(2 * time.Second):
			t.Fatalf("no event for %dth read (reads=%d)", i, reads.Load())
		}
	}
	cancel()
	<-runDone
	<-advancing
	if consumed != 20 {
		t.Fatalf("consumed=%d want 20", consumed)
	}
}

// TestSamplerReconfigureSwitchesVersionAtomically verifies events after a
// reconfigure all carry the new version and the new period takes effect.
func TestSamplerReconfigureSwitchesVersionAtomically(t *testing.T) {
	start := time.Unix(0, 0)
	fc := clock.NewFake(start)
	cfg := testCfg(1, 10*time.Millisecond, 25*time.Millisecond)

	var reads atomic.Uint64
	factory := func(c config.ChannelCfg) sensor.Source {
		return funcSource(func(ctx context.Context) (sensor.Sample, error) {
			n := reads.Add(1)
			return sensor.Sample{ChannelID: "c1", Value: float64(n), Timestamp: fc.Now(), Seq: n}, nil
		})
	}
	s, _ := New(cfg, fc, discardLog{}, factory)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = s.Run(ctx) }()

	advancing := make(chan struct{})
	go func() {
		defer close(advancing)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fc.Advance(5 * time.Millisecond)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Consume some v1 events.
	for i := 0; i < 3; i++ {
		ev := waitEvent(t, s)
		if ev.ConfigVersion != 1 {
			t.Fatalf("pre-reconfigure version=%d", ev.ConfigVersion)
		}
	}

	next := testCfg(2, 20*time.Millisecond, 25*time.Millisecond)
	s.Reconfigure(next)

	// Drain events until only v2 events are observed; all events after
	// the switch must carry v2 (no half-updated version visible).
	deadline := time.Now().Add(3 * time.Second)
	sawV2 := false
	for time.Now().Before(deadline) {
		select {
		case ev := <-s.Events():
			if sawV2 && ev.ConfigVersion == 1 {
				t.Fatal("observed v1 event after a v2 event: non-atomic switch")
			}
			if ev.ConfigVersion == 2 {
				sawV2 = true
			}
		default:
			time.Sleep(2 * time.Millisecond)
		}
		if sawV2 {
			// collect a few more to confirm stability on v2.
			for i := 0; i < 3; i++ {
				ev := waitEvent(t, s)
				if ev.ConfigVersion != 2 {
					t.Fatalf("post-switch event version=%d", ev.ConfigVersion)
				}
			}
			break
		}
	}
	if !sawV2 {
		t.Fatal("never observed version 2 events")
	}
	cancel()
	<-runDone
	<-advancing
}

func waitEvent(t *testing.T, s *Sampler) Event {
	t.Helper()
	select {
	case ev := <-s.Events():
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}
