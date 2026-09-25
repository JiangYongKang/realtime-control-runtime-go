package control

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

func testCfg(v int64) *config.Config {
	return &config.Config{
		Version: v,
		Period:  config.Duration{Duration: 10 * time.Millisecond},
		Channels: []config.Channel{
			{ID: "a", MinValue: -100, MaxValue: 100, Required: true},
			{ID: "b", MinValue: -100, MaxValue: 100, Required: true},
		},
		Threshold: map[string]float64{"a": 5},
		Safety: config.Safety{
			DataTimeout:        config.Duration{Duration: 30 * time.Millisecond},
			ConsecutiveFaults:  3,
			DecisionDeadline:   config.Duration{Duration: 20 * time.Millisecond},
			RecoveryStableRuns: 2,
			SafeOutput:         -42,
		},
	}
}

func accepted(ch model.ChannelID, seq int64, at time.Time) model.Envelope {
	return model.Envelope{
		Sample:   model.Sample{Channel: ch, Seq: seq, Time: at, Value: 1},
		RecvTime: at,
		Verdict:  model.VerdictAccepted,
	}
}

func fault(ch model.ChannelID, at time.Time) model.Envelope {
	return model.Envelope{
		Sample:   model.Sample{Channel: ch, Time: at, Err: errSentinel},
		RecvTime: at,
		Verdict:  model.VerdictFault,
	}
}

var errSentinel = sterr("fault")

type sterr string

func (e sterr) Error() string { return string(e) }

func freshSnap(t0 time.Time) map[model.ChannelID]model.Envelope {
	return map[model.ChannelID]model.Envelope{
		"a": accepted("a", 1, t0),
		"b": accepted("b", 1, t0),
	}
}

func newController(t *testing.T) (Controller, config.Store) {
	t.Helper()
	st, err := config.NewStore(testCfg(1))
	if err != nil {
		t.Fatal(err)
	}
	ctrl, err := New(Options{Store: st, Log: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	return ctrl, st
}

func TestNormalAndThreshold(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, _ := newController(t)
	d := ctrl.Evaluate(Input{Now: t0, Snapshot: freshSnap(t0)})
	if d.State != model.StateNormal || d.Output != 1 {
		t.Fatalf("normal wrong: %+v", d)
	}
	// threshold exceed on a -> degraded
	snap := freshSnap(t0)
	e := snap["a"]
	e.Sample.Value = 9
	snap["a"] = e
	d = ctrl.Evaluate(Input{Now: t0.Add(10 * time.Millisecond), Snapshot: snap,
		NewEnvelopes: []model.Envelope{e}})
	if d.State != model.StateDegraded {
		t.Fatalf("want degraded, got %s reasons=%v", d.State, d.Reasons)
	}
}

func TestDataTimeoutEntersSafe(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, _ := newController(t)
	ctrl.Evaluate(Input{Now: t0, Snapshot: freshSnap(t0)})
	// advance beyond data timeout without new data
	d := ctrl.Evaluate(Input{Now: t0.Add(31 * time.Millisecond), Snapshot: freshSnap(t0)})
	if d.State != model.StateSafe || d.Output != -42 {
		t.Fatalf("want safe/output -42, got %s %v reasons=%v", d.State, d.Output, d.Reasons)
	}
}

func TestDeadlineExceededImmediateSafe(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, _ := newController(t)
	d := ctrl.Evaluate(Input{Now: t0, Snapshot: freshSnap(t0), DeadlineExceeded: true})
	if d.State != model.StateSafe {
		t.Fatalf("deadline must force safe, got %s", d.State)
	}
}

func TestConsecutiveFaultsThenStableRecovery(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, _ := newController(t)
	now := t0
	snap := freshSnap(now)
	ctrl.Evaluate(Input{Now: now, Snapshot: snap})

	// Two faults: not yet safe (threshold is 3). Keep b fresh each tick.
	bSeq := int64(1)
	freshB := func() model.Envelope {
		bSeq++
		e := accepted("b", bSeq, now)
		snap["b"] = e
		return e
	}
	for i := 0; i < 2; i++ {
		now = now.Add(10 * time.Millisecond)
		f := fault("a", now)
		b := freshB()
		snap["a"] = f
		ctrl.Evaluate(Input{Now: now, Snapshot: snap, NewEnvelopes: []model.Envelope{f, b}})
	}
	if ctrl.State() != model.StateNormal {
		t.Fatal("two faults must not yet trip safe")
	}
	// Third consecutive fault -> safe
	now = now.Add(10 * time.Millisecond)
	f := fault("a", now)
	b := freshB()
	snap["a"] = f
	d := ctrl.Evaluate(Input{Now: now, Snapshot: snap, NewEnvelopes: []model.Envelope{f, b}})
	if d.State != model.StateSafe {
		t.Fatalf("third fault must trip safe, got %s", d.State)
	}

	// One healthy tick is NOT enough (stable runs required = 2)
	now = now.Add(10 * time.Millisecond)
	ok := accepted("a", 2, now)
	b = freshB()
	snap["a"] = ok
	d = ctrl.Evaluate(Input{Now: now, Snapshot: snap, NewEnvelopes: []model.Envelope{ok, b}})
	if d.State != model.StateSafe {
		t.Fatalf("single healthy tick must not recover, got %s reasons=%v", d.State, d.Reasons)
	}
	// Second consecutive healthy tick -> normal
	now = now.Add(10 * time.Millisecond)
	ok = accepted("a", 3, now)
	b = freshB()
	snap["a"] = ok
	d = ctrl.Evaluate(Input{Now: now, Snapshot: snap, NewEnvelopes: []model.Envelope{ok, b}})
	if d.State != model.StateNormal {
		t.Fatalf("two healthy ticks must recover, got %s reasons=%v", d.State, d.Reasons)
	}

	// A single fault blip during normal operation must not immediately
	// re-trip: it requires the configured streak again (no flapping).
	now = now.Add(10 * time.Millisecond)
	blip := fault("a", now)
	b = freshB()
	snap["a"] = blip
	d = ctrl.Evaluate(Input{Now: now, Snapshot: snap, NewEnvelopes: []model.Envelope{blip, b}})
	if d.State != model.StateNormal {
		t.Fatalf("single fault blip must not flap to safe, got %s", d.State)
	}
}

func TestConfigVersionObserved(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, st := newController(t)
	d := ctrl.Evaluate(Input{Now: t0, Snapshot: freshSnap(t0)})
	if d.Version != 1 {
		t.Fatalf("v=%d", d.Version)
	}
	c2 := testCfg(2)
	c2.Safety.SafeOutput = 7
	if _, err := st.Update(context.Background(), c2); err != nil {
		t.Fatal(err)
	}
	// force safe to observe new safe output
	d = ctrl.Evaluate(Input{Now: t0.Add(31 * time.Millisecond), Snapshot: freshSnap(t0)})
	if d.Version != 2 || d.Output != 7 {
		t.Fatalf("new version not effective: v=%d out=%v", d.Version, d.Output)
	}
}

// TestStartupGrace verifies that a channel that has never delivered is not a
// hard fault until the data timeout elapses from the first evaluation.
func TestStartupGrace(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, _ := newController(t)
	// Only channel a present; required channel b has never delivered.
	snap := map[model.ChannelID]model.Envelope{"a": accepted("a", 1, t0)}

	// Within grace: must not enter safe.
	d := ctrl.Evaluate(Input{Now: t0.Add(10 * time.Millisecond), Snapshot: snap})
	if d.State == model.StateSafe {
		t.Fatalf("missing b within grace must not be safe, reasons=%v", d.Reasons)
	}
	// After the grace window without b: safe.
	d = ctrl.Evaluate(Input{Now: t0.Add(31 * time.Millisecond), Snapshot: snap})
	if d.State != model.StateSafe {
		t.Fatalf("missing b after grace must be safe, got %s reasons=%v", d.State, d.Reasons)
	}
}

// TestBackpressureDegrades verifies a rising drop counter is surfaced as a
// soft backpressure fact that degrades authority.
func TestBackpressureDegrades(t *testing.T) {
	t0 := time.Unix(0, 0)
	ctrl, _ := newController(t)
	in := Input{Now: t0, Snapshot: freshSnap(t0)}
	if d := ctrl.Evaluate(in); d.State != model.StateNormal {
		t.Fatalf("baseline: %s", d.State)
	}
	in.Now = t0.Add(5 * time.Millisecond)
	in.Stats.Dropped = 2
	d := ctrl.Evaluate(in)
	if d.State != model.StateDegraded {
		t.Fatalf("drops must degrade, got %s reasons=%v", d.State, d.Reasons)
	}
	// No further drops: stable recovery after RecoveryStableRuns(=2) ticks.
	in.Stats.Dropped = 2
	d = ctrl.Evaluate(in)
	if d.State != model.StateDegraded {
		t.Fatalf("one healthy tick insufficient, got %s", d.State)
	}
	d = ctrl.Evaluate(in)
	if d.State != model.StateNormal {
		t.Fatalf("expected recovery, got %s reasons=%v", d.State, d.Reasons)
	}
}
