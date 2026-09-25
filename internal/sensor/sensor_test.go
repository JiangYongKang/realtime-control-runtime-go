package sensor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScriptSourceReplayThenUnavailable(t *testing.T) {
	t0 := time.Unix(1000, 0)
	src := NewScriptSource("c",
		ScriptStep{Sample: Sample{ChannelID: "c", Value: 1, Timestamp: t0, Seq: 1}},
		ScriptStep{Err: ErrUnavailable},
	)
	if src.ID() != "c" {
		t.Fatalf("id=%s", src.ID())
	}
	got, err := src.Read(context.Background())
	if err != nil || got.Value != 1 || got.Seq != 1 {
		t.Fatalf("first read = %+v, %v", got, err)
	}
	if _, err := src.Read(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("second read err = %v", err)
	}
	if _, err := src.Read(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("exhausted source should keep failing, got %v", err)
	}
}

func TestSimSourceFaultInjection(t *testing.T) {
	clk := time.Unix(0, 0)
	src := NewSimSource(SimConfig{
		ChannelID:    "c",
		Base:         5,
		Amplitude:    0,
		FailEveryN:   3,
		FailFromRead: 5,
	}, func() time.Time { return clk })

	var vals []float64
	var fails int
	for i := 0; i < 6; i++ {
		smp, err := src.Read(context.Background())
		if err != nil {
			fails++
			continue
		}
		vals = append(vals, smp.Value)
	}
	// read 1 ok, 2 ok, 3 fail (every 3), 4 ok, 5,6 fail (from 5).
	if fails != 3 {
		t.Fatalf("fails=%d want 3", fails)
	}
	if len(vals) != 3 {
		t.Fatalf("successes=%d want 3", len(vals))
	}
	if src.Failures() != 3 || src.Reads() != 6 {
		t.Fatalf("counters failures=%d reads=%d", src.Failures(), src.Reads())
	}
	for _, v := range vals {
		if v != 5 {
			t.Fatalf("amplitude 0 must yield base value, got %v", vals)
		}
	}
}

func TestSimSourceHonorsContextCancel(t *testing.T) {
	src := NewSimSource(SimConfig{ChannelID: "c"}, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := src.Read(ctx); err == nil {
		t.Fatal("expected error from canceled context")
	}
}
