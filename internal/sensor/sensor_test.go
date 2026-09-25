package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

func TestMockSourceOrdering(t *testing.T) {
	m := NewMockSource([]model.ChannelID{"a"})
	ctx := context.Background()
	m.Push(model.Sample{Channel: "a", Seq: 1})
	m.Push(model.Sample{Channel: "a", Seq: 2, Err: ErrReadFault})
	s1, err := m.Read(ctx)
	if err != nil || s1.Seq != 1 {
		t.Fatalf("got %+v %v", s1, err)
	}
	s2, _ := m.Read(ctx)
	if s2.Err == nil {
		t.Fatal("expected fault sample")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(ctx); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if m.Push(model.Sample{}) {
		t.Fatal("Push after Close must fail")
	}
}

func TestPatternSourceTiming(t *testing.T) {
	f := clock.NewFake(time.Unix(1000, 0))
	ps := NewPatternSource(f, []model.ChannelID{"a", "b"}, 10*time.Millisecond,
		WithFaultFunc(func(ch model.ChannelID, seq int64) bool { return ch == "b" && seq == 1 }))
	ctx := context.Background()
	done := make(chan model.Sample, 3)
	go func() {
		for i := 0; i < 3; i++ {
			s, err := ps.Read(ctx)
			if err != nil {
				return
			}
			done <- s
		}
	}()
	// First sample is due immediately; let the goroutine arm subsequent timers
	// between steps, matching deterministic fake-clock usage.
	waitGet := func() model.Sample {
		select {
		case s := <-done:
			return s
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for sample")
			return model.Sample{}
		}
	}
	got := []model.Sample{waitGet()}
	for i := 0; i < 2; i++ {
		f.Advance(10 * time.Millisecond)
		got = append(got, waitGet())
	}
	if got[0].Channel != "a" || got[1].Channel != "b" || got[1].Err == nil || got[2].Channel != "a" {
		t.Fatalf("unexpected sequence: %+v %+v %+v", got[0], got[1], got[2])
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
}
