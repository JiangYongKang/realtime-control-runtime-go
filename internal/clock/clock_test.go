package clock

import (
	"testing"
	"time"
)

func TestFakeClockNowAndAdvance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFake(start)
	if got := fc.Now(); !got.Equal(start) {
		t.Fatalf("Now = %v, want %v", got, start)
	}
	fc.Advance(50 * time.Millisecond)
	if got := fc.Now(); !got.Equal(start.Add(50 * time.Millisecond)) {
		t.Fatalf("After advance Now = %v", got)
	}
	// Non-positive advance is a no-op.
	fc.Advance(0)
	fc.Advance(-time.Second)
	if got := fc.Now(); !got.Equal(start.Add(50 * time.Millisecond)) {
		t.Fatalf("zero/negative advance changed time: %v", got)
	}
}

func TestFakeTickerFiresOnDueAdvance(t *testing.T) {
	fc := NewFake(time.Unix(0, 0))
	tk := fc.NewTicker(100 * time.Millisecond)

	fc.Advance(99 * time.Millisecond)
	select {
	case <-tk.Ch():
		t.Fatal("tick fired before period elapsed")
	default:
	}

	fc.Advance(1 * time.Millisecond)
	select {
	case got := <-tk.Ch():
		if !got.Equal(time.Unix(0, int64(100*time.Millisecond))) {
			t.Fatalf("tick time = %v", got)
		}
	default:
		t.Fatal("tick did not fire when due")
	}
}

func TestFakeTickerDropsTickWhenConsumerSlow(t *testing.T) {
	fc := NewFake(time.Unix(0, 0))
	tk := fc.NewTicker(10 * time.Millisecond)

	// Buffer holds one pending tick; advance through many periods.
	fc.Advance(10 * time.Millisecond)  // buffered tick 1
	fc.Advance(100 * time.Millisecond) // schedule catches up; tick 2 dropped attempt
	got := <-tk.Ch()
	_ = got
	// Exactly one tick must be deliverable: no unbounded queue.
	select {
	case <-tk.Ch():
		t.Fatal("ticker queued more than one tick for a slow consumer")
	default:
	}
}

func TestFakeTickerStop(t *testing.T) {
	fc := NewFake(time.Unix(0, 0))
	tk := fc.NewTicker(10 * time.Millisecond)
	tk.Stop()
	fc.Advance(50 * time.Millisecond)
	select {
	case <-tk.Ch():
		t.Fatal("stopped ticker fired")
	default:
	}
}

func TestRealTickerFires(t *testing.T) {
	rc := Real()
	tk := rc.NewTicker(5 * time.Millisecond)
	defer tk.Stop()
	select {
	case <-tk.Ch():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("real ticker did not fire")
	}
}

func TestFakeTimerFiresOnDueAdvance(t *testing.T) {
	fc := NewFake(time.Unix(0, 0))
	tm := fc.NewTimer(50 * time.Millisecond)

	fc.Advance(49 * time.Millisecond)
	select {
	case <-tm.Ch():
		t.Fatal("timer fired before deadline")
	default:
	}

	fc.Advance(1 * time.Millisecond)
	select {
	case got := <-tm.Ch():
		if !got.Equal(time.Unix(0, int64(50*time.Millisecond))) {
			t.Fatalf("timer fire time = %v", got)
		}
	default:
		t.Fatal("timer did not fire when due")
	}

	// One-shot: further advances deliver nothing more.
	fc.Advance(time.Second)
	select {
	case <-tm.Ch():
		t.Fatal("one-shot timer fired twice")
	default:
	}
}

func TestFakeTimerStop(t *testing.T) {
	fc := NewFake(time.Unix(0, 0))
	tm := fc.NewTimer(10 * time.Millisecond)
	tm.Stop()
	fc.Advance(50 * time.Millisecond)
	select {
	case <-tm.Ch():
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestRealTimerFires(t *testing.T) {
	rc := Real()
	tm := rc.NewTimer(5 * time.Millisecond)
	defer tm.Stop()
	select {
	case <-tm.Ch():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("real timer did not fire")
	}
}
