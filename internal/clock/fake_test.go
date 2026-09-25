package clock

import (
	"testing"
	"time"
)

func TestFakeTimer(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(5 * time.Millisecond)
	f.Advance(4 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("timer fired too early")
	default:
	}
	f.Advance(1 * time.Millisecond)
	select {
	case <-tm.C():
	default:
		t.Fatal("timer should have fired")
	}
	// one-shot: no second value
	f.Advance(10 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("one-shot timer fired twice")
	default:
	}
}

func TestFakeTimerStopReset(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(5 * time.Millisecond)
	if !tm.Stop() {
		t.Fatal("expected Stop to report active")
	}
	f.Advance(10 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
	tm.Reset(2 * time.Millisecond)
	f.Advance(2 * time.Millisecond)
	select {
	case <-tm.C():
	default:
		t.Fatal("reset timer did not fire")
	}
}

func TestFakeTicker(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tk := f.NewTicker(3 * time.Millisecond)
	f.Advance(3 * time.Millisecond)
	select {
	case <-tk.C():
	default:
		t.Fatal("ticker should fire at period")
	}
	// Advance multiple periods; coalesced into one buffered value.
	f.Advance(10 * time.Millisecond)
	drained := 0
drain:
	for {
		select {
		case <-tk.C():
			drained++
		default:
			break drain
		}
	}
	if drained != 1 {
		t.Fatalf("expected coalesced single tick, got %d", drained)
	}
	tk.Stop()
	nt, ntk := f.Waiters()
	if nt != 0 || ntk != 0 {
		t.Fatalf("leak after stop: timers=%d tickers=%d", nt, ntk)
	}
}
