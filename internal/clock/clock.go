// Package clock provides an injectable time source so production code and
// tests share the exact same scheduling primitives. The fake implementation
// is driven manually, making timing-sensitive behaviour deterministic.
package clock

import (
	"sync"
	"time"
)

// Clock abstracts wall-clock reads and periodic ticks.
type Clock interface {
	// Now returns the current (possibly simulated) time.
	Now() time.Time
	// NewTicker creates a ticker with the given positive period.
	NewTicker(d time.Duration) Ticker
}

// Ticker is a minimal ticker handle.
type Ticker interface {
	// Ch delivers scheduled tick times. Ticks are dropped if the
	// consumer cannot keep up, mirroring time.Ticker semantics.
	Ch() <-chan time.Time
	// Stop releases the ticker; drained ticks are not delivered.
	Stop()
}

// ---------------------------------------------------------------------------
// Real clock
// ---------------------------------------------------------------------------

type realClock struct{}

// Real returns a process-backed clock.
func Real() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	return &realTicker{t: time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (r *realTicker) Ch() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()                { r.t.Stop() }

// ---------------------------------------------------------------------------
// Fake clock
// ---------------------------------------------------------------------------

type fakeTicker struct {
	f      *FakeClock
	ch     chan time.Time
	period time.Duration
	next   time.Time
	stop   bool
}

func (t *fakeTicker) Ch() <-chan time.Time { return t.ch }
func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	t.stop = true
	t.f.mu.Unlock()
}

// FakeClock is a manually controlled clock. Time only moves through Advance,
// which deterministically fires every registered ticker that becomes due.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

// NewFake builds a fake clock anchored at start.
func NewFake(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the simulated current time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTicker registers a ticker; its first tick is one period after creation.
// If that first tick is already due at registration time (the clock was
// advanced before the consumer registered), the tick is delivered
// immediately so it cannot be lost to a scheduling race.
func (f *FakeClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	f.mu.Lock()
	now := f.now
	t := &fakeTicker{
		f:      f,
		ch:     make(chan time.Time, 1),
		period: d,
		next:   now.Add(d),
	}
	f.tickers = append(f.tickers, t)

	// If already due at registration time, deliver immediately so the
	// first tick cannot be lost to a startup scheduling race.
	if !t.next.After(now) {
		fireAt := t.next
		for !t.next.After(now) {
			t.next = t.next.Add(t.period)
		}
		select {
		case t.ch <- fireAt:
		default:
		}
	}
	f.mu.Unlock()
	return t
}

// Advance moves the clock forward by d and fires due tickers. Tickers fire at
// most once per Advance call (at the final time), and the tick is dropped if
// the consumer has not drained the previous one — the same drop policy as a
// real time.Ticker. Firing order is registration order for determinism.
func (f *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	type fire struct {
		t      *fakeTicker
		fireAt time.Time
	}
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	fires := make([]fire, 0, len(f.tickers))
	for _, t := range f.tickers {
		if !t.stop && !t.next.After(now) {
			fireAt := t.next
			// Catch up the schedule without queueing missed ticks.
			for !t.next.After(now) {
				t.next = t.next.Add(t.period)
			}
			fires = append(fires, fire{t, fireAt})
		}
	}
	f.mu.Unlock()
	for _, fr := range fires {
		select {
		case fr.t.ch <- fr.fireAt:
		default:
			// Consumer slow: drop the tick like time.Ticker.
		}
	}
}
