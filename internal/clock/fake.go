package clock

import (
	"sync"
	"time"
)

// fakeTimer is a timer scheduled on a Fake clock.
type fakeTimer struct {
	c   chan time.Time
	now time.Time // deadline (absolute fake time); zero when stopped/fired
}

// fakeTicker is a periodic ticker scheduled on a Fake clock.
type fakeTicker struct {
	c      chan time.Time
	period time.Duration
	next   time.Time // next absolute firing time; zero when stopped
}

// Fake is a manually advanced clock. Timers fire synchronously inside
// Advance, so tests are fully deterministic: there are no background
// goroutines and no scheduling races.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	tickers []*fakeTicker
}

// NewFake returns a fake clock anchored at t0.
func NewFake(t0 time.Time) *Fake {
	return &Fake{now: t0}
}

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer creates a fake timer firing at now+d.
func (f *Fake) NewTimer(d time.Duration) Timer {
	if d <= 0 {
		panic("clock: non-positive duration for NewTimer")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{c: make(chan time.Time, 1), now: f.now.Add(d)}
	f.timers = append(f.timers, t)
	return fakeTimerHandle{f: f, t: t}
}

// NewTicker creates a fake periodic ticker.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive duration for NewTicker")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTicker{c: make(chan time.Time, 1), period: d, next: f.now.Add(d)}
	f.tickers = append(f.tickers, t)
	return fakeTickerHandle{f: f, t: t}
}

// After returns a channel that receives once at now+d.
func (f *Fake) After(d time.Duration) <-chan time.Time { return f.NewTimer(d).C() }

// Sleep advances the fake clock by d.
func (f *Fake) Sleep(d time.Duration) { f.Advance(d) }

// Advance moves the fake clock forward by d and fires every due timer and
// ticker exactly per elapsed period. Firing sends are non-blocking: if a
// consumer has not drained the buffered (capacity 1) channel, the tick is
// coalesced/dropped — the same way a slow consumer loses ticks with a real
// time.Ticker, so the sampler can never be blocked by time delivery.
func (f *Fake) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	f.mu.Lock()
	f.now = f.now.Add(d)
	deadline := f.now
	var fireTimers []*fakeTimer
	kept := f.timers[:0]
	for _, t := range f.timers {
		if t.now.IsZero() || t.now.After(deadline) {
			kept = append(kept, t)
			continue
		}
		fireTimers = append(fireTimers, t)
	}
	f.timers = kept
	var fireTickers []*fakeTicker
	for _, tk := range f.tickers {
		if tk.next.IsZero() || tk.next.After(deadline) {
			continue
		}
		fireTickers = append(fireTickers, tk)
	}
	f.mu.Unlock()

	// Fire outside the lock; channels are buffered so this never blocks.
	for _, t := range fireTimers {
		f.mu.Lock()
		at := t.now
		t.now = time.Time{}
		f.mu.Unlock()
		select {
		case t.c <- at:
		default: // stale pending value; timer channel capacity is 1
		}
	}
	for _, tk := range fireTickers {
		f.mu.Lock()
		// Advance next to the first slot strictly past the deadline in O(1)
		// arithmetic (no per-period loop while holding the lock, so a large
		// jump can never hold up other clock users).
		elapsedPeriods := deadline.Sub(tk.next)/tk.period + 1
		jumped := time.Duration(elapsedPeriods) * tk.period
		last := tk.next.Add(jumped - tk.period)
		tk.next = tk.next.Add(jumped)
		f.mu.Unlock()
		select {
		case tk.c <- last:
		default: // consumer lagging; coalesce to the most recent tick
		}
	}
}

// stopTimer removes/stops a timer, reporting whether it was still pending.
func (f *Fake) stopTimer(t *fakeTimer) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.now.IsZero() {
		return false
	}
	t.now = time.Time{}
	for i, x := range f.timers {
		if x == t {
			f.timers = append(f.timers[:i], f.timers[i+1:]...)
			return true
		}
	}
	return true
}

// resetTimer reschedules a timer and reports whether it was pending.
func (f *Fake) resetTimer(t *fakeTimer, d time.Duration) bool {
	if d <= 0 {
		panic("clock: non-positive duration for Reset")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	pending := !t.now.IsZero()
	t.now = f.now.Add(d)
	if !pending {
		f.timers = append(f.timers, t)
	}
	return pending
}

// stopTicker stops and removes a ticker.
func (f *Fake) stopTicker(t *fakeTicker) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t.next = time.Time{}
	for i, x := range f.tickers {
		if x == t {
			f.tickers = append(f.tickers[:i], f.tickers[i+1:]...)
			return
		}
	}
}

// Waiters reports how many timers/tickers are currently scheduled.
// Intended for leak assertions in tests.
func (f *Fake) Waiters() (timers, tickers int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers), len(f.tickers)
}

// The interface adapter methods below close over their owning Fake.

type fakeTimerHandle struct {
	f *Fake
	t *fakeTimer
}

func (h fakeTimerHandle) C() <-chan time.Time { return h.t.c }
func (h fakeTimerHandle) Stop() bool          { return h.f.stopTimer(h.t) }
func (h fakeTimerHandle) Reset(d time.Duration) bool {
	return h.f.resetTimer(h.t, d)
}

type fakeTickerHandle struct {
	f *Fake
	t *fakeTicker
}

func (h fakeTickerHandle) C() <-chan time.Time { return h.t.c }
func (h fakeTickerHandle) Stop()               { h.f.stopTicker(h.t) }
