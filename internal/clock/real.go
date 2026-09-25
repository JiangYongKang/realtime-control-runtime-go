package clock

import "time"

// Real implements Clock with the host wall clock.
type Real struct{}

// NewReal returns a real wall clock.
func NewReal() Real { return Real{} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool {
	if d <= 0 {
		// Matches time.Timer semantics; avoid a non-positive panic by
		// expressing the intent (fire immediately) explicitly.
		d = time.Nanosecond
	}
	return r.t.Reset(d)
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// NewTimer creates a real timer. d must be positive.
func (Real) NewTimer(d time.Duration) Timer { return realTimer{t: time.NewTimer(d)} }

// NewTicker creates a real ticker. d must be positive.
func (Real) NewTicker(d time.Duration) Ticker { return realTicker{t: time.NewTicker(d)} }

// After returns a channel that receives after d.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Sleep blocks for d.
func (Real) Sleep(d time.Duration) { time.Sleep(d) }
