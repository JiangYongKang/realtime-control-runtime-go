// Package clock abstracts wall-clock access so the scheduling logic can be
// tested against a deterministically controllable fake clock.
package clock

import "time"

// Timer is the abstract counterpart of *time.Timer.
type Timer interface {
	// C returns the channel on which the firing time is delivered.
	C() <-chan time.Time
	// Stop prevents the timer from firing; reports whether it was stopped
	// before it could fire.
	Stop() bool
	// Reset changes the expiry to now+d and reports whether the timer was
	// active before the reset.
	Reset(d time.Duration) bool
}

// Ticker is the abstract counterpart of *time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Clock provides time primitives.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}
