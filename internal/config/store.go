package config

import (
	"context"
	"strconv"
	"sync"
)

// Store is the concurrency-safe, monotonically versioned config holder.
type Store interface {
	// Current returns the active deep-copied configuration and its version.
	Current() (*Config, int64)
	// Update validates and atomically installs c. The candidate version must
	// be strictly greater than the active version; otherwise it is rejected
	// and the old behavior stays in effect.
	Update(ctx context.Context, c *Config) (int64, error)
	// Subscribe returns a buffered channel receiving every newly installed
	// copy. Subscribers are best-effort: a lagging subscriber coalesces to
	// the newest configuration rather than blocking updates.
	Subscribe() <-chan *Config
	// Close releases all subscriber channels.
	Close()
}

// subBuffer is the per-subscriber coalescing buffer. The store never blocks
// on slow subscribers; it drops the pending older copy instead.
const subBuffer = 4

type store struct {
	mu      sync.RWMutex
	current *Config
	version int64
	subs    []chan *Config
	closed  bool
}

// NewStore creates a store seeded with initial (already validated) config.
// The store takes a deep copy; the caller may keep mutating its own value.
func NewStore(initial *Config) (Store, error) {
	if err := initial.Validate(); err != nil {
		return nil, err
	}
	return &store{current: initial.Clone(), version: initial.Version}, nil
}

// Current returns a copy of the active config plus its version. Holding the
// returned value never blocks an Update and never reflects later changes.
func (s *store) Current() (*Config, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current.Clone(), s.version
}

// Update validates and atomically installs c under a single write-lock, so a
// concurrent reader can never observe a half-updated period/threshold mix.
func (s *store) Update(ctx context.Context, c *Config) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := c.Validate(); err != nil {
		return 0, err
	}
	cp := c.Clone()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}
	if cp.Version <= s.version {
		return s.version, &ConflictError{Installed: s.version, Candidate: cp.Version}
	}
	s.current = cp
	s.version = cp.Version
	// Notify subscribers non-blockingly (coalesce).
	for _, ch := range s.subs {
		select {
		case ch <- cp.Clone():
		default:
			// Drop the older pending value and replace with the newest so
			// the subscriber eventually converges to the latest config.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- cp.Clone():
			default:
			}
		}
	}
	return s.version, nil
}

// Subscribe registers a new subscriber and immediately delivers the active
// configuration, so consumers always start in sync.
func (s *store) Subscribe() <-chan *Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan *Config, subBuffer)
	if s.current != nil {
		ch <- s.current.Clone()
	}
	s.subs = append(s.subs, ch)
	return ch
}

// Close releases subscribers.
func (s *store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, ch := range s.subs {
		close(ch)
	}
	s.subs = nil
}

// ConflictError reports a rejected non-monotonic version update.
type ConflictError struct {
	Installed int64
	Candidate int64
}

func (e *ConflictError) Error() string {
	return "configuration version conflict: installed=" +
		strconv.FormatInt(e.Installed, 10) + " candidate=" + strconv.FormatInt(e.Candidate, 10)
}

// ErrStoreClosed is returned when updating a closed store.
var ErrStoreClosed = closedErr{}

type closedErr struct{}

func (closedErr) Error() string { return "configuration store closed" }
