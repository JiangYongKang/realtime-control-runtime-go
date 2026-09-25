package sensor

import (
	"context"
	"errors"
	"sync"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// ErrClosed is returned by Read after Close.
var ErrClosed = errors.New("sensor: source closed")

// MockSource is a controllable source driven by an in-memory queue. Tests
// decide exactly which sample (ordered, duplicate, late or faulty) is
// delivered and when.
type MockSource struct {
	mu       sync.Mutex
	queue    []model.Sample
	notify   chan struct{} // signaled (non-blocking) when queue gains an item
	channels []model.ChannelID
	closed   bool
}

// NewMockSource returns an empty mock source advertising channels.
func NewMockSource(channels []model.ChannelID) *MockSource {
	return &MockSource{
		notify:   make(chan struct{}, 1),
		channels: append([]model.ChannelID(nil), channels...),
	}
}

// Push queues a sample for later delivery. It never blocks. Returns false
// when the source is closed.
func (m *MockSource) Push(s model.Sample) bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.queue = append(m.queue, s)
	m.mu.Unlock()
	select {
	case m.notify <- struct{}{}:
	default:
	}
	return true
}

// Read pops the next queued sample, blocking until one is available, ctx is
// cancelled or the source is closed.
func (m *MockSource) Read(ctx context.Context) (model.Sample, error) {
	for {
		m.mu.Lock()
		if len(m.queue) > 0 {
			s := m.queue[0]
			m.queue = m.queue[1:]
			m.mu.Unlock()
			return s, nil
		}
		if m.closed {
			m.mu.Unlock()
			return model.Sample{}, ErrClosed
		}
		ch := m.notify
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return model.Sample{}, ctx.Err()
		case <-ch:
			// re-check the queue under lock
		}
	}
}

// TryRead returns a queued sample immediately without blocking. ok is false
// when the queue is empty. Sources supporting this method let a tick-driven
// sampler poll without internal timers.
func (m *MockSource) TryRead() (s model.Sample, ok bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.queue) > 0 {
		s := m.queue[0]
		m.queue = m.queue[1:]
		return s, true, nil
	}
	if m.closed {
		return model.Sample{}, false, ErrClosed
	}
	return model.Sample{}, false, nil
}

// Channels returns the configured channels.
func (m *MockSource) Channels() []model.ChannelID {
	return append([]model.ChannelID(nil), m.channels...)
}

// Close marks the source as closed and unblocks any pending Read.
func (m *MockSource) Close() error {
	m.mu.Lock()
	wasClosed := m.closed
	m.closed = true
	m.mu.Unlock()
	if !wasClosed {
		select {
		case m.notify <- struct{}{}:
		default:
		}
	}
	return nil
}
