package sensor

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// PatternOption customizes a PatternSource.
type PatternOption func(*PatternSource)

// WithAmplitude scales the generated sine wave.
func WithAmplitude(a float64) PatternOption {
	return func(p *PatternSource) { p.amplitude = a }
}

// WithFaultFunc installs a hook that, when it returns true for a channel and
// sequence, makes the next sample an explicit read fault.
func WithFaultFunc(f func(ch model.ChannelID, seq int64) bool) PatternOption {
	return func(p *PatternSource) { p.fault = f }
}

// PatternSource generates periodic synthetic samples round-robin across its
// channels. Timing derives from the injected clock, so the same source works
// both in production (real clock) and in tests (fake clock).
type PatternSource struct {
	clk       clock.Clock
	channels  []model.ChannelID
	period    time.Duration
	amplitude float64
	fault     func(ch model.ChannelID, seq int64) bool

	mu      sync.Mutex
	seqs    map[model.ChannelID]int64
	phase   int
	next    time.Time
	started bool
	closed  bool
}

// NewPatternSource creates a source emitting for channels once per period.
// Each individual Read blocks until the next emission instant.
func NewPatternSource(clk clock.Clock, channels []model.ChannelID, period time.Duration, opts ...PatternOption) *PatternSource {
	p := &PatternSource{
		clk:       clk,
		channels:  append([]model.ChannelID(nil), channels...),
		period:    period,
		amplitude: 1.0,
		seqs:      make(map[model.ChannelID]int64, len(channels)),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Read returns the next generated sample, honoring cancellation.
func (p *PatternSource) Read(ctx context.Context) (model.Sample, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return model.Sample{}, ErrClosed
	}
	if !p.started {
		p.started = true
		p.next = p.clk.Now()
	}
	wait := p.next.Sub(p.clk.Now())
	p.mu.Unlock()

	if wait > 0 {
		tm := p.clk.NewTimer(wait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return model.Sample{}, ctx.Err()
		case <-tm.C():
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return model.Sample{}, ErrClosed
	}
	ch := p.channels[p.phase%len(p.channels)]
	p.phase++
	p.next = p.next.Add(p.period)
	seq := p.seqs[ch] + 1
	p.seqs[ch] = seq
	now := p.clk.Now()

	if p.fault != nil && p.fault(ch, seq) {
		return model.Sample{Channel: ch, Seq: seq, Time: now, Err: ErrReadFault}, nil
	}
	v := p.amplitude * math.Sin(float64(seq)*math.Pi/8)
	return model.Sample{Channel: ch, Seq: seq, Time: now, Value: v}, nil
}

// Channels returns the generated channels.
func (p *PatternSource) Channels() []model.ChannelID {
	return append([]model.ChannelID(nil), p.channels...)
}

// Close stops the source.
func (p *PatternSource) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}
