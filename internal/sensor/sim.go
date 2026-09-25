package sensor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// SimConfig parameterises a formula-driven simulated device.
type SimConfig struct {
	// ChannelID tags every produced sample.
	ChannelID string
	// Base + Amplitude*sin(2*pi*t/PeriodS) is the clean waveform.
	Base      float64
	Amplitude float64
	PeriodS   float64
	// FailEveryN > 0 makes every Nth read fail (intermittent faults).
	FailEveryN int
	// FailFromRead > 0 makes read number FailFromRead and all later
	// reads fail permanently (dead-device simulation).
	FailFromRead int
}

// SimSource emulates a device using a deterministic formula and clock. Faults
// are injected deterministically by read ordinal, so scenarios reproduce
// exactly under both real and fake clocks.
type SimSource struct {
	cfg SimConfig
	clk func() time.Time

	mu       sync.Mutex // guards start and reads during setup
	start    time.Time
	reads    uint64
	failures uint64
}

// NewSimSource builds a simulated source. now supplies the unified time base.
func NewSimSource(cfg SimConfig, now func() time.Time) *SimSource {
	return &SimSource{cfg: cfg, clk: now, start: now()}
}

// ID returns the channel identifier.
func (s *SimSource) ID() string { return s.cfg.ChannelID }

// Read computes one sample or injects a fault.
func (s *SimSource) Read(ctx context.Context) (Sample, error) {
	if err := ctx.Err(); err != nil {
		return Sample{}, err
	}
	n := atomic.AddUint64(&s.reads, 1)
	if s.cfg.FailEveryN > 0 && n%uint64(s.cfg.FailEveryN) == 0 {
		atomic.AddUint64(&s.failures, 1)
		return Sample{}, ErrUnavailable
	}
	if s.cfg.FailFromRead > 0 && n >= uint64(s.cfg.FailFromRead) {
		atomic.AddUint64(&s.failures, 1)
		return Sample{}, ErrUnavailable
	}

	now := s.clk()
	s.mu.Lock()
	start := s.start
	s.mu.Unlock()
	elapsed := now.Sub(start).Seconds()

	v := s.cfg.Base
	if s.cfg.PeriodS > 0 && s.cfg.Amplitude != 0 {
		v += s.cfg.Amplitude * sin2pi(elapsed/s.cfg.PeriodS)
	}
	return Sample{
		ChannelID: s.cfg.ChannelID,
		Value:     v,
		Timestamp: now,
		Seq:       n,
	}, nil
}

// Reads reports the total number of Read calls.
func (s *SimSource) Reads() uint64 { return atomic.LoadUint64(&s.reads) }

// Failures reports how many reads injected a fault.
func (s *SimSource) Failures() uint64 { return atomic.LoadUint64(&s.failures) }
