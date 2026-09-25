// Package sampler performs periodic multi-channel sampling on a unified time
// base. Every ingest attempt receives a deterministic, distinguishable
// outcome (accepted / duplicate / out-of-order stale / sequence gap / read
// error). Memory stays bounded: per-channel state is a single slot, and the
// output channel is a fixed-size buffer — a slow downstream causes drops
// (counted) rather than unbounded growth or blocked sampling.
package sampler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

// Outcome is the deterministic classification of an ingest attempt.
type Outcome int

const (
	// OutcomeUnknown is the zero value and never emitted.
	OutcomeUnknown Outcome = iota
	// OutcomeAccepted: sample is newer than the channel's latest and fresh.
	OutcomeAccepted
	// OutcomeDuplicate: same timestamp and sequence as the latest sample.
	OutcomeDuplicate
	// OutcomeStale: timestamp is older than the latest, or older than the
	// unified clock minus MaxLag (late arrival).
	OutcomeStale
	// OutcomeGapped: newer than the latest but Seq skips one or more values.
	OutcomeGapped
	// OutcomeReadError: the data source failed to produce a sample.
	OutcomeReadError
)

// String gives outcomes stable log names.
func (o Outcome) String() string {
	switch o {
	case OutcomeAccepted:
		return "accepted"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeStale:
		return "stale"
	case OutcomeGapped:
		return "gapped"
	case OutcomeReadError:
		return "read_error"
	default:
		return "unknown"
	}
}

// Event is emitted for every ingest decision, including rejections.
type Event struct {
	ChannelID string
	Outcome   Outcome
	// Detail explains the deterministic reason for the outcome.
	Detail string
	Sample sensor.Sample
	// ObservedAt is the unified-clock time the sampler made the decision.
	ObservedAt time.Time
	// ConfigVersion is the active configuration when the event was made.
	ConfigVersion int64
	// Err is set for OutcomeReadError.
	Err error
}

// Logger is the minimal logging surface used by the sampler.
type Logger interface {
	Printf(format string, args ...any)
}

// channelState is the bounded per-channel cache: exactly one latest slot
// plus bookkeeping. No queue is ever retained.
type channelState struct {
	latest  sensor.Sample
	hasData bool
}

// Sampler polls sources periodically and publishes classified events.
type Sampler struct {
	clk    clock.Clock
	log    Logger
	out    chan Event
	source func(ch config.ChannelCfg) sensor.Source

	mu      sync.Mutex
	cfg     *config.Config
	states  map[string]*channelState
	recvCfg chan *config.Config

	drops      atomic.Uint64
	readErrors atomic.Uint64
	accepted   atomic.Uint64
}

// New builds a sampler. sourceFactory builds a data source for each channel
// configuration; nil selects a generic simulated source.
func New(cfg *config.Config, clk clock.Clock, log Logger, sourceFactory func(config.ChannelCfg) sensor.Source) (*Sampler, error) {
	if err := config.Validate(cfg); err != nil {
		return nil, err
	}
	s := &Sampler{
		clk:     clk,
		log:     log,
		cfg:     cfg,
		states:  make(map[string]*channelState),
		recvCfg: make(chan *config.Config, 1),
		source:  sourceFactory,
	}
	// Output depth is fixed at construction and bounded regardless of
	// downstream speed or configuration changes.
	s.out = make(chan Event, 4*len(cfg.Channels)+1)
	for _, ch := range cfg.Channels {
		s.states[ch.ID] = &channelState{}
	}
	return s, nil
}

// Events returns the bounded, non-blocking output channel.
func (s *Sampler) Events() <-chan Event { return s.out }

// Reconfigure atomically installs a new configuration version. Channel
// latest-slots are retained across reconfigurations to preserve ordering
// judgements. This call never blocks on the sampling goroutines.
func (s *Sampler) Reconfigure(cfg *config.Config) {
	s.mu.Lock()
	s.cfg = cfg
	for _, ch := range cfg.Channels {
		if _, ok := s.states[ch.ID]; !ok {
			s.states[ch.ID] = &channelState{}
		}
	}
	for id := range s.states {
		if _, ok := cfg.Channel(id); !ok {
			delete(s.states, id)
		}
	}
	s.mu.Unlock()

	// Coalesce: only the newest pending version needs to be delivered.
	select {
	case s.recvCfg <- cfg:
	default:
		select {
		case <-s.recvCfg:
		default:
		}
		select {
		case s.recvCfg <- cfg:
		default:
		}
	}
}

// Ingest classifies a successfully read sample against the channel's single
// bounded slot, updates the slot and counters, emits the resulting event and
// returns it. It is deterministic and safe to call directly in tests.
func (s *Sampler) Ingest(ch config.ChannelCfg, smp sensor.Sample, observed time.Time) Event {
	smp.ChannelID = ch.ID
	ev, st := s.classify(ch, smp, observed)
	if ev.Outcome == OutcomeAccepted || ev.Outcome == OutcomeGapped {
		st.hasData = true
		st.latest = smp
		if ev.Outcome == OutcomeAccepted {
			s.accepted.Add(1)
		}
	}
	s.log.Printf("sampler: channel=%s outcome=%s value=%.4f ts=%s obs=%s lag=%s %s",
		ch.ID, ev.Outcome, smp.Value, smp.Timestamp.Format(time.RFC3339Nano),
		observed.Format(time.RFC3339Nano), observed.Sub(smp.Timestamp), ev.Detail)
	s.emit(ev)
	return ev
}

// IngestError records and emits a source read failure for the channel.
func (s *Sampler) IngestError(chID string, readErr error, observed time.Time) Event {
	s.readErrors.Add(1)
	ev := Event{
		ChannelID:     chID,
		Outcome:       OutcomeReadError,
		Detail:        readErr.Error(),
		ObservedAt:    observed,
		ConfigVersion: s.cfgVersion(),
		Err:           readErr,
	}
	s.log.Printf("sampler: read error channel=%s err=%v at=%s", chID, readErr, observed.Format(time.RFC3339Nano))
	s.emit(ev)
	return ev
}

// classify classifies a sample against the channel slot without side effects
// beyond reading the slot.
func (s *Sampler) classify(ch config.ChannelCfg, smp sensor.Sample, observed time.Time) (Event, *channelState) {
	s.mu.Lock()
	st, ok := s.states[ch.ID]
	if !ok {
		st = &channelState{}
		s.states[ch.ID] = st
	}
	version := s.cfg.Version
	s.mu.Unlock()
	ev := Event{
		ChannelID:     ch.ID,
		Sample:        smp,
		ObservedAt:    observed,
		ConfigVersion: version,
	}

	// Expired relative to the unified clock: late arrival from the device.
	if ch.MaxLag > 0 && smp.Timestamp.Before(observed.Add(-ch.MaxLag)) {
		ev.Outcome = OutcomeStale
		ev.Detail = "sample older than clock minus max_lag"
		return ev, st
	}

	if !st.hasData {
		ev.Outcome = OutcomeAccepted
		return ev, st
	}
	last := st.latest

	switch {
	case smp.Timestamp.Equal(last.Timestamp) && smp.Seq == last.Seq:
		ev.Outcome = OutcomeDuplicate
		ev.Detail = "timestamp and seq equal latest"
	case smp.Timestamp.Before(last.Timestamp):
		ev.Outcome = OutcomeStale
		ev.Detail = "timestamp older than latest"
	case smp.Seq == last.Seq:
		// Same timestamp ordering handled above; equal seq at a newer
		// timestamp is a retransmission of the last reading.
		ev.Outcome = OutcomeDuplicate
		ev.Detail = "seq equal latest"
	case smp.Seq < last.Seq:
		ev.Outcome = OutcomeStale
		ev.Detail = "seq older than latest"
	case smp.Seq > last.Seq+1:
		ev.Outcome = OutcomeGapped
		ev.Detail = "seq gap detected"
	default:
		ev.Outcome = OutcomeAccepted
	}
	return ev, st
}

// DropCount reports events discarded because the downstream was full.
func (s *Sampler) DropCount() uint64 { return s.drops.Load() }

// ReadErrorCount reports total source read failures.
func (s *Sampler) ReadErrorCount() uint64 { return s.readErrors.Load() }

// AcceptedCount reports total accepted samples.
func (s *Sampler) AcceptedCount() uint64 { return s.accepted.Load() }

// emit performs the non-blocking bounded send: sampling never blocks on a
// slow consumer; overflow is counted.
func (s *Sampler) emit(ev Event) {
	select {
	case s.out <- ev:
	default:
		s.drops.Add(1)
		s.log.Printf("sampler: backpressure drop channel=%s outcome=%s drops=%d", ev.ChannelID, ev.Outcome, s.drops.Load())
	}
}

// generation is one set of per-channel polling goroutines.
type generation struct {
	cancels map[string]context.CancelFunc
	wg      sync.WaitGroup
}

func (g *generation) cancel() {
	for _, c := range g.cancels {
		c()
	}
}

// Run starts one polling goroutine per channel and blocks until ctx ends. On
// every configuration version the previous generation is fully stopped and
// joined before the next one starts, so period changes take effect at a
// single atomic boundary and two goroutines never poll the same channel.
func (s *Sampler) Run(ctx context.Context) error {
	cur := s.snapshotCfg()
	gen := s.startGeneration(ctx, cur)
	defer func() {
		gen.cancel()
		gen.wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next := <-s.recvCfg:
			gen.cancel()
			gen.wg.Wait()
			gen = s.startGeneration(ctx, next)
			s.log.Printf("sampler: reconfigured to version %d with %d channels", next.Version, len(next.Channels))
		}
	}
}

func (s *Sampler) startGeneration(parent context.Context, cfg *config.Config) *generation {
	g := &generation{cancels: map[string]context.CancelFunc{}}
	for _, ch := range cfg.Channels {
		childCtx, cancel := context.WithCancel(parent)
		g.cancels[ch.ID] = cancel
		g.wg.Add(1)
		go s.runChannel(childCtx, &g.wg, ch)
	}
	return g
}

func (s *Sampler) snapshotCfg() *config.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// runChannel polls one channel at its configured period using the injected
// clock's ticker, so tests drive timing deterministically.
func (s *Sampler) runChannel(ctx context.Context, wg *sync.WaitGroup, initial config.ChannelCfg) {
	defer wg.Done()
	src := s.source(initial)
	ticker := s.clk.NewTicker(initial.Period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Ch():
			if ctx.Err() != nil {
				return
			}
			ch := initial
			observed := s.clk.Now()
			smp, err := src.Read(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.IngestError(ch.ID, err, observed)
				continue
			}
			s.Ingest(ch, smp, observed)
		}
	}
}

func (s *Sampler) cfgVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.Version
}
