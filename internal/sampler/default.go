package sampler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

// default envelope channel capacity: bounded, small backpressure buffer.
const defaultChanCap = 64

// Options configures the default sampler.
type Options struct {
	Clock clock.Clock
	// Period is the sampling period and the baseline read budget.
	Period time.Duration
	// StaleHorizon classifies a sample as stale when its source timestamp
	// is older than now-StaleHorizon. Zero defaults to 3*Period.
	StaleHorizon time.Duration
	// OutputCap bounds the envelope channel. Zero uses defaultChanCap.
	OutputCap int
	// ReadBudget bounds each blocking Read so a quiet source cannot pin a
	// tick. Zero uses Period.
	ReadBudget time.Duration
	Log        func(format string, args ...any)
}

type channelState struct {
	lastSeq  int64
	haveSeq  bool
	latest   model.Envelope
	haveData bool
}

type defaultSampler struct {
	opts    Options
	sources []sensor.Sensor

	mu     sync.Mutex
	states map[model.ChannelID]*channelState
	stats  Stats
	period time.Duration

	out chan model.Envelope
}

// New creates the default sampler from one or more sources.
func New(opts Options, sources ...sensor.Sensor) (Sampler, error) {
	if opts.Clock == nil {
		return nil, errors.New("sampler: clock is required")
	}
	if opts.Period <= 0 {
		return nil, errors.New("sampler: period must be positive")
	}
	if len(sources) == 0 {
		return nil, errors.New("sampler: at least one source required")
	}
	if opts.StaleHorizon <= 0 {
		opts.StaleHorizon = 3 * opts.Period
	}
	if opts.OutputCap <= 0 {
		opts.OutputCap = defaultChanCap
	}
	if opts.ReadBudget <= 0 {
		opts.ReadBudget = opts.Period
	}
	if opts.Log == nil {
		opts.Log = func(string, ...any) {}
	}
	return &defaultSampler{
		opts:    opts,
		sources: sources,
		states:  make(map[model.ChannelID]*channelState),
		period:  opts.Period,
		out:     make(chan model.Envelope, opts.OutputCap),
	}, nil
}

func (s *defaultSampler) Envelopes() <-chan model.Envelope { return s.out }

// SetPeriod updates the cadence baseline and derived stale horizon atomically.
func (s *defaultSampler) SetPeriod(d time.Duration) {
	if d <= 0 {
		return
	}
	s.mu.Lock()
	s.period = d
	if s.opts.StaleHorizon == 3*s.opts.Period {
		s.opts.StaleHorizon = 3 * d
	}
	s.opts.Period = d
	s.mu.Unlock()
}

// Poll reads each source at most once and classifies results. Sources that
// support an immediate non-blocking read are polled without timers; other
// sources get a bounded read that cannot exceed one period.
func (s *defaultSampler) Poll(ctx context.Context) {
	s.mu.Lock()
	budget := s.opts.ReadBudget
	s.mu.Unlock()

	for _, src := range s.sources {
		if ctx.Err() != nil {
			return
		}
		if isrc, ok := src.(immediateSource); ok {
			smp, has, err := isrc.TryRead()
			if err != nil {
				s.reportSourceError(src, err)
				continue
			}
			if has {
				s.classify(smp)
			}
			continue
		}

		readCtx, cancelRead := context.WithTimeout(ctx, budget)
		smp, err := src.Read(readCtx)
		cancelRead()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			s.reportSourceError(src, err)
			continue
		}
		s.classify(smp)
	}
}

// immediateSource is implemented by sources that can return a queued sample
// without blocking (e.g. the mock source used in tests).
type immediateSource interface {
	TryRead() (model.Sample, bool, error)
}

// reportSourceError turns a source-level read error into fault envelopes on
// all advertised channels so the safety layer observes the device failure.
func (s *defaultSampler) reportSourceError(src sensor.Sensor, err error) {
	for _, ch := range src.Channels() {
		s.classify(model.Sample{Channel: ch, Time: s.opts.Clock.Now(), Err: err})
	}
}

// classify applies the deterministic ordering/staleness rules and updates
// the bounded per-channel state.
func (s *defaultSampler) classify(smp model.Sample) {
	now := s.opts.Clock.Now()
	env := model.Envelope{Sample: smp, RecvTime: now}

	s.mu.Lock()
	st := s.states[smp.Channel]
	if st == nil {
		st = &channelState{}
		s.states[smp.Channel] = st
	}

	switch {
	case smp.Err != nil:
		// Fault samples do not advance the sequence cursor: a later retry
		// with the same seq is still expected.
		env.Verdict = model.VerdictFault
		s.stats.Faults++
		st.latest = env
		st.haveData = true
	case st.haveSeq && smp.Seq == st.lastSeq:
		env.Verdict = model.VerdictDuplicate
		s.stats.Duplicates++
	case st.haveSeq && smp.Seq < st.lastSeq:
		env.Verdict = model.VerdictOutOfOrder
		s.stats.OutOfOrder++
	case s.opts.StaleHorizon > 0 && now.Sub(smp.Time) > s.opts.StaleHorizon:
		env.Verdict = model.VerdictStale
		s.stats.Stale++
	default:
		env.Verdict = model.VerdictAccepted
		s.stats.Accepted++
		st.lastSeq = smp.Seq
		st.haveSeq = true
		st.latest = env
		st.haveData = true
	}
	s.mu.Unlock()

	s.opts.Log("sampler: ch=%s seq=%d verdict=%s recv=%s srcTime=%s",
		smp.Channel, smp.Seq, env.Verdict,
		now.Format("15:04:05.000"), smp.Time.Format("15:04:05.000"))

	// Non-blocking bounded delivery: a slow downstream causes a counted
	// drop but never blocks sampling or allocates an unbounded queue.
	select {
	case s.out <- env:
	default:
		s.mu.Lock()
		s.stats.Dropped++
		s.mu.Unlock()
		s.opts.Log("sampler: ch=%s seq=%d DROPPED (downstream backpressure)", smp.Channel, smp.Seq)
	}
}

func (s *defaultSampler) Snapshot() map[model.ChannelID]model.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[model.ChannelID]model.Envelope, len(s.states))
	for k, v := range s.states {
		if v.haveData {
			out[k] = v.latest
		}
	}
	return out
}

func (s *defaultSampler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
