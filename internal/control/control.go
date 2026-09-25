// Package control implements deadline monitoring and the safety state
// machine. It consumes classified sample events and periodically evaluates
// whether every channel has fresh, valid data. Timeouts, missing data,
// consecutive source anomalies or threshold violations move the system into
// a safe state; recovery requires an unbroken run of healthy control ticks,
// so a single jitter can never cause flapping.
package control

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
)

// State is the safety state.
type State int

const (
	// StateNormal: control decisions are based on healthy data.
	StateNormal State = iota
	// StateSafe: actuators must be held in their predefined safe posture.
	StateSafe
)

// String gives states stable log names.
func (s State) String() string {
	switch s {
	case StateNormal:
		return "NORMAL"
	case StateSafe:
		return "SAFE"
	default:
		return "UNKNOWN"
	}
}

// Reason is the deterministic trigger behind a decision or transition.
type Reason string

const (
	ReasonHealthy        Reason = "healthy"
	ReasonThreshold      Reason = "threshold_violation"
	ReasonDeadlineMissed Reason = "deadline_missed"
	ReasonNoData         Reason = "no_data"
	ReasonConsecutiveErr Reason = "consecutive_anomaly"
	ReasonRecovering     Reason = "recovering"
)

// ChannelStatus records the evidence for one channel at a control tick.
type ChannelStatus struct {
	ChannelID string
	// Fresh reports whether the newest sample is within DecisionDeadline.
	Fresh bool
	// Age is time since the newest usable sample (zero if none).
	Age       time.Duration
	LastValue float64
	LastSeq   uint64
	// Errors is the count of consecutive read errors/gaps since the
	// last healthy sample.
	Errors int
	// ThresholdViolation marks an accepted value outside valid/warn bounds.
	ThresholdViolation bool
}

// Decision records one control evaluation with the evidence behind it.
type Decision struct {
	At            time.Time
	State         State
	Previous      State
	Reason        Reason
	Detail        string
	ConfigVersion int64
	// HealthyRun is the current consecutive healthy-tick counter.
	HealthyRun int
	Channels   []ChannelStatus
}

// Logger is the minimal logging surface used by the controller.
type Logger interface {
	Printf(format string, args ...any)
}

// channelView is the bounded per-channel evidence store.
type channelView struct {
	lastEvent sampler.Event // newest accepted/gapped event
	hasData   bool
	errRun    int
	violation bool
}

// Controller consumes sample events and emits safety decisions.
type Controller struct {
	getCfg func() *config.Config
	clk    clock.Clock
	log    Logger

	mu      sync.Mutex
	views   map[string]*channelView
	state   State
	since   time.Time
	healthy int

	recvCfg chan *config.Config
	out     chan Decision

	decisions atomic.Uint64
}

// New builds a controller. getCfg returns the active configuration snapshot.
func New(getCfg func() *config.Config, clk clock.Clock, log Logger) *Controller {
	return &Controller{
		getCfg:  getCfg,
		clk:     clk,
		log:     log,
		views:   map[string]*channelView{},
		recvCfg: make(chan *config.Config, 1),
		out:     make(chan Decision, 64),
	}
}

// Decisions returns the bounded decision output channel.
func (c *Controller) Decisions() <-chan Decision { return c.out }

// State returns the current safety state.
func (c *Controller) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// DecisionCount reports the total number of emitted decisions.
func (c *Controller) DecisionCount() uint64 { return c.decisions.Load() }

// Reconfigure installs a new config snapshot non-blockingly; only the newest
// version matters. Per-channel evidence is retained.
func (c *Controller) Reconfigure(cfg *config.Config) {
	c.mu.Lock()
	for _, ch := range cfg.Channels {
		if _, ok := c.views[ch.ID]; !ok {
			c.views[ch.ID] = &channelView{}
		}
	}
	for id := range c.views {
		if _, ok := cfg.Channel(id); !ok {
			delete(c.views, id)
		}
	}
	c.mu.Unlock()
	select {
	case c.recvCfg <- cfg:
	default:
		select {
		case <-c.recvCfg:
		default:
		}
		select {
		case c.recvCfg <- cfg:
		default:
		}
	}
}

// violating reports whether a value is outside the channel's valid range or
// the configured warning band.
func violating(ch config.ChannelCfg, thr config.ThresholdCfg, v float64) bool {
	return v < ch.MinValue || v > ch.MaxValue || v < thr.WarnLow || v > thr.WarnHigh
}

// applyEvent folds one classified sampler event into the evidence store.
func (c *Controller) applyEvent(cfg *config.Config, ev sampler.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.views[ev.ChannelID]
	if !ok {
		v = &channelView{}
		c.views[ev.ChannelID] = v
	}
	switch ev.Outcome {
	case sampler.OutcomeAccepted:
		v.lastEvent = ev
		v.hasData = true
		v.errRun = 0
		ch, _ := cfg.Channel(ev.ChannelID)
		v.violation = violating(ch, cfg.Threshold, ev.Sample.Value)
	case sampler.OutcomeGapped:
		// A gap delivers a usable value but signals lost samples, so it
		// refreshes data while counting as an anomaly.
		v.lastEvent = ev
		v.hasData = true
		v.errRun++
		ch, _ := cfg.Channel(ev.ChannelID)
		v.violation = violating(ch, cfg.Threshold, ev.Sample.Value)
	case sampler.OutcomeReadError:
		v.errRun++
	case sampler.OutcomeDuplicate, sampler.OutcomeStale:
		// Reordered/retransmitted/expired evidence does not alter state.
	}
}

// evaluate builds a decision from current evidence at time now.
func (c *Controller) evaluate(cfg *config.Config, now time.Time) Decision {
	c.mu.Lock()
	prev := c.state
	statuses := make([]ChannelStatus, 0, len(cfg.Channels))
	reason := ReasonHealthy
	badChannel := ""
	for _, ch := range cfg.Channels {
		v := c.views[ch.ID]
		if v == nil {
			v = &channelView{}
		}
		st := ChannelStatus{ChannelID: ch.ID, Errors: v.errRun, ThresholdViolation: v.violation}
		if !v.hasData {
			st.Fresh = false
			if reason == ReasonHealthy {
				reason = ReasonNoData
				badChannel = ch.ID
			}
		} else {
			st.LastValue = v.lastEvent.Sample.Value
			st.LastSeq = v.lastEvent.Sample.Seq
			st.Age = now.Sub(v.lastEvent.Sample.Timestamp)
			st.Fresh = st.Age <= cfg.Safety.DecisionDeadline
			switch {
			case v.violation:
				if reason == ReasonHealthy {
					reason = ReasonThreshold
					badChannel = ch.ID
				}
			case !st.Fresh:
				if reason == ReasonHealthy {
					reason = ReasonDeadlineMissed
					badChannel = ch.ID
				}
			case v.errRun >= cfg.Safety.MaxConsecutiveErr:
				if reason == ReasonHealthy {
					reason = ReasonConsecutiveErr
					badChannel = ch.ID
				}
			}
		}
		statuses = append(statuses, st)
	}

	healthy := reason == ReasonHealthy
	if healthy {
		c.healthy++
	} else {
		c.healthy = 0
	}

	var next State
	detail := ""
	switch c.state {
	case StateNormal:
		if !healthy {
			next = StateSafe
			c.since = now
			detail = "enter safe: " + string(reason) + " channel=" + badChannel
		} else {
			next = StateNormal
			c.since = now
		}
	case StateSafe:
		if healthy && c.healthy >= cfg.Safety.RecoverGoodTicks {
			next = StateNormal
			c.since = now
			detail = "recover normal after " + strconv.Itoa(c.healthy) + " healthy ticks"
		} else {
			next = StateSafe
			if healthy {
				reason = ReasonRecovering
			}
			detail = "hold safe: " + string(reason) + " healthy_run=" + strconv.Itoa(c.healthy)
		}
	}
	c.state = next
	state := c.state
	run := c.healthy
	c.mu.Unlock()

	return Decision{
		At:            now,
		State:         state,
		Previous:      prev,
		Reason:        reason,
		Detail:        detail,
		ConfigVersion: cfg.Version,
		HealthyRun:    run,
		Channels:      statuses,
	}
}

// emit performs a bounded non-blocking send. Safety is enforced internally
// regardless of consumer speed; a flooded consumer loses decision records
// but never stalls the loop.
func (c *Controller) emit(d Decision) {
	select {
	case c.out <- d:
	default:
		select {
		case <-c.out:
		default:
		}
		select {
		case c.out <- d:
		default:
		}
	}
	c.decisions.Add(1)
}

// Run evaluates the safety state on a fixed control period while consuming
// sample events. The ticker is rebuilt atomically with each config version.
// It blocks until ctx is canceled.
func (c *Controller) Run(ctx context.Context, events <-chan sampler.Event) error {
	cfg := c.getCfg()
	ticker := c.clk.NewTicker(cfg.Safety.ControlPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next := <-c.recvCfg:
			ticker.Stop()
			cfg = next
			ticker = c.clk.NewTicker(cfg.Safety.ControlPeriod)
			c.log.Printf("control: reconfigured to version %d", cfg.Version)
		case ev, ok := <-events:
			if !ok {
				events = nil
				break
			}
			c.applyEvent(cfg, ev)
		case now := <-ticker.Ch():
			d := c.evaluate(cfg, now)
			c.logDecision(d)
			c.emit(d)
		}
	}
}

func (c *Controller) logDecision(d Decision) {
	c.log.Printf("control: at=%s %s->%s reason=%s v=%d healthy_run=%d %s",
		d.At.Format(time.RFC3339Nano), d.Previous, d.State, d.Reason,
		d.ConfigVersion, d.HealthyRun, d.Detail)
}
