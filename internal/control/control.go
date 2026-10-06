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
	// ReasonWarmup: within the post-start/reload grace window, first
	// samples have not arrived yet; the safe state is not (yet) commanded
	// for missing data alone.
	ReasonWarmup Reason = "warmup"
	// ReasonCustom: a registered custom judge produced the health verdict.
	ReasonCustom Reason = "custom_judge"
	// ReasonJudgeTimeout: the custom judge exceeded the configured judge
	// timeout; the runtime forced the safe state on its own.
	ReasonJudgeTimeout Reason = "judge_timeout"
	// ReasonJudgeBusy: a previously abandoned judgment is still running,
	// so no new judgment was started and the safe state is held.
	ReasonJudgeBusy Reason = "judge_busy"
)

// ChannelStatus records the evidence for one channel at a control tick.
type ChannelStatus struct {
	ChannelID string
	// HasData reports whether any usable sample was ever accepted.
	HasData bool
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
	// Warmup marks a decision taken inside the warm-up grace window:
	// missing first data is tolerated and reported, not treated as SAFE.
	Warmup bool
	// ForcedSafe marks a SAFE conclusion imposed by the runtime itself
	// because the custom judge exceeded its time budget or was still
	// busy; the judge's own verdict did not participate.
	ForcedSafe bool
	// JudgeDuration is the wall time the custom judge actually took for
	// this decision (zero when the default policy ran or the judge was
	// abandoned before answering).
	JudgeDuration time.Duration
}

// JudgeInput is the per-tick evidence handed to a registered custom judge.
// It is exactly the same sampling evidence and configuration the default
// policy sees.
type JudgeInput struct {
	// At is the unified-clock time of the control tick.
	At time.Time
	// State is the controller's current safety state.
	State State
	// HealthyRun is the current consecutive healthy-tick counter.
	HealthyRun int
	// Config is the active immutable configuration snapshot.
	Config *config.Config
	// Channels holds the per-channel evidence at this tick.
	Channels []ChannelStatus
}

// Verdict is the health conclusion returned by a custom judge for one tick.
type Verdict struct {
	// Healthy reports whether the sampled evidence is acceptable.
	Healthy bool
	// Reason optionally names the verdict; empty selects ReasonCustom.
	Reason Reason
	// Detail is free-form evidence text carried into the decision log.
	Detail string
}

// Judge is a locally registered control judgment replacing the default
// policy. Implementations must be self-contained (no external services);
// they may be arbitrarily slow — the runtime enforces the configured judge
// timeout and abandons late judgments without ever letting them override a
// safety conclusion already issued.
type Judge interface {
	Judge(in JudgeInput) Verdict
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

// judgeCall tracks one in-flight custom judgment.
type judgeCall struct {
	seq   int64
	res   chan Verdict // buffered 1: a late judge never blocks
	start time.Time
}

// Controller consumes sample events and emits safety decisions.
type Controller struct {
	getCfg func() *config.Config
	clk    clock.Clock
	log    Logger
	judge  Judge

	mu      sync.Mutex
	views   map[string]*channelView
	state   State
	since   time.Time
	healthy int
	// Warm-up grace: active until warmupUntil or until every channel has
	// delivered its first usable sample, whichever comes first.
	warmupUntil time.Time
	warmupOn    bool

	recvCfg chan *config.Config
	out     chan Decision

	// outstanding is an abandoned judge call that has not returned yet;
	// at most one exists, bounding goroutines even for a permanently
	// stuck judge.
	outstanding *judgeCall
	judgeSeq    int64

	decisions     atomic.Uint64
	judgeTimeouts atomic.Uint64
	lateVerdicts  atomic.Uint64
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

// SetJudge registers a custom local judgment replacing the default policy.
// It must be called before Run; nil restores the default policy.
func (c *Controller) SetJudge(j Judge) { c.judge = j }

// JudgeTimeoutCount reports how many judgments were abandoned for exceeding
// the configured judge timeout.
func (c *Controller) JudgeTimeoutCount() uint64 { return c.judgeTimeouts.Load() }

// LateVerdictCount reports how many abandoned judgments eventually returned
// and were discarded without affecting any decision.
func (c *Controller) LateVerdictCount() uint64 { return c.lateVerdicts.Load() }

// State returns the current safety state.
func (c *Controller) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// DecisionCount reports the total number of emitted decisions.
func (c *Controller) DecisionCount() uint64 { return c.decisions.Load() }

// Reconfigure installs a new config snapshot non-blockingly; only the newest
// version matters. Per-channel evidence is retained. A configured warm-up
// grace is re-armed so the first samples of the new generation are awaited
// before missing data alone can command SAFE.
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
	c.armWarmupLocked(cfg, c.clk.Now())
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

// armWarmupLocked (re)arms the warm-up grace window. The caller holds mu.
func (c *Controller) armWarmupLocked(cfg *config.Config, now time.Time) {
	if cfg.Safety.WarmupGrace <= 0 {
		c.warmupOn = false
		return
	}
	c.warmupOn = true
	c.warmupUntil = now.Add(cfg.Safety.WarmupGrace)
}

// warmupActiveLocked reports whether the warm-up window is still open at
// now, and closes it early once every channel has first data. The caller
// holds mu.
func (c *Controller) warmupActiveLocked(cfg *config.Config, now time.Time) bool {
	if !c.warmupOn {
		return false
	}
	if !now.Before(c.warmupUntil) {
		c.warmupOn = false
		return false
	}
	all := true
	for _, ch := range cfg.Channels {
		v := c.views[ch.ID]
		if v == nil || !v.hasData {
			all = false
			break
		}
	}
	if all && len(cfg.Channels) > 0 {
		// First batch complete: leave the grace window immediately.
		c.warmupOn = false
		return false
	}
	return true
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

// statusesLocked builds the per-channel evidence snapshot. The caller holds mu.
func (c *Controller) statusesLocked(cfg *config.Config, now time.Time) []ChannelStatus {
	statuses := make([]ChannelStatus, 0, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		v := c.views[ch.ID]
		if v == nil {
			v = &channelView{}
		}
		st := ChannelStatus{
			ChannelID:          ch.ID,
			HasData:            v.hasData,
			Errors:             v.errRun,
			ThresholdViolation: v.violation,
		}
		if v.hasData {
			st.LastValue = v.lastEvent.Sample.Value
			st.LastSeq = v.lastEvent.Sample.Seq
			st.Age = now.Sub(v.lastEvent.Sample.Timestamp)
			st.Fresh = st.Age <= cfg.Safety.DecisionDeadline
		}
		statuses = append(statuses, st)
	}
	return statuses
}

// defaultReason derives the default-policy health reason from evidence.
func defaultReason(cfg *config.Config, statuses []ChannelStatus) (Reason, string) {
	reason := ReasonHealthy
	bad := ""
	for _, st := range statuses {
		switch {
		case !st.HasData:
			if reason == ReasonHealthy {
				reason = ReasonNoData
				bad = st.ChannelID
			}
		case st.ThresholdViolation:
			if reason == ReasonHealthy {
				reason = ReasonThreshold
				bad = st.ChannelID
			}
		case !st.Fresh:
			if reason == ReasonHealthy {
				reason = ReasonDeadlineMissed
				bad = st.ChannelID
			}
		case st.Errors >= cfg.Safety.MaxConsecutiveErr:
			if reason == ReasonHealthy {
				reason = ReasonConsecutiveErr
				bad = st.ChannelID
			}
		}
	}
	return reason, bad
}

// warmupDecision returns a distinguishable warm-up decision when the grace
// window is open and first samples are still missing. Real anomalies are
// never masked: a channel already accumulating consecutive read errors, or
// one that has delivered data (violations, staleness), ends the masking.
func (c *Controller) warmupDecision(cfg *config.Config, now time.Time) (Decision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.warmupActiveLocked(cfg, now) {
		return Decision{}, false
	}
	missing := false
	for _, ch := range cfg.Channels {
		v := c.views[ch.ID]
		if v == nil || !v.hasData {
			missing = true
		}
		if v != nil && v.errRun >= cfg.Safety.MaxConsecutiveErr {
			// Genuine consecutive anomaly: the grace window must not
			// cover it; fall through to the normal evaluation.
			return Decision{}, false
		}
	}
	if !missing {
		return Decision{}, false
	}
	return Decision{
		At:            now,
		State:         c.state,
		Previous:      c.state,
		Reason:        ReasonWarmup,
		Detail:        "warm-up grace until " + c.warmupUntil.Format(time.RFC3339Nano) + ": awaiting first samples",
		ConfigVersion: cfg.Version,
		HealthyRun:    c.healthy,
		Channels:      c.statusesLocked(cfg, now),
		Warmup:        true,
	}, true
}

// transitionLocked applies the safety state machine to a health verdict and
// builds the decision. The caller holds mu. extraDetail carries verdict
// evidence from a custom judge; forced marks a runtime-imposed safe
// conclusion; judgeDur records how long the custom judge took.
func (c *Controller) transitionLocked(cfg *config.Config, now time.Time, statuses []ChannelStatus, healthy bool, reason Reason, badChannel, extraDetail string, forced bool, judgeDur time.Duration) Decision {
	prev := c.state
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
			detail = "enter safe: " + string(reason)
			if badChannel != "" {
				detail += " channel=" + badChannel
			}
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
	if extraDetail != "" {
		detail += " (" + extraDetail + ")"
	}
	c.state = next
	return Decision{
		At:            now,
		State:         next,
		Previous:      prev,
		Reason:        reason,
		Detail:        detail,
		ConfigVersion: cfg.Version,
		HealthyRun:    c.healthy,
		Channels:      statuses,
		ForcedSafe:    forced,
		JudgeDuration: judgeDur,
	}
}

// evaluate builds a decision from current evidence at time now using the
// default policy.
func (c *Controller) evaluate(cfg *config.Config, now time.Time) Decision {
	if d, ok := c.warmupDecision(cfg, now); ok {
		return d
	}
	c.mu.Lock()
	statuses := c.statusesLocked(cfg, now)
	reason, bad := defaultReason(cfg, statuses)
	d := c.transitionLocked(cfg, now, statuses, reason == ReasonHealthy, reason, bad, "", false, 0)
	c.mu.Unlock()
	return d
}

// decide produces the decision for one control tick: the registered custom
// judge when present (bounded by the configured judge timeout), otherwise
// the default policy.
func (c *Controller) decide(cfg *config.Config, now time.Time) Decision {
	if c.judge == nil {
		return c.evaluate(cfg, now)
	}
	// Warm-up masking is evidence-based and applies with or without a judge.
	if d, ok := c.warmupDecision(cfg, now); ok {
		return d
	}
	return c.judgeDecision(cfg, now)
}

// judgeDecision runs the custom judge for one tick under the configured
// timeout. A judgment that exceeds its budget is abandoned: the runtime
// forces SAFE on its own, and the late verdict — if it ever arrives — is
// reaped and discarded without touching any conclusion already issued. At
// most one abandoned call is outstanding, so even a permanently stuck judge
// cannot accumulate goroutines.
func (c *Controller) judgeDecision(cfg *config.Config, now time.Time) Decision {
	timeout := cfg.Safety.EffectiveJudgeTimeout()

	c.mu.Lock()
	statuses := c.statusesLocked(cfg, now)
	// Reap a previously abandoned call that has since returned.
	if oc := c.outstanding; oc != nil {
		select {
		case <-oc.res:
			c.outstanding = nil
			c.lateVerdicts.Add(1)
			c.log.Printf("control: discarded late verdict seq=%d; forced-safe conclusion stands", oc.seq)
		default:
		}
	}
	if c.outstanding != nil {
		// The abandoned judgment is still running: hold SAFE without
		// spawning another call.
		d := c.transitionLocked(cfg, now, statuses, false, ReasonJudgeBusy, "",
			"previous judgment seq="+strconv.FormatInt(c.outstanding.seq, 10)+" still in flight", true, 0)
		c.mu.Unlock()
		return d
	}
	c.judgeSeq++
	call := &judgeCall{seq: c.judgeSeq, res: make(chan Verdict, 1), start: c.clk.Now()}
	in := JudgeInput{
		At:         now,
		State:      c.state,
		HealthyRun: c.healthy,
		Config:     cfg,
		Channels:   statuses,
	}
	judge := c.judge
	c.mu.Unlock()

	go func() { call.res <- judge.Judge(in) }()
	timer := c.clk.NewTimer(timeout)
	defer timer.Stop()

	select {
	case v := <-call.res:
		dur := c.clk.Now().Sub(call.start)
		c.mu.Lock()
		reason := v.Reason
		if reason == "" {
			reason = ReasonCustom
		}
		d := c.transitionLocked(cfg, now, statuses, v.Healthy, reason, "", v.Detail, false, dur)
		c.mu.Unlock()
		return d
	case <-timer.Ch():
		c.judgeTimeouts.Add(1)
		c.mu.Lock()
		c.outstanding = call
		d := c.transitionLocked(cfg, now, statuses, false, ReasonJudgeTimeout, "",
			"judgment seq="+strconv.FormatInt(call.seq, 10)+" exceeded "+timeout.String(), true, 0)
		c.mu.Unlock()
		c.log.Printf("control: judge seq=%d exceeded timeout %s; runtime forced SAFE", call.seq, timeout)
		return d
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
	c.mu.Lock()
	c.armWarmupLocked(cfg, c.clk.Now())
	c.mu.Unlock()
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
			d := c.decide(cfg, now)
			c.logDecision(d)
			c.emit(d)
		}
	}
}

func (c *Controller) logDecision(d Decision) {
	c.log.Printf("control: at=%s %s->%s reason=%s v=%d healthy_run=%d warmup=%t forced_safe=%t judge_dur=%s late_verdicts=%d %s",
		d.At.Format(time.RFC3339Nano), d.Previous, d.State, d.Reason,
		d.ConfigVersion, d.HealthyRun, d.Warmup, d.ForcedSafe, d.JudgeDuration,
		c.lateVerdicts.Load(), d.Detail)
}
