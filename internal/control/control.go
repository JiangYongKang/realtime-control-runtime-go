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
	// StateWarming: startup/reload grace window; the first samples have
	// not arrived yet. It is distinguishable from SAFE: no safe command is
	// asserted, but missing data is not yet treated as a fault.
	StateWarming
)

// String gives states stable log names.
func (s State) String() string {
	switch s {
	case StateNormal:
		return "NORMAL"
	case StateSafe:
		return "SAFE"
	case StateWarming:
		return "WARMING"
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
	ReasonWarming        Reason = "warming"
	ReasonJudgeTimeout   Reason = "judge_timeout"
	ReasonJudgeError     Reason = "judge_error"
	ReasonJudgeUnsafe    Reason = "judge_unsafe"
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
	// Warming reports the verdict was produced inside a startup/reload
	// warm-up grace window (State == StateWarming).
	Warming bool
	// ForcedSafe reports the runtime overrode the verdict (a custom judge
	// was slow, errored or missing) and asserted the safe state itself.
	ForcedSafe bool
	// JudgeLatency is how long the custom decision function actually ran
	// before its verdict was consumed or abandoned.
	JudgeLatency time.Duration
	// JudgeTimedOut marks evaluations where the judge did not return
	// within the configured decision time limit.
	JudgeTimedOut bool
}

// JudgeInput is the evidence handed to a locally registered decision
// function: the same sampled results and active configuration the built-in
// policy uses.
type JudgeInput struct {
	At            time.Time
	Config        *config.Config
	Channels      []ChannelStatus
	ConfigVersion int64
}

// Verdict is a custom judge's conclusion. Safe=true asks the runtime to hold
// the safe posture; Detail is carried into the decision record.
type Verdict struct {
	Safe   bool
	Detail string
}

// Judge is a locally registered decision function that replaces the built-in
// policy. Implementations must honour ctx: the runtime cancels it once the
// configured decision time limit elapses, and a function that ignores
// cancellation is abandoned (its late result discarded) without blocking the
// control loop.
type Judge func(ctx context.Context, in JudgeInput) (Verdict, error)

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
	// warmupUntil marks the end of the current startup/reload grace
	// window; the zero value means no warm-up is in effect.
	warmupUntil time.Time

	recvCfg chan *config.Config
	out     chan Decision

	decisions atomic.Uint64

	// Optional locally registered decision function and its effective
	// time limit. judge == nil keeps the built-in policy.
	judge        Judge
	judgeTimeout time.Duration
	calls        chan judgeCall
	results      chan judgeResult
	workerWG     sync.WaitGroup

	lateDropped atomic.Uint64
	timeouts    atomic.Uint64
}

// judgeCall asks the single judge worker to evaluate one tick.
type judgeCall struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	seq       uint64
	in        JudgeInput
	issuedAt  time.Time
}

// judgeResult is the worker's reply; seq binds it to its tick.
type judgeResult struct {
	seq     uint64
	v       Verdict
	err     error
	latency time.Duration
}

// New builds a controller with the built-in decision policy. getCfg returns
// the active configuration snapshot.
func New(getCfg func() *config.Config, clk clock.Clock, log Logger) *Controller {
	return buildController(getCfg, clk, log, nil, 0)
}

// NewWithJudge builds a controller whose built-in policy is replaced by the
// locally registered judge. timeout bounds each evaluation; passing 0 makes
// the effective limit the active control_period. A non-positive timeout in
// configuration is treated the same way at run start.
func NewWithJudge(getCfg func() *config.Config, clk clock.Clock, log Logger, judge Judge, timeout time.Duration) *Controller {
	return buildController(getCfg, clk, log, judge, timeout)
}

func buildController(getCfg func() *config.Config, clk clock.Clock, log Logger, judge Judge, timeout time.Duration) *Controller {
	c := &Controller{
		getCfg:  getCfg,
		clk:     clk,
		log:     log,
		views:   map[string]*channelView{},
		recvCfg: make(chan *config.Config, 1),
		out:     make(chan Decision, 64),
		judge:   judge,
	}
	if judge != nil {
		c.judgeTimeout = timeout
		// Buffered depth 1: one result may be awaiting reaping (a
		// superseded tick's late result is discarded in the loop).
		c.calls = make(chan judgeCall, 1)
		c.results = make(chan judgeResult, 1)
	}
	return c
}

// Decisions returns the bounded decision output channel.
func (c *Controller) Decisions() <-chan Decision { return c.out }

// LateResultCount reports custom-judge results that finished after their
// tick had already been abandoned and were therefore discarded.
func (c *Controller) LateResultCount() uint64 { return c.lateDropped.Load() }

// JudgeTimeoutCount reports ticks on which the custom judge exceeded its
// configured decision time limit.
func (c *Controller) JudgeTimeoutCount() uint64 { return c.timeouts.Load() }

// State returns the current safety state.
func (c *Controller) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// DecisionCount reports the total number of emitted decisions.
func (c *Controller) DecisionCount() uint64 { return c.decisions.Load() }

// Reconfigure installs a new config snapshot non-blockingly; only the newest
// version matters. Per-channel evidence is retained. A fresh warm-up grace
// window is opened so the first post-reload ticks with no data yet are
// reported as warming rather than safe.
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
	if cfg.Safety.WarmupGrace > 0 {
		c.warmupUntil = c.clk.Now().Add(cfg.Safety.WarmupGrace)
	} else {
		c.warmupUntil = time.Time{}
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

// evidence is the per-tick summary folded from the channel views.
type evidence struct {
	statuses []ChannelStatus
	reason   Reason
	// allNoData reports every configured channel has never accepted data.
	allNoData bool
	// badChannel identifies the first channel that broke health.
	badChannel string
}

// collectEvidence snapshots per-channel evidence under the lock but does not
// mutate the state machine.
func (c *Controller) collectEvidence(cfg *config.Config, now time.Time) evidence {
	c.mu.Lock()
	defer c.mu.Unlock()
	statuses := make([]ChannelStatus, 0, len(cfg.Channels))
	reason := ReasonHealthy
	badChannel := ""
	allNoData := true
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
			allNoData = false
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
	return evidence{statuses: statuses, reason: reason, allNoData: allNoData, badChannel: badChannel}
}

// inWarmup reports whether now is still inside an active grace window.
func (c *Controller) inWarmup(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.warmupUntil.IsZero() && !now.After(c.warmupUntil)
}

// tickOutcome classifies one tick before the state machine runs.
type tickOutcome struct {
	healthy    bool
	warming    bool
	reason     Reason
	forcedSafe bool
}

// evaluate builds a decision from current evidence at time now using the
// built-in policy (no registered custom judge).
func (c *Controller) evaluate(cfg *config.Config, now time.Time) Decision {
	ev := c.collectEvidence(cfg, now)
	out := tickOutcome{healthy: ev.reason == ReasonHealthy, reason: ev.reason}
	if !out.healthy && ev.allNoData && c.inWarmup(now) {
		out.warming = true
		out.reason = ReasonWarming
	}
	return c.commit(cfg, now, ev, out, 0, false)
}

// commit applies one classified tick outcome to the state machine and builds
// the immutable decision record. It is the only place state changes, so a
// late judge result can never rewrite an already committed decision.
func (c *Controller) commit(cfg *config.Config, now time.Time, ev evidence, out tickOutcome, judgeLatency time.Duration, judgeTimedOut bool) Decision {
	c.mu.Lock()
	prev := c.state

	switch {
	case out.warming:
		// Grace window: data simply has not arrived yet. Do not assert
		// safe; leave the healthy streak untouched.
		if c.healthy != 0 {
			c.healthy = 0
		}
	case out.healthy:
		c.healthy++
	default:
		c.healthy = 0
	}

	var next State
	detail := ""
	reason := out.reason
	switch {
	case out.warming:
		next = StateWarming
		detail = "warm-up: no data yet within grace channel=" + ev.badChannel
	case out.healthy:
		switch c.state {
		case StateSafe:
			if c.healthy >= cfg.Safety.RecoverGoodTicks {
				next = StateNormal
				detail = "recover normal after " + strconv.Itoa(c.healthy) + " healthy ticks"
			} else {
				next = StateSafe
				reason = ReasonRecovering
				detail = "hold safe: healthy_run=" + strconv.Itoa(c.healthy)
			}
		default:
			next = StateNormal
		}
	default:
		next = StateSafe
		c.since = now
		if c.state != StateSafe {
			detail = "enter safe: " + string(reason) + " channel=" + ev.badChannel
			if out.forcedSafe {
				detail = "forced safe (fallback): " + string(reason)
			}
		} else {
			detail = "hold safe: " + string(reason) + " healthy_run=0"
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
		Channels:      ev.statuses,
		Warming:       out.warming,
		ForcedSafe:    out.forcedSafe,
		JudgeLatency:  judgeLatency,
		JudgeTimedOut: judgeTimedOut,
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

// runJudgeWorker executes custom judges sequentially. It exits once ctx ends
// after the in-flight call returns. A judge that ignores cancellation can
// occupy this single worker for good, which is why at most one evaluation is
// ever in flight: the control loop then stops dispatching and keeps asserting
// safe on its own instead of queuing unbounded calls or goroutines.
func (c *Controller) runJudgeWorker(ctx context.Context) {
	defer c.workerWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case call := <-c.calls:
			v, err := c.judge(call.ctx, call.in)
			res := judgeResult{seq: call.seq, v: v, err: err, latency: c.clk.Now().Sub(call.issuedAt)}
			select {
			case c.results <- res:
			default:
				// The slot still holds an un-reaped result (e.g. an even
				// earlier late reply). This verdict can never bind to a
				// tick; drop it and count the late discard.
				c.lateDropped.Add(1)
			}
		}
	}
}

// effectiveJudgeTimeout resolves the per-tick decision time limit.
func (c *Controller) effectiveJudgeTimeout(cfg *config.Config) time.Duration {
	if cfg.Safety.JudgeTimeout > 0 {
		return cfg.Safety.JudgeTimeout
	}
	if c.judgeTimeout > 0 {
		return c.judgeTimeout
	}
	return cfg.Safety.ControlPeriod
}

// dispatchJudge hands one tick to the worker unless the worker is still busy
// with the previous evaluation. Returns whether the call was accepted.
func (c *Controller) dispatchJudge(call judgeCall) bool {
	if c.judge == nil {
		return false
	}
	select {
	case c.calls <- call:
		return true
	default:
		return false
	}
}

// Run evaluates the safety state on a fixed control period while consuming
// sample events. The ticker is rebuilt atomically with each config version.
// It blocks until ctx is canceled.
func (c *Controller) Run(ctx context.Context, events <-chan sampler.Event) error {
	cfg := c.getCfg()
	// Open the initial warm-up window (cold start).
	if cfg.Safety.WarmupGrace > 0 {
		c.mu.Lock()
		c.warmupUntil = c.clk.Now().Add(cfg.Safety.WarmupGrace)
		c.mu.Unlock()
	}
	ticker := c.clk.NewTicker(cfg.Safety.ControlPeriod)
	defer ticker.Stop()

	if c.judge != nil {
		c.workerWG.Add(1)
		go c.runJudgeWorker(ctx)
		// The worker is not awaited on Run return: a judge that ignores
		// ctx cancellation could otherwise block process exit. It is
		// expected to exit on its own once the last in-flight call honors
		// cancellation; a deliberately non-cooperative judge may leak this
		// single worker goroutine by design (bounded: at most one).
	}

	var seq uint64
	var pending *judgeCall
	var deadlineCh <-chan time.Time
	var deadlineTimer clock.Ticker
	clearDeadline := func() {
		if deadlineTimer != nil {
			deadlineTimer.Stop()
			deadlineTimer = nil
		}
		deadlineCh = nil
		pending = nil
	}

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
		case res := <-c.results:
			// Only the outstanding tick's result may commit; a result for
			// an abandoned tick is discarded without touching state, so a
			// late verdict can never flip an already issued safe decision
			// nor trigger an extra transition.
			if pending == nil || res.seq != pending.seq {
				c.lateDropped.Add(1)
				c.log.Printf("control: late judge result discarded seq=%d", res.seq)
				break
			}
			ev := c.collectEvidence(cfg, pending.in.At)
			out := c.classifyJudgeResult(cfg, pending.in.At, ev, res)
			d := c.commit(cfg, pending.in.At, ev, out, res.latency, false)
			c.logDecision(d)
			c.emit(d)
			clearDeadline()
		case <-deadlineCh:
			// The judge did not finish in time. Assert safe ourselves and
			// abandon the call; any result it produces later is a late
			// discard. The deadline is driven by the injected clock, so
			// the bound holds deterministically in tests too.
			if pending == nil {
				break
			}
			pending.ctxCancel()
			latency := c.clk.Now().Sub(pending.issuedAt)
			c.timeouts.Add(1)
			ev := c.collectEvidence(cfg, pending.in.At)
			out := tickOutcome{healthy: false, warming: false, reason: ReasonJudgeTimeout, forcedSafe: true}
			if ev.allNoData && c.inWarmup(pending.in.At) {
				// Missing data alone is still covered by the grace
				// window; the judge cannot even be meaningfully fed yet.
				out = tickOutcome{warming: true, reason: ReasonWarming}
			}
			d := c.commit(cfg, pending.in.At, ev, out, latency, true)
			c.log.Printf("control: judge timeout seq=%d limit=%s latency=%s", pending.seq, c.effectiveJudgeTimeout(cfg), latency)
			c.logDecision(d)
			c.emit(d)
			clearDeadline()
		case now := <-ticker.Ch():
			if pending != nil {
				// Previous evaluation still outstanding (worker stuck).
				// Fall back immediately; do not queue behind it.
				c.fallbackSafe(cfg, now, "judge worker busy")
				continue
			}
			ev := c.collectEvidence(cfg, now)
			// Warm-up short-circuit: while every channel is still data-less
			// inside the grace window, do not even invoke the judge.
			if ev.allNoData && c.inWarmup(now) {
				out := tickOutcome{warming: true, reason: ReasonWarming}
				d := c.commit(cfg, now, ev, out, 0, false)
				c.logDecision(d)
				c.emit(d)
				continue
			}
			if c.judge == nil {
				d := c.evaluateWith(cfg, now, ev)
				c.logDecision(d)
				c.emit(d)
				continue
			}
			seq++
			limit := c.effectiveJudgeTimeout(cfg)
			jctx, cancel := context.WithTimeout(ctx, limit)
			call := judgeCall{
				ctx:       jctx,
				ctxCancel: cancel,
				seq:       seq,
				issuedAt:  now,
				in: JudgeInput{
					At:            now,
					Config:        cfg,
					Channels:      ev.statuses,
					ConfigVersion: cfg.Version,
				},
			}
			if !c.dispatchJudge(call) {
				// Worker blocked on an earlier un-cancellable judge:
				// enforce safe without waiting.
				cancel()
				c.fallbackSafe(cfg, now, "judge worker occupied")
				continue
			}
			pending = &call
			deadlineTimer = c.clk.NewTicker(limit)
			deadlineCh = deadlineTimer.Ch()
		}
	}
}

// evaluateWith applies the built-in policy to pre-collected evidence.
func (c *Controller) evaluateWith(cfg *config.Config, now time.Time, ev evidence) Decision {
	out := tickOutcome{healthy: ev.reason == ReasonHealthy, reason: ev.reason}
	if !out.healthy && ev.allNoData && c.inWarmup(now) {
		out.warming = true
		out.reason = ReasonWarming
	}
	return c.commit(cfg, now, ev, out, 0, false)
}

// fallbackSafe asserts the safe state on the runtime's own initiative when a
// custom judge cannot be relied upon (busy worker or dispatch failure).
func (c *Controller) fallbackSafe(cfg *config.Config, now time.Time, detail string) {
	c.timeouts.Add(1)
	ev := c.collectEvidence(cfg, now)
	out := tickOutcome{reason: ReasonJudgeTimeout, forcedSafe: true}
	if ev.allNoData && c.inWarmup(now) {
		out = tickOutcome{warming: true, reason: ReasonWarming}
	}
	d := c.commit(cfg, now, ev, out, 0, true)
	c.log.Printf("control: forced safe: %s", detail)
	c.logDecision(d)
	c.emit(d)
}

// classifyJudgeResult turns a returned (or late-but-accepted) judge result
// into the tick outcome. Built-in hard evidence (clear threshold/deadline/
// anomaly problems) is recorded alongside: an error verdict always fails
// safe regardless of what the judge claimed.
func (c *Controller) classifyJudgeResult(cfg *config.Config, now time.Time, ev evidence, res judgeResult) tickOutcome {
	if res.err != nil {
		return tickOutcome{reason: ReasonJudgeError, forcedSafe: true}
	}
	if res.v.Safe {
		reason := ReasonJudgeUnsafe
		return tickOutcome{reason: reason}
	}
	// The judge said "go". The runtime still never masks hard evidence:
	// a present threshold/deadline/anomaly condition forces safe even if
	// the local algorithm missed it.
	if ev.reason != ReasonHealthy && ev.reason != ReasonNoData {
		return tickOutcome{reason: ev.reason}
	}
	if ev.reason == ReasonNoData {
		// Data missing outside the grace window: judge cannot bless this.
		return tickOutcome{reason: ReasonNoData}
	}
	return tickOutcome{healthy: true, reason: ReasonHealthy}
}

func (c *Controller) logDecision(d Decision) {
	c.log.Printf("control: at=%s %s->%s reason=%s v=%d healthy_run=%d warming=%t forced_safe=%t judge_latency=%s judge_timeout=%t %s",
		d.At.Format(time.RFC3339Nano), d.Previous, d.State, d.Reason,
		d.ConfigVersion, d.HealthyRun, d.Warming, d.ForcedSafe,
		d.JudgeLatency, d.JudgeTimedOut, d.Detail)
}
