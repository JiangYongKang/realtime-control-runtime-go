package control

import (
	"sync"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// Options configures the default controller.
type Options struct {
	Store config.Store
	Log   func(format string, args ...any)
}

type chHealth struct {
	consecutiveFaults int
	stableTicks       int
}

type defaultController struct {
	store config.Store
	log   func(format string, args ...any)

	mu          sync.Mutex
	state       model.SafetyState
	health      map[model.ChannelID]*chHealth
	version     int64
	forceGen    int64 // generation of the latest deadline-driven ForceSafe
	start       time.Time
	started     bool
	lastDropped int64
}

// New creates a controller bound to the versioned config store. The active
// configuration is read fresh on every Evaluate, so a concurrent atomic
// config update is reflected at the next tick with its version recorded in
// the emitted Decision.
func New(opts Options) (Controller, error) {
	if opts.Store == nil {
		return nil, errNoStore
	}
	if opts.Log == nil {
		opts.Log = func(string, ...any) {}
	}
	cfg, version := opts.Store.Current()
	return &defaultController{
		store:   opts.Store,
		log:     opts.Log,
		state:   model.StateNormal,
		health:  make(map[model.ChannelID]*chHealth, len(cfg.Channels)),
		version: version,
	}, nil
}

func (c *defaultController) State() model.SafetyState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *defaultController) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = model.StateNormal
	c.health = make(map[model.ChannelID]*chHealth)
	c.lastDropped = 0
	c.log("controller: reset -> normal")
}

// Evaluate applies the rules in a fixed, explainable order and returns a
// Decision carrying every input fact that mattered.
func (c *defaultController) Evaluate(in Input) model.Decision {
	cfg, version := c.store.Current()

	c.mu.Lock()
	// A deadline may have fired while this evaluation was blocked before
	// the lock (e.g. a slow controller). Such a stale evaluation must not
	// touch any state; return a neutral decision reflecting current state.
	if in.Generation != 0 && in.Generation <= c.forceGen {
		cur := c.state
		cfg, version := c.store.Current()
		c.mu.Unlock()
		c.log("controller: stale generation=%d <= forceGen=%d discarded", in.Generation, c.forceGen)
		return model.Decision{
			Time: in.Now, State: cur, Previous: cur,
			Reasons: []string{"evaluation_superseded"}, Values: map[model.ChannelID]float64{},
			Output: safeOutput(cur, cfg), Version: version,
		}
	}
	if !c.started {
		c.start = in.Now
		c.started = true
	}
	prev := c.state
	// Rebuild health entries for channels present in this config version so
	// a config that adds a channel starts clean, while shared channels keep
	// their history.
	for _, ch := range cfg.Channels {
		id := model.ChannelID(ch.ID)
		if c.health[id] == nil {
			c.health[id] = &chHealth{}
		}
	}

	var hard []string    // force safe
	var soft []string    // force degraded
	var healthy []string // facts supporting recovery

	// Rule 0: decision deadline overrun is an immediate hard fault.
	if in.DeadlineExceeded {
		hard = append(hard, string(model.ReasonDecisionDeadline))
	}

	// Update per-channel fault streaks from fresh envelopes.
	for _, e := range in.NewEnvelopes {
		h := c.health[e.Sample.Channel]
		if h == nil {
			h = &chHealth{}
			c.health[e.Sample.Channel] = h
		}
		if e.Verdict == model.VerdictFault {
			h.consecutiveFaults++
		} else if e.Verdict == model.VerdictAccepted {
			h.consecutiveFaults = 0
		}
	}

	// Per-channel data timeout and consecutive-fault rules.
	for _, ch := range cfg.Channels {
		id := model.ChannelID(ch.ID)
		h := c.health[id]
		if h == nil {
			h = &chHealth{}
			c.health[id] = h
		}
		env, seen := in.Snapshot[id]
		if ch.Required {
			switch {
			case !seen && in.Now.Sub(c.start) > cfg.Safety.DataTimeout.Duration:
				// Never delivered within the startup grace period: hard fault.
				hard = append(hard, "no_data:"+string(id))
			case !seen:
				// Startup grace: not enough elapsed time to call this a
				// fault yet. Recorded as an explanatory, non-state-changing
				// fact so the wait remains visible in the decision reasons.
				healthy = append(healthy, "warming:"+string(id))
			case seen && in.Now.Sub(env.RecvTime) > cfg.Safety.DataTimeout.Duration:
				hard = append(hard, string(model.ReasonDataTimeout)+":"+string(id))
			}
		}
		if h.consecutiveFaults >= cfg.Safety.ConsecutiveFaults {
			hard = append(hard, string(model.ReasonConsecutiveErrors)+":"+string(id))
		}
		if seen && env.Verdict == model.VerdictAccepted &&
			in.Now.Sub(env.RecvTime) <= cfg.Safety.DataTimeout.Duration {
			healthy = append(healthy, string(id))
		}
	}

	// Backpressure: if the sampler dropped envelopes since the last tick
	// (slow downstream), the data set may be incomplete. This is a soft
	// fact that degrades authority and stays visible in the reasons.
	if in.Stats.Dropped > c.lastDropped {
		soft = append(soft, string(model.ReasonBackpressure))
	}
	c.lastDropped = in.Stats.Dropped

	// Threshold violations are soft facts: degraded authority, not safe.
	for _, ch := range cfg.Channels {
		id := model.ChannelID(ch.ID)
		env, seen := in.Snapshot[id]
		if !seen || env.Verdict != model.VerdictAccepted {
			continue
		}
		if limit, ok := cfg.Threshold[ch.ID]; ok && env.Sample.Value > limit {
			soft = append(soft, string(model.ReasonThresholdViolation)+":"+ch.ID)
		}
		if env.Sample.Value < ch.MinValue || env.Sample.Value > ch.MaxValue {
			soft = append(soft, "out_of_range:"+ch.ID)
		}
	}

	// Target state from this tick's facts. Hard wins over soft.
	target := model.StateNormal
	if len(soft) > 0 {
		target = model.StateDegraded
	}
	if len(hard) > 0 {
		target = model.StateSafe
	}

	// State transition with recovery hysteresis.
	switch {
	case target == model.StateSafe:
		c.state = model.StateSafe
		for _, h := range c.health {
			h.stableTicks = 0
		}
	case target == model.StateDegraded:
		// From safe, degradation still counts as an unsafe condition; stay
		// safe until the stable-recovery condition is met.
		if c.state != model.StateSafe {
			c.state = model.StateDegraded
		}
		for _, h := range c.health {
			h.stableTicks = 0
		}
	default: // all facts healthy this tick
		if c.state != model.StateNormal {
			for _, h := range c.health {
				h.stableTicks++
			}
			// Use the minimum streak across channels: every tracked channel
			// must have been healthy for the full window, so a single
			// jittering channel resets recovery for the whole system.
			minStreak := minStreak(c.health)
			if minStreak >= cfg.Safety.RecoveryStableRuns {
				c.state = model.StateNormal
				for _, h := range c.health {
					h.stableTicks = 0
					h.consecutiveFaults = 0
				}
			} else {
				healthy = append(healthy, string(model.ReasonRecoveryProgress))
			}
		}
	}

	values := make(map[model.ChannelID]float64, len(in.Snapshot))
	for id, e := range in.Snapshot {
		if e.Verdict == model.VerdictAccepted {
			values[id] = e.Sample.Value
		}
	}

	output := safeOutput(c.state, cfg)
	if c.state == model.StateNormal {
		output = averageValues(values)
	}

	reasons := make([]string, 0, len(hard)+len(soft)+len(healthy))
	reasons = append(reasons, hard...)
	reasons = append(reasons, soft...)
	reasons = append(reasons, healthy...)
	if len(reasons) == 0 {
		reasons = append(reasons, string(model.ReasonTickOK))
	}
	newState := c.state
	c.version = version
	c.mu.Unlock()

	d := model.Decision{
		Time:     in.Now,
		State:    newState,
		Previous: prev,
		Reasons:  reasons,
		Values:   values,
		Output:   output,
		Version:  version,
	}
	c.log("controller: tick state %s->%s v%d reasons=%v output=%.3f",
		prev, newState, version, reasons, output)
	return d
}

func safeOutput(st model.SafetyState, cfg *config.Config) float64 {
	if st == model.StateSafe {
		return cfg.Safety.SafeOutput
	}
	return 0
}

func averageValues(values map[model.ChannelID]float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func minStreak(m map[model.ChannelID]*chHealth) int {
	n := 0
	first := true
	for _, h := range m {
		if first || h.stableTicks < n {
			n = h.stableTicks
			first = false
		}
	}
	return n
}

// ForceSafe commands the safe state immediately and records the generation
// so that an in-flight evaluation at this or an older generation is discarded.
func (c *defaultController) ForceSafe(now time.Time, generation int64, reason string) model.Decision {
	cfg, version := c.store.Current()
	c.mu.Lock()
	prev := c.state
	if generation > c.forceGen {
		c.forceGen = generation
	}
	c.state = model.StateSafe
	for _, h := range c.health {
		h.stableTicks = 0
	}
	newState := c.state
	c.version = version
	c.mu.Unlock()

	d := model.Decision{
		Time:     now,
		State:    model.StateSafe,
		Previous: prev,
		Reasons:  []string{reason},
		Values:   map[model.ChannelID]float64{},
		Output:   cfg.Safety.SafeOutput,
		Version:  version,
	}
	c.log("controller: FORCE SAFE %s->%s gen=%d v%d reason=%s output=%.3f",
		prev, newState, generation, version, reason, cfg.Safety.SafeOutput)
	return d
}
