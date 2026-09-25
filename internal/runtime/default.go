package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/control"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

// maxPendingEnvelopes bounds the envelopes accumulated for one control tick.
// Extra envelopes between ticks are coalesced (their effect is already in the
// sampler snapshot/stats), keeping memory bounded under any slowdown.
const maxPendingEnvelopes = 256

// decisionCap bounds the decision output channel.
const decisionCap = 32

// Options configures the runtime.
type Options struct {
	Clock   clock.Clock
	Initial *config.Config
	Sources []sensor.Sensor
	// Controller optionally overrides the default controller. It exists
	// mainly to inject a deliberately slow controller when testing the
	// decision-deadline path.
	Controller control.Controller
	// Log receives human-readable log lines including key inputs and the
	// facts behind each verdict/decision.
	Log func(format string, args ...any)
}

type defaultRuntime struct {
	clk     clock.Clock
	store   config.Store
	sources []sensor.Sensor
	log     func(format string, args ...any)

	smp       sampler.Sampler
	ctrl      control.Controller
	decisions chan model.Decision
	ready     chan struct{}
	readyOnce sync.Once
}

// New builds a runtime from injectable sources and a validated initial
// configuration. No external service or real device is required.
func New(opts Options) (Runtime, error) {
	if opts.Clock == nil {
		return nil, errors.New("runtime: clock is required")
	}
	if opts.Initial == nil {
		return nil, errors.New("runtime: initial config is required")
	}
	if len(opts.Sources) == 0 {
		return nil, errors.New("runtime: at least one sensor source required")
	}
	if opts.Log == nil {
		opts.Log = func(string, ...any) {}
	}
	st, err := config.NewStore(opts.Initial)
	if err != nil {
		return nil, err
	}
	smp, err := sampler.New(sampler.Options{
		Clock:  opts.Clock,
		Period: opts.Initial.Period.Duration,
		Log:    opts.Log,
	}, opts.Sources...)
	if err != nil {
		return nil, err
	}
	ctrl := opts.Controller
	if ctrl == nil {
		var err error
		ctrl, err = control.New(control.Options{Store: st, Log: opts.Log})
		if err != nil {
			return nil, err
		}
	}
	return &defaultRuntime{
		clk:       opts.Clock,
		store:     st,
		sources:   opts.Sources,
		log:       opts.Log,
		smp:       smp,
		ctrl:      ctrl,
		decisions: make(chan model.Decision, decisionCap),
		ready:     make(chan struct{}),
	}, nil
}

func (r *defaultRuntime) Decisions() <-chan model.Decision { return r.decisions }

func (r *defaultRuntime) Ready() <-chan struct{} { return r.ready }

func (r *defaultRuntime) Reload(ctx context.Context, c *config.Config) (int64, error) {
	return r.store.Update(ctx, c)
}

// Run launches the single scheduler plus a config watcher and returns only
// after both goroutines have stopped. The scheduler ticker is the one and
// only time base for sampling and control.
func (r *defaultRuntime) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// periodChanges carries freshly installed periods from the watcher to
	// the scheduler. Buffered so the watcher never blocks the store.
	periodChanges := make(chan periodChange, 8)

	localReady := make(chan struct{}, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go r.watchConfig(runCtx, &wg, localReady, periodChanges)
	go r.scheduler(runCtx, &wg, localReady, periodChanges)
	for i := 0; i < 2; i++ {
		select {
		case <-runCtx.Done():
			wg.Wait()
			r.store.Close()
			return runCtx.Err()
		case <-localReady:
		}
	}
	r.readyOnce.Do(func() { close(r.ready) })

	<-runCtx.Done()
	wg.Wait()
	close(r.decisions)
	r.store.Close()
	return nil
}

type periodChange struct {
	period int64 // nanoseconds
}

// watchConfig forwards newly installed configurations' period to the
// scheduler. Thresholds and safety policy are read fresh from the store by
// the controller on every tick, so only cadence needs explicit fan-out; the
// atomicity guarantees live in the config store (copy-on-write under lock).
func (r *defaultRuntime) watchConfig(
	ctx context.Context, wg *sync.WaitGroup, ready chan<- struct{}, changes chan<- periodChange,
) {
	defer wg.Done()
	sub := r.store.Subscribe()
	boot, _ := r.store.Current()
	lastV := boot.Version
	<-sub // consume bootstrap copy; scheduler already uses this period
	ready <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			return
		case c, ok := <-sub:
			if !ok {
				return
			}
			if c == nil || c.Version == lastV {
				continue
			}
			lastV = c.Version
			r.log("runtime: config version=%d accepted period=%s", c.Version, c.Period.Duration)
			select {
			case changes <- periodChange{period: int64(c.Period.Duration)}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// scheduler owns the single ticker. Within each tick it polls all sources,
// drains the freshly classified envelopes, takes a snapshot and evaluates the
// controller under a deadline guard.
func (r *defaultRuntime) scheduler(
	ctx context.Context, wg *sync.WaitGroup, ready chan<- struct{}, changes <-chan periodChange,
) {
	defer wg.Done()
	cfg, _ := r.store.Current()
	period := cfg.Period.Duration
	ticker := r.clk.NewTicker(period)
	defer ticker.Stop()
	ready <- struct{}{}

	var pending []model.Envelope
	var generation int64

	for {
		select {
		case <-ctx.Done():
			return
		case pc := <-changes:
			p := periodFromNanos(pc.period)
			if p > 0 && p != period {
				ticker.Stop()
				ticker = r.clk.NewTicker(p)
				period = p
				r.smp.SetPeriod(p)
				r.log("runtime: scheduler period switched to %s", p)
			}
			continue
		case <-ticker.C():
		}

		// 1) sample every source exactly once (bounded, never blocks)
		r.smp.Poll(ctx)

		// 2) drain envelopes produced by this poll into a bounded pending set
	drain:
		for {
			select {
			case e := <-r.smp.Envelopes():
				if len(pending) < maxPendingEnvelopes {
					pending = append(pending, e)
				}
			default:
				break drain
			}
		}

		// 3) decide under the deadline guard
		generation++
		gen := generation
		cfg, _ = r.store.Current()
		now := r.clk.Now()
		st := r.smp.Stats()
		in := control.Input{
			Now:          now,
			Snapshot:     r.smp.Snapshot(),
			NewEnvelopes: pending,
			Stats:        control.SamplerStats{Faults: st.Faults, Dropped: st.Dropped},
			Generation:   gen,
		}
		pending = pending[:0]

		d := r.evaluateWithDeadline(ctx, cfg, gen, in)
		// A zero decision means evaluation was abandoned due to shutdown;
		// do not publish it as a (misleading) actuator command.
		if d.Time.IsZero() {
			return
		}
		r.emit(ctx, d)
	}
}

// evaluateWithDeadline runs one evaluation and forces a safe decision if it
// cannot complete within the configured decision deadline. The in-flight
// goroutine shares only read-only inputs; the deadline path calls ForceSafe
// with its own arguments, so there is no data race. A late evaluation is
// discarded by the controller's generation guard.
func (r *defaultRuntime) evaluateWithDeadline(
	ctx context.Context, cfg *config.Config, generation int64, in control.Input,
) model.Decision {
	type result struct{ d model.Decision }
	done := make(chan result, 1)
	start := r.clk.Now()
	go func() {
		done <- result{d: r.ctrl.Evaluate(in)}
	}()

	deadline := r.clk.NewTimer(cfg.Safety.DecisionDeadline.Duration)
	defer deadline.Stop()
	select {
	case <-ctx.Done():
		return model.Decision{}
	case res := <-done:
		if r.clk.Now().Sub(start) > cfg.Safety.DecisionDeadline.Duration {
			r.log("runtime: decision completed %.3fms after deadline %s -> safe",
				float64(r.clk.Now().Sub(start))/1e6, cfg.Safety.DecisionDeadline.Duration)
			return r.ctrl.ForceSafe(in.Now, generation, string(model.ReasonDecisionDeadline))
		}
		return res.d
	case <-deadline.C():
		d := r.ctrl.ForceSafe(in.Now, generation, string(model.ReasonDecisionDeadline))
		r.log("runtime: decision deadline %s exceeded -> safe (gen=%d)",
			cfg.Safety.DecisionDeadline.Duration, generation)
		return d
	}
}

func (r *defaultRuntime) emit(ctx context.Context, d model.Decision) {
	select {
	case r.decisions <- d:
	default:
		r.log("runtime: decision dropped (observer backpressure) state=%s", d.State)
	}
	if d.Transitioned() {
		r.log("runtime: STATE TRANSITION %s -> %s v%d reasons=%v",
			d.Previous, d.State, d.Version, d.Reasons)
	}
}
