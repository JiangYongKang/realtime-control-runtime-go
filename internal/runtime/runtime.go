// Package runtime wires sampling, control, configuration and fault handling
// into one locally runnable process. All external edges are injected: the
// clock, the sensor sources and the logger — no real devices or services.
package runtime

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/control"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sampler"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

// Logger is the standard logging surface shared by components.
type Logger interface {
	Printf(format string, args ...any)
}

// Runtime owns the component lifecycle.
type Runtime struct {
	Clk        clock.Clock
	CfgMgr     *config.Manager
	Sampler    *sampler.Sampler
	Controller *control.Controller
	log        Logger

	decisionDrain func(control.Decision)
}

// Options describes how to build a runtime.
type Options struct {
	Clock clock.Clock
	Log   Logger
	// Sources maps channel IDs to custom sources; nil channels fall back
	// to simulation sources built from the config.
	Sources map[string]sensor.Source
	// DecisionDrain, if set, receives every emitted decision.
	DecisionDrain func(control.Decision)
	// Judge, if set, replaces the default control policy with a local
	// custom judgment. The runtime bounds every invocation by
	// safety.judge_timeout and forces SAFE itself when the judgment is
	// late; abandoned late verdicts are discarded and never revise a
	// conclusion already issued.
	Judge control.Judge
}

// simFactory builds a simulated source for each channel config.
func simFactory(clk clock.Clock, custom map[string]sensor.Source) func(config.ChannelCfg) sensor.Source {
	return func(ch config.ChannelCfg) sensor.Source {
		if custom != nil {
			if src, ok := custom[ch.ID]; ok {
				return src
			}
		}
		return sensor.NewSimSource(sensor.SimConfig{
			ChannelID:    ch.ID,
			Base:         ch.Sim.Base,
			Amplitude:    ch.Sim.Amplitude,
			PeriodS:      ch.Sim.PeriodS,
			FailEveryN:   ch.Sim.FailEveryN,
			FailFromRead: ch.Sim.FailFromRead,
		}, clk.Now)
	}
}

// New builds a runtime from a validated initial config.
func New(cfg *config.Config, opts Options) (*Runtime, error) {
	if err := config.Validate(cfg); err != nil {
		return nil, err
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.Real()
	}
	lg := opts.Log
	if lg == nil {
		lg = log.New(os.Stdout, "rtr ", log.LstdFlags|log.Lmicroseconds)
	}
	mgr, err := config.NewManager(cfg)
	if err != nil {
		return nil, err
	}
	smp, err := sampler.New(mgr.Current(), clk, lg, simFactory(clk, opts.Sources))
	if err != nil {
		return nil, err
	}
	ctl := control.New(mgr.Current, clk, lg)
	if opts.Judge != nil {
		ctl.SetJudge(opts.Judge)
		lg.Printf("runtime: custom control judge registered; per-tick budget %s", mgr.Current().Safety.EffectiveJudgeTimeout())
	}
	return &Runtime{
		Clk:           clk,
		CfgMgr:        mgr,
		Sampler:       smp,
		Controller:    ctl,
		log:           lg,
		decisionDrain: opts.DecisionDrain,
	}, nil
}

// Reload validates and atomically applies a new configuration version. The
// order is: validate -> install in the manager -> notify sampler and
// controller. A rejected update leaves every component on the old version;
// readers therefore never observe a half-updated system.
func (r *Runtime) Reload(next *config.Config) (*config.Config, error) {
	installed, err := r.CfgMgr.Update(next)
	if err != nil {
		return r.CfgMgr.Current(), err
	}
	r.Sampler.Reconfigure(installed)
	r.Controller.Reconfigure(installed)
	r.log.Printf("runtime: configuration version %d installed atomically", installed.Version)
	return installed, nil
}

// Run starts sampling and control and blocks until ctx is canceled.
func (r *Runtime) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Sampler.Run(runCtx); err != nil && runCtx.Err() == nil {
			errCh <- err
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Controller.Run(runCtx, r.Sampler.Events()); err != nil && runCtx.Err() == nil {
			errCh <- err
		}
	}()
	if r.decisionDrain != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			drain(runCtx, r.Controller.Decisions(), r.decisionDrain)
		}()
	}

	<-runCtx.Done()
	cancel()
	wg.Wait()
	return nil
}

// drain forwards decisions to a user callback until the context ends.
func drain(ctx context.Context, in <-chan control.Decision, fn func(control.Decision)) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-in:
			if !ok {
				return
			}
			fn(d)
		}
	}
}
