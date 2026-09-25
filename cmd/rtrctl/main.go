// Command rtrctl runs the local real-time sampling and control runtime with
// an injectable synthetic sensor source. No real device, external service or
// credentials are required.
//
// Usage:
//
//	go run ./cmd/rtrctl -config configs/example.json -duration 5s
//
// Flags allow injecting a fault burst and a live config reload to demonstrate
// the safety state machine and atomic, versioned reconfiguration.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/clock"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/runtime"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/sensor"
)

func main() {
	cfgPath := flag.String("config", "configs/example.json", "path to JSON configuration")
	duration := flag.Duration("duration", 10*time.Second, "how long to run (0 = until interrupted)")
	faultAt := flag.Duration("fault-at", 0, "inject a fault burst at this elapsed time (0 disables)")
	faultBurst := flag.Int("fault-burst", 3, "number of consecutive fault samples in the burst")
	faultChannel := flag.String("fault-channel", "", "channel id that faults during the burst (default: all channels)")
	reloadAt := flag.Duration("reload-at", 0, "atomically reload a tweaked config at this elapsed time (0 disables)")
	flag.Parse()

	cfg, err := config.LoadFile(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	printf := func(format string, args ...any) { logger.Output(2, fmt.Sprintf(format, args...)) }

	clk := clock.NewReal()
	ids := make([]model.ChannelID, 0, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		ids = append(ids, model.ChannelID(ch.ID))
	}

	// Deterministic synthetic source; optionally inject a fault burst on one
	// channel (to reach the consecutive-fault threshold) or all channels.
	var faulting atomic.Bool
	target := model.ChannelID(*faultChannel)
	src := sensor.NewPatternSource(clk, ids, cfg.Period.Duration,
		sensor.WithFaultFunc(func(ch model.ChannelID, seq int64) bool {
			if !faulting.Load() {
				return false
			}
			return target == "" || ch == target
		}),
	)

	rt, err := runtime.New(runtime.Options{
		Clock: clk, Initial: cfg, Sources: []sensor.Sensor{src}, Log: printf,
	})
	if err != nil {
		log.Fatalf("build runtime: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	runErr := make(chan error, 1)
	go func() { runErr <- rt.Run(ctx) }()
	<-rt.Ready()
	printf("rtrctl: started version=%d period=%s channels=%d",
		cfg.Version, cfg.Period.Duration, len(ids))

	// Demo timers for fault injection and live reload (real wall clock).
	faultTimer := maybeTimer(*faultAt)
	reloadTimer := maybeTimer(*reloadAt)
	burstLeft := *faultBurst

	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		for d := range rt.Decisions() {
			printf("decision t=%s state=%s output=%.3f v%d reasons=%v values=%v",
				d.Time.Format("15:04:05.000"), d.State, d.Output, d.Version, d.Reasons, d.Values)
		}
	}()

	for {
		select {
		case <-faultTimer:
			which := *faultChannel
			if which == "" {
				which = "<all channels>"
			}
			printf("rtrctl: injecting fault burst of %d on %s", burstLeft, which)
			faulting.Store(true)
			go func() {
				// Clear the burst after enough time for N periods; the
				// pattern source faults while the flag is set.
				// Keep faulting long enough for N samples of the (round-robin)
				// target channel to pass: N periods times the channel count.
				time.Sleep(cfg.Period.Duration * time.Duration(burstLeft) * time.Duration(len(ids)))
				faulting.Store(false)
				printf("rtrctl: fault burst cleared")
			}()
		case <-reloadTimer:
			cur, v := reloadConfig(ctx, rt, *cfgPath, printf)
			if cur != nil {
				cfg = cur
				_ = v
			}
		case <-ctx.Done():
			printf("rtrctl: shutting down")
			src.Close()
			<-runErr
			<-observerDone
			return
		case err := <-runErr:
			if err != nil && ctx.Err() == nil {
				log.Fatalf("runtime: %v", err)
			}
			<-observerDone
			return
		}
	}
}

func maybeTimer(at time.Duration) <-chan time.Time {
	if at <= 0 {
		return nil
	}
	return time.After(at)
}

// reloadConfig re-reads the file, bumps the version monotonically and applies
// it atomically; an invalid or stale file is rejected with old behavior kept.
func reloadConfig(ctx context.Context, rt runtime.Runtime, path string, printf func(string, ...any)) (*config.Config, int64) {
	c, err := config.LoadFile(path)
	if err != nil {
		printf("rtrctl: reload rejected (invalid file, old behavior kept): %v", err)
		return nil, 0
	}
	v, err := rt.Reload(ctx, c)
	if err != nil {
		printf("rtrctl: reload rejected: %v", err)
		return nil, 0
	}
	printf("rtrctl: live reload applied version=%d period=%s", c.Version, c.Period.Duration)
	return c, v
}
