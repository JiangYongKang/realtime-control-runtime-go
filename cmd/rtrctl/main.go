// Command rtrctl is the local entry point for the realtime sampling and
// control runtime. It loads a JSON configuration, runs simulated sensor
// sources, logs every ingest outcome and control decision, and reloads
// configuration atomically on SIGHUP (the reloaded file must carry a
// strictly greater version number).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/control"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/runtime"
)

func main() {
	cfgPath := flag.String("config", "configs/config.example.json", "path to the JSON configuration file")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := log.New(os.Stdout, "rtr ", log.LstdFlags|log.Lmicroseconds)

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}

	rt, err := runtime.New(cfg, runtime.Options{
		Log: logger,
		DecisionDrain: func(d control.Decision) {
			if d.Previous != d.State {
				logger.Printf("STATE TRANSITION %s -> %s (%s)", d.Previous, d.State, d.Detail)
			}
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "start runtime:", err)
		os.Exit(1)
	}

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			next, err := loadConfig(*cfgPath)
			if err != nil {
				logger.Printf("reload ignored: %v (keeping version %d)", err, rt.CfgMgr.Current().Version)
				continue
			}
			if _, err := rt.Reload(next); err != nil {
				logger.Printf("reload rejected (%v); keeping version %d", err, rt.CfgMgr.Current().Version)
			}
		}
	}()

	logger.Printf("starting runtime with config version %d (%d channels)", cfg.Version, len(cfg.Channels))
	if err := rt.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "runtime:", err)
		os.Exit(1)
	}
	logger.Printf("runtime stopped")
}

func loadConfig(path string) (*config.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return config.DecodeJSON(f)
}
