// Package runtime wires sampling, control, configuration and fault handling
// together and owns their goroutine lifecycles.
package runtime

import (
	"context"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/config"
	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// Runtime is the top-level runnable system.
type Runtime interface {
	// Run starts all components and blocks until ctx is cancelled. All
	// goroutines are stopped before Run returns.
	Run(ctx context.Context) error
	// Ready is closed once the scheduler has armed its ticker. With a
	// controllable clock the caller must wait for Ready before advancing
	// time, otherwise the first tick could be missed.
	Ready() <-chan struct{}
	// Reload validates and atomically applies a new configuration. Invalid
	// or stale (non-monotonic version) candidates are rejected and the old
	// behavior stays in effect.
	Reload(ctx context.Context, c *config.Config) (int64, error)
	// Decisions exposes the bounded stream of control decisions.
	Decisions() <-chan model.Decision
}
