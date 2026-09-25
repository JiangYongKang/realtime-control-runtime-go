// Package sensor provides injectable sample sources so the runtime never
// depends on a real device.
package sensor

import (
	"context"
	"errors"

	"github.com/highcumontoa/realtime-control-runtime-go/internal/model"
)

// ErrReadFault is carried by samples whose source read failed.
var ErrReadFault = errors.New("sensor: read fault")

// Sensor is an injectable data source for one or more channels.
type Sensor interface {
	// Read returns the next sample. It must honor ctx cancellation.
	Read(ctx context.Context) (model.Sample, error)
	// Channels lists the channel identifiers this source produces.
	Channels() []model.ChannelID
	// Close releases any resources held by the source.
	Close() error
}
