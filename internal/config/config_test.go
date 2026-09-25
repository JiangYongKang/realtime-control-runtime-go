package config

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func baseConfig() *Config {
	return &Config{
		Version: 1,
		Channels: []ChannelCfg{
			{
				ID:       "c1",
				Period:   10 * time.Millisecond,
				MaxLag:   20 * time.Millisecond,
				MinValue: 0,
				MaxValue: 10,
			},
		},
		Threshold: ThresholdCfg{WarnLow: 1, WarnHigh: 9},
		Safety: SafetyCfg{
			ControlPeriod:     10 * time.Millisecond,
			DecisionDeadline:  50 * time.Millisecond,
			MaxConsecutiveErr: 2,
			RecoverGoodTicks:  3,
		},
	}
}

func TestValidateRejectsBadConfigs(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
		want error
	}{
		{"no channels", func(c *Config) { c.Channels = nil }, ErrNoChannels},
		{"duplicate id", func(c *Config) {
			c.Channels = append(c.Channels, c.Channels[0])
		}, ErrDuplicateID},
		{"bad period", func(c *Config) { c.Channels[0].Period = 0 }, ErrBadPeriod},
		{"negative lag", func(c *Config) { c.Channels[0].MaxLag = -1 }, ErrNegativeLag},
		{"inverted value range", func(c *Config) {
			c.Channels[0].MinValue = 11
		}, ErrBadValueRange},
		{"inverted thresholds", func(c *Config) {
			c.Threshold.WarnHigh = 0
		}, ErrBadThresholds},
		{"zero deadline", func(c *Config) {
			c.Safety.DecisionDeadline = 0
		}, ErrBadDeadline},
		{"zero control period", func(c *Config) {
			c.Safety.ControlPeriod = 0
		}, ErrBadDeadline},
		{"zero err count", func(c *Config) {
			c.Safety.MaxConsecutiveErr = 0
		}, ErrBadErrorCount},
		{"zero recover ticks", func(c *Config) {
			c.Safety.RecoverGoodTicks = 0
		}, ErrBadRecoverTicks},
		{"flap risk recover<=errs", func(c *Config) {
			c.Safety.RecoverGoodTicks = 2
		}, ErrFlapRisk},
		{"bad sim period", func(c *Config) {
			c.Channels[0].Sim.Amplitude = 1
		}, ErrBadSim},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.mut(c)
			if err := Validate(c); !errors.Is(err, tc.want) {
				t.Fatalf("Validate err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestManagerUpdateVersionMustIncrease(t *testing.T) {
	m, err := NewManager(baseConfig())
	if err != nil {
		t.Fatal(err)
	}
	same := baseConfig()
	same.Version = 1
	if _, err := m.Update(same); !errors.Is(err, ErrVersionRewind) {
		t.Fatalf("same version err = %v, want ErrVersionRewind", err)
	}
	older := baseConfig()
	older.Version = 0
	if _, err := m.Update(older); !errors.Is(err, ErrVersionRewind) {
		t.Fatalf("older version err = %v", err)
	}
	if v := m.Current().Version; v != 1 {
		t.Fatalf("active version changed to %d after rejected updates", v)
	}
}

func TestManagerRejectsInvalidAndKeepsOld(t *testing.T) {
	m, _ := NewManager(baseConfig())
	bad := baseConfig()
	bad.Version = 2
	bad.Safety.DecisionDeadline = -1
	if _, err := m.Update(bad); !errors.Is(err, ErrBadDeadline) {
		t.Fatalf("Update err = %v", err)
	}
	if v := m.Current().Version; v != 1 {
		t.Fatalf("rejected update installed version %d", v)
	}
	good := baseConfig()
	good.Version = 3
	good.Channels[0].Period = 25 * time.Millisecond
	got, err := m.Update(good)
	if err != nil {
		t.Fatalf("valid update: %v", err)
	}
	if got.Version != 3 || got.Channels[0].Period != 25*time.Millisecond {
		t.Fatalf("installed config unexpected: %+v", got)
	}
}

func TestManagerSnapshotIsolation(t *testing.T) {
	m, _ := NewManager(baseConfig())
	snap := m.Current()
	snap.Channels[0].ID = "mutated-by-caller"
	if m.Current().Channels[0].ID != "c1" {
		t.Fatal("caller mutation leaked into the active snapshot")
	}
}

func TestConcurrentReadersNeverSeePartialState(t *testing.T) {
	m, _ := NewManager(baseConfig())
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c := m.Current()
					// Every observed snapshot must be self-consistent.
					if err := Validate(c); err != nil {
						t.Errorf("observed invalid snapshot: %v", err)
						return
					}
					if c.Version < 1 || c.Safety.RecoverGoodTicks <= c.Safety.MaxConsecutiveErr {
						t.Errorf("observed partial snapshot: %+v", c.Safety)
						return
					}
				}
			}
		}()
	}
	for v := int64(2); v <= 20; v++ {
		c := baseConfig()
		c.Version = v
		c.Channels[0].Period = time.Duration(10+v) * time.Millisecond
		c.Threshold.WarnHigh = 9 + float64(v)
		if _, err := m.Update(c); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestJSONRoundTrip(t *testing.T) {
	c := baseConfig()
	c.Channels[0].Sim = SimSpec{Base: 2, Amplitude: 0.5, PeriodS: 3, FailEveryN: 7}
	var buf bytes.Buffer
	if err := EncodeJSON(&buf, c); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"period": "10ms"`) {
		t.Fatalf("duration not string encoded: %s", buf.String())
	}
	decoded, err := DecodeJSON(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version != 1 || decoded.Channels[0].Period != 10*time.Millisecond {
		t.Fatalf("decoded mismatch: %+v", decoded)
	}
	if decoded.Channels[0].Sim.FailEveryN != 7 {
		t.Fatalf("sim spec lost: %+v", decoded.Channels[0].Sim)
	}
}

func TestJSONRejectsUnknownField(t *testing.T) {
	_, err := DecodeJSON(bytes.NewBufferString(`{"version":1,"bogus":1,"channels":[]}`))
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}
