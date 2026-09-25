package config

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func baseConfig() *Config {
	return &Config{
		Version:   1,
		Period:    Duration{10 * time.Millisecond},
		Channels:  []Channel{{ID: "a", MinValue: -10, MaxValue: 10, Required: true}},
		Threshold: map[string]float64{"a": 5},
		Safety: Safety{
			DataTimeout:        Duration{50 * time.Millisecond},
			ConsecutiveFaults:  3,
			DecisionDeadline:   Duration{20 * time.Millisecond},
			RecoveryStableRuns: 2,
			SafeOutput:         -1,
		},
	}
}

func TestValidate(t *testing.T) {
	c := baseConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := *c
	bad.Version = 0
	if err := bad.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
	bad = *c
	bad.Channels = append(bad.Channels, Channel{ID: "a"})
	if err := bad.Validate(); err == nil {
		t.Fatal("duplicate channel must be rejected")
	}
	bad = *c
	bad.Threshold = map[string]float64{"zzz": 1}
	if err := bad.Validate(); err == nil {
		t.Fatal("unknown threshold channel must be rejected")
	}
}

func TestDurationJSON(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"250ms"`), &d); err != nil || d.Duration != 250*time.Millisecond {
		t.Fatalf("string parse: %v %v", d, err)
	}
	if err := json.Unmarshal([]byte(`1000`), &d); err != nil || d.Duration != 1000 {
		t.Fatalf("number parse: %v %v", d, err)
	}
	b, _ := json.Marshal(Duration{3 * time.Second})
	if string(b) != `"3s"` {
		t.Fatalf("marshal: %s", b)
	}
}

func TestStoreAtomicVersioned(t *testing.T) {
	st, err := NewStore(baseConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// stale version rejected, old config stays
	old, v0 := st.Current()
	if _, err := st.Update(ctx, baseConfig()); err == nil {
		t.Fatal("equal version must conflict")
	} else {
		var ce *ConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("want ConflictError, got %T %v", err, err)
		}
	}
	if cur, v := st.Current(); v != v0 || cur.Period != old.Period {
		t.Fatal("rejected update must not change state")
	}

	// invalid config rejected
	bad := baseConfig()
	bad.Version = 2
	bad.Period = Duration{-1}
	if _, err := st.Update(ctx, bad); err == nil {
		t.Fatal("invalid config must be rejected")
	}

	// valid monotonic update installs atomically
	good := baseConfig()
	good.Version = 2
	good.Period = Duration{20 * time.Millisecond}
	v2, err := st.Update(ctx, good)
	if err != nil || v2 != 2 {
		t.Fatalf("update: v=%d err=%v", v2, err)
	}
	cur, v := st.Current()
	if v != 2 || cur.Period.Duration != 20*time.Millisecond {
		t.Fatal("update not visible")
	}
	// mutation of the original must not affect the store
	good.Period = Duration{999 * time.Millisecond}
	good.Threshold["a"] = 999
	cur2, _ := st.Current()
	if cur2.Period.Duration == 999*time.Millisecond || cur2.Threshold["a"] == 999 {
		t.Fatal("store must defend its copy")
	}
}

func TestStoreSubscribeAndConcurrent(t *testing.T) {
	st, _ := NewStore(baseConfig())
	sub := st.Subscribe()
	// immediate delivery of active config
	select {
	case c := <-sub:
		if c.Version != 1 {
			t.Fatal("subscriber bootstrap wrong")
		}
	default:
		t.Fatal("subscriber must receive current config immediately")
	}

	const n = 50
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() { // readers
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c, v := st.Current()
				if c.Version != v {
					t.Errorf("torn read: cfg version %d vs store %d", c.Version, v)
					return
				}
			}
		}
	}()
	go func() { // slow subscriber must never block writes
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-sub:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	for i := int64(2); i <= n+1; i++ {
		c := baseConfig()
		c.Version = i
		c.Period = Duration{time.Duration(i) * time.Millisecond}
		if _, err := st.Update(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	st.Close()
}
