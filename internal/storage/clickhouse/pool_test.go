package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultConfigKeepsAWarmPoolUnderTheCloudIdleCutoff(t *testing.T) {
	t.Setenv(ConnMaxLifetimeEnv, "")
	config := DefaultConfig("clickhouse://u:p@ch.example.invalid:9440/db?secure=true")
	if config.MaxIdleConns < config.MaxOpenConns {
		t.Fatalf("idle %d < open %d: a burst redials (TLS handshake per call at 20 ms RTT)", config.MaxIdleConns, config.MaxOpenConns)
	}
	if config.ConnMaxLifetime >= 15*time.Minute {
		t.Fatalf("lifetime %v is not under the 15 minute Cloud idle scaling", config.ConnMaxLifetime)
	}
	if config.DialTimeout < 10*time.Second {
		t.Fatalf("dial timeout %v too small for a TCP+TLS handshake at 20 ms RTT", config.DialTimeout)
	}
}

func TestConnMaxLifetimeEnvOverrides(t *testing.T) {
	for value, want := range map[string]time.Duration{"90s": 90 * time.Second, "": DefaultConnMaxLifetime, "junk": DefaultConnMaxLifetime, "-1s": DefaultConnMaxLifetime} {
		t.Setenv(ConnMaxLifetimeEnv, value)
		if got := PoolConnMaxLifetime(); got != want {
			t.Errorf("env %q: got %v, want %v", value, got, want)
		}
	}
}

func TestHTTPTransportKeepsTheIdlePool(t *testing.T) {
	config := DefaultConfig("http://ch.example.invalid:8123/default")
	options, err := clickHouseOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{}
	if _, err := options.TransportFunc(transport); err != nil {
		t.Fatal(err)
	}
	if transport.MaxIdleConns != config.MaxIdleConns || transport.MaxIdleConnsPerHost != config.MaxIdleConns {
		t.Fatalf("transport idle = %d/%d, want %d", transport.MaxIdleConns, transport.MaxIdleConnsPerHost, config.MaxIdleConns)
	}
}

func TestWarmPoolPingsEveryIdleSlotAndSwallowsErrors(t *testing.T) {
	var pings atomic.Int32
	WarmPool(context.Background(), func(context.Context) error {
		pings.Add(1)
		return errors.New("down")
	}, 4)
	if pings.Load() != 4 {
		t.Fatalf("pings = %d, want 4", pings.Load())
	}
	WarmPool(context.Background(), func(context.Context) error { t.Error("pinged with idle 0"); return nil }, 0)
}

func TestWarmPoolIsBounded(t *testing.T) {
	start := time.Now()
	WarmPool(context.Background(), func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, 2)
	if elapsed := time.Since(start); elapsed > WarmBudget+2*time.Second {
		t.Fatalf("warm-up took %v, budget %v", elapsed, WarmBudget)
	}
}

func TestDialTimeoutEnvOverrides(t *testing.T) {
	for value, want := range map[string]time.Duration{"3s": 3 * time.Second, "": DefaultDialTimeout, "junk": DefaultDialTimeout, "0s": DefaultDialTimeout} {
		t.Setenv(DialTimeoutEnv, value)
		if got := PoolDialTimeout(); got != want {
			t.Errorf("env %q: got %v, want %v", value, got, want)
		}
	}
}

func TestAsyncInsertDefaultsToOffAndADSNSettingWins(t *testing.T) {
	options, err := clickHouseOptions(DefaultConfig("clickhouse://u:p@ch.example.invalid:9440/db"))
	if err != nil {
		t.Fatal(err)
	}
	if got := options.Settings[AsyncInsertSetting]; got != 0 {
		t.Fatalf("async_insert = %v, want 0", got)
	}
	options, err = clickHouseOptions(DefaultConfig("clickhouse://u:p@ch.example.invalid:9440/db?async_insert=1"))
	if err != nil {
		t.Fatal(err)
	}
	if got := options.Settings[AsyncInsertSetting]; got == 0 {
		t.Fatalf("an explicit DSN async_insert was overwritten: %v", got)
	}
}

func TestWithAsyncInsertOff(t *testing.T) {
	for in, want := range map[string]string{
		"clickhouse://h:9000/db":                "clickhouse://h:9000/db?async_insert=0",
		"clickhouse://h:9000/db?secure=true":    "clickhouse://h:9000/db?secure=true&async_insert=0",
		"clickhouse://h:9000/db?async_insert=1": "clickhouse://h:9000/db?async_insert=1",
	} {
		if got := WithAsyncInsertOff(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}
