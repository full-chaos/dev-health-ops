package poolstat

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

func newPool(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://role:secret@" + poolpg.Serve(t) + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func scrape(t *testing.T, source *Source) string {
	t.Helper()
	var out bytes.Buffer
	if err := source.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestGaugeTracksTheWorkPoolAndNeverPanicsOnAnUnusablePool(t *testing.T) {
	pool := newPool(t, 2)
	source := New("svc_database_pool_saturation_ratio", pool, nil)
	if text := scrape(t, source); !strings.Contains(text, `svc_database_pool_saturation_ratio{pool="domain"} 0`) ||
		!strings.Contains(text, `svc_database_pool_saturation_ratio_max_conns{pool="domain"} 2`) {
		t.Fatalf("idle scrape = %q", text)
	}
	var held []*pgxpool.Conn
	for i := 0; i < 2; i++ {
		connection, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, connection)
	}
	t.Cleanup(func() {
		for _, connection := range held {
			connection.Release()
		}
	})
	if text := scrape(t, source); !strings.Contains(text, `svc_database_pool_saturation_ratio{pool="domain"} 1`) ||
		!strings.Contains(text, `svc_database_pool_saturation_ratio_acquired_conns{pool="domain"} 2`) {
		t.Fatalf("saturated scrape = %q", text)
	}
	// nil and zero-value pools report zeros instead of panicking.
	for _, unusable := range []*pgxpool.Pool{nil, {}} {
		if text := scrape(t, New("svc_database_pool_saturation_ratio", unusable, nil)); !strings.Contains(text, `{pool="domain"} 0`) {
			t.Fatalf("unusable pool scrape = %q", text)
		}
	}
}

// Saturation is LOUD but rate-limited, and says readiness does not depend on it.
func TestSaturationLogIsLoudAndRateLimited(t *testing.T) {
	pool := newPool(t, 1)
	connection, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(connection.Release)
	var logs bytes.Buffer
	source := New("svc_database_pool_saturation_ratio", pool, slog.New(slog.NewTextHandler(&logs, nil)))
	clock := time.Unix(1_700_000_000, 0)
	source.SetClock(func() time.Time { return clock })
	for i := 0; i < 4; i++ {
		scrape(t, source)
	}
	if got := strings.Count(logs.String(), "readiness does not depend on it"); got != 1 {
		t.Fatalf("%d log lines within the interval, want 1: %q", got, logs.String())
	}
	clock = clock.Add(LogInterval + time.Second)
	scrape(t, source)
	if got := strings.Count(logs.String(), "readiness does not depend on it"); got != 2 {
		t.Fatalf("%d log lines after the interval, want 2", got)
	}
	// An idle pool logs nothing.
	var quiet bytes.Buffer
	idle := New("svc_database_pool_saturation_ratio", newPool(t, 2), slog.New(slog.NewTextHandler(&quiet, nil)))
	scrape(t, idle)
	if quiet.Len() != 0 {
		t.Fatalf("an idle pool logged: %q", quiet.String())
	}
}
