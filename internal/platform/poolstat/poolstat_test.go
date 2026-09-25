package poolstat

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
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

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func hold(t *testing.T, pool *pgxpool.Pool, count int) {
	t.Helper()
	var held []*pgxpool.Conn
	for i := 0; i < count; i++ {
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
}

// Saturation is LOUD but rate-limited, and says readiness does not depend on it.
func TestSaturationLogIsLoudAndRateLimited(t *testing.T) {
	pool := newPool(t, 1)
	hold(t, pool, 1)
	var logs bytes.Buffer
	source := New("svc_database_pool_saturation_ratio", pool, slog.New(slog.NewTextHandler(&logs, nil)))
	clock := time.Unix(1_700_000_000, 0)
	source.SetClock(func() time.Time { return clock })
	for i := 0; i < 4; i++ {
		source.Sample()
	}
	if got := strings.Count(logs.String(), "readiness does not depend on it"); got != 1 {
		t.Fatalf("%d log lines within the interval, want 1: %q", got, logs.String())
	}
	clock = clock.Add(LogInterval + time.Second)
	source.Sample()
	if got := strings.Count(logs.String(), "readiness does not depend on it"); got != 2 {
		t.Fatalf("%d log lines after the interval, want 2", got)
	}
	// An idle pool logs nothing.
	var quiet bytes.Buffer
	New("svc_database_pool_saturation_ratio", newPool(t, 2), slog.New(slog.NewTextHandler(&quiet, nil))).Sample()
	if quiet.Len() != 0 {
		t.Fatalf("an idle pool logged: %q", quiet.String())
	}
}

// r4 P1: the warning must not depend on a metrics scrape. A running Source logs
// on its own ticker; scraping never logs.
func TestSaturationWarningNeedsNoMetricsScrape(t *testing.T) {
	pool := newPool(t, 1)
	hold(t, pool, 1)
	var logs syncBuffer
	source := New("svc_database_pool_saturation_ratio", pool, slog.New(slog.NewTextHandler(&logs, nil)))
	source.SetSampleInterval(10 * time.Millisecond)
	var clockMu sync.Mutex
	clock := time.Unix(1_700_000_000, 0)
	source.SetClock(func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock })
	if err := source.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForLines := func(want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for strings.Count(logs.String(), "readiness does not depend on it") < want {
			if time.Now().After(deadline) {
				t.Fatalf("expected %d saturation warnings without any metrics scrape, got %d", want, strings.Count(logs.String(), "readiness does not depend on it"))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitForLines(1)
	// The ticker keeps sampling: once the rate limit allows, the next tick logs again.
	clockMu.Lock()
	clock = clock.Add(LogInterval + time.Second)
	clockMu.Unlock()
	waitForLines(2)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := source.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	// Scraping is read-only: it never logs.
	before := strings.Count(logs.String(), "readiness does not depend on it")
	source.SetClock(func() time.Time { return time.Now().Add(time.Hour) }) // the rate limit would allow a line
	scrape(t, source)
	if after := strings.Count(logs.String(), "readiness does not depend on it"); after != before {
		t.Fatalf("a scrape logged (%d -> %d lines)", before, after)
	}
	// A source that was never started shuts down cleanly.
	if err := New("m", pool, nil).Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
