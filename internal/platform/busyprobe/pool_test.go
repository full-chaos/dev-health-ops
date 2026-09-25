package busyprobe

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

func saturated(pool *pgxpool.Pool) bool {
	stat := pool.Stat()
	return stat.MaxConns() > 0 && stat.AcquiredConns() >= stat.MaxConns()
}

func newPool(t *testing.T, server *poolpg.Server, maxConns int32) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://role:secret@" + server.Addr() + "/db?sslmode=disable")
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

// Real pgxpool connections against the fake server: the opener turns a real
// acquire timeout on a fully acquired pool into a busy pass, and the probe
// returns its connection to the pool.
func TestOpenerOnARealSaturatedPool(t *testing.T) {
	pool := newPool(t, poolpg.Start(t), 2)
	probe := selfprobe.NewPool(pool)
	if err := selfprobe.Once(context.Background(), probe); err != nil {
		t.Fatalf("probe on an idle pool: %v", err)
	}
	if acquired := pool.Stat().AcquiredConns(); acquired != 0 {
		t.Fatalf("the probe left %d connections acquired after its rollback", acquired)
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
	counter := NewCounter("probe_busy_total", []string{"execution_liveness"}, nil)
	opener := Opener{
		Inner: probe, Check: "execution_liveness",
		Saturated: func() bool { return saturated(pool) },
		Progress:  func(context.Context) error { return nil }, Counter: counter,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	if err := selfprobe.Once(ctx, opener); err != nil {
		t.Fatalf("probe on a saturated pool with progress = %v, want a busy pass", err)
	}
	if elapsed := time.Since(started); elapsed > AcquireWait+time.Second {
		t.Fatalf("the busy pass took %s, want about %s", elapsed, AcquireWait)
	}
	opener.Progress = func(context.Context) error { return context.Canceled }
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	if err := selfprobe.Once(ctx2, opener); err == nil {
		t.Fatal("a saturated pool with no progress passed as busy")
	}
}

// r1 P1 (executed by the reviewer): a database that accepts the connection and
// never answers BEGIN made the probe hold the pool's only connection (so the pool
// looked saturated), time out, and read as BUSY. A deadline on the BEGIN itself is
// not an acquire timeout: it must stay a failure.
func TestAStalledBeginOnASaturatedPoolIsNotBusy(t *testing.T) {
	server := poolpg.Start(t)
	server.StallBegin(true)
	pool := newPool(t, server, 1)
	counter := NewCounter("probe_busy_total", []string{"execution_liveness"}, nil)
	opener := Opener{
		Inner: selfprobe.NewPool(pool), Check: "execution_liveness",
		Saturated: func() bool { return saturated(pool) },
		Progress:  func(context.Context) error { return nil }, Counter: counter,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := selfprobe.Once(ctx, opener); err == nil {
		t.Fatal("a probe whose BEGIN never answered passed as busy")
	}
	var metrics bytes.Buffer
	_ = counter.WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `probe_busy_total{check="execution_liveness"} 0`) {
		t.Fatalf("a stalled BEGIN was counted as a busy pass: %q", metrics.String())
	}
}
