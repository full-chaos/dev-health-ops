package busyprobe

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// Real pgxpool connections against the fake server: Saturated and PoolProgress
// read the pool's real Stat(), and the opener turns a real acquire timeout on a
// fully acquired pool into a busy pass.
func TestOpenerOnARealSaturatedPool(t *testing.T) {
	addr := fakepg.Serve(t)
	config, err := pgxpool.ParseConfig("postgres://role:secret@" + addr + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if Saturated(pool) {
		t.Fatal("an idle pool is saturated")
	}
	// The pool works: a probe transaction opens and rolls back, and the probe
	// gives its connection back (a leak here would saturate the pool for real).
	if err := selfprobe.Once(context.Background(), selfprobe.NewPool(pool)); err != nil {
		t.Fatalf("probe on an idle pool: %v", err)
	}
	if acquired := pool.Stat().AcquiredConns(); acquired != 0 {
		t.Fatalf("the probe left %d connections acquired after its rollback", acquired)
	}

	// Two "jobs" hold every connection.
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
	if !Saturated(pool) {
		t.Fatal("a pool with every connection acquired is not saturated")
	}

	counter := NewCounter("probe_busy_total", []string{"execution_liveness"}, nil)
	probe := selfprobe.NewPool(pool)
	progress := NewPoolProgress(pool, time.Minute, func() int64 { return selfprobe.OwnAcquires(probe) })
	opener := Opener{
		Inner: probe, Check: "execution_liveness",
		Saturated: func() bool { return Saturated(pool) }, Progress: progress.Ready, Counter: counter,
	}
	// Inside a caller budget, like the registry's: the busy pass arrives in time.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	if err := selfprobe.Once(ctx, opener); err != nil {
		t.Fatalf("probe on a saturated pool with progress = %v, want a busy pass", err)
	}
	if elapsed := time.Since(started); elapsed > AcquireWait+time.Second {
		t.Fatalf("the busy pass took %s, want about %s", elapsed, AcquireWait)
	}
	// A wedged pool (no acquire completes within the window) is broken.
	wedged := NewPoolProgress(pool, time.Nanosecond, func() int64 { return selfprobe.OwnAcquires(probe) })
	time.Sleep(5 * time.Millisecond)
	if err := wedged.Ready(context.Background()); err != nil {
		t.Fatalf("first observation moves the count: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := wedged.Ready(context.Background()); err == nil {
		t.Fatal("a pool whose acquire count stopped advancing is green")
	}
	opener.Progress = wedged.Ready
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
	server := fakepg.Start(t)
	server.StallBegin(true)
	config, err := pgxpool.ParseConfig("postgres://role:secret@" + server.Addr() + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	probe := selfprobe.NewPool(pool)
	counter := NewCounter("probe_busy_total", []string{"execution_liveness"}, nil)
	opener := Opener{
		Inner: probe, Check: "execution_liveness",
		Saturated: func() bool { return Saturated(pool) },
		Progress:  NewPoolProgress(pool, time.Minute, func() int64 { return selfprobe.OwnAcquires(probe) }).Ready,
		Counter:   counter,
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

// The probe's own acquires are not evidence that the pool's WORK is moving: a
// pool wedged by stuck work must go red even while probes keep acquiring.
func TestOwnAcquiresDoNotCountAsProgress(t *testing.T) {
	addr := fakepg.Serve(t)
	config, err := pgxpool.ParseConfig("postgres://role:secret@" + addr + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	probe := selfprobe.NewPool(pool)
	withOwn := NewPoolProgress(pool, 50*time.Millisecond, func() int64 { return selfprobe.OwnAcquires(probe) })
	withoutOwn := NewPoolProgress(pool, 50*time.Millisecond, nil)
	if err := withOwn.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = withoutOwn.Ready(context.Background())
	time.Sleep(80 * time.Millisecond)
	// Only the probe acquires: the pool's count advances, the work's does not.
	for i := 0; i < 3; i++ {
		if err := selfprobe.Once(context.Background(), probe); err != nil {
			t.Fatal(err)
		}
	}
	if err := withOwn.Ready(context.Background()); err == nil {
		t.Fatal("probe-only acquires read as the work progressing")
	}
	if err := withoutOwn.Ready(context.Background()); err != nil {
		t.Fatalf("control: counting the probe's own acquires was expected to (wrongly) look like progress: %v", err)
	}
	if got := selfprobe.OwnAcquires(probe); got != 3 {
		t.Fatalf("OwnAcquires = %d, want 3", got)
	}
}
