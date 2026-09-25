package busyprobe

import (
	"context"
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
	// The pool works: a probe transaction opens and rolls back.
	if err := selfprobe.Once(context.Background(), selfprobe.NewPool(pool)); err != nil {
		t.Fatalf("probe on an idle pool: %v", err)
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
	progress := NewPoolProgress(pool, time.Minute)
	opener := Opener{
		Inner: selfprobe.NewPool(pool), Check: "execution_liveness",
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
	wedged := NewPoolProgress(pool, time.Nanosecond)
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
