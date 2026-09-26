//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/workersignals"
)

func lockWaitCount(t *testing.T, outcome string) uint64 {
	t.Helper()
	var scraped strings.Builder
	if err := workersignals.MetricsSource().WritePrometheus(&scraped); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("dev_health_sync_dispatch_advisory_lock_wait_total{outcome=%q} ", outcome)
	for _, line := range strings.Split(scraped.String(), "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			count, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return count
		}
	}
	t.Fatalf("no %s sample", prefix)
	return 0
}

// CHAOS-6920: the bounded advisory-lock wait is a counter by outcome on /metrics: a pass that
// takes its locks counts "acquired", one that gives the connection back counts "busy". Before
// this, "lock_busy_requeued" existed only as a log line and the series was absent from a scrape
// (the rev 189 prod sampler could not tell a clean window from "not exposed").
func TestAdvisoryLockWaitOutcomesAreCounted(t *testing.T) {
	withSmallDispatchPool(t, 4, func(ctx context.Context, pool *pgxpool.Pool) {
		keys := []int64{424242}
		acquiredBefore, busyBefore := lockWaitCount(t, workersignals.LockOutcomeAcquired), lockWaitCount(t, workersignals.LockOutcomeBusy)

		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, keys[0]); err != nil {
			t.Fatal(err)
		}

		waiter, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = waiter.Rollback(ctx) }()
		if err := acquireAdvisoryLocksBounded(ctx, waiter, keys, "test", 300*time.Millisecond); !isDispatchLockBusy(err) {
			t.Fatalf("acquire with the lock held = %v, want ErrDispatchLockBusy", err)
		}
		if got := lockWaitCount(t, workersignals.LockOutcomeBusy); got != busyBefore+1 {
			t.Fatalf("busy = %d, want %d", got, busyBefore+1)
		}
		if got := lockWaitCount(t, workersignals.LockOutcomeAcquired); got != acquiredBefore {
			t.Fatalf("acquired moved to %d on a busy miss, want %d", got, acquiredBefore)
		}

		if err := holder.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err := acquireAdvisoryLocksBounded(ctx, waiter, keys, "test", 300*time.Millisecond); err != nil {
			t.Fatalf("acquire after the holder released = %v", err)
		}
		if got := lockWaitCount(t, workersignals.LockOutcomeAcquired); got != acquiredBefore+1 {
			t.Fatalf("acquired = %d, want %d", got, acquiredBefore+1)
		}
	})
}
