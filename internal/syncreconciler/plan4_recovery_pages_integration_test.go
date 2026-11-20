//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-4 C4: N buckets get a signal at the same instant that no sweep acknowledges (each lands after its sweep's
// acknowledgement). One materializer tick at +60 s (Step limit 100, the production page) must enqueue every bucket's
// sweep: 101 and 1,000 buckets, each within one tick plus 60 s.
func TestP4EveryUnacknowledgedSignalIsRecoveredWithinOneTick(t *testing.T) {
	for _, buckets := range []int{101, 1000} {
		t.Run(fmt.Sprint(buckets), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			instance := pgschema.Start(ctx, t)
			defer instance.Close(context.Background())
			pool, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			sweeps := &recordingBucketSweeps{}
			materializer, err := NewMaterializer(pool)
			if err != nil {
				t.Fatal(err)
			}
			materializer.WithBucketSweeps(sweeps)
			signaledAt := time.Date(2026, time.November, 20, 6, 0, 0, 0, time.UTC)
			for i := 0; i < buckets; i++ {
				if err := syncrunrollup.BumpBucketGeneration(ctx, pool, fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x142600+i), "github", "medium", signaledAt); err != nil {
					t.Fatal(err)
				}
			}
			tick := signaledAt.Add(60 * time.Second)
			if _, err := materializer.Step(ctx, tick, tick.Add(-15*time.Minute), 100); err != nil {
				t.Fatal(err)
			}
			distinct := map[string]bool{}
			for _, key := range sweeps.enqueued {
				distinct[key] = true
			}
			t.Logf("%d buckets signalled at T, unacknowledged: one tick at T+60s enqueued %d sweeps (%d distinct buckets)", buckets, len(sweeps.enqueued), len(distinct))
			if len(distinct) != buckets {
				t.Fatalf("one tick at T+60s recovered %d of %d unacknowledged buckets", len(distinct), buckets)
			}
		})
	}
}

// C4 cap: 10,001 unacknowledged buckets: one tick recovers 10,000 (bucketSignalRecoveryPerTick) and carries its
// cursor; the next tick recovers the last one.
func TestB4SignalRecoveryCarriesItsCursorPastTheTickCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sweeps := &recordingBucketSweeps{}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	materializer.WithBucketSweeps(sweeps)
	signaledAt := time.Date(2026, time.November, 20, 6, 0, 0, 0, time.UTC)
	const buckets = bucketSignalRecoveryPerTick + 1
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_dispatch_bucket_signals (org_id, provider, cost_class, generation, acked_generation, updated_at)
SELECT '00000000-0000-4000-8000-' || lpad(to_hex(1442000 + n), 12, '0'), 'github', 'medium', 1, 0, $2
FROM generate_series(1, $1::int) AS n`, buckets, signaledAt); err != nil {
		t.Fatal(err)
	}
	tick := signaledAt.Add(60 * time.Second)
	first, err := materializer.Step(ctx, tick, tick.Add(-15*time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	second, err := materializer.Step(ctx, tick.Add(time.Second), tick.Add(time.Second-15*time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	distinct := map[string]bool{}
	for _, key := range sweeps.enqueued {
		distinct[key] = true
	}
	t.Logf("%d buckets: tick 1 recovered %d, tick 2 recovered %d, %d distinct", buckets, first.BucketSweepsRecovered, second.BucketSweepsRecovered, len(distinct))
	if first.BucketSweepsRecovered != bucketSignalRecoveryPerTick || len(distinct) != buckets {
		t.Fatalf("tick 1 recovered %d (want %d), %d distinct buckets after tick 2 (want %d)",
			first.BucketSweepsRecovered, bucketSignalRecoveryPerTick, len(distinct), buckets)
	}
}

// N1 on the reconciler writer (CHAOS-14250 plan 4): lease repair frees an expired lease's slot and, in its own
// transaction under the bucket lock it took first, reserves the slot for the bucket's oldest eligible waiter.
func TestB4LeaseRepairReservesTheFreedSlotForTheOldestWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	now := time.Date(2026, time.November, 20, 6, 0, 0, 0, time.UTC)
	const org = "00000000-0000-4000-8000-000000142701"
	holder, waiter := "00000000-0000-4000-9150-000000000001", "00000000-0000-4000-9150-000000000002"
	seedLeaseRepairRun(t, ctx, pool, holder, org, "running")
	seedLeaseRepairRun(t, ctx, pool, waiter, org, "running")
	seedLeaseRepairUnit(t, ctx, pool, "00000000-0000-4000-9151-000000000001", holder, org, "github", "incremental", "commits", 0, now.Add(-time.Minute))
	seedLeaseRepairUnit(t, ctx, pool, "00000000-0000-4000-9151-000000000002", waiter, org, "github", "incremental", "commits", 0, now.Add(time.Hour))
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE public.sync_runs SET created_at = $2 WHERE id = $1::uuid`, []any{waiter, now.Add(-48 * time.Hour)}},
		{`UPDATE public.sync_run_units SET status = 'planned', lease_owner = NULL, lease_expires_at = NULL WHERE sync_run_id = $1::uuid`, []any{waiter}},
		{`INSERT INTO public.sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
		  VALUES ('00000000-0000-4000-9152-000000000002', $1, $2, 'success', 1, $3, $3, $3)`, []any{waiter, org, now}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	repair, err := NewLeaseRepair(pool)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repair.Step(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	var reserved int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_bucket_reservations WHERE sync_run_id = $1::uuid`, waiter).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	t.Logf("lease repair selected %d, retried %d; the waiting run holds %d reservation(s)", result.Selected, result.Retried, reserved)
	if result.Selected != 1 || reserved != 1 {
		t.Fatalf("lease repair freed %d slot(s) and reserved %d for the oldest waiter, want 1 and 1", result.Selected, reserved)
	}
}
