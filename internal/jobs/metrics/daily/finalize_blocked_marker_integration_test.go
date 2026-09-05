//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-4290, #2241 r3 Finding 2.
//
// The finalize policy says a deterministically failing native family exhausts
// its River attempts and is then RECORDED by CHAOS-5040's blocked marker. That
// was false: reconcileBlockedRunsSQL fired only on a failed_permanent
// PARTITION, and a stranded finalize has none -- every partition succeeded and
// only the cross-partition step failed. blocked_at stayed NULL, so the run was
// invisible to the sweep built to surface wedged runs.
//
// This asserts the run REACHES blocked_at, not merely that it retries. Those
// are different claims and only the second one rules out a silent strand: a run
// that retries forever and a run that stops unmarked look identical to an
// operator, because neither produces a row anything reports on.
func TestAFailedFinalizeReachesTheBlockedMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		orgID       = "00000000-0000-4000-8000-000000000041"
		strandedRun = "00000000-0000-4000-8000-000000000042"
		healthyRun  = "00000000-0000-4000-8000-000000000043"
	)
	now := time.Date(2026, 9, 4, 18, 0, 0, 0, time.UTC)

	// The stranded shape: finalization terminally failed, EVERY partition
	// succeeded. This is precisely the shape the old predicate could not see.
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,'2026-09-04','daily-v1','running','failed',$3,$3)`,
		strandedRun, orgID, now); err != nil {
		t.Fatal(err)
	}
	// The CONTROL run: same org, finalization still pending, partitions
	// succeeded. It must NOT be marked. Without it a predicate that marked
	// every running run would pass this test.
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,'2026-09-03','daily-v1','running','pending',$3,$3)`,
		healthyRun, orgID, now); err != nil {
		t.Fatal(err)
	}
	for index, runID := range []string{strandedRun, healthyRun} {
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions
            (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at)
            VALUES ($1,$2,0,'[]'::jsonb,'succeeded',1,$3,$3)`,
			"00000000-0000-4000-8000-00000000005"+string(rune('0'+index)), runID, now); err != nil {
			t.Fatal(err)
		}
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	outcome, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Marked != 1 {
		t.Fatalf("marked %d run(s), want exactly 1 -- the stranded finalize, and not "+
			"the healthy run alongside it", outcome.Marked)
	}

	var blockedAt *time.Time
	var reason *string
	if err := pool.QueryRow(ctx,
		`SELECT blocked_at, blocked_reason FROM daily_metrics_runs WHERE id = $1`,
		strandedRun).Scan(&blockedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if blockedAt == nil {
		t.Fatal("blocked_at is NULL for a run whose finalization terminally failed with " +
			"every partition succeeded -- it is stranded and invisible to the sweep")
	}
	if reason == nil || *reason != BlockedReasonFinalizeFailed {
		t.Fatalf("blocked_reason = %v, want %q -- an operator needs to know the "+
			"partitions are fine and only the finalize needs re-running",
			reason, BlockedReasonFinalizeFailed)
	}

	// The control must be untouched.
	var healthyBlockedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT blocked_at FROM daily_metrics_runs WHERE id = $1`,
		healthyRun).Scan(&healthyBlockedAt); err != nil {
		t.Fatal(err)
	}
	if healthyBlockedAt != nil {
		t.Fatal("a run with finalization still pending was marked blocked -- the " +
			"predicate is marking healthy runs, which would make the signal useless")
	}
}
