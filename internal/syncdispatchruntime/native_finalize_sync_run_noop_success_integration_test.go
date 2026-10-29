//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	noDatasetJobRun   = "00000000-0000-4000-8000-0000000000f8"
	noDatasetBackfill = "00000000-0000-4000-8000-0000000000f9"
)

type canonicalConfigStamp struct {
	lastSyncAt      *time.Time
	lastSyncSuccess *bool
	lastSyncError   *string
	lastSyncStats   *string
}

type zeroUnitOutcome struct {
	status         string
	errorText      *string
	startedAt      *time.Time
	completedAt    *time.Time
	resultReason   *string
	backfillStatus string
	jobRunStatus   int
	before, after  canonicalConfigStamp
}

func readCanonicalConfigStamp(t *testing.T, ctx context.Context, pool *pgxpool.Pool) canonicalConfigStamp {
	t.Helper()
	var stamp canonicalConfigStamp
	if err := pool.QueryRow(ctx, `
SELECT last_sync_at, last_sync_success, last_sync_error, last_sync_stats::text
FROM sync_configurations WHERE id=$1`, finalizeTestSyncConfig).
		Scan(&stamp.lastSyncAt, &stamp.lastSyncSuccess, &stamp.lastSyncError, &stamp.lastSyncStats); err != nil {
		t.Fatal(err)
	}
	return stamp
}

// finalizeZeroUnitRun plans a zero-unit run with the given planner result, links a backfill job and an in-flight job
// run to it, finalizes it through the real NativeFinalizeSyncRunService on the real schema and reads the rows back.
func finalizeZeroUnitRun(t *testing.T, plannerResult string) zeroUnitOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	seedFinalizeRoute(t, ctx, pool)
	insertZeroUnitRun(t, ctx, pool, plannerResult, "")
	pgseed.ScheduledJob(ctx, t, pool, noDatasetJobRun, finalizeTestOrg)
	pgseed.JobRun(ctx, t, pool, noDatasetJobRun, noDatasetJobRun, jobRunStatusRunning, `{"sync_run_id":"`+finalizeTestRun+`"}`)
	if _, err := pool.Exec(ctx, `
INSERT INTO backfill_jobs (id, org_id, sync_config_id, celery_task_id, status, since_date, before_date)
VALUES ($1, $2, $3, $4, 'running', '2026-01-01', '2026-01-02')`,
		noDatasetBackfill, finalizeTestOrg, finalizeTestSyncConfig, "sync_run:"+finalizeTestRun); err != nil {
		t.Fatal(err)
	}

	// A real earlier sync stamped the configuration: a no-op run must leave it, not merely leave a NULL as NULL.
	if _, err := pool.Exec(ctx, `UPDATE sync_configurations
SET last_sync_at=$2, last_sync_success=TRUE, last_sync_error=NULL, last_sync_stats='{"units":7}'::json WHERE id=$1`,
		finalizeTestSyncConfig, time.Date(2026, time.October, 1, 6, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	var out zeroUnitOutcome
	out.before = readCanonicalConfigStamp(t, ctx, pool)
	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status, error, started_at, completed_at, result->>'reason' FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&out.status, &out.errorText, &out.startedAt, &out.completedAt, &out.resultReason); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM backfill_jobs WHERE id=$1`, noDatasetBackfill).Scan(&out.backfillStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM job_runs WHERE id=$1`, noDatasetJobRun).Scan(&out.jobRunStatus); err != nil {
		t.Fatal(err)
	}
	out.after = readCanonicalConfigStamp(t, ctx, pool)
	return out
}

func sameStamp(a, b canonicalConfigStamp) bool {
	eqTime := func(x, y *time.Time) bool { return (x == nil && y == nil) || (x != nil && y != nil && x.Equal(*y)) }
	eqBool := func(x, y *bool) bool { return (x == nil && y == nil) || (x != nil && y != nil && *x == *y) }
	eqText := func(x, y *string) bool { return (x == nil && y == nil) || (x != nil && y != nil && *x == *y) }
	return eqTime(a.lastSyncAt, b.lastSyncAt) && eqBool(a.lastSyncSuccess, b.lastSyncSuccess) &&
		eqText(a.lastSyncError, b.lastSyncError) && eqText(a.lastSyncStats, b.lastSyncStats)
}

// TestNativeFinalizeSyncRunNoDatasetForProviderIsANoOpSuccess pins D6467 on the real finalize path: a zero-unit run
// whose planner reason is no_dataset_for_provider persists success with no error, a started_at equal to completed_at
// (it was NULL), the reason in result, completed backfill and successful job run rows, and leaves the canonical sync
// configuration's last_sync_* fields exactly as they were.
func TestNativeFinalizeSyncRunNoDatasetForProviderIsANoOpSuccess(t *testing.T) {
	out := finalizeZeroUnitRun(t, `{"reason":"no_dataset_for_provider"}`)
	if out.status != syncRunStatusSuccess {
		t.Fatalf("status=%q want=%q", out.status, syncRunStatusSuccess)
	}
	if out.errorText != nil {
		t.Fatalf("error=%q want NULL", *out.errorText)
	}
	if out.completedAt == nil || out.startedAt == nil || !out.startedAt.Equal(*out.completedAt) {
		t.Fatalf("started_at=%v completed_at=%v, want started_at set equal to completed_at", out.startedAt, out.completedAt)
	}
	if out.resultReason == nil || *out.resultReason != "no_dataset_for_provider" {
		t.Fatalf("result.reason=%v want no_dataset_for_provider", out.resultReason)
	}
	if out.backfillStatus != "completed" {
		t.Fatalf("backfill_jobs.status=%q want completed", out.backfillStatus)
	}
	if out.jobRunStatus != jobRunStatusSuccess {
		t.Fatalf("job_runs.status=%d want=%d (success)", out.jobRunStatus, jobRunStatusSuccess)
	}
	if !sameStamp(out.before, out.after) {
		t.Fatalf("canonical sync_configurations last_sync_* changed by a no-op success: before=%+v after=%+v", out.before, out.after)
	}
}

// TestNativeFinalizeSyncRunGenericZeroUnitStaysFailedAndStamps is the twin: a generic zero-unit reason keeps the loud
// failure and DOES stamp the canonical configuration.
func TestNativeFinalizeSyncRunGenericZeroUnitStaysFailedAndStamps(t *testing.T) {
	out := finalizeZeroUnitRun(t, `{"reason":"some_other_reason"}`)
	if out.status != syncRunStatusFailed {
		t.Fatalf("status=%q want=%q", out.status, syncRunStatusFailed)
	}
	if out.errorText == nil || *out.errorText == "" {
		t.Fatal("error is empty, want the generic zero-unit error")
	}
	if out.backfillStatus != "failed" {
		t.Fatalf("backfill_jobs.status=%q want failed", out.backfillStatus)
	}
	if out.jobRunStatus != jobRunStatusFailed {
		t.Fatalf("job_runs.status=%d want=%d (failed)", out.jobRunStatus, jobRunStatusFailed)
	}
	if sameStamp(out.before, out.after) {
		t.Fatal("canonical sync_configurations last_sync_* not stamped by a failed zero-unit run")
	}
	if out.after.lastSyncSuccess == nil || *out.after.lastSyncSuccess {
		t.Fatalf("last_sync_success=%v want false", out.after.lastSyncSuccess)
	}
}
