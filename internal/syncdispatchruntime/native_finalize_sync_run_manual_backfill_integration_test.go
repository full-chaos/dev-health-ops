//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	manualBackfillOccurrence = "sha256:5b0c8f6e3a1d4c2f9e7a6b5d4c3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e"
	manualBackfillOther      = "sha256:0000000000000000000000000000000000000000000000000000000000000001"
	manualBackfillJob        = "00000000-0000-4000-8000-0000000000e1"
	manualBackfillOtherJob   = "00000000-0000-4000-8000-0000000000e2"
	manualBackfillScheduled  = "00000000-0000-4000-8000-0000000000e3"
	manualBackfillJobRun     = "00000000-0000-4000-8000-0000000000e4"
)

type manualBackfillOutcome struct {
	status, bystanderStatus string
	errorMessage            *string
	totalChunks, doneChunks int
	failedChunks            int
	completedAt             *time.Time
	runStatus               string
}

// finalizeManualBackfill finalizes a run that an admin backfill (a manual trigger) planned: the backfill_jobs row is
// keyed "occurrence:<id>" (or with an idempotency prefix), never "sync_run:<id>", and the occurrence row links it to
// the run. A second backfill job of another occurrence, not linked to the run, must stay as it was.
func finalizeManualBackfill(t *testing.T, taskID, plannerResult string, successUnits int) manualBackfillOutcome {
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
	// The run of an occurrence has the materializer's deterministic id, the only link the finalizer can read.
	runID, err := schedulersync.DeterministicSyncRunID(manualBackfillOccurrence)
	if err != nil {
		t.Fatal(err)
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{
		ID: runID, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
		Status: "dispatching", TotalUnits: successUnits, ResultJSON: plannerResult,
	})
	pgseed.SyncDispatchOutbox(ctx, t, pool, finalizeTestOutbox, runID, finalizeTestOrg, "finalize_sync_run", "dispatched", "river", finalizeRouteGeneration)
	args := newFinalizeArgs()
	args.RunID = runID
	pgseed.EnsureSyncIntegration(ctx, t, pool, finalizeTestOrg, finalizeTestIntegration, finalizeTestSource)
	if successUnits > 0 {
		since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		before := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: finalizeTestUnit, RunID: runID, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
			SourceID: finalizeTestSource, Status: "success", SinceAt: &since, BeforeAt: &before, CostClass: "heavy",
		})
	}
	pgseed.ScheduledJob(ctx, t, pool, manualBackfillScheduled, finalizeTestOrg)
	pgseed.JobRun(ctx, t, pool, manualBackfillJobRun, manualBackfillScheduled, jobRunStatusRunning, `{"sync_run_id":"`+runID+`"}`)
	for _, occurrence := range []struct {
		id     string
		linked bool
	}{{manualBackfillOccurrence, true}, {manualBackfillOther, false}} {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
    (occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at,
     reconcile_status, sync_run_id, job_run_id)
VALUES ($1, 1, $2, $3::uuid, $4::uuid, now(), now(), $5::text, CASE WHEN $6::boolean THEN $7::uuid END, CASE WHEN $6::boolean THEN $8::uuid END)`,
			occurrence.id, finalizeTestOrg, finalizeTestSyncConfig, manualBackfillScheduled,
			map[bool]string{true: "completed", false: "pending"}[occurrence.linked], occurrence.linked, runID, manualBackfillJobRun); err != nil {
			t.Fatal(err)
		}
	}
	for _, job := range []struct{ id, task string }{{manualBackfillJob, taskID}, {manualBackfillOtherJob, "occurrence:" + manualBackfillOther}} {
		if _, err := pool.Exec(ctx, `
INSERT INTO backfill_jobs (id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks, completed_chunks, failed_chunks)
VALUES ($1, $2, $3, $4, 'pending', '2026-01-01', '2026-01-02', 0, 0, 0)`,
			job.id, finalizeTestOrg, finalizeTestSyncConfig, job.task); err != nil {
			t.Fatal(err)
		}
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, args); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var out manualBackfillOutcome
	if err := pool.QueryRow(ctx, `SELECT status, error_message, total_chunks, completed_chunks, failed_chunks, completed_at FROM backfill_jobs WHERE id=$1`, manualBackfillJob).
		Scan(&out.status, &out.errorMessage, &out.totalChunks, &out.doneChunks, &out.failedChunks, &out.completedAt); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM backfill_jobs WHERE id=$1`, manualBackfillOtherJob).Scan(&out.bystanderStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, runID).Scan(&out.runStatus); err != nil {
		t.Fatal(err)
	}
	return out
}

func requireBystanderPending(t *testing.T, out manualBackfillOutcome) {
	t.Helper()
	if out.bystanderStatus != "pending" {
		t.Fatalf("a backfill job of another occurrence, not linked to this run, is %q, want pending", out.bystanderStatus)
	}
}

// TestFinalizeResolvesTheManualBackfillJobOfASuccessfulRun: an admin backfill job keyed by its occurrence must reach
// completed when its run finalizes success, with the unit counts the scheduled path writes.
func TestFinalizeResolvesTheManualBackfillJobOfASuccessfulRun(t *testing.T) {
	out := finalizeManualBackfill(t, "occurrence:"+manualBackfillOccurrence, "", 1)
	if out.runStatus != syncRunStatusSuccess {
		t.Fatalf("run status=%q want success", out.runStatus)
	}
	if out.status != "completed" || out.totalChunks != 1 || out.doneChunks != 1 || out.failedChunks != 0 || out.completedAt == nil || out.errorMessage != nil {
		t.Fatalf("manual backfill job = %+v, want completed 1/1/0 with completed_at and no error", out)
	}
	requireBystanderPending(t, out)
}

// TestFinalizeResolvesTheIdempotencyKeyedManualBackfillJob: a keyed submit's task id carries "idem:<hash>;" before the
// occurrence marker.
func TestFinalizeResolvesTheIdempotencyKeyedManualBackfillJob(t *testing.T) {
	out := finalizeManualBackfill(t, "idem:0123456789abcdef0123456789abcdef;occurrence:"+manualBackfillOccurrence, "", 1)
	if out.status != "completed" {
		t.Fatalf("keyed manual backfill job status=%q want completed", out.status)
	}
	requireBystanderPending(t, out)
}

// TestFinalizeResolvesTheManualBackfillJobOfANoOpRun: a run that planned no unit because the provider has no such
// dataset is a no-op success (D6467), and its job is completed, not failed and not pending.
func TestFinalizeResolvesTheManualBackfillJobOfANoOpRun(t *testing.T) {
	out := finalizeManualBackfill(t, "occurrence:"+manualBackfillOccurrence, `{"reason":"no_dataset_for_provider"}`, 0)
	if out.runStatus != syncRunStatusSuccess {
		t.Fatalf("run status=%q want success", out.runStatus)
	}
	if out.status != "completed" || out.errorMessage != nil {
		t.Fatalf("no-op manual backfill job = %+v, want completed with no error", out)
	}
	requireBystanderPending(t, out)
}

// TestFinalizeFailsTheManualBackfillJobOfAFailedRun: a zero-unit run with another reason fails, and so does its job.
func TestFinalizeFailsTheManualBackfillJobOfAFailedRun(t *testing.T) {
	out := finalizeManualBackfill(t, "occurrence:"+manualBackfillOccurrence, `{"reason":"some_other_reason"}`, 0)
	if out.runStatus != syncRunStatusFailed {
		t.Fatalf("run status=%q want failed", out.runStatus)
	}
	if out.status != "failed" || out.errorMessage == nil || *out.errorMessage == "" {
		t.Fatalf("failed manual backfill job = %+v, want failed with the run's error", out)
	}
	requireBystanderPending(t, out)
}

// TestFinalizeStillResolvesTheScheduledBackfillJob: the existing sync_run:<id> key keeps working.
func TestFinalizeStillResolvesTheScheduledBackfillJob(t *testing.T) {
	runID, err := schedulersync.DeterministicSyncRunID(manualBackfillOccurrence)
	if err != nil {
		t.Fatal(err)
	}
	out := finalizeManualBackfill(t, "sync_run:"+runID, "", 1)
	if out.status != "completed" {
		t.Fatalf("scheduled backfill job status=%q want completed", out.status)
	}
	requireBystanderPending(t, out)
}
