//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testFeatureDisabledMessage stands in for a real
// CanonicalIncidentDecisionForUpdate reason in tests that exercise
// terminalizeFeatureDisabledRun/Graph directly (below terminalizeFeatureDisabledPlan,
// which is what actually resolves a real reason) -- the specific reason
// value is immaterial to these tests, which assert on the run/unit
// termination mechanics, not on message content.
var testFeatureDisabledMessage = canonicalIncidentFeatureDisabledMessage(scheduledsync.FeatureDecisionReasonGlobalDisabled)

// createFeatureDisabledTables is a self-contained schema for this file:
// createFinalizeTables' sync_runs/sync_run_units carry the full column set
// terminalizeFeatureDisabledRun/Graph need (status, total_units, result,
// etc.), which the reference-discovery test file's leaner schema does not,
// so this is its own copy rather than a reuse of either.
func createFeatureDisabledTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE sync_dispatch_outbox (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id text NOT NULL, kind text NOT NULL,
 status text NOT NULL, available_at timestamptz NOT NULL, attempts int NOT NULL DEFAULT 0,
 dispatched_at timestamptz NULL, dispatched_transport text NULL, dispatched_route_generation bigint NULL,
 transport_job_id text NULL, claim_token text NULL, claim_transport text NULL,
 claim_route_generation bigint NULL, claim_expires_at timestamptz NULL, last_error text NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE (sync_run_id, kind)
);
CREATE TABLE sync_runs (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NULL,
 status text NOT NULL, total_units int NOT NULL DEFAULT 0, completed_units int NOT NULL DEFAULT 0,
 failed_units int NOT NULL DEFAULT 0, completed_at timestamptz NULL, result json NULL, error text NULL
);
CREATE TABLE sync_run_units (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, source_id uuid NULL, status text NOT NULL,
 available_at timestamptz NULL, lease_owner text NULL, lease_expires_at timestamptz NULL,
 error text NULL, result json NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE sync_run_reference_discoveries (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL UNIQUE, org_id text NOT NULL,
 status text NOT NULL, attempts int NOT NULL DEFAULT 0, available_at timestamptz NOT NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 completed_at timestamptz NULL, error text NULL, result json NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE integrations (
 id uuid PRIMARY KEY, provider text NOT NULL
);
CREATE TABLE integration_datasets (
 id uuid PRIMARY KEY, integration_id uuid NOT NULL, dataset_key text NOT NULL, is_enabled boolean NOT NULL
);
CREATE TABLE backfill_jobs (
 id uuid PRIMARY KEY, org_id text NOT NULL, celery_task_id text NULL, status text NOT NULL,
 total_chunks int NOT NULL DEFAULT 0, completed_chunks int NOT NULL DEFAULT 0,
 failed_chunks int NOT NULL DEFAULT 0, error_message text NULL, completed_at timestamptz NULL
);
CREATE TABLE scheduled_jobs (
 id uuid PRIMARY KEY
);
CREATE TABLE job_runs (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES scheduled_jobs(id),
 status int NOT NULL, completed_at timestamptz NULL, result json NULL, error text NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	featureDisabledTestOrg         = "00000000-0000-4000-8000-0000000000e1"
	featureDisabledTestRun         = "00000000-0000-4000-8000-0000000000e2"
	featureDisabledTestIntegration = "00000000-0000-4000-8000-0000000000e3"
)

func startFeatureDisabledPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

func withTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func seedFeatureDisabledRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, totalUnits int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id, org_id, integration_id, status, total_units, completed_units, failed_units)
VALUES ($1, $2, $3, 'dispatching', $4, 0, 0)`,
		featureDisabledTestRun, featureDisabledTestOrg, featureDisabledTestIntegration, totalUnits); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO integrations (id, provider) VALUES ($1, 'github')`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}
}

func insertFeatureDisabledUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, status string, leaseOwner *string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, provider, dataset_key, status, lease_owner)
VALUES ($1, $2, $3, 'github', 'prs', $4, $5)`,
		id, featureDisabledTestOrg, featureDisabledTestRun, status, leaseOwner); err != nil {
		t.Fatal(err)
	}
}

// TestTerminalizeFeatureDisabledRunBulkAndRaceSafeRunning pins
// terminalize_feature_disabled_run's two-phase termination: the bulk
// unclaimed-status update, and the per-RUNNING-unit lease-owner-matched
// update -- specifically that a RUNNING unit with a NULL lease_owner is
// still terminalized (IS NOT DISTINCT FROM, not a plain `=` which never
// matches NULL). An already-terminal SUCCESS unit must be left untouched.
func TestTerminalizeFeatureDisabledRunBulkAndRaceSafeRunning(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 5)

	planned := "00000000-0000-4000-8000-0000000001a1"
	retrying := "00000000-0000-4000-8000-0000000001a2"
	dispatching := "00000000-0000-4000-8000-0000000001a3"
	runningNilOwner := "00000000-0000-4000-8000-0000000001a4"
	success := "00000000-0000-4000-8000-0000000001a5"
	insertFeatureDisabledUnit(t, ctx, pool, planned, "planned", nil)
	insertFeatureDisabledUnit(t, ctx, pool, retrying, "retrying", nil)
	insertFeatureDisabledUnit(t, ctx, pool, dispatching, "dispatching", nil)
	insertFeatureDisabledUnit(t, ctx, pool, runningNilOwner, "running", nil)
	insertFeatureDisabledUnit(t, ctx, pool, success, "success", nil)

	var transition FeatureDisabledRunTransition
	now := time.Now().UTC()
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		result, err := terminalizeFeatureDisabledRun(ctx, tx, run, testFeatureDisabledMessage, now)
		if err != nil {
			t.Fatalf("terminalizeFeatureDisabledRun: %v", err)
		}
		transition = result
		if run.status != syncRunStatusFailed {
			t.Fatalf("run.status=%q want=%q", run.status, syncRunStatusFailed)
		}
		if run.completedAt == nil {
			t.Fatal("run.completedAt not set when RunTerminal")
		}
		if run.completedUnits != 1 || run.failedUnits != 4 {
			t.Fatalf("run.completedUnits=%d run.failedUnits=%d want 1,4", run.completedUnits, run.failedUnits)
		}
	})

	if transition.FailedUnits != 4 {
		t.Fatalf("FailedUnits=%d want=4 (planned+retrying+dispatching+running-with-nil-lease)", transition.FailedUnits)
	}
	if transition.RunningUnits != 0 {
		t.Fatalf("RunningUnits=%d want=0", transition.RunningUnits)
	}
	if !transition.RunTerminal {
		t.Fatal("RunTerminal=false want=true (4 failed + 1 success == 5 total)")
	}

	for _, id := range []string{planned, retrying, dispatching, runningNilOwner} {
		var status string
		var errorText *string
		if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, id).Scan(&status, &errorText); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusFailed {
			t.Fatalf("unit %s status=%q want=failed", id, status)
		}
		if errorText == nil || *errorText != testFeatureDisabledMessage {
			t.Fatalf("unit %s error=%v want=%q", id, errorText, testFeatureDisabledMessage)
		}
	}
	var successStatus string
	var successError *string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, success).Scan(&successStatus, &successError); err != nil {
		t.Fatal(err)
	}
	if successStatus != syncRunUnitStatusSuccess || successError != nil {
		t.Fatalf("already-terminal success unit was touched: status=%q error=%v", successStatus, successError)
	}

	var runStatus string
	var completedUnits, failedUnits int
	var runError string
	if err := pool.QueryRow(ctx, `SELECT status, completed_units, failed_units, error FROM sync_runs WHERE id=$1`, featureDisabledTestRun).
		Scan(&runStatus, &completedUnits, &failedUnits, &runError); err != nil {
		t.Fatal(err)
	}
	if runStatus != syncRunStatusFailed || completedUnits != 1 || failedUnits != 4 {
		t.Fatalf("sync_runs row status=%q completed=%d failed=%d want failed,1,4", runStatus, completedUnits, failedUnits)
	}
	if runError != testFeatureDisabledMessage {
		t.Fatalf("sync_runs.error=%q want=%q", runError, testFeatureDisabledMessage)
	}
}

// TestTerminalizeFeatureDisabledRunLeavesGenuinelyNonterminalStateAlone pins
// the guard terminalizeFeatureDisabledPlan relies on: a unit sitting outside
// every status terminalizeFeatureDisabledRun knows how to close out (a
// defensive path -- the production status enum only has five values, but
// nothing at the SQL layer here enforces that) must leave RunTerminal
// false, and terminalizeFeatureDisabledPlan must refuse to proceed to graph
// termination in that case rather than silently treating a stuck run as
// done.
func TestTerminalizeFeatureDisabledRunLeavesGenuinelyNonterminalStateAlone(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	stuck := "00000000-0000-4000-8000-0000000001b1"
	insertFeatureDisabledUnit(t, ctx, pool, stuck, "some_unmodeled_status", nil)

	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		_, err := terminalizeFeatureDisabledPlan(ctx, tx, run, scheduledsync.FeatureDecisionReasonGlobalDisabled, time.Now().UTC())
		if !errors.Is(err, ErrFeatureDisabledPlanNotTerminal) {
			t.Fatalf("terminalizeFeatureDisabledPlan error=%v want=ErrFeatureDisabledPlanNotTerminal", err)
		}
	})
}

// TestTerminalizeFeatureDisabledGraphTerminatesLedgerOutboxAndObservers pins
// _terminalize_feature_disabled_graph end to end: the reference-discovery
// ledger goes FAILED, every pending outbox row for the run is dispatched
// with the feature_disabled sentinel, a finalize_sync_run outbox row is
// created in that same terminal state when none existed, and
// observeTerminalSyncRun's BackfillJob/JobRun side effects fire.
func TestTerminalizeFeatureDisabledGraphTerminatesLedgerOutboxAndObservers(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001c1", "planned", nil)

	ledgerID := "00000000-0000-4000-8000-0000000001c2"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, available_at)
VALUES ($1, $2, $3, 'planned', now())`, ledgerID, featureDisabledTestRun, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	dispatchOutboxID := "00000000-0000-4000-8000-0000000001c3"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id, sync_run_id, org_id, kind, status, available_at, created_at, updated_at)
VALUES ($1, $2, $3, 'dispatch_sync_run', 'pending', now(), now(), now())`,
		dispatchOutboxID, featureDisabledTestRun, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	jobID := "00000000-0000-4000-8000-0000000001c4"
	if _, err := pool.Exec(ctx, `INSERT INTO scheduled_jobs (id) VALUES ($1)`, jobID); err != nil {
		t.Fatal(err)
	}
	jobRunID := "00000000-0000-4000-8000-0000000001c5"
	if _, err := pool.Exec(ctx, `
INSERT INTO job_runs (id, job_id, status, result) VALUES ($1, $2, $3, $4::json)`,
		jobRunID, jobID, jobRunStatusRunning, `{"sync_run_id":"`+featureDisabledTestRun+`"}`); err != nil {
		t.Fatal(err)
	}
	backfillID := "00000000-0000-4000-8000-0000000001c6"
	if _, err := pool.Exec(ctx, `
INSERT INTO backfill_jobs (id, org_id, celery_task_id, status) VALUES ($1, $2, $3, 'running')`,
		backfillID, featureDisabledTestOrg, "sync_run:"+featureDisabledTestRun); err != nil {
		t.Fatal(err)
	}

	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		if _, err := terminalizeFeatureDisabledPlan(ctx, tx, run, scheduledsync.FeatureDecisionReasonGlobalDisabled, time.Now().UTC()); err != nil {
			t.Fatalf("terminalizeFeatureDisabledPlan: %v", err)
		}
	})

	var ledgerStatus string
	var ledgerError *string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_reference_discoveries WHERE id=$1`, ledgerID).
		Scan(&ledgerStatus, &ledgerError); err != nil {
		t.Fatal(err)
	}
	if ledgerStatus != discoveryStatusFailed {
		t.Fatalf("ledger status=%q want=%q", ledgerStatus, discoveryStatusFailed)
	}
	if ledgerError == nil || !strings.Contains(*ledgerError, "canonical incident ingestion is disabled") {
		t.Fatalf("ledger error=%v want sanitized feature-disabled message", ledgerError)
	}

	var dispatchStatus, dispatchLastError string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM sync_dispatch_outbox WHERE id=$1`, dispatchOutboxID).
		Scan(&dispatchStatus, &dispatchLastError); err != nil {
		t.Fatal(err)
	}
	if dispatchStatus != "dispatched" || dispatchLastError != featureDisabledErrorCategory {
		t.Fatalf("dispatch_sync_run outbox status=%q last_error=%q want dispatched/%s", dispatchStatus, dispatchLastError, featureDisabledErrorCategory)
	}

	var finalizeCount int
	var finalizeStatus, finalizeLastError string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeStatus, &finalizeLastError); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeCount); err != nil {
		t.Fatal(err)
	}
	if finalizeCount != 1 {
		t.Fatalf("finalize_sync_run outbox rows=%d want=1", finalizeCount)
	}
	if finalizeStatus != "dispatched" || finalizeLastError != featureDisabledErrorCategory {
		t.Fatalf("finalize outbox status=%q last_error=%q want dispatched/%s", finalizeStatus, finalizeLastError, featureDisabledErrorCategory)
	}

	var jobRunStatus int
	if err := pool.QueryRow(ctx, `SELECT status FROM job_runs WHERE id=$1`, jobRunID).Scan(&jobRunStatus); err != nil {
		t.Fatal(err)
	}
	if jobRunStatus != jobRunStatusFailed {
		t.Fatalf("job_runs.status=%d want=%d (failed)", jobRunStatus, jobRunStatusFailed)
	}
	var backfillStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM backfill_jobs WHERE id=$1`, backfillID).Scan(&backfillStatus); err != nil {
		t.Fatal(err)
	}
	if backfillStatus != "failed" {
		t.Fatalf("backfill_jobs.status=%q want=failed", backfillStatus)
	}
}

// TestTerminalizeFeatureDisabledGraphUpsertsAnExistingFinalizeRow pins the
// ON CONFLICT (sync_run_id, kind) upsert path: a finalize_sync_run outbox
// row that already exists (in any prior state) is overwritten in place,
// never duplicated -- mirroring Python's find-or-create-else-update branch.
func TestTerminalizeFeatureDisabledGraphUpsertsAnExistingFinalizeRow(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001d1", "planned", nil)
	existingFinalizeID := "00000000-0000-4000-8000-0000000001d2"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id, sync_run_id, org_id, kind, status, available_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'pending', now(), now(), now())`,
		existingFinalizeID, featureDisabledTestRun, featureDisabledTestOrg, outboxKindFinalizeSyncRun); err != nil {
		t.Fatal(err)
	}

	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		if _, err := terminalizeFeatureDisabledPlan(ctx, tx, run, scheduledsync.FeatureDecisionReasonGlobalDisabled, time.Now().UTC()); err != nil {
			t.Fatalf("terminalizeFeatureDisabledPlan: %v", err)
		}
	})

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("finalize outbox rows=%d want=1 (upsert must not duplicate)", count)
	}
	var id, status string
	if err := pool.QueryRow(ctx, `SELECT id::text, status FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&id, &status); err != nil {
		t.Fatal(err)
	}
	if id != existingFinalizeID {
		t.Fatalf("finalize outbox row id=%s want the pre-existing row %s reused, not replaced", id, existingFinalizeID)
	}
	if status != "dispatched" {
		t.Fatalf("finalize outbox status=%q want=dispatched", status)
	}
}

// TestArmFeatureDisabledFinalizeIsRaceSafeAndIdempotent pins
// _arm_feature_disabled_finalize's bool return: true only on the call that
// actually creates the row.
func TestArmFeatureDisabledFinalizeIsRaceSafeAndIdempotent(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)

	var firstArmed, secondArmed bool
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		firstArmed, err = armFeatureDisabledFinalize(ctx, tx, featureDisabledTestOrg, featureDisabledTestRun, time.Now().UTC())
		if err != nil {
			t.Fatalf("first armFeatureDisabledFinalize: %v", err)
		}
	})
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		secondArmed, err = armFeatureDisabledFinalize(ctx, tx, featureDisabledTestOrg, featureDisabledTestRun, time.Now().UTC())
		if err != nil {
			t.Fatalf("second armFeatureDisabledFinalize: %v", err)
		}
	})
	if !firstArmed {
		t.Fatal("first call armed=false want=true")
	}
	if secondArmed {
		t.Fatal("second call armed=true want=false (row already exists)")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("finalize outbox rows=%d want=1", count)
	}
}

// TestSyncRunRequiresCanonicalIncidentFeatureUnitScopeWins pins unit-scope
// precedence: pagerduty/incidents legacy-targets to "operational" (a gated
// target), so a run with a unit already planned against it requires the
// feature regardless of the integration's own dataset configuration.
func TestSyncRunRequiresCanonicalIncidentFeatureUnitScopeWins(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, provider, dataset_key, status)
VALUES ('00000000-0000-4000-8000-0000000001e1', $1, $2, 'pagerduty', 'incidents', 'planned')`,
		featureDisabledTestOrg, featureDisabledTestRun); err != nil {
		t.Fatal(err)
	}
	// An integration-level scope that would NOT require the feature, proving
	// the unit scope -- not this -- decided the answer.
	if _, err := pool.Exec(ctx, `
INSERT INTO integration_datasets (id, integration_id, dataset_key, is_enabled)
VALUES ('00000000-0000-4000-8000-0000000001e2', $1, 'users', true)`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}

	var requires bool
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		requires, err = syncRunRequiresCanonicalIncidentFeature(ctx, tx, featureDisabledTestRun, featureDisabledTestIntegration)
		if err != nil {
			t.Fatalf("syncRunRequiresCanonicalIncidentFeature: %v", err)
		}
	})
	if !requires {
		t.Fatal("requires=false want=true (pagerduty/incidents unit scope is gated)")
	}
}

// TestSyncRunRequiresCanonicalIncidentFeatureIntegrationFallbackRespectsIsEnabled
// pins the no-units-yet fallback path AND that a disabled integration
// dataset is excluded, matching IntegrationDataset.is_enabled.is_(True) in
// the Python query.
func TestSyncRunRequiresCanonicalIncidentFeatureIntegrationFallbackRespectsIsEnabled(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 0)
	if _, err := pool.Exec(ctx, `
INSERT INTO integration_datasets (id, integration_id, dataset_key, is_enabled)
VALUES ('00000000-0000-4000-8000-0000000001f1', $1, 'incidents', false)`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE integrations SET provider = 'pagerduty' WHERE id = $1`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}

	var requires bool
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		requires, err = syncRunRequiresCanonicalIncidentFeature(ctx, tx, featureDisabledTestRun, featureDisabledTestIntegration)
		if err != nil {
			t.Fatalf("syncRunRequiresCanonicalIncidentFeature: %v", err)
		}
	})
	if requires {
		t.Fatal("requires=true want=false (the only configured dataset scope is disabled)")
	}

	if _, err := pool.Exec(ctx, `UPDATE integration_datasets SET is_enabled = true WHERE integration_id = $1`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		requires, err = syncRunRequiresCanonicalIncidentFeature(ctx, tx, featureDisabledTestRun, featureDisabledTestIntegration)
		if err != nil {
			t.Fatalf("syncRunRequiresCanonicalIncidentFeature: %v", err)
		}
	})
	if !requires {
		t.Fatal("requires=false want=true once the pagerduty/incidents dataset scope is enabled")
	}
}
