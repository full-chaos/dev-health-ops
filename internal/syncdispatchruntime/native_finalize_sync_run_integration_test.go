//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// zeroUnitFinalizationCount reads back one (provider, reason) series from a
// real *jobruntime.MetricsCollector's exposition text -- the collector has
// no exported per-series accessor (by design: WritePrometheus/PrometheusText
// is the only sanctioned read path), so tests observe it the same way an
// operator's scrape would.
func zeroUnitFinalizationCount(t *testing.T, collector *jobruntime.MetricsCollector, provider, reason string) uint64 {
	t.Helper()
	text := collector.PrometheusText()
	prefix := fmt.Sprintf(`devhealth_sync_run_zero_unit_finalizations_total{provider=%q,reason=%q} `, provider, reason)
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, prefix) {
			var value uint64
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, prefix), "%d", &value); err != nil {
				t.Fatalf("parse counter line %q: %v", line, err)
			}
			return value
		}
	}
	return 0
}

func createFinalizeTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE sync_dispatch_transport_routes (
 kind text PRIMARY KEY, transport text NOT NULL, generation bigint NOT NULL,
 paused boolean NOT NULL, rollback_transport text NOT NULL
);
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
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 status text NOT NULL, total_units int NOT NULL DEFAULT 0, completed_units int NOT NULL DEFAULT 0,
 failed_units int NOT NULL DEFAULT 0, completed_at timestamptz NULL, result json NULL, error text NULL
);
CREATE TABLE sync_run_units (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, source_id uuid NOT NULL, status text NOT NULL,
 error text NULL, result json NULL, processor_flags json NULL
);
CREATE TABLE integrations (
 id uuid PRIMARY KEY, provider text NOT NULL
);
CREATE TABLE sync_configurations (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL, parent_id uuid NULL,
 sync_options json NOT NULL, created_at timestamptz NOT NULL,
 last_sync_at timestamptz NULL, last_sync_success boolean NULL, last_sync_error text NULL,
 last_sync_stats json NULL
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
);
CREATE TABLE sync_compute_checkpoints (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
 source_id uuid NULL, provider text NOT NULL, dataset_key text NOT NULL, compute_type text NOT NULL,
 status text NOT NULL, checkpointed_at timestamptz NOT NULL, metadata json NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 CONSTRAINT uq_sync_compute_checkpoint_unit_type UNIQUE (sync_run_id, sync_run_unit_id, compute_type)
);
CREATE TABLE sync_run_post_dispatches (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, kind text NOT NULL,
 dispatched_at timestamptz NOT NULL,
 UNIQUE (sync_run_id, kind)
);
CREATE TABLE sync_coverage_projections (
 org_id text NOT NULL, sync_config_id uuid NOT NULL, invalidated_at timestamptz NULL,
 PRIMARY KEY (org_id, sync_config_id)
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	finalizeTestOrg         = "00000000-0000-4000-8000-0000000000f1"
	finalizeTestRun         = "00000000-0000-4000-8000-0000000000f2"
	finalizeTestOutbox      = "00000000-0000-4000-8000-0000000000f3"
	finalizeTestIntegration = "00000000-0000-4000-8000-0000000000f4"
	finalizeTestSyncConfig  = "00000000-0000-4000-8000-0000000000f5"
	finalizeTestUnit        = "00000000-0000-4000-8000-0000000000f6"
	finalizeTestSource      = "00000000-0000-4000-8000-0000000000f7"
)

func seedFinalizeRoute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
		 VALUES ('finalize_sync_run','river',1,false,'celery')`,
		`INSERT INTO sync_dispatch_outbox
		    (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
		 VALUES ('` + finalizeTestOutbox + `','` + finalizeTestRun + `','` + finalizeTestOrg + `',
		         'finalize_sync_run','dispatched',now(),'river',1,now(),now())`,
		`INSERT INTO integrations (id, provider) VALUES ('` + finalizeTestIntegration + `','github')`,
		`INSERT INTO sync_configurations (id,org_id,integration_id,parent_id,sync_options,created_at)
		 VALUES ('` + finalizeTestSyncConfig + `','` + finalizeTestOrg + `','` + finalizeTestIntegration + `',
		         NULL,'{}'::json, now())`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func newFinalizeArgs() FinalizeSyncRunArgs {
	return FinalizeSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: finalizeTestOrg, RunID: finalizeTestRun,
		DispatchOutbox: finalizeTestOutbox, RouteGeneration: 1,
	}}
}

func insertZeroUnitRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, plannerResult, plannerError string) {
	t.Helper()
	var resultArg any
	if plannerResult == "" {
		resultArg = nil
	} else {
		resultArg = plannerResult
	}
	var errorArg any
	if plannerError == "" {
		errorArg = nil
	} else {
		errorArg = plannerError
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id,org_id,integration_id,status,total_units,completed_units,failed_units,result,error)
VALUES ($1,$2,$3,'dispatching',0,0,0,$4::json,$5)`,
		finalizeTestRun, finalizeTestOrg, finalizeTestIntegration, resultArg, errorArg); err != nil {
		t.Fatal(err)
	}
}

// TestNativeFinalizeSyncRunPreservesPlannerZeroUnitCause pins the CHAOS-4159
// behaviour merged in PR #1881 (zero-unit-finalize-preserve-cause): a
// zero-unit run finalizes FAILED, but the planner's recorded error/reason
// wins over the generic "No sync units planned" residual. Read from
// sync_units.py:2117-2167 before writing this test.
func TestNativeFinalizeSyncRunPreservesPlannerZeroUnitCause(t *testing.T) {
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
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)
	insertZeroUnitRun(t, ctx, pool,
		`{"reason":"pagerduty_credential_unavailable","error_category":"pagerduty_credential_unavailable"}`,
		"PagerDuty credential is missing for this integration")

	metrics, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var status, resultError string
	var resultJSON []byte
	if err := pool.QueryRow(ctx, `SELECT status, error, result FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&status, &resultError, &resultJSON); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusFailed {
		t.Fatalf("status=%q want=%q (a zero-unit run must still finalize FAILED, CHAOS-4159 keeps this)", status, syncRunStatusFailed)
	}
	// The planner's cause must survive verbatim -- NOT be overwritten by the
	// generic "No sync units planned" residual this function used to write
	// unconditionally.
	if resultError != "PagerDuty credential is missing for this integration" {
		t.Fatalf("error=%q want planner cause preserved, got residual/other", resultError)
	}
	if got := string(resultJSON); !strings.Contains(got, `"reason":"pagerduty_credential_unavailable"`) {
		t.Fatalf("result=%s missing planner-derived reason", got)
	}

	// Telemetry: exactly one zero-unit finalization, labeled with the
	// planner's cause, not the generic residual -- incremented only AFTER
	// commit.
	if count := zeroUnitFinalizationCount(t, metrics, "github", "pagerduty_credential_unavailable"); count != 1 {
		t.Fatalf("zero-unit counter[github,pagerduty_credential_unavailable]=%d want=1", count)
	}
	if count := zeroUnitFinalizationCount(t, metrics, "github", zeroUnitGenericReason); count != 0 {
		t.Fatalf("zero-unit counter[github,%s]=%d want=0 (must not fall back to generic when planner recorded a cause)", zeroUnitGenericReason, count)
	}

	// Idempotency: calling Finalize again must not double-count the counter
	// or fail -- it hits the once-only ledger's unique-constraint branch.
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("second Finalize: %v", err)
	}
	if count := zeroUnitFinalizationCount(t, metrics, "github", "pagerduty_credential_unavailable"); count != 1 {
		t.Fatalf("zero-unit counter after re-finalize=%d want=1 (must not double count)", count)
	}
	var dispatchRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_post_dispatches WHERE sync_run_id=$1`, finalizeTestRun).Scan(&dispatchRows); err != nil {
		t.Fatal(err)
	}
	if dispatchRows != 1 {
		t.Fatalf("sync_run_post_dispatches rows=%d want=1 (once-only ledger)", dispatchRows)
	}
	var outboxRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='post_sync'`, finalizeTestRun).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if outboxRows != 1 {
		t.Fatalf("post_sync outbox wakeup rows=%d want=1 (armed once, not re-armed on re-finalize)", outboxRows)
	}
}

// TestNativeFinalizeSyncRunZeroUnitFallsBackToGenericCause pins the OTHER
// half of the CHAOS-4159 contract: when the planner recorded nothing, the
// generic residual is used -- it must never be left blank, which would read
// as "nothing to say" rather than "not captured".
func TestNativeFinalizeSyncRunZeroUnitFallsBackToGenericCause(t *testing.T) {
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
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)
	insertZeroUnitRun(t, ctx, pool, "", "")

	metrics, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var status, resultError string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_runs WHERE id=$1`, finalizeTestRun).Scan(&status, &resultError); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusFailed {
		t.Fatalf("status=%q want=%q", status, syncRunStatusFailed)
	}
	if resultError != zeroUnitGenericError {
		t.Fatalf("error=%q want generic %q", resultError, zeroUnitGenericError)
	}
	if count := zeroUnitFinalizationCount(t, metrics, "github", zeroUnitGenericReason); count != 1 {
		t.Fatalf("zero-unit counter[github,%s]=%d want=1", zeroUnitGenericReason, count)
	}

	// Canonical config stamp: last_sync_success=false, last_sync_error is the
	// sanitized generic message (a zero-unit run never stamps success).
	var lastSuccess bool
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT last_sync_success, last_sync_error FROM sync_configurations WHERE id=$1`, finalizeTestSyncConfig).
		Scan(&lastSuccess, &lastError); err != nil {
		t.Fatal(err)
	}
	if lastSuccess {
		t.Fatal("last_sync_success=true for a zero-unit FAILED run")
	}
	if lastError != zeroUnitGenericError {
		t.Fatalf("last_sync_error=%q want=%q", lastError, zeroUnitGenericError)
	}
}

// TestNativeFinalizeSyncRunAggregatesSuccessfulUnits pins the non-zero-unit
// aggregation path end to end: a single successful unit finalizes SUCCESS,
// stamps the canonical config as a success, updates a matching in-flight
// JobRun, and writes exactly one work-graph compute checkpoint for the unit
// (sync_units.py:2080-2116 aggregation, :2319-2374 checkpointing).
func TestNativeFinalizeSyncRunAggregatesSuccessfulUnits(t *testing.T) {
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
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id,org_id,integration_id,status,total_units,completed_units,failed_units)
VALUES ($1,$2,$3,'dispatching',1,0,0)`, finalizeTestRun, finalizeTestOrg, finalizeTestIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'github','commits',$4,'success')`,
		finalizeTestUnit, finalizeTestOrg, finalizeTestRun, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	// An in-flight JobRun this SyncRun should terminalize -- PENDING (0),
	// result carries sync_run_id so the (status IN (0,1)) + result match
	// query finds it.
	jobRunID := "00000000-0000-4000-8000-0000000000f8"
	if _, err := pool.Exec(ctx, `INSERT INTO scheduled_jobs (id) VALUES ($1)`, jobRunID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO job_runs (id, job_id, status, result) VALUES ($1, $1, 0, $2::json)`,
		jobRunID, `{"sync_run_id":"`+finalizeTestRun+`"}`); err != nil {
		t.Fatal(err)
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var status string
	var completedUnits, failedUnits int
	if err := pool.QueryRow(ctx, `SELECT status, completed_units, failed_units FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&status, &completedUnits, &failedUnits); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusSuccess || completedUnits != 1 || failedUnits != 0 {
		t.Fatalf("status=%q completed=%d failed=%d, want success/1/0", status, completedUnits, failedUnits)
	}

	var lastSuccess bool
	if err := pool.QueryRow(ctx, `SELECT last_sync_success FROM sync_configurations WHERE id=$1`, finalizeTestSyncConfig).Scan(&lastSuccess); err != nil {
		t.Fatal(err)
	}
	if !lastSuccess {
		t.Fatal("last_sync_success=false for a fully successful run")
	}

	var jobRunStatus int
	if err := pool.QueryRow(ctx, `SELECT status FROM job_runs WHERE id=$1`, jobRunID).Scan(&jobRunStatus); err != nil {
		t.Fatal(err)
	}
	if jobRunStatus != jobRunStatusSuccess {
		t.Fatalf("job_runs.status=%d want=%d (SUCCESS)", jobRunStatus, jobRunStatusSuccess)
	}

	var checkpointCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM sync_compute_checkpoints
WHERE sync_run_id=$1 AND sync_run_unit_id=$2 AND compute_type='work_graph'`,
		finalizeTestRun, finalizeTestUnit).Scan(&checkpointCount); err != nil {
		t.Fatal(err)
	}
	if checkpointCount != 1 {
		t.Fatalf("compute checkpoint rows=%d want=1 (github/commits is a git legacy target)", checkpointCount)
	}

	// Re-finalize must not duplicate the checkpoint row (ON CONFLICT DO
	// NOTHING on uq_sync_compute_checkpoint_unit_type).
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("second Finalize: %v", err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM sync_compute_checkpoints
WHERE sync_run_id=$1 AND sync_run_unit_id=$2 AND compute_type='work_graph'`,
		finalizeTestRun, finalizeTestUnit).Scan(&checkpointCount); err != nil {
		t.Fatal(err)
	}
	if checkpointCount != 1 {
		t.Fatalf("compute checkpoint rows after re-finalize=%d want=1 (no duplicate)", checkpointCount)
	}
}
