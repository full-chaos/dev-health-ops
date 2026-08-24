//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
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
 since_at timestamptz NULL, before_at timestamptz NULL,
 cost_class text NOT NULL DEFAULT 'medium', mode text NOT NULL DEFAULT 'incremental',
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
CREATE TABLE integration_sources (
 id uuid PRIMARY KEY
);
CREATE TABLE sync_compute_checkpoints (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
 source_id uuid NULL REFERENCES integration_sources(id), provider text NOT NULL, dataset_key text NOT NULL,
 compute_type text NOT NULL,
 status text NOT NULL, window_start timestamptz NULL, window_end timestamptz NULL,
 checkpointed_at timestamptz NOT NULL, metadata json NULL,
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
	if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id) VALUES ($1)`, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	unitSinceAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	unitBeforeAt := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,since_at,before_at,cost_class,mode)
VALUES ($1,$2,$3,'github','commits',$4,'success',$5,$6,'heavy','incremental')`,
		finalizeTestUnit, finalizeTestOrg, finalizeTestRun, finalizeTestSource, unitSinceAt, unitBeforeAt); err != nil {
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

	// The checkpoint must carry the unit's compute WINDOW and audit
	// classification (window_start/window_end from since_at/before_at, plus
	// cost_class/mode in metadata) -- a row that exists but is missing these
	// is incomplete for replay/audit even though the row count check above
	// would not catch it (codex adversarial review, CHAOS-4175).
	var windowStart, windowEnd time.Time
	var metadataRaw []byte
	if err := pool.QueryRow(ctx, `
SELECT window_start, window_end, metadata FROM sync_compute_checkpoints
WHERE sync_run_id=$1 AND sync_run_unit_id=$2 AND compute_type='work_graph'`,
		finalizeTestRun, finalizeTestUnit).Scan(&windowStart, &windowEnd, &metadataRaw); err != nil {
		t.Fatal(err)
	}
	if !windowStart.Equal(unitSinceAt) || !windowEnd.Equal(unitBeforeAt) {
		t.Fatalf("checkpoint window = [%s, %s], want [%s, %s]", windowStart, windowEnd, unitSinceAt, unitBeforeAt)
	}
	var metadata map[string]any
	if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["cost_class"] != "heavy" || metadata["mode"] != "incremental" {
		t.Fatalf("checkpoint metadata missing cost_class/mode: %v", metadata)
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

// TestNativeFinalizeSyncRunSurvivesAGenuineComputeCheckpointFailure (codex
// adversarial review, CHAOS-4175) pins that a checkpoint insert failure that
// is NOT the expected unique-constraint race (here: a dangling source_id
// violating the FK to integration_sources) does not poison the rest of
// Finalize's transaction. Postgres marks a transaction ABORTED on any
// statement error, even one the application only logs and continues past
// -- so without a per-checkpoint savepoint, this one bad unit's checkpoint
// would have rolled back the run's status, canonical config stamp,
// observers, and post-sync handoff along with it, silently turning a
// documented "best-effort, log and continue" checkpoint policy into a hard
// failure of the whole finalization.
func TestNativeFinalizeSyncRunSurvivesAGenuineComputeCheckpointFailure(t *testing.T) {
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
VALUES ($1,$2,$3,'dispatching',2,0,0)`, finalizeTestRun, finalizeTestOrg, finalizeTestIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id) VALUES ($1)`, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	// Unit A: valid, registered source -- its checkpoint must succeed.
	unitA := "00000000-0000-4000-8000-0000000000fa"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'github','commits',$4,'success')`,
		unitA, finalizeTestOrg, finalizeTestRun, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	// Unit B: a DANGLING source_id with no integration_sources row --
	// its checkpoint insert genuinely fails on the FK, distinct from the
	// expected/tolerated unique-constraint race.
	unitB := "00000000-0000-4000-8000-0000000000fb"
	danglingSource := "00000000-0000-4000-8000-0000000000fc"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'github','commits',$4,'success')`,
		unitB, finalizeTestOrg, finalizeTestRun, danglingSource); err != nil {
		t.Fatal(err)
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v (a tolerated checkpoint failure must not fail finalize)", err)
	}

	var status string
	var completedUnits int
	if err := pool.QueryRow(ctx, `SELECT status, completed_units FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&status, &completedUnits); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusSuccess || completedUnits != 2 {
		t.Fatalf("status=%q completed=%d, want success/2 -- one unit's checkpoint failure must not roll back the run's own finalization",
			status, completedUnits)
	}
	var lastSuccess bool
	if err := pool.QueryRow(ctx, `SELECT last_sync_success FROM sync_configurations WHERE id=$1`, finalizeTestSyncConfig).Scan(&lastSuccess); err != nil {
		t.Fatal(err)
	}
	if !lastSuccess {
		t.Fatal("canonical config stamp was rolled back by a checkpoint failure it should be isolated from")
	}
	var dispatchRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_post_dispatches WHERE sync_run_id=$1`, finalizeTestRun).Scan(&dispatchRows); err != nil {
		t.Fatal(err)
	}
	if dispatchRows != 1 {
		t.Fatal("post-sync once-only ledger was rolled back by a checkpoint failure it should be isolated from")
	}
	var checkpointCountA int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM sync_compute_checkpoints WHERE sync_run_id=$1 AND sync_run_unit_id=$2`,
		finalizeTestRun, unitA).Scan(&checkpointCountA); err != nil {
		t.Fatal(err)
	}
	if checkpointCountA != 1 {
		t.Fatalf("unit A's own checkpoint = %d rows, want 1 (its sibling's failure must not take it down too)", checkpointCountA)
	}
	var checkpointCountB int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM sync_compute_checkpoints WHERE sync_run_id=$1 AND sync_run_unit_id=$2`,
		finalizeTestRun, unitB).Scan(&checkpointCountB); err != nil {
		t.Fatal(err)
	}
	if checkpointCountB != 0 {
		t.Fatalf("unit B's checkpoint = %d rows, want 0 (its FK violation should have been rolled back to its own savepoint)", checkpointCountB)
	}
}

// TestNativeFinalizeSyncRunCheckpointUsesUnitOrgIDNotRunOrgID (codex
// adversarial review round 2, CHAOS-4175) pins that a compute checkpoint's
// org_id comes from the UNIT row, not the run row -- sync_units.py's
// _checkpoint_successful_compute_inputs builds
// `SyncComputeCheckpoint(org_id=str(unit.org_id), ...)`. Every earlier test
// in this file used the SAME org_id for the run and its unit, which can
// never distinguish "read from run" from "read from unit" -- this is the
// one test that gives the two sources different values.
func TestNativeFinalizeSyncRunCheckpointUsesUnitOrgIDNotRunOrgID(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id) VALUES ($1)`, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	// The unit's org_id deliberately DIFFERS from the run's org_id
	// (finalizeTestOrg). Real production data should never actually diverge
	// like this, but the port must read the field Python reads regardless.
	unitOrgID := "00000000-0000-4000-8000-0000000000fd"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'github','commits',$4,'success')`,
		finalizeTestUnit, unitOrgID, finalizeTestRun, finalizeTestSource); err != nil {
		t.Fatal(err)
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var checkpointOrgID string
	if err := pool.QueryRow(ctx, `
SELECT org_id FROM sync_compute_checkpoints WHERE sync_run_id=$1 AND sync_run_unit_id=$2`,
		finalizeTestRun, finalizeTestUnit).Scan(&checkpointOrgID); err != nil {
		t.Fatal(err)
	}
	if checkpointOrgID != unitOrgID {
		t.Fatalf("checkpoint org_id=%q, want the UNIT's org_id=%q (not the run's org_id=%q)",
			checkpointOrgID, unitOrgID, finalizeTestOrg)
	}
}

// TestNativeFinalizeSyncRunZeroUnitProviderIsNormalized (codex adversarial
// review round 2, CHAOS-4175) pins that a mixed-case or whitespace-padded
// Integration.provider value is normalized (trim + lowercase) before being
// used as a telemetry label, matching sync_units.py::_run_provider's
// `provider.strip().lower()` exactly. Without normalization, the label
// would fail ZeroUnitFinalizationObserver's case-sensitive known-provider
// check and silently collapse to "unknown", hiding the real provider.
func TestNativeFinalizeSyncRunZeroUnitProviderIsNormalized(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
		 VALUES ('finalize_sync_run','river',1,false,'celery')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sync_dispatch_outbox
		    (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
		 VALUES ($1,$2,$3,'finalize_sync_run','dispatched',now(),'river',1,now(),now())`,
		finalizeTestOutbox, finalizeTestRun, finalizeTestOrg); err != nil {
		t.Fatal(err)
	}
	// Mixed-case, whitespace-padded provider -- exactly what
	// _run_provider's .strip().lower() normalizes away.
	if _, err := pool.Exec(ctx, `INSERT INTO integrations (id, provider) VALUES ($1, '  PagerDuty  ')`, finalizeTestIntegration); err != nil {
		t.Fatal(err)
	}
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
	if count := zeroUnitFinalizationCount(t, metrics, "pagerduty", zeroUnitGenericReason); count != 1 {
		t.Fatalf("zero-unit counter[pagerduty,%s]=%d want=1 (mixed-case/whitespace provider must normalize)", zeroUnitGenericReason, count)
	}
	if count := zeroUnitFinalizationCount(t, metrics, "unknown", zeroUnitGenericReason); count != 0 {
		t.Fatalf("zero-unit counter[unknown,%s]=%d want=0 (a real, known provider must never fall back to unknown)", zeroUnitGenericReason, count)
	}
}

// TestCheckpointSuccessfulComputeInputsAbortsOnFatalSavepointFailure (codex
// adversarial review round 3, CHAOS-4175) pins that a FATAL checkpoint
// failure -- one where opening/closing the per-checkpoint savepoint itself
// fails, not an ordinary recovered INSERT error -- propagates an error out
// of checkpointSuccessfulComputeInputs rather than being logged and
// swallowed like a recovered failure is.
//
// A genuine "RELEASE SAVEPOINT fails mid-transaction" is an infrastructure
// fault (a dying connection), not something reproducible with SQL alone --
// the same gap exists in native_post_sync.go's own precedent for this exact
// failure mode (no dedicated test there either, only the doc comment this
// port's insertComputeCheckpoint cites). What IS deterministically
// reproducible is the sibling fatal path: tx.Begin(ctx) (opening the
// savepoint) failing because the caller's transaction is already closed.
// This test forces exactly that by committing the outer transaction BEFORE
// calling checkpointSuccessfulComputeInputs against it.
func TestCheckpointSuccessfulComputeInputsAbortsOnFatalSavepointFailure(t *testing.T) {
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

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the outer tx early (to close it before use): %v", err)
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := &finalizeSyncRun{id: finalizeTestRun, orgID: finalizeTestOrg}
	units := []finalizeSyncRunUnit{{
		id:         finalizeTestUnit,
		orgID:      finalizeTestOrg,
		status:     syncRunUnitStatusSuccess,
		provider:   "github",
		datasetKey: "commits",
		sourceID:   finalizeTestSource,
	}}
	if err := service.checkpointSuccessfulComputeInputs(ctx, tx, run, units, time.Now().UTC()); err == nil {
		t.Fatal("checkpointSuccessfulComputeInputs returned nil against an already-closed transaction, want a fatal error")
	}
}
