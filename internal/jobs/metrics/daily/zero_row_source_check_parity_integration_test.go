//go:build integration

package daily

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/cicd"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The source check must count exactly the rows the family keeps, read before
// the family computes. Each cell below is one way the two used to disagree.
var parityDay = time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)

type parityStack struct {
	pool *pgxpool.Pool
	conn driver.Conn
}

func startParityStack(t *testing.T, ctx context.Context) parityStack {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { postgres.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createDailyTables(t, ctx, pool)

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	for _, statement := range []string{
		"CREATE TABLE ci_pipeline_runs (`repo_id` UUID, `run_id` String, `status` Nullable(String), `queued_at` Nullable(DateTime64(3, 'UTC')), `started_at` DateTime64(3, 'UTC'), `finished_at` Nullable(DateTime64(3, 'UTC')), `last_synced` DateTime64(3, 'UTC'), `org_id` String DEFAULT 'default', `pipeline_name` Nullable(String), `provider` LowCardinality(String) DEFAULT '', `duration_seconds` Nullable(Float64), `queue_seconds` Nullable(Float64), `retry_count` UInt32 DEFAULT 0, `cancel_reason` Nullable(String), `trigger_source` Nullable(String), `commit_hash` Nullable(String), `branch` Nullable(String), `pr_number` Nullable(UInt32), `team_id` Nullable(String), `service_id` Nullable(String)) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id)",
		"CREATE TABLE cicd_metrics_daily (`repo_id` UUID, `day` Date, `pipelines_count` UInt32, `success_rate` Float64, `avg_duration_minutes` Nullable(Float64), `p90_duration_minutes` Nullable(Float64), `avg_queue_minutes` Nullable(Float64), `computed_at` DateTime('UTC'), `org_id` String DEFAULT 'default') ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)",
		"CREATE TABLE deployments (`repo_id` UUID, `deployment_id` String, `status` Nullable(String), `environment` Nullable(String), `started_at` Nullable(DateTime64(3, 'UTC')), `finished_at` Nullable(DateTime64(3, 'UTC')), `deployed_at` Nullable(DateTime64(3, 'UTC')), `merged_at` Nullable(DateTime64(3, 'UTC')), `pull_request_number` Nullable(UInt32), `pull_request_tier` LowCardinality(String) DEFAULT '', `release_ref` String DEFAULT '', `release_ref_confidence` Float64 DEFAULT 0., `last_synced` DateTime64(3, 'UTC'), `org_id` String DEFAULT 'default') ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, deployment_id)",
		"CREATE TABLE deploy_metrics_daily (`repo_id` UUID, `day` Date, `deployments_count` UInt32, `failed_deployments_count` UInt32, `deploy_time_p50_hours` Nullable(Float64), `lead_time_p50_hours` Nullable(Float64), `computed_at` DateTime('UTC'), `org_id` String DEFAULT 'default') ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)",
		"CREATE TABLE test_suite_results (`repo_id` UUID, `run_id` String, `suite_id` String, `suite_name` String, `framework` Nullable(String), `environment` Nullable(String), `total_count` UInt32, `passed_count` UInt32, `failed_count` UInt32, `skipped_count` UInt32, `error_count` UInt32 DEFAULT 0, `quarantined_count` UInt32 DEFAULT 0, `retried_count` UInt32 DEFAULT 0, `duration_seconds` Nullable(Float64), `started_at` Nullable(DateTime64(3, 'UTC')), `finished_at` Nullable(DateTime64(3, 'UTC')), `team_id` Nullable(String), `service_id` Nullable(String), `org_id` LowCardinality(String) DEFAULT '', `last_synced` DateTime64(3, 'UTC'), `report_slot` LowCardinality(String) DEFAULT '', `run_attempt` UInt32 DEFAULT 0, `legacy_suite_ids` Array(String) DEFAULT []) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id, suite_id)",
		"CREATE TABLE test_case_results (`repo_id` UUID, `run_id` String, `suite_id` String, `case_id` String, `case_name` String, `class_name` Nullable(String), `status` LowCardinality(String), `duration_seconds` Nullable(Float64), `retry_attempt` UInt32 DEFAULT 0, `failure_message` Nullable(String), `failure_type` Nullable(String), `stack_trace` Nullable(String), `is_quarantined` UInt8 DEFAULT 0, `org_id` LowCardinality(String) DEFAULT '', `last_synced` DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id, suite_id, case_id)",
		"CREATE TABLE coverage_snapshots (`repo_id` UUID, `run_id` String, `snapshot_id` String, `report_format` Nullable(String), `lines_total` Nullable(UInt32), `lines_covered` Nullable(UInt32), `line_coverage_pct` Nullable(Float64), `branches_total` Nullable(UInt32), `branches_covered` Nullable(UInt32), `branch_coverage_pct` Nullable(Float64), `functions_total` Nullable(UInt32), `functions_covered` Nullable(UInt32), `commit_hash` Nullable(String), `branch` Nullable(String), `pr_number` Nullable(UInt32), `team_id` Nullable(String), `service_id` Nullable(String), `org_id` LowCardinality(String) DEFAULT '', `last_synced` DateTime64(3, 'UTC'), `report_slot` LowCardinality(String) DEFAULT '', `run_attempt` UInt32 DEFAULT 0, `legacy_snapshot_ids` Array(String) DEFAULT []) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id, snapshot_id)",
		"CREATE TABLE testops_pipeline_stability (`repo_id` UUID, `day` Date, `stability_index` Float64, `success_rate_7d` Float64, `success_rate_trend` Float64, `failure_clustering_score` Float64, `median_recovery_time_seconds` Nullable(Float64), `team_id` Nullable(String), `service_id` Nullable(String), `org_id` LowCardinality(String) DEFAULT '', `computed_at` DateTime('UTC')) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)",
		"CREATE TABLE testops_quality_drag (`repo_id` UUID, `day` Date, `drag_hours` Nullable(Float64), `failure_rework_hours` Nullable(Float64), `flake_investigation_hours` Nullable(Float64), `queue_wait_hours` Nullable(Float64), `retry_overhead_hours` Nullable(Float64), `factors_json` String DEFAULT '{}', `team_id` Nullable(String), `service_id` Nullable(String), `org_id` LowCardinality(String) DEFAULT '', `computed_at` DateTime('UTC')) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)",
		"CREATE TABLE testops_release_confidence (`repo_id` UUID, `day` Date, `confidence_score` Float64, `pipeline_success_factor` Float64, `test_pass_factor` Float64, `coverage_factor` Float64, `flake_penalty` Float64, `regression_penalty` Float64, `factors_json` String DEFAULT '{}', `team_id` Nullable(String), `service_id` Nullable(String), `org_id` LowCardinality(String) DEFAULT '', `computed_at` DateTime('UTC')) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)",
		"CREATE TABLE incident_metrics_daily (`repo_id` UUID, `day` Date, `incidents_count` UInt32, `mttr_p50_hours` Nullable(Float64), `mttr_p90_hours` Nullable(Float64), `computed_at` DateTime('UTC'), `org_id` String DEFAULT 'default') ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)",
		`CREATE TABLE operational_incidents (
    org_id String, id String, service_id Nullable(String),
    source_revision DateTime64(6, 'UTC'), source_conflict_key String, ingest_revision UInt128,
    is_deleted UInt8, started_at Nullable(DateTime64(3, 'UTC')),
    resolved_at Nullable(DateTime64(3, 'UTC')), normalized_status String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
		`CREATE TABLE operational_service_repository_mappings (
    org_id String, id String, service_id String, repo_id Nullable(UUID),
    source_revision DateTime64(6, 'UTC'), source_conflict_key String, ingest_revision UInt128,
    is_deleted UInt8, is_active UInt8, valid_from Nullable(DateTime64(6, 'UTC')),
    valid_to Nullable(DateTime64(6, 'UTC'))
) ENGINE = ReplacingMergeTree ORDER BY (org_id, id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	applyTestopsRepositoryOwnershipSchema(ctx, t, conn)
	return parityStack{pool: pool, conn: conn}
}

type parityScope struct {
	orgID       string
	runID       string
	partitionID string
	repoIDs     []RepositoryID
	repoUUIDs   []uuid.UUID
}

func (stack parityStack) newScope(t *testing.T, ctx context.Context, repos int) parityScope {
	t.Helper()
	scope := parityScope{orgID: uuid.NewString(), runID: uuid.NewString(), partitionID: uuid.NewString()}
	for range repos {
		id := uuid.New()
		scope.repoUUIDs = append(scope.repoUUIDs, id)
		scope.repoIDs = append(scope.repoIDs, RepositoryID(id.String()))
	}
	insertPartitionScope(t, ctx, stack.pool, scope.runID, scope.partitionID, scope.orgID, parityDay.Format("2006-01-02"), scope.repoIDs)
	return scope
}

func (scope parityScope) run() Run {
	return Run{ID: scope.runID, OrganizationID: scope.orgID, Status: "running", TargetDay: parityDay}
}

func (scope parityScope) partition() Partition {
	return Partition{ID: scope.partitionID, RunID: scope.runID, RepoIDs: scope.repoIDs}
}

type pipelineRun struct {
	status     string
	nullStatus bool
	runID      string
	startedAt  time.Time
	finishedAt *time.Time
	lastSynced time.Time
}

func (stack parityStack) insertRuns(t *testing.T, ctx context.Context, orgID string, repoID uuid.UUID, runs ...pipelineRun) {
	t.Helper()
	batch, err := stack.conn.PrepareBatch(ctx, "INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, finished_at, last_synced, org_id)")
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range runs {
		status := "success"
		if run.status != "" {
			status = run.status
		}
		statusPtr := &status
		if run.nullStatus {
			statusPtr = nil
		}
		if err := batch.Append(repoID, run.runID, statusPtr, run.startedAt, run.finishedAt, run.lastSynced, orgID); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

type deploymentRow struct {
	status     *string
	deployedAt *time.Time
	startedAt  *time.Time
}

func (stack parityStack) insertDeployments(t *testing.T, ctx context.Context, orgID string, repoID uuid.UUID, rows ...deploymentRow) {
	t.Helper()
	batch, err := stack.conn.PrepareBatch(ctx, "INSERT INTO deployments (repo_id, deployment_id, status, started_at, deployed_at, last_synced, org_id)")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := batch.Append(repoID, uuid.NewString(), row.status, row.startedAt, row.deployedAt, time.Now().UTC(), orgID); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// seedTestopsOutputs keeps testops_risk out of a cell: its three outputs exist
// for every repo of the scope, so only the family under test can be flagged.
func (stack parityStack) seedTestopsOutputs(t *testing.T, ctx context.Context, scope parityScope) {
	t.Helper()
	for _, table := range []string{"testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability"} {
		for _, repoID := range scope.repoUUIDs {
			if err := stack.conn.Exec(ctx, "INSERT INTO "+table+" (org_id, repo_id, day) VALUES (?, ?, ?)", scope.orgID, repoID, parityDay); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func (stack parityStack) insertIncident(
	t *testing.T, ctx context.Context, orgID string, repoID uuid.UUID, startedAt, resolvedAt time.Time, validFrom time.Time,
) {
	t.Helper()
	serviceID := uuid.NewString()
	if err := stack.conn.Exec(ctx, `
INSERT INTO operational_incidents
    (org_id, id, service_id, source_revision, source_conflict_key, ingest_revision, is_deleted,
     started_at, resolved_at, normalized_status, last_synced)
VALUES (?, ?, ?, ?, 'source-1', 1, 0, ?, ?, 'resolved', ?)`,
		orgID, uuid.NewString(), serviceID, startedAt, startedAt, resolvedAt, resolvedAt); err != nil {
		t.Fatal(err)
	}
	if err := stack.conn.Exec(ctx, `
INSERT INTO operational_service_repository_mappings
    (org_id, id, service_id, repo_id, source_revision, source_conflict_key, ingest_revision, is_deleted, is_active, valid_from, valid_to)
VALUES (?, ?, ?, ?, ?, 'source-1', 1, 0, 1, ?, NULL)`,
		orgID, uuid.NewString(), serviceID, repoID, startedAt, validFrom); err != nil {
		t.Fatal(err)
	}
	if err := stack.conn.Exec(ctx, "INSERT INTO repos (org_id, id) VALUES (?, ?)", orgID, repoID); err != nil {
		t.Fatal(err)
	}
}

func (stack parityStack) outputExists(t *testing.T, ctx context.Context, table, orgID string) bool {
	t.Helper()
	var count uint64
	if err := stack.conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE org_id = ?", orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count > 0
}

// afterCompute runs the real family, then lands rows the way a concurrent sync
// does: after the family read its source, before the check.
type afterCompute struct {
	family NativeFamilyExecutor
	land   func(context.Context) error
}

func (executor afterCompute) ComputeFamily(ctx context.Context, run Run, partition Partition) (int, error) {
	rows, err := executor.family.ComputeFamily(ctx, run, partition)
	if err != nil {
		return rows, err
	}
	return rows, executor.land(ctx)
}

type writesNothing struct{}

func (writesNothing) ComputeFamily(context.Context, Run, Partition) (int, error) { return 0, nil }

type partitionOutcome struct {
	err      error
	store    *fakeStore
	families []string
}

func (stack parityStack) work(t *testing.T, ctx context.Context, scope parityScope, families map[string]NativeFamilyExecutor) partitionOutcome {
	t.Helper()
	checker, err := NewClickHouseSourceDataChecker(stack.pool, stack.conn)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{
		partitionClaim: &PartitionClaim{Partition: scope.partition(), Token: uuid.NewString(), LeaseDuration: 30 * time.Second},
		run:            scope.run(),
	}
	handler, err := NewPartitionHandler(store, fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetNativeFamilies(families); err != nil {
		t.Fatal(err)
	}
	observer := &recordingZeroRowsObserver{}
	handler.SetSourceDataChecker(checker)
	handler.SetZeroRowsObserver(observer)
	err = handler.Work(ctx, partitionExecutionFor(scope.partitionID, scope.runID, scope.orgID))
	return partitionOutcome{err: err, store: store, families: observer.families}
}

func (outcome partitionOutcome) wantCompleted(t *testing.T) {
	t.Helper()
	if outcome.err != nil || outcome.store.partitionCompletions != 1 || len(outcome.families) != 0 {
		t.Fatalf("Work err=%v completions=%d flagged=%v, want completed with no flag",
			outcome.err, outcome.store.partitionCompletions, outcome.families)
	}
}

func (outcome partitionOutcome) wantZeroRowsCancel(t *testing.T, families ...string) {
	t.Helper()
	if !errors.Is(outcome.err, ErrZeroRowsWithSourceData) || !strings.Contains(outcome.err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work err=%v, want permanent zero_rows_with_source_data", outcome.err)
	}
	if outcome.store.partitionCompletions != 0 || !slices.Equal(outcome.families, families) {
		t.Fatalf("completions=%d flagged=%v, want 0 and %v", outcome.store.partitionCompletions, outcome.families, families)
	}
}

// probeSourceFamilies runs the check while every output of the scope is still
// empty, so a flagged family means "the check sees source rows".
func probeSourceFamilies(t *testing.T, ctx context.Context, stack parityStack, scope parityScope) []string {
	t.Helper()
	checker, err := NewClickHouseSourceDataChecker(stack.pool, stack.conn)
	if err != nil {
		t.Fatal(err)
	}
	families, err := zeroRowFamiliesNow(ctx, checker, scope.partitionID)
	if err != nil {
		t.Fatal(err)
	}
	return families
}

func at(clock string) time.Time {
	parsed, err := time.Parse("2006-01-02 15:04:05.000", clock)
	if err != nil {
		panic(err)
	}
	return parsed
}

func ptrString(value string) *string { return &value }

func TestZeroRowSourceCheckMatchesWhatTheFamilyKeeps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	stack := startParityStack(t, ctx)

	cicdExecutor, err := NewCICDExecutor(stack.conn)
	if err != nil {
		t.Fatal(err)
	}
	deployExecutor, err := NewDeployExecutor(stack.conn)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("C1_deployment_lands_between_family_read_and_check", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"deploy": afterCompute{
			family: deployExecutor,
			land: func(context.Context) error {
				stack.insertDeployments(t, ctx, scope.orgID, scope.repoUUIDs[0],
					deploymentRow{status: ptrString("success"), deployedAt: ptrTime(at("2026-08-25 01:32:56.000"))})
				return nil
			},
		}})
		outcome.wantCompleted(t)
	})

	t.Run("C2_pipeline_run_lands_between_family_read_and_check", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		stack.seedTestopsOutputs(t, ctx, scope)
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"cicd": afterCompute{
			family: cicdExecutor,
			land: func(context.Context) error {
				stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
					runID: "late", startedAt: at("2026-08-25 00:10:00.000"),
					finishedAt: ptrTime(at("2026-08-25 00:20:00.000")), lastSynced: at("2026-08-25 00:21:00.000"),
				})
				return nil
			},
		}})
		outcome.wantCompleted(t)
	})

	t.Run("C3_only_canceled_deployments", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		stack.insertDeployments(t, ctx, scope.orgID, scope.repoUUIDs[0],
			deploymentRow{status: ptrString("cancelled"), deployedAt: ptrTime(at("2026-08-25 09:00:00.000"))},
			deploymentRow{status: ptrString(" Canceled "), deployedAt: ptrTime(at("2026-08-25 10:00:00.000"))})
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"deploy": deployExecutor})
		outcome.wantCompleted(t)
	})

	t.Run("C4_old_run_version_the_FINAL_read_replaced", func(t *testing.T) {
		if err := stack.conn.Exec(ctx, "SYSTEM STOP MERGES ci_pipeline_runs"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stack.conn.Exec(context.Background(), "SYSTEM START MERGES ci_pipeline_runs") })
		scope := stack.newScope(t, ctx, 1)
		stack.seedTestopsOutputs(t, ctx, scope)
		stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
			runID: "rerun", startedAt: at("2026-08-25 10:00:00.000"),
			finishedAt: ptrTime(at("2026-08-25 11:00:00.000")), lastSynced: at("2026-08-25 11:00:00.000"),
		})
		stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
			runID: "rerun", startedAt: at("2026-08-25 10:00:00.000"),
			finishedAt: ptrTime(at("2026-08-26 00:30:00.000")), lastSynced: at("2026-08-26 00:30:00.000"),
		})
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"cicd": cicdExecutor})
		outcome.wantCompleted(t)
	})

	t.Run("C5_incident_mapping_valid_between_the_two_as_of_instants", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		now := time.Now().UTC()
		stack.insertIncident(t, ctx, scope.orgID, scope.repoUUIDs[0],
			at("2026-08-25 01:00:00.000"), at("2026-08-25 02:00:00.000"), now.Add(-time.Minute))
		incidentExecutor, err := NewIncidentExecutor(stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		incidentExecutor.nowUTC = func() time.Time { return now.Add(-time.Hour) }
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"incident": incidentExecutor})
		outcome.wantCompleted(t)
	})

	t.Run("C6_guard_fires_when_the_family_writes_nothing_over_kept_rows", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		stack.seedTestopsOutputs(t, ctx, scope)
		stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
			runID: "kept", startedAt: at("2026-08-25 10:00:00.000"),
			finishedAt: ptrTime(at("2026-08-25 10:30:00.000")), lastSynced: at("2026-08-25 10:31:00.000"),
		})
		stack.insertDeployments(t, ctx, scope.orgID, scope.repoUUIDs[0],
			deploymentRow{status: ptrString("success"), deployedAt: ptrTime(at("2026-08-25 12:00:00.000"))})
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"cicd": writesNothing{}, "deploy": writesNothing{}})
		outcome.wantZeroRowsCancel(t, "cicd", "deploy")
	})

	t.Run("C12_old_run_version_the_testops_FINAL_read_replaced", func(t *testing.T) {
		if err := stack.conn.Exec(ctx, "SYSTEM STOP MERGES ci_pipeline_runs"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stack.conn.Exec(context.Background(), "SYSTEM START MERGES ci_pipeline_runs") })
		scope := stack.newScope(t, ctx, 1)
		stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
			runID: "moved", startedAt: at("2026-08-25 10:00:00.000"),
			finishedAt: ptrTime(at("2026-08-25 11:00:00.000")), lastSynced: at("2026-08-25 11:00:00.000"),
		})
		stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
			runID: "moved", startedAt: at("2026-08-24 10:00:00.000"),
			finishedAt: ptrTime(at("2026-08-24 11:00:00.000")), lastSynced: at("2026-08-26 00:30:00.000"),
		})
		testopsRisk, err := NewTestopsRiskExecutor(stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"testops_risk": testopsRisk})
		outcome.wantCompleted(t)
	})

	t.Run("C10_cross_day_runs_read_then_a_run_lands", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		stack.seedTestopsOutputs(t, ctx, scope)
		stack.insertCrossDayRuns(t, ctx, scope)
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"cicd": afterCompute{
			family: cicdExecutor,
			land: func(context.Context) error {
				stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
					runID: "landed", startedAt: at("2026-08-25 00:10:00.000"),
					finishedAt: ptrTime(at("2026-08-25 00:20:00.000")), lastSynced: at("2026-08-25 00:21:00.000"),
				})
				return nil
			},
		}})
		outcome.wantCompleted(t)
	})

	t.Run("C10n_cross_day_runs_alone_never_flag", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 1)
		stack.seedTestopsOutputs(t, ctx, scope)
		stack.insertCrossDayRuns(t, ctx, scope)
		outcome := stack.work(t, ctx, scope, map[string]NativeFamilyExecutor{"cicd": cicdExecutor})
		outcome.wantCompleted(t)
	})

	t.Run("C11_source_predicate_equals_kept_rows", func(t *testing.T) {
		cicdCases := []struct {
			name string
			run  pipelineRun
			kept bool
		}{
			{"started_day_before_finished_in_day", pipelineRun{startedAt: at("2026-08-24 23:59:59.999"), finishedAt: ptrTime(at("2026-08-25 00:00:01.000"))}, false},
			{"started_and_finished_at_day_edges", pipelineRun{startedAt: at("2026-08-25 00:00:00.000"), finishedAt: ptrTime(at("2026-08-25 23:59:59.999"))}, true},
			{"finished_at_next_day_start", pipelineRun{startedAt: at("2026-08-25 12:00:00.000"), finishedAt: ptrTime(at("2026-08-26 00:00:00.000"))}, false},
			{"still_running", pipelineRun{startedAt: at("2026-08-25 12:00:00.000")}, false},
		}
		for _, testCase := range cicdCases {
			t.Run("cicd_"+testCase.name, func(t *testing.T) {
				scope := stack.newScope(t, ctx, 1)
				testCase.run.runID, testCase.run.lastSynced = "run", at("2026-08-26 01:00:00.000")
				stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], testCase.run)
				stack.assertKeptParity(t, ctx, scope, "cicd", "cicd_metrics_daily", cicdExecutor, testCase.kept)
			})
		}
		deletedRun := func(runID, status string, nullStatus bool) pipelineRun {
			return pipelineRun{
				runID: runID, status: status, nullStatus: nullStatus,
				startedAt: at("2026-08-25 10:00:00.000"), finishedAt: ptrTime(at("2026-08-25 11:00:00.000")),
				lastSynced: at("2026-08-26 01:00:00.000"),
			}
		}
		testopsRiskExecutor, err := NewTestopsRiskExecutor(stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		stack.createBaselineTable(t, ctx, "testops_pipeline_metrics_daily")
		testopsPipelineExecutor, err := NewTestopsPipelineExecutor(stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		deletedCases := []struct {
			name string
			runs []pipelineRun
			kept bool
		}{
			{"deleted_only_day", []pipelineRun{deletedRun("gone", "deleted", false)}, false},
			{"deleted_mixed_case_padded_only_day", []pipelineRun{deletedRun("gone", " Deleted ", false)}, false},
			{"null_status_day", []pipelineRun{deletedRun("unknown", "", true)}, true},
			{"deleted_and_completed_day", []pipelineRun{deletedRun("gone", "deleted", false), deletedRun("done", "success", false)}, true},
		}
		for _, testCase := range deletedCases {
			t.Run("cicd_"+testCase.name, func(t *testing.T) {
				scope := stack.newScope(t, ctx, 1)
				stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], testCase.runs...)
				stack.assertKeptParity(t, ctx, scope, "cicd", "cicd_metrics_daily", cicdExecutor, testCase.kept)
			})
			t.Run("testops_pipeline_"+testCase.name, func(t *testing.T) {
				scope := stack.newScope(t, ctx, 1)
				stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], testCase.runs...)
				stack.assertTestopsPipelineKept(t, ctx, scope, testopsPipelineExecutor, testCase.kept)
			})
			t.Run("testops_risk_"+testCase.name, func(t *testing.T) {
				scope := stack.newScope(t, ctx, 1)
				stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], testCase.runs...)
				stack.assertKeptParity(t, ctx, scope, "testops_risk", "testops_release_confidence", testopsRiskExecutor, testCase.kept)
			})
		}
		deployCases := []struct {
			name string
			row  deploymentRow
			kept bool
		}{
			{"success_at_day_start", deploymentRow{status: ptrString("success"), deployedAt: ptrTime(at("2026-08-25 00:00:00.000"))}, true},
			{"at_next_day_start", deploymentRow{status: ptrString("success"), deployedAt: ptrTime(at("2026-08-26 00:00:00.000"))}, false},
			{"just_before_day", deploymentRow{status: ptrString("success"), deployedAt: ptrTime(at("2026-08-24 23:59:59.999"))}, false},
			{"cancelled", deploymentRow{status: ptrString("cancelled"), deployedAt: ptrTime(at("2026-08-25 08:00:00.000"))}, false},
			{"canceled_with_tab", deploymentRow{status: ptrString("canceled\t"), deployedAt: ptrTime(at("2026-08-25 08:00:00.000"))}, false},
			{"null_deployed_at_started_in_day", deploymentRow{status: ptrString("success"), startedAt: ptrTime(at("2026-08-25 08:00:00.000"))}, false},
			{"null_status", deploymentRow{deployedAt: ptrTime(at("2026-08-25 08:00:00.000"))}, true},
		}
		for _, testCase := range deployCases {
			t.Run("deploy_"+testCase.name, func(t *testing.T) {
				scope := stack.newScope(t, ctx, 1)
				stack.insertDeployments(t, ctx, scope.orgID, scope.repoUUIDs[0], testCase.row)
				stack.assertKeptParity(t, ctx, scope, "deploy", "deploy_metrics_daily", deployExecutor, testCase.kept)
			})
		}
		incidentExecutor, err := NewIncidentExecutor(stack.conn)
		if err != nil {
			t.Fatal(err)
		}
		incidentCases := []struct {
			name                string
			startedAt, resolved time.Time
			kept                bool
		}{
			{"started_and_resolved_in_day", at("2026-08-25 01:00:00.000"), at("2026-08-25 02:00:00.000"), true},
			{"resolved_next_day", at("2026-08-25 23:00:00.000"), at("2026-08-26 00:00:00.000"), false},
			{"started_day_before", at("2026-08-24 23:00:00.000"), at("2026-08-25 01:00:00.000"), false},
		}
		for _, testCase := range incidentCases {
			t.Run("incident_"+testCase.name, func(t *testing.T) {
				scope := stack.newScope(t, ctx, 1)
				stack.insertIncident(t, ctx, scope.orgID, scope.repoUUIDs[0], testCase.startedAt, testCase.resolved, at("2026-08-01 00:00:00.000"))
				stack.assertKeptParity(t, ctx, scope, "incident", "incident_metrics_daily", incidentExecutor, testCase.kept)
			})
		}
	})

	t.Run("C9_census_check_reads_each_family_source_predicate", func(t *testing.T) {
		scope := stack.newScope(t, ctx, 2)
		recorder := &recordingConn{Conn: stack.conn}
		checker, err := NewClickHouseSourceDataChecker(stack.pool, recorder)
		if err != nil {
			t.Fatal(err)
		}
		asOf := time.Now().UTC().Truncate(time.Microsecond)
		sourceCtx := withSourceAsOf(ctx, asOf)
		if _, err := checker.SourceSnapshot(sourceCtx, scope.partitionID, asOf); err != nil {
			t.Fatal(err)
		}
		checks := recorder.take()

		loaderFamilies := map[string]NativeFamilyExecutor{}
		if loaderFamilies["cicd"], err = NewCICDExecutor(recorder); err != nil {
			t.Fatal(err)
		}
		if loaderFamilies["deploy"], err = NewDeployExecutor(recorder); err != nil {
			t.Fatal(err)
		}
		if loaderFamilies["incident"], err = NewIncidentExecutor(recorder); err != nil {
			t.Fatal(err)
		}
		if loaderFamilies["testops_risk"], err = NewTestopsRiskExecutor(recorder); err != nil {
			t.Fatal(err)
		}
		contract, err := remaining.ConfiguredOperationalOrderingContract()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range []struct {
			name, check, loaderSource string
			args                      []string
		}{
			{"cicd", "SELECT 1" + cicd.PipelineRunsSource + "\nLIMIT 1", cicd.PipelineRunsSource, []string{"org_id", "repo_ids", "start", "end"}},
			{"deploy", "SELECT 1" + deploySource + "\nLIMIT 1", deploySource, []string{"org_id", "repo_ids", "start", "end", "canceled_statuses"}},
			{"incident", incidentSourceExistsSQL(contract), incidentsStartedSQL(contract), []string{"org_id", "repo_ids", "start", "end", "as_of"}},
			// testops reads one repository per query, so repo_ids differs by design.
			{"testops_risk", "SELECT 1" + testopsPipelineRunsSource + "\nLIMIT 1", testopsPipelineRunsSource, []string{"org_id", "start", "end"}},
			{"testops_risk", "SELECT 1" + testopsSuiteResultsSource + "\nLIMIT 1", testopsSuiteResultsSource, []string{"org_id", "start", "end"}},
		} {
			t.Run(family.name, func(t *testing.T) {
				if _, err := loaderFamilies[family.name].ComputeFamily(sourceCtx, scope.run(), scope.partition()); err != nil {
					t.Fatal(err)
				}
				check := findQuery(checks, family.check)
				loader := findQueryContaining(recorder.take(), family.loaderSource)
				if check == nil || loader == nil {
					t.Fatalf("check query found=%t, loader query on the shared source found=%t", check != nil, loader != nil)
				}
				for _, name := range family.args {
					if check.args[name] == "" || check.args[name] != loader.args[name] {
						t.Errorf("argument %s: check=%q loader=%q", name, check.args[name], loader.args[name])
					}
				}
			})
		}
		deployCheck := findQuery(checks, "SELECT 1"+deploySource+"\nLIMIT 1")
		if want := fmt.Sprint(deploymentstatus.CanceledStatuses); deployCheck == nil || deployCheck.args["canceled_statuses"] != want {
			t.Fatalf("deploy check does not bind deploymentstatus.CanceledStatuses (%s)", want)
		}
	})

	t.Run("C7p_deploy_status_forms_sql_keep_equals_classify_keep", func(t *testing.T) {
		for _, status := range deployStatusForms() {
			scope := stack.newScope(t, ctx, 1)
			row := deploymentRow{status: status, deployedAt: ptrTime(at("2026-08-25 08:00:00.000"))}
			stack.insertDeployments(t, ctx, scope.orgID, scope.repoUUIDs[0], row)
			goKeeps := status == nil || deploymentstatus.Classify(*status) != deploymentstatus.Canceled
			sqlKeeps := slices.Contains(probeSourceFamilies(t, ctx, stack, scope), "deploy")
			if sqlKeeps != goKeeps {
				t.Errorf("status %s: source check keeps=%t, Classify keeps=%t", describeStatus(status), sqlKeeps, goKeeps)
			}
		}
	})
}

type recordedQuery struct {
	query string
	args  map[string]string
}

// recordingConn records each read with its named arguments, then runs it.
type recordingConn struct {
	driver.Conn
	queries []recordedQuery
}

func (conn *recordingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	recorded := recordedQuery{query: query, args: map[string]string{}}
	for _, arg := range args {
		if named, ok := arg.(driver.NamedValue); ok {
			recorded.args[named.Name] = fmt.Sprint(named.Value)
		}
	}
	conn.queries = append(conn.queries, recorded)
	return conn.Conn.Query(ctx, query, args...)
}

func (conn *recordingConn) take() []recordedQuery {
	taken := conn.queries
	conn.queries = nil
	return taken
}

func findQuery(queries []recordedQuery, query string) *recordedQuery {
	for index := range queries {
		if queries[index].query == query {
			return &queries[index]
		}
	}
	return nil
}

func findQueryContaining(queries []recordedQuery, fragment string) *recordedQuery {
	for index := range queries {
		if strings.Contains(queries[index].query, fragment) {
			return &queries[index]
		}
	}
	return nil
}

func (stack parityStack) insertCrossDayRuns(t *testing.T, ctx context.Context, scope parityScope) {
	t.Helper()
	for index, clock := range []string{"23:51", "23:53", "23:55", "23:57"} {
		stack.insertRuns(t, ctx, scope.orgID, scope.repoUUIDs[0], pipelineRun{
			runID: "cross-day-" + string(rune('a'+index)), startedAt: at("2026-08-24 " + clock + ":00.000"),
			finishedAt: ptrTime(at("2026-08-25 00:0" + string(rune('1'+index)) + ":00.000")), lastSynced: at("2026-08-25 00:30:00.000"),
		})
	}
}

// assertKeptParity checks, for one family and one repo, that the check sees a
// source row exactly when the real family writes a row for it.
func (stack parityStack) assertKeptParity(
	t *testing.T, ctx context.Context, scope parityScope, family, outputTable string, executor NativeFamilyExecutor, kept bool,
) {
	t.Helper()
	sourceSeen := slices.Contains(probeSourceFamilies(t, ctx, stack, scope), family)
	if _, err := executor.ComputeFamily(ctx, scope.run(), scope.partition()); err != nil {
		t.Fatal(err)
	}
	written := stack.outputExists(t, ctx, outputTable, scope.orgID)
	if written != kept {
		t.Fatalf("fixture: %s wrote=%t, want %t", family, written, kept)
	}
	if sourceSeen != written {
		t.Fatalf("%s: source check sees=%t, family wrote=%t", family, sourceSeen, written)
	}
}

func (stack parityStack) createBaselineTable(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range baseline.Objects {
		if object.Name == name {
			if err := stack.conn.Exec(ctx, object.Create); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("%s is not in the baseline", name)
}

// The testops pipeline family has no zero-row flag of its own: its source read
// (shared with testops_risk) must still see exactly the runs it keeps.
func (stack parityStack) assertTestopsPipelineKept(
	t *testing.T, ctx context.Context, scope parityScope, executor NativeFamilyExecutor, kept bool,
) {
	t.Helper()
	checker, err := NewClickHouseSourceDataChecker(stack.pool, stack.conn)
	if err != nil {
		t.Fatal(err)
	}
	sourceSeen, err := checker.existsWith(ctx, "SELECT 1"+testopsPipelineRunsSource+"\nLIMIT 1",
		testopsSourceArgs(scope.orgID, scope.repoUUIDs, parityDay, parityDay.AddDate(0, 0, 1))...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(ctx, scope.run(), scope.partition()); err != nil {
		t.Fatal(err)
	}
	written := stack.outputExists(t, ctx, "testops_pipeline_metrics_daily", scope.orgID)
	if written != kept {
		t.Fatalf("fixture: testops_pipeline wrote=%t, want %t", written, kept)
	}
	if sourceSeen != written {
		t.Fatalf("testops_pipeline: source read sees=%t, family wrote=%t", sourceSeen, written)
	}
}

func deployStatusForms() []*string {
	spaces := []string{"", " ", "\t", "\n", "\r", "\v", "\f", "\u0085", " ", "　", " \t"}
	forms := []*string{nil}
	for _, alias := range []string{"canceled", "cancelled"} {
		for _, spelled := range []string{alias, strings.ToUpper(alias), strings.ToUpper(alias[:1]) + alias[1:]} {
			for _, space := range spaces {
				forms = append(forms, ptrString(space+spelled+space), ptrString(spelled+space))
			}
		}
	}
	for _, other := range []string{"can celed", "canceled!", "success", "SUCCESS", " failure ", "failed", "error", "inactive", "blocked", "pending", "weird", ""} {
		forms = append(forms, ptrString(other))
	}
	return forms
}

func describeStatus(status *string) string {
	if status == nil {
		return "NULL"
	}
	return strings.NewReplacer("\t", `\t`, "\n", `\n`, "\r", `\r`, "\v", `\v`, "\f", `\f`).Replace(`"` + *status + `"`)
}
