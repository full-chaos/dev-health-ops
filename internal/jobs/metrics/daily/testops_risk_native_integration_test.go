//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTestopsRiskExecutorComputeFamilyWritesAllThreeTablesAgainstRealClickHouse
// is a real-ClickHouse proof that TestopsRiskExecutor.ComputeFamily (CHAOS-4294)
// reads the raw testops tables (ci_pipeline_runs, test_suite_results,
// test_case_results, coverage_snapshots) and writes all three testops_risk
// output tables through the real production entry point -- not a direct
// compute-function call, so it also proves the SQL loaders (JOIN/semi-join
// shape, org/repo/day scoping) and the ClickHouse batch writers round-trip
// correctly, which TestTestopsRiskComputeMatchesLivePythonProduction
// (the live-Python-oracle test) does not exercise -- that test feeds
// pre-built Go row structs directly, bypassing SQL entirely.
func TestTestopsRiskExecutorComputeFamilyWritesAllThreeTablesAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range []string{
		// Matches 000_raw_tables.sql's ci_pipeline_runs plus every column
		// 029_testops_tables.sql ALTERs onto it.
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    pipeline_name Nullable(String), provider LowCardinality(String) DEFAULT '',
    duration_seconds Nullable(Float64), queue_seconds Nullable(Float64),
    retry_count UInt32 DEFAULT 0, cancel_reason Nullable(String), trigger_source Nullable(String),
    commit_hash Nullable(String), branch Nullable(String), pr_number Nullable(UInt32),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
		`CREATE TABLE test_suite_results (
    repo_id UUID, run_id String, suite_id String, suite_name String,
    framework Nullable(String), environment Nullable(String),
    total_count UInt32, passed_count UInt32, failed_count UInt32, skipped_count UInt32,
    error_count UInt32 DEFAULT 0, quarantined_count UInt32 DEFAULT 0, retried_count UInt32 DEFAULT 0,
    duration_seconds Nullable(Float64), started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')), team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id, suite_id)`,
		`CREATE TABLE test_case_results (
    repo_id UUID, run_id String, suite_id String, case_id String, case_name String,
    class_name Nullable(String), status LowCardinality(String), duration_seconds Nullable(Float64),
    retry_attempt UInt32 DEFAULT 0, failure_message Nullable(String), failure_type Nullable(String),
    stack_trace Nullable(String), is_quarantined UInt8 DEFAULT 0,
    org_id LowCardinality(String) DEFAULT '', last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id, suite_id, case_id)`,
		`CREATE TABLE coverage_snapshots (
    repo_id UUID, run_id String, snapshot_id String, report_format Nullable(String),
    lines_total Nullable(UInt32), lines_covered Nullable(UInt32), line_coverage_pct Nullable(Float64),
    branches_total Nullable(UInt32), branches_covered Nullable(UInt32), branch_coverage_pct Nullable(Float64),
    functions_total Nullable(UInt32), functions_covered Nullable(UInt32),
    commit_hash Nullable(String), branch Nullable(String), pr_number Nullable(UInt32),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id, snapshot_id)`,
		`CREATE TABLE testops_release_confidence (
    repo_id UUID, day Date, confidence_score Float64, pipeline_success_factor Float64,
    test_pass_factor Float64, coverage_factor Float64, flake_penalty Float64, regression_penalty Float64,
    factors_json String DEFAULT '{}', team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		`CREATE TABLE testops_quality_drag (
    repo_id UUID, day Date, drag_hours Float64, failure_rework_hours Float64,
    flake_investigation_hours Float64, queue_wait_hours Float64, retry_overhead_hours Float64,
    factors_json String DEFAULT '{}', team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		`CREATE TABLE testops_pipeline_stability (
    repo_id UUID, day Date, stability_index Float64, success_rate_7d Float64, success_rate_trend Float64,
    failure_clustering_score Float64, median_recovery_time_seconds Nullable(Float64),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const orgID = "00000000-0000-4000-8000-000000000009"
	repoID := "00000000-0000-4000-8000-0000000000a1"

	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id) VALUES
(toUUID('`+repoID+`'), 'run-1', 'success', toDateTime64('2026-08-15 09:00:00', 3, 'UTC'), toDateTime64('2026-08-15 09:01:00', 3, 'UTC'), toDateTime64('2026-08-15 09:11:00', 3, 'UTC'), now64(3), 0, '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced) VALUES
(toUUID('`+repoID+`'), 'run-1', 'suite-1', 'unit', 10, 9, 1, 0, toDateTime64('2026-08-15 09:01:00', 3, 'UTC'), toDateTime64('2026-08-15 09:05:00', 3, 'UTC'), '`+orgID+`', now64(3))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO test_case_results (repo_id, run_id, suite_id, case_id, case_name, status, org_id, last_synced) VALUES
(toUUID('`+repoID+`'), 'run-1', 'suite-1', 'c1', 'test_one', 'passed', '`+orgID+`', now64(3))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO coverage_snapshots (repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, org_id, last_synced) VALUES
(toUUID('`+repoID+`'), 'run-1', 'snap-1', 1000, 900, 90.0, '`+orgID+`', now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewTestopsRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)}
	partition := Partition{
		ID:      "00000000-0000-4000-8000-000000000121",
		RunID:   "00000000-0000-4000-8000-000000000120",
		RepoIDs: []RepositoryID{RepositoryID(repoID)},
	}

	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatal(err)
	}
	// 1 release_confidence + 1 quality_drag + 1 pipeline_stability row.
	if written != 3 {
		t.Fatalf("written=%d, want 3", written)
	}

	assertOneRow(ctx, t, conn, "testops_release_confidence", orgID)
	assertOneRow(ctx, t, conn, "testops_quality_drag", orgID)
	assertOneRow(ctx, t, conn, "testops_pipeline_stability", orgID)

	var confidenceScore float64
	rows, err := conn.Query(ctx, `SELECT confidence_score FROM testops_release_confidence WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no testops_release_confidence row")
	}
	if err := rows.Scan(&confidenceScore); err != nil {
		t.Fatal(err)
	}
	// success_rate=1.0, pass_rate=1.0, coverage=90 -> pipeline_factor=0.4,
	// test_factor=0.3, cov_factor=0.2*0.9=0.18, flake_factor=0.1*(1-0)=0.1;
	// base=0.98; no flake/regression penalty -> confidence_score=0.98.
	if confidenceScore < 0.97 || confidenceScore > 0.99 {
		t.Fatalf("confidence_score=%v, want ~0.98", confidenceScore)
	}
}

func assertOneRow(ctx context.Context, t *testing.T, conn driver.Conn, table, orgID string) {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+` WHERE org_id = ?`, orgID).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if count != 1 {
		t.Fatalf("%s row count=%d, want 1", table, count)
	}
}
