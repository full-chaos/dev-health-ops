//go:build integration

package daily

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTestopsRiskExecutorStreamsPastTheOldRowCapAgainstRealClickHouse is the
// regression proof for the removed test_case_results materialisation: a
// single (org, repo, day) partition with MORE test_case_results rows than
// the old DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS default (200,000) must still
// compute successfully, with that env var left UNSET (the production
// default), through the exact same ComputeFamily entry point
// PartitionHandler calls.
//
// Before the fix this fixture would have made loadTestopsSuiteAndCaseRows
// read past the cap and return errTestopsRowCapExceeded, which
// ComputeFamily returned as a hard failure for the whole partition. The
// production path now streams test_case_results into a
// testops.TestAccumulator via a GROUP BY case_name pushdown
// (loadNativeTestopsCaseGroups), so peak memory is bounded by the distinct
// case_name count, not the row count, and there is no cap left to trip.
func TestTestopsRiskExecutorStreamsPastTheOldRowCapAgainstRealClickHouse(t *testing.T) {
	if _, isSet := os.LookupEnv("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS"); isSet {
		t.Fatal("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS must be unset for this test to prove the default-env behavior")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
		`CREATE TABLE teams (
    id String, name String, members Array(String), repo_patterns Array(String), org_id String
) ENGINE = ReplacingMergeTree ORDER BY (id)`,
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
    repo_id UUID, day Date, drag_hours Nullable(Float64), failure_rework_hours Nullable(Float64),
    flake_investigation_hours Nullable(Float64), queue_wait_hours Nullable(Float64), retry_overhead_hours Nullable(Float64),
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
	repoID := "00000000-0000-4000-8000-0000000000c1"
	day := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	started := day.Add(9 * time.Hour)

	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id) VALUES
(toUUID('`+repoID+`'), 'run-1', 'success', ?, ?, ?, now64(3), 0, '`+orgID+`')`,
		started, started, started.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// A repo-day this size (~210k test_case_results rows) is exactly the
	// shape production hit: comfortably above the old 200k default cap, but
	// realistic for a single busy CI day (the ticket's own prod numbers ran
	// as high as 759k rows/day).
	const rowCount = 210_000
	if err := conn.Exec(ctx, `
INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced) VALUES
(toUUID('`+repoID+`'), 'run-1', 'suite-1', 'big-suite', ?, ?, ?, 0, ?, ?, '`+orgID+`', now64(3))`,
		uint32(rowCount), uint32(rowCount-1), uint32(1), started, started.Add(9*time.Minute)); err != nil {
		t.Fatal(err)
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	synced := started.Add(9 * time.Minute)
	repoUUID := uuid.MustParse(repoID)
	for i := 0; i < rowCount; i++ {
		status := "passed"
		if i == 0 {
			status = "failed" // matches the suite's own failed_count=1
		}
		caseID := fmt.Sprintf("c%d", i)
		if err := batch.Append(
			repoUUID, "run-1", "suite-1", caseID, caseID, status, uint32(0), orgID, synced,
		); err != nil {
			t.Fatalf("append test_case_results row %d: %v", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send test_case_results batch: %v", err)
	}

	var seeded uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM test_case_results WHERE org_id = ? AND repo_id = toUUID(?)`, orgID, repoID).Scan(&seeded); err != nil {
		t.Fatal(err)
	}
	if seeded <= 200_000 {
		t.Fatalf("seeded %d test_case_results rows, want > 200,000 -- the fixture would not have tripped the old cap", seeded)
	}

	executor, err := NewTestopsRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: day}
	partition := Partition{
		ID:      "00000000-0000-4000-8000-000000000141",
		RunID:   "00000000-0000-4000-8000-000000000140",
		RepoIDs: []RepositoryID{RepositoryID(repoID)},
	}

	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatalf("ComputeFamily failed on a %d-row test_case_results day with the default env: %v", rowCount, err)
	}
	if written != 3 {
		t.Fatalf("written=%d, want 3 (release_confidence + quality_drag + pipeline_stability)", written)
	}

	assertOneRow(ctx, t, conn, "testops_release_confidence", orgID)
	assertOneRow(ctx, t, conn, "testops_quality_drag", orgID)
	assertOneRow(ctx, t, conn, "testops_pipeline_stability", orgID)
}
