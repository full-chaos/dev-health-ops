//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

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
func TestTestopsRiskTestsUnknownAgainstRealClickHouse(t *testing.T) {
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
    org_id LowCardinality(String) DEFAULT '', last_synced DateTime64(3, 'UTC'),
    report_slot LowCardinality(String) DEFAULT '', run_attempt UInt32 DEFAULT 0, legacy_suite_ids Array(String) DEFAULT []
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
    last_synced DateTime64(3, 'UTC'),
    report_slot LowCardinality(String) DEFAULT '', run_attempt UInt32 DEFAULT 0, legacy_snapshot_ids Array(String) DEFAULT []
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id, snapshot_id)`,
		`CREATE TABLE testops_release_confidence (
    repo_id UUID, day Date, confidence_score Float64, pipeline_success_factor Float64,
    test_pass_factor Float64, coverage_factor Float64, flake_penalty Float64, regression_penalty Float64,
    factors_json String DEFAULT '{}', team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		// drag_hours/failure_rework_hours/flake_investigation_hours/
		// queue_wait_hours/retry_overhead_hours are Nullable(Float64) since
		// migration 091_testops_quality_drag_nullable_fields.sql (CHAOS-4806,
		// ruling R73) -- a plain Float64 here would refuse the writer's
		// *float64 nil writes with a real ClickHouse error instead of the
		// production schema's actual NULL.
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
	applyTestopsRepositoryOwnershipSchema(ctx, t, conn)

	const orgID = "00000000-0000-4000-8000-000000000009"
	day := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	repos := map[string]string{
		"history":      "00000000-0000-4000-8000-0000000000a1", // a suite on D-1, none on D
		"none":         "00000000-0000-4000-8000-0000000000b2", // CI runs, no suite ever
		"old":          "00000000-0000-4000-8000-0000000000c3", // last suite on D-30
		"marker":       "00000000-0000-4000-8000-0000000000d4", // no history, a run of D named partial, synced D+1
		"markerOK":     "00000000-0000-4000-8000-0000000000e5", // no history, a run of D complete
		"markerOtherD": "00000000-0000-4000-8000-0000000000f6", // no history, a partial run of D-1 only
	}
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TABLE testops_run_report_completeness (org_id LowCardinality(String), repo_id UUID, run_id String, run_attempt UInt32, provider LowCardinality(String), complete UInt8, reasons Array(LowCardinality(String)), reports_expected UInt32, reports_admitted UInt32, last_synced DateTime64(3,'UTC'), verdict_version UInt64) ENGINE = ReplacingMergeTree(verdict_version) ORDER BY (org_id, repo_id, run_id, run_attempt)`)
	for _, repo := range repos {
		for _, d := range []string{"2026-08-14", "2026-08-15"} {
			exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id) VALUES (toUUID('` + repo + `'), 'run-` + d + `', 'success', toDateTime64('` + d + ` 09:00:00', 3, 'UTC'), toDateTime64('` + d + ` 09:01:00', 3, 'UTC'), toDateTime64('` + d + ` 09:11:00', 3, 'UTC'), now64(3), 0, '` + orgID + `')`)
		}
	}
	suite := func(repo, suiteDay string) {
		exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced) VALUES (toUUID('` + repo + `'), 'old-` + suiteDay + `', 's1', 'unit', 10, 9, 1, 0, toDateTime64('` + suiteDay + ` 09:01:00', 3, 'UTC'), toDateTime64('` + suiteDay + ` 09:05:00', 3, 'UTC'), '` + orgID + `', toDateTime64('` + suiteDay + ` 10:00:00', 3, 'UTC'))`)
	}
	suite(repos["history"], "2026-08-14")
	suite(repos["old"], "2026-07-16")
	mark := func(repo, runDay string, complete int, synced string) {
		exec(fmt.Sprintf(`INSERT INTO testops_run_report_completeness (org_id, repo_id, run_id, run_attempt, provider, complete, reasons, reports_expected, reports_admitted, last_synced, verdict_version) VALUES ('%s', toUUID('%s'), 'run-%s', 1, 'github', %d, [], 2, 1, toDateTime64('%s 01:00:00', 3, 'UTC'), %d)`, orgID, repo, runDay, complete, synced, complete))
	}
	mark(repos["marker"], "2026-08-15", 0, "2026-08-16")
	mark(repos["markerOK"], "2026-08-15", 1, "2026-08-15")
	mark(repos["markerOtherD"], "2026-08-14", 0, "2026-08-15")
	ids := []RepositoryID{}
	for _, repo := range repos {
		seedTestopsAuthoritativeOwner(ctx, t, conn, orgID, uuid.MustParse(repo), day)
		ids = append(ids, RepositoryID(repo))
	}
	executor, err := NewTestopsRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(ctx, Run{OrganizationID: orgID, TargetDay: day},
		Partition{ID: "00000000-0000-4000-8000-000000000121", RunID: "00000000-0000-4000-8000-000000000120", RepoIDs: ids}); err != nil {
		t.Fatal(err)
	}

	const naFactors = `{"pipeline_success_rate": 1.0, "test_pass_rate": null, "coverage_pct": 0.0, "flake_rate": 0.0, "failure_recurrence": 0.0, "coverage_delta_pct": 0.0, "base_score": 0.5, "flake_penalty": 0.0, "regression_penalty": 0.0}`
	const unknownFactors = `{"pipeline_success_rate": 1.0, "test_pass_rate": null, "coverage_pct": 0.0, "flake_rate": null, "failure_recurrence": null, "coverage_delta_pct": 0.0, "base_score": 0.4, "flake_penalty": 0.0, "regression_penalty": 0.0}`
	cells := []struct {
		name    string
		unknown bool
	}{
		{"history", true}, {"none", false}, {"old", false}, {"marker", true}, {"markerOK", false}, {"markerOtherD", false},
	}
	for _, cell := range cells {
		repo := repos[cell.name]
		var score float64
		var factors string
		if err := conn.QueryRow(ctx, `SELECT confidence_score, factors_json FROM testops_release_confidence WHERE org_id = ? AND repo_id = toUUID(?)`, orgID, repo).Scan(&score, &factors); err != nil {
			t.Fatalf("%s: %v", cell.name, err)
		}
		var drag float64
		var flakeHours *float64
		if err := conn.QueryRow(ctx, `SELECT drag_hours, flake_investigation_hours FROM testops_quality_drag WHERE org_id = ? AND repo_id = toUUID(?)`, orgID, repo).Scan(&drag, &flakeHours); err != nil {
			t.Fatalf("%s: %v", cell.name, err)
		}
		wantScore, wantFactors := 0.5, naFactors
		if cell.unknown {
			wantScore, wantFactors = 0.4, unknownFactors
		}
		if score != wantScore || factors != wantFactors {
			t.Errorf("%s release confidence: score=%v factors=%s, want %v %s", cell.name, score, factors, wantScore, wantFactors)
		}
		if cell.unknown == (flakeHours != nil) || (!cell.unknown && *flakeHours != 0) {
			t.Errorf("%s quality drag: flake hours present=%v, want unknown=%v", cell.name, flakeHours != nil, cell.unknown)
		}
	}
}
