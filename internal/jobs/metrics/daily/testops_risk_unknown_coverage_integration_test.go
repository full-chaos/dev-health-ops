//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Unknown coverage (NULL pct, no snapshot, NaN) against a measured 0%, through ComputeFamily and the stored row.
func TestTestopsRiskUnknownCoverageStoredAgainstRealClickHouse(t *testing.T) {
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
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	chschema.CreateTables(ctx, t, conn, "test_case_results", "test_suite_results", "coverage_snapshots", "testops_run_report_completeness", "testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability")
	applyTestopsRepositoryOwnershipSchema(ctx, t, conn)

	const orgID = "00000000-0000-4000-8000-000000000009"
	day := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	repos := map[string]string{
		"zero": "00000000-0000-4000-8000-0000000000a1",
		"null": "00000000-0000-4000-8000-0000000000b2",
		"none": "00000000-0000-4000-8000-0000000000c3",
		"nan":  "00000000-0000-4000-8000-0000000000d4",
	}
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	ids := []RepositoryID{}
	for name, repo := range repos {
		exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, retry_count, org_id) VALUES (toUUID('` + repo + `'), 'run-1', 'success', toDateTime64('2026-08-15 09:00:00', 3, 'UTC'), toDateTime64('2026-08-15 09:01:00', 3, 'UTC'), toDateTime64('2026-08-15 09:11:00', 3, 'UTC'), now64(3), 0, '` + orgID + `')`)
		exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, started_at, finished_at, org_id, last_synced) VALUES (toUUID('` + repo + `'), 'run-1', 'suite-1', 'unit', 10, 9, 1, 0, toDateTime64('2026-08-15 09:01:00', 3, 'UTC'), toDateTime64('2026-08-15 09:05:00', 3, 'UTC'), '` + orgID + `', toDateTime64('2026-08-15 10:00:00', 3, 'UTC'))`)
		exec(`INSERT INTO test_case_results (repo_id, run_id, suite_id, case_id, case_name, status, org_id, last_synced) VALUES (toUUID('` + repo + `'), 'run-1', 'suite-1', 'c1', 'test_one', 'passed', '` + orgID + `', now64(3))`)
		switch name {
		case "zero":
			exec(`INSERT INTO coverage_snapshots (repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, org_id, last_synced) VALUES (toUUID('` + repo + `'), 'run-1', 'snap-1', 1000, 0, 0.0, '` + orgID + `', now64(3))`)
		case "nan":
			exec(`INSERT INTO coverage_snapshots (repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, org_id, last_synced) VALUES (toUUID('` + repo + `'), 'run-1', 'snap-1', 0, 0, nan, '` + orgID + `', now64(3))`)
		case "null":
			exec(`INSERT INTO coverage_snapshots (repo_id, run_id, snapshot_id, lines_total, lines_covered, line_coverage_pct, org_id, last_synced) VALUES (toUUID('` + repo + `'), 'run-1', 'snap-1', NULL, NULL, NULL, '` + orgID + `', now64(3))`)
		}
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
	scores := map[string]float64{}
	factors := map[string]string{}
	for name, repo := range repos {
		var score float64
		var fj string
		if err := conn.QueryRow(ctx, `SELECT confidence_score, factors_json FROM testops_release_confidence WHERE org_id = ? AND repo_id = toUUID(?)`, orgID, repo).Scan(&score, &fj); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		scores[name], factors[name] = score, fj
		t.Logf("stored %s: confidence_score=%v factors_json=%s", name, score, fj)
	}
	// Known: pipeline 1.0, pass 0.9, flake 0. 0% keeps the coverage weight (0.77); unknown coverage is left out and the
	// other weights renormalized (0.77 / 0.8 = 0.9625).
	if scores["zero"] != 0.77 {
		t.Errorf("stored score for 0%% coverage %v, want 0.77", scores["zero"])
	}
	for _, name := range []string{"null", "none", "nan"} {
		if scores[name] != 0.9625 {
			t.Errorf("stored score for %s coverage %v, want 0.9625 (coverage left out, weights renormalized)", name, scores[name])
		}
		if !strings.Contains(factors[name], `"coverage_pct": null`) || !strings.Contains(factors[name], `"coverage_delta_pct": null`) {
			t.Errorf("stored factors_json for %s coverage holds a value, want null: %s", name, factors[name])
		}
	}
	if !strings.Contains(factors["zero"], `"coverage_pct": 0.0`) || !strings.Contains(factors["zero"], `"coverage_delta_pct": null`) {
		t.Errorf("stored factors_json for 0%% coverage with no prior snapshot: want coverage_pct 0.0, coverage_delta_pct null: %s", factors["zero"])
	}
}
