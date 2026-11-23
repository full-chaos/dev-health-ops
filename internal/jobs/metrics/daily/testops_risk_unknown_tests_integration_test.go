//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A repo-day with CI runs and no test row is unknown (tests exist but were not read) or not applicable (no tests), by a suite in
// the 29 days before the day or a partial completeness row of a run of the day. Real ClickHouse, through ComputeFamily.
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
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	chschema.CreateTables(ctx, t, conn, "test_case_results", "test_suite_results", "coverage_snapshots", "testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability", "testops_run_report_completeness")
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

	// No coverage row: coverage is unknown, so each base is divided by 0.8 ((0.4+0.1)/0.8 and 0.4/0.8).
	const naFactors = `{"pipeline_success_rate": 1.0, "test_pass_rate": null, "coverage_pct": null, "flake_rate": 0.0, "failure_recurrence": 0.0, "coverage_delta_pct": null, "base_score": 0.625, "flake_penalty": 0.0, "regression_penalty": 0.0}`
	const unknownFactors = `{"pipeline_success_rate": 1.0, "test_pass_rate": null, "coverage_pct": null, "flake_rate": null, "failure_recurrence": null, "coverage_delta_pct": null, "base_score": 0.5, "flake_penalty": 0.0, "regression_penalty": 0.0}`
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
		wantScore, wantFactors := 0.625, naFactors
		if cell.unknown {
			wantScore, wantFactors = 0.5, unknownFactors
		}
		if score != wantScore || factors != wantFactors {
			t.Errorf("%s release confidence: score=%v factors=%s, want %v %s", cell.name, score, factors, wantScore, wantFactors)
		}
		if cell.unknown == (flakeHours != nil) || (!cell.unknown && *flakeHours != 0) {
			t.Errorf("%s quality drag: flake hours present=%v, want unknown=%v", cell.name, flakeHours != nil, cell.unknown)
		}
	}
}
