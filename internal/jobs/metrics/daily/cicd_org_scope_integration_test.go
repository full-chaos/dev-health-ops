//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestCICDComputeFamilyWritesRealOrgIDAndIsolatesTenants is CICDExecutor's
// live-ClickHouse proof, run through the real production entry point
// (CICDExecutor.ComputeFamily), not a unit test of the writer in isolation --
// same shape as repo_user_commit's TestComputeFamilyWritesRealOrgIDAndIsolatesTenants:
//
//  1. Red-on-baseline shape: a cicd partition for org A must leave org-scoped
//     rows behind -- `SELECT count() FROM cicd_metrics_daily WHERE org_id =
//     <org A>` > 0.
//  2. Cross-tenant guard: two orgs, each with its own repo, run in the same
//     process. Org A's org-scoped read must see ONLY org A's row (never org
//     B's, and never a stray "" row), and vice versa.
//  3. Window-filter proof: a pipeline run that started inside the target day
//     but finished the NEXT day must be excluded (the DOUBLE WINDOW FILTER
//     documented on the cicd package) -- exercised via repo B, which has one
//     in-window run and one cross-midnight run; its pipelines_count must be
//     1, not 2.
func TestCICDComputeFamilyWritesRealOrgIDAndIsolatesTenants(t *testing.T) {
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
		// Exact production shape: 000_raw_tables.sql + org_id added by
		// migration 027 (027_add_org_id_to_sorting_keys.py's
		// TABLES_NEEDING_ORG_ID_COLUMN list does NOT include ci_pipeline_runs
		// as needing the column added -- it already had it via migration 024
		// -- but the sorting key stays (repo_id, run_id) per that file's
		// TABLES map having no ci_pipeline_runs entry; org_id is filtered,
		// not keyed).
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')), started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
		// Exact production sorting key: migration 027_add_org_id_to_sorting_keys.py.
		`CREATE TABLE cicd_metrics_daily (
    repo_id UUID, day Date, pipelines_count UInt32, success_rate Float64,
    avg_duration_minutes Nullable(Float64), p90_duration_minutes Nullable(Float64),
    avg_queue_minutes Nullable(Float64), computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
	)
	repoA := "00000000-0000-4000-8000-0000000000a1"
	repoB := "00000000-0000-4000-8000-0000000000b1"

	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id) VALUES
(toUUID('`+repoA+`'), 'a-run-1', 'success',
 toDateTime64('2026-08-24 09:55:00', 3, 'UTC'), toDateTime64('2026-08-24 10:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 10:10:00', 3, 'UTC'), now64(3), '`+orgA+`'),
(toUUID('`+repoB+`'), 'b-run-in-window', 'failed',
 toDateTime64('2026-08-24 11:55:00', 3, 'UTC'), toDateTime64('2026-08-24 12:00:00', 3, 'UTC'),
 toDateTime64('2026-08-24 12:20:00', 3, 'UTC'), now64(3), '`+orgB+`'),
(toUUID('`+repoB+`'), 'b-run-cross-midnight', 'success',
 toDateTime64('2026-08-24 23:50:00', 3, 'UTC'), toDateTime64('2026-08-24 23:55:00', 3, 'UTC'),
 toDateTime64('2026-08-25 00:10:00', 3, 'UTC'), now64(3), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewCICDExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	runA := Run{OrganizationID: orgA, TargetDay: targetDay}
	partitionA := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repoA)},
	}
	if _, err := executor.ComputeFamily(ctx, runA, partitionA); err != nil {
		t.Fatalf("org A partition: %v", err)
	}

	runB := Run{OrganizationID: orgB, TargetDay: targetDay}
	partitionB := Partition{
		ID: "00000000-0000-4000-8000-0000000000c3", RunID: "00000000-0000-4000-8000-0000000000c2",
		RepoIDs: []RepositoryID{RepositoryID(repoB)},
	}
	if _, err := executor.ComputeFamily(ctx, runB, partitionB); err != nil {
		t.Fatalf("org B partition: %v", err)
	}

	assertOrgScopedCount(ctx, t, conn, "cicd_metrics_daily", orgA, 1)
	assertOrgScopedCount(ctx, t, conn, "cicd_metrics_daily", orgB, 1)
	assertOrgScopedCount(ctx, t, conn, "cicd_metrics_daily", "", 0)

	// Point 3: window-filter proof -- repo B's pipelines_count must be 1
	// (the cross-midnight run's finished_at falls outside [start,end) for
	// 2026-08-24, so the loader never fetches it for this day).
	row := conn.QueryRow(ctx, "SELECT pipelines_count FROM cicd_metrics_daily WHERE org_id = ? AND repo_id = ?", orgB, repoB)
	var pipelinesCount uint32
	if err := row.Scan(&pipelinesCount); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if pipelinesCount != 1 {
		t.Fatalf("repo B pipelines_count = %d, want 1 (cross-midnight run must be excluded)", pipelinesCount)
	}
}

// assertOrgScopedCount is defined in repo_user_commit_org_scope_integration_test.go
// (same package, same build tag) -- reused here rather than redefined.
