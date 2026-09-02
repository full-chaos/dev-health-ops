//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFileHotspotsFamiliesWriteRealOrgIDAndIsolateTenants is CHAOS-4277's
// live-ClickHouse proof, run through the real production entry points
// (FileHotspotsExecutor.ComputeFamily and FileRiskHotspotsExecutor.
// ComputeFamily), following the exact pattern
// TestComputeFamilyWritesRealOrgIDAndIsolatesTenants
// (repo_user_commit_org_scope_integration_test.go, CHAOS-4341) established:
//
//  1. Red-on-baseline shape: a partition for org A must leave org-scoped
//     rows behind in BOTH file_metrics_daily and file_hotspot_daily.
//  2. Cross-tenant guard: two orgs, each with its own repo and its own
//     commit/complexity/blame data, run in the same process. Org A's
//     org-scoped read must see ONLY org A's rows, never org B's and never a
//     stray "" row, and vice versa.
//  3. Activity gating (file_hotspots only): org A's repo has a commit on the
//     target day (active); org B's repo has ONLY a commit 20 days before the
//     target day (inactive that day, but still inside the 30-day window) --
//     org B must get ZERO file_metrics_daily rows despite having window
//     churn, proving loadActiveRepoIDs' same-day gate actually gates.
//  4. Complexity-only union (file_risk_hotspots only): org B's repo has a
//     file_complexity_snapshots row for a path (idle_complex.py) with NO
//     churn at all in the window, ALONGSIDE its own churned file
//     (b_only.py, from its git commit). file_hotspot_daily must carry BOTH
//     -- 2 rows, the union, not the intersection or churn-only set -- proving
//     the complexity-only file is not silently dropped (no activity gate on
//     this family either, see FileRiskHotspotsExecutor's doc comment).
func TestFileHotspotsFamiliesWriteRealOrgIDAndIsolateTenants(t *testing.T) {
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
		`CREATE TABLE git_commits (
    repo_id UUID, hash String, author_name Nullable(String), author_email Nullable(String),
    committer_when DateTime64(3, 'UTC'), org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, hash)`,
		`CREATE TABLE git_commit_stats (
    repo_id UUID, commit_hash String, file_path String, additions Int32, deletions Int32,
    org_id String
) ENGINE = MergeTree ORDER BY (repo_id, commit_hash, file_path)`,
		`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, author_email Nullable(String), author_name Nullable(String),
    created_at DateTime, merged_at Nullable(DateTime), first_review_at Nullable(DateTime),
    first_comment_at Nullable(DateTime), changes_requested_count UInt32, reviews_count UInt32,
    comments_count UInt32, additions Nullable(UInt32), deletions Nullable(UInt32),
    changed_files Nullable(UInt32), org_id String
) ENGINE = MergeTree ORDER BY (repo_id, number)`,
		`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, reviewer String, submitted_at DateTime, state String,
    org_id String
) ENGINE = MergeTree ORDER BY (repo_id, number, reviewer)`,
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, finished_at DateTime, org_id String
) ENGINE = MergeTree ORDER BY (repo_id, finished_at)`,
		`CREATE TABLE deployments (
    repo_id UUID, deployed_at DateTime, org_id String
) ENGINE = MergeTree ORDER BY (repo_id, deployed_at)`,
		`CREATE TABLE file_complexity_snapshots (
    repo_id UUID, as_of_day Date, file_path String, language String, loc UInt32,
    functions_count UInt32, cyclomatic_total UInt32, cyclomatic_avg Float64,
    high_complexity_functions UInt32, very_high_complexity_functions UInt32,
    computed_at DateTime, org_id String
) ENGINE = MergeTree ORDER BY (repo_id, as_of_day, file_path)`,
		`CREATE TABLE git_blame (
    repo_id UUID, path String, line_no UInt32, author_email Nullable(String),
    author_name Nullable(String), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, path, line_no)`,
		// Exact production sorting keys: migration 027_add_org_id_to_sorting_keys.py.
		`CREATE TABLE file_metrics_daily (
    repo_id UUID, day Date, path String, churn UInt32, contributors UInt32,
    commits_count UInt32, hotspot_score Float64, computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day, path)`,
		// churn_loc_30d is UInt64 in production (migration
		// 007_complexity_investment_issues.sql:43) -- exact production type,
		// not UInt32 like its sibling columns (codex round 3, P2: a fixture
		// mismatch here would let a uint32-truncating writer bug pass green).
		`CREATE TABLE file_hotspot_daily (
    repo_id UUID, day Date, file_path String, churn_loc_30d UInt64, churn_commits_30d UInt32,
    cyclomatic_total UInt32, cyclomatic_avg Float64, blame_concentration Nullable(Float64),
    risk_score Float64, computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day, file_path)`,
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
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	activeDay := "2026-08-24 12:00:00"
	inactiveDayInWindow := "2026-08-04 12:00:00" // 20 days before target, inside the 30-day window, outside same-day activity

	if err := conn.Exec(ctx, `
INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'a1', 'Dev A', 'dev-a@example.com', toDateTime64('`+activeDay+`', 3, 'UTC'), '`+orgA+`', now64(3)),
(toUUID('`+repoB+`'), 'b1', 'Dev B', 'dev-b@example.com', toDateTime64('`+inactiveDayInWindow+`', 3, 'UTC'), '`+orgB+`', now64(3))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id) VALUES
(toUUID('`+repoA+`'), 'a1', 'a_only.py', 10, 5, '`+orgA+`'),
(toUUID('`+repoB+`'), 'b1', 'b_only.py', 20, 5, '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}
	// org B's repo has a complexity-only file with NO churn at all -- proves
	// the union case for file_risk_hotspots.
	if err := conn.Exec(ctx, `
INSERT INTO file_complexity_snapshots
    (repo_id, as_of_day, file_path, language, loc, functions_count, cyclomatic_total,
     cyclomatic_avg, high_complexity_functions, very_high_complexity_functions, computed_at, org_id) VALUES
(toUUID('`+repoB+`'), '2026-08-24', 'idle_complex.py', 'python', 300, 15, 60, 4.0, 2, 0, now(), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}

	hotspotsExecutor, err := NewFileHotspotsExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	riskExecutor, err := NewFileRiskHotspotsExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}

	runA := Run{OrganizationID: orgA, TargetDay: targetDay}
	partitionA := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repoA)},
	}
	runB := Run{OrganizationID: orgB, TargetDay: targetDay}
	partitionB := Partition{
		ID: "00000000-0000-4000-8000-0000000000c3", RunID: "00000000-0000-4000-8000-0000000000c2",
		RepoIDs: []RepositoryID{RepositoryID(repoB)},
	}

	hotspotsWrittenA, err := hotspotsExecutor.ComputeFamily(ctx, runA, partitionA)
	if err != nil {
		t.Fatalf("file_hotspots org A partition: %v", err)
	}
	if hotspotsWrittenA == 0 {
		t.Fatal("file_hotspots org A: expected rows written for an active repo, got 0")
	}
	hotspotsWrittenB, err := hotspotsExecutor.ComputeFamily(ctx, runB, partitionB)
	if err != nil {
		t.Fatalf("file_hotspots org B partition: %v", err)
	}
	if hotspotsWrittenB != 0 {
		t.Fatalf("file_hotspots org B: expected 0 rows for a repo with no SAME-DAY activity (window-only churn), got %d", hotspotsWrittenB)
	}

	riskWrittenA, err := riskExecutor.ComputeFamily(ctx, runA, partitionA)
	if err != nil {
		t.Fatalf("file_risk_hotspots org A partition: %v", err)
	}
	if riskWrittenA == 0 {
		t.Fatal("file_risk_hotspots org A: expected rows written, got 0")
	}
	riskWrittenB, err := riskExecutor.ComputeFamily(ctx, runB, partitionB)
	if err != nil {
		t.Fatalf("file_risk_hotspots org B partition: %v", err)
	}
	if riskWrittenB == 0 {
		t.Fatal("file_risk_hotspots org B: expected rows written for the complexity-only idle file despite no same-day activity gate, got 0")
	}

	for _, table := range []string{"file_metrics_daily", "file_hotspot_daily"} {
		assertOrgScopedCount(ctx, t, conn, table, "", 0)
	}
	assertOrgScopedCount(ctx, t, conn, "file_metrics_daily", orgA, 1)
	assertOrgScopedCount(ctx, t, conn, "file_metrics_daily", orgB, 0)
	assertOrgScopedCount(ctx, t, conn, "file_hotspot_daily", orgA, 1)
	// org B's repo has BOTH its own churned file (b_only.py, from its git
	// commit) AND the complexity-only idle file (idle_complex.py, no churn
	// at all) -- file_risk_hotspots unions the two sets, so org B gets TWO
	// rows here, not one. This is the union assertion itself: if the union
	// silently degraded to an intersection (or churn-only), this count would
	// drop to 1 and idle_complex.py would vanish from the query below.
	assertOrgScopedCount(ctx, t, conn, "file_hotspot_daily", orgB, 2)

	// The complexity-only idle file must be present among org B's rows,
	// proving the union case actually surfaced it (not merely a count that
	// happens to match by coincidence).
	row := conn.QueryRow(ctx, "SELECT count() FROM file_hotspot_daily WHERE org_id = ? AND file_path = ?", orgB, "idle_complex.py")
	var idleCount uint64
	if err := row.Scan(&idleCount); err != nil {
		t.Fatalf("scan org B idle_complex.py count: %v", err)
	}
	if idleCount != 1 {
		t.Fatalf("org B file_hotspot_daily rows for idle_complex.py = %d, want 1", idleCount)
	}
	row = conn.QueryRow(ctx, "SELECT count() FROM file_hotspot_daily WHERE org_id = ? AND file_path = ?", orgB, "b_only.py")
	var churnedCount uint64
	if err := row.Scan(&churnedCount); err != nil {
		t.Fatalf("scan org B b_only.py count: %v", err)
	}
	if churnedCount != 1 {
		t.Fatalf("org B file_hotspot_daily rows for b_only.py = %d, want 1", churnedCount)
	}
}
