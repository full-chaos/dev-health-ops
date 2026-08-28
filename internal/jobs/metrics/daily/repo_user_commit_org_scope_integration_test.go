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

// TestComputeFamilyWritesRealOrgIDAndIsolatesTenants is CHAOS-4341's live-
// ClickHouse proof, run through the real production entry point
// (RepoUserCommitExecutor.ComputeFamily), not a unit test of the writer in
// isolation:
//
//  1. Red-on-baseline shape: a repo_user_commit partition for org A must
//     leave org-scoped rows behind -- `SELECT count() FROM repo_metrics_daily
//     WHERE org_id = <org A>` > 0. Before this ticket's fix, the writer
//     hard-coded org_id="", so this assertion fails against unfixed code
//     even though the partition itself "succeeds" (matching the exact prod
//     shape: 580/580 partitions succeeded, 0 org-scoped rows -- CHAOS-4341,
//     deploy 5.3 readback #2).
//  2. Cross-tenant guard: two orgs, each with its own repo, run in the same
//     process. Org A's org-scoped read must see ONLY org A's row (never org
//     B's, and never a stray "" row), and vice versa.
func TestComputeFamilyWritesRealOrgIDAndIsolatesTenants(t *testing.T) {
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
		`CREATE TABLE work_items (
    repo_id UUID, type String, started_at Nullable(DateTime), completed_at Nullable(DateTime),
    org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, org_id, type)`,
		// Exact production sorting keys: migration 027_add_org_id_to_sorting_keys.py.
		`CREATE TABLE repo_metrics_daily (
    repo_id UUID, day Date, commits_count UInt32, total_loc_touched UInt32,
    avg_commit_size_loc Float64, large_commit_ratio Float64, prs_merged UInt32,
    median_pr_cycle_hours Float64, pr_cycle_p75_hours Float64, pr_cycle_p90_hours Float64,
    prs_with_first_review UInt32, pr_first_review_p50_hours Nullable(Float64),
    pr_first_review_p90_hours Nullable(Float64), pr_review_time_p50_hours Nullable(Float64),
    pr_pickup_time_p50_hours Nullable(Float64), large_pr_ratio Float64, pr_rework_ratio Float64,
    pr_size_p50_loc Nullable(Float64), pr_size_p90_loc Nullable(Float64),
    pr_comments_per_100_loc Nullable(Float64), pr_reviews_per_100_loc Nullable(Float64),
    rework_churn_ratio_30d Float64, single_owner_file_ratio_30d Float64,
    review_load_top_reviewer_ratio Float64, bus_factor UInt32, code_ownership_gini Float64,
    mttr_hours Nullable(Float64), change_failure_rate Float64, computed_at DateTime,
    org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
		`CREATE TABLE user_metrics_daily (
    repo_id UUID, day Date, author_email String, commits_count UInt32, loc_added UInt32,
    loc_deleted UInt32, files_changed UInt32, large_commits_count UInt32,
    avg_commit_size_loc Float64, prs_authored UInt32, prs_merged UInt32,
    avg_pr_cycle_hours Float64, median_pr_cycle_hours Float64, pr_cycle_p75_hours Float64,
    pr_cycle_p90_hours Float64, prs_with_first_review UInt32,
    pr_first_review_p50_hours Nullable(Float64), pr_first_review_p90_hours Nullable(Float64),
    pr_review_time_p50_hours Nullable(Float64), pr_pickup_time_p50_hours Nullable(Float64),
    reviews_given UInt32, changes_requested_given UInt32, reviews_received UInt32,
    review_reciprocity Float64, pr_interruption_load UInt32, context_spread_count UInt32,
    review_request_load UInt32, team_id String, team_name String, active_hours Float64,
    weekend_days UInt8, identity_id String, computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, author_email, day)`,
		`CREATE TABLE commit_metrics (
    repo_id UUID, commit_hash String, day Date, author_email String, total_loc UInt32,
    files_changed UInt32, size_bucket String, computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day, author_email, commit_hash)`,
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
INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'a1', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '`+orgA+`', now64(3)),
(toUUID('`+repoB+`'), 'b1', 'Dev B', 'dev-b@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '`+orgB+`', now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewRepoUserCommitExecutor(conn)
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

	for _, table := range []string{"repo_metrics_daily", "user_metrics_daily", "commit_metrics"} {
		// Point 1: red-on-baseline -- org-scoped read must see org A's row.
		assertOrgScopedCount(ctx, t, conn, table, orgA, 1)
		// Point 2: cross-tenant guard -- org A's read must NOT see org B's
		// row, org B's read must NOT see org A's, and neither org's read may
		// pick up a stray org_id="" row (the exact pre-fix shape).
		assertOrgScopedCount(ctx, t, conn, table, orgB, 1)
		assertOrgScopedCount(ctx, t, conn, table, "", 0)
	}
}

func assertOrgScopedCount(ctx context.Context, t *testing.T, conn driver.Conn, table, orgID string, want int) {
	t.Helper()
	row := conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE org_id = ?", orgID)
	var got uint64
	if err := row.Scan(&got); err != nil {
		t.Fatalf("%s org_id=%q: query row: %v", table, orgID, err)
	}
	if int(got) != want {
		t.Fatalf("%s: count(org_id=%q) = %d, want %d", table, orgID, got, want)
	}
}
