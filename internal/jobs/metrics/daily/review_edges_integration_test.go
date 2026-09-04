//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// reviewEdgesSchema is the production shape of the three tables this family
// touches: the two raw sync sources from 000_raw_tables.sql (both
// ReplacingMergeTree(last_synced), with org_id added by migration 024) and the
// plain-MergeTree output from 004_quality_delivery_metrics.sql.
var reviewEdgesSchema = []string{
	`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, title Nullable(String), body Nullable(String),
    state Nullable(String), author_name Nullable(String), author_email Nullable(String),
    created_at DateTime64(3, 'UTC'), merged_at Nullable(DateTime64(3, 'UTC')),
    closed_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, number)`,
	`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, review_id String, reviewer String, state String,
    submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, number, review_id)`,
	// org_id is NOT in this table's sort key -- migration 024 added the column
	// to a table migration 004 had already created without one.
	`CREATE TABLE review_edges_daily (
    repo_id UUID, day Date, reviewer String, author String,
    reviews_count UInt32, computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day)
  ORDER BY (repo_id, reviewer, author, day)`,
}

// TestReviewEdgesComputeFamilyDeduplicatesResyncedRows is the loader-level
// proof, and it is the ONLY layer that can see this family's most important
// behaviour change.
//
// Python queries both ReplacingMergeTree sources RAW -- no FINAL, no argMax
// (loaders/clickhouse.py:283-320) -- so a re-synced review row is COUNTED
// TWICE and inflates reviews_count, and a re-synced PR row makes the author
// last-write-wins over an unordered result set. The native loader dedups with
// argMax(col, last_synced) GROUP BY each table's own ORDER BY key, so:
//
//   - the duplicated review is counted ONCE (native count is LOWER than
//     Python's here, and correct), and
//   - the PR author is deterministically the latest-synced value.
//
// The frozen golden cannot see either: it feeds identical rows to both sides,
// which is exactly the point -- the compute is unchanged and the divergence
// lives entirely in WHICH rows reach it.
func TestReviewEdgesComputeFamilyDeduplicatesResyncedRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA  = "00000000-0000-4000-8000-0000000000a0"
		orgB  = "00000000-0000-4000-8000-0000000000b0"
		repoA = "00000000-0000-4000-8000-0000000000a1"
		repoB = "00000000-0000-4000-8000-0000000000b1"
	)

	// PR 1: two rows, same key, different last_synced. argMax must take the
	// LATER author (ann@…), never the earlier decoy.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repoA+`'), 1, 'Stale', 'stale@example.com', '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+orgA+`'),
(toUUID('`+repoA+`'), 1, 'Ann',   'ann@example.com',   '2026-08-24 08:00:00.000', NULL, '2026-08-24 09:00:00.000', '`+orgA+`'),
(toUUID('`+repoB+`'), 1, 'Dee',   'dee@example.com',   '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}

	// review r1 is INGESTED TWICE (the CHAOS-5045 re-ingestion shape). Python
	// would count it twice; the native loader counts it once.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews
(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+repoA+`'), 1, 'r1', 'Bob', 'APPROVED',  '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+orgA+`'),
(toUUID('`+repoA+`'), 1, 'r1', 'Bob', 'APPROVED',  '2026-08-24 10:00:00.000', '2026-08-24 11:00:00.000', '`+orgA+`'),
(toUUID('`+repoA+`'), 1, 'r2', 'Cal', 'COMMENTED', '2026-08-24 10:30:00.000', '2026-08-24 10:30:00.000', '`+orgA+`'),
(toUUID('`+repoB+`'), 1, 'r9', 'Eve', 'APPROVED',  '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+orgB+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewReviewEdgesExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	rowsWritten, err := executor.ComputeFamily(ctx,
		Run{ID: "run-a", OrganizationID: orgA, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)},
		Partition{ID: "partition-a", RunID: "run-a", RepoIDs: []RepositoryID{RepositoryID(repoA)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 2 {
		t.Fatalf("wrote %d rows, want 2 (Bob->ann and Cal->ann)", rowsWritten)
	}

	type edge struct {
		reviewer string
		author   string
		count    uint32
	}
	rows, err := conn.Query(ctx, `
SELECT reviewer, author, reviews_count FROM review_edges_daily
WHERE org_id = ? ORDER BY reviewer`, orgA)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []edge
	for rows.Next() {
		var e edge
		if err := rows.Scan(&e.reviewer, &e.author, &e.count); err != nil {
			t.Fatal(err)
		}
		got = append(got, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want := []edge{
		// count 1, NOT 2: the duplicated r1 is deduplicated by last_synced.
		{"Bob", "ann@example.com", 1},
		{"Cal", "ann@example.com", 1},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d edges %+v, want %d %+v", len(got), got, len(want), want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("edge %d: got %+v, want %+v", index, got[index], want[index])
		}
	}
	// The author proves argMax took the LATER PR row: 'stale@example.com'
	// would mean the dedup picked by insertion order instead of last_synced.
	for _, e := range got {
		if e.author == "stale@example.com" {
			t.Error("author resolved to the earlier-synced PR row -- argMax(last_synced) is not taking effect")
		}
	}

	// Cross-tenant: org B's repo is in the table but not in this partition,
	// and its rows must not appear under org A.
	var strayRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM review_edges_daily WHERE org_id != ?`, orgA,
	).Scan(&strayRows); err != nil {
		t.Fatal(err)
	}
	if strayRows != 0 {
		t.Errorf("%d row(s) written outside org A", strayRows)
	}
}

// TestReviewEdgesDropsAReviewWhoseParentPullRequestIsOutsideTheDayWindow is the
// end-to-end proof of the dropped-edge quirk, through real SQL rather than a
// hand-built row list: the PR loader's window is `created_at` OR `merged_at` in
// the day, so a review submitted today on a PR created LAST week and not merged
// today finds no author and its edge vanishes.
//
// Mirrored deliberately from reviews.py:52-54. If this ever starts producing an
// edge, the Python producer's behaviour changed and the port must follow
// through this test, not silently.
func TestReviewEdgesDropsAReviewWhoseParentPullRequestIsOutsideTheDayWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range reviewEdgesSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org  = "00000000-0000-4000-8000-0000000000c0"
		repo = "00000000-0000-4000-8000-0000000000c1"
	)
	// PR 1 was created a week before the target day and never merged: outside
	// BOTH arms of the window predicate. PR 2 was created that day.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
(repo_id, number, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'Old', 'old@example.com', '2026-08-17 08:00:00.000', NULL, '2026-08-17 08:00:00.000', '`+org+`'),
(toUUID('`+repo+`'), 2, 'New', 'new@example.com', '2026-08-24 08:00:00.000', NULL, '2026-08-24 08:00:00.000', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}
	// Both reviews are submitted ON the target day.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews
(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+repo+`'), 1, 'r1', 'Bob', 'APPROVED', '2026-08-24 10:00:00.000', '2026-08-24 10:00:00.000', '`+org+`'),
(toUUID('`+repo+`'), 2, 'r2', 'Bob', 'APPROVED', '2026-08-24 10:30:00.000', '2026-08-24 10:30:00.000', '`+org+`')
`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewReviewEdgesExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	rowsWritten, err := executor.ComputeFamily(ctx,
		Run{ID: "run-c", OrganizationID: org, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)},
		Partition{ID: "partition-c", RunID: "run-c", RepoIDs: []RepositoryID{RepositoryID(repo)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 1 {
		t.Fatalf("wrote %d rows, want 1 -- the review of the out-of-window PR must be dropped", rowsWritten)
	}
	var author string
	if err := conn.QueryRow(ctx,
		`SELECT author FROM review_edges_daily WHERE org_id = ?`, org,
	).Scan(&author); err != nil {
		t.Fatal(err)
	}
	if author != "new@example.com" {
		t.Errorf("author = %q, want new@example.com (the in-window PR)", author)
	}
}
