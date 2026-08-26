//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestComputeFamilyMultiRepoRowsSurviveClickHouseSecondPrecisionTruncation is
// the real-ClickHouse boundary proof codex round 3 required (CHAOS-4276):
// team_metrics_daily.computed_at is `DateTime('UTC')` -- ClickHouse's
// SECOND-precision type, not `DateTime64` -- so two Go time.Time values that
// are merely "not equal" in memory can still collapse to an IDENTICAL
// persisted value if they fall in the same wall-clock second, reopening the
// exact argMax(computed_at) tie round 2's fix was meant to close. A
// same-package unit test against a recording batch (as round 2 shipped)
// cannot observe this: it only sees the pre-serialization time.Time values,
// never what ClickHouse actually stores or how argMax resolves a real tie.
//
// This test drives TeamWellbeingExecutor.ComputeFamily end-to-end (the real
// production entry point, not a direct numerical/computeWellbeingPerRepo
// call) against a partition with TWO repos owned by the SAME team, so the
// team's two rows are genuinely written through ComputeFamily's
// base+index-second timestamp scheme (wellbeing_native_executor.go)
// and then reads them back the same way a real consumer does --
// argMax(commits_count, computed_at) grouped by (org_id, team_id, day),
// mirroring cognitive_load.py/clickhouse_dedup.py's own dedup query. If the
// two rows' computed_at values truncated to the same second, this would be
// nondeterministic across runs; asserting a SPECIFIC, expected winner
// (repo B, the later index, higher UNIX second) makes any flake in either
// direction visible immediately.
func TestComputeFamilyMultiRepoRowsSurviveClickHouseSecondPrecisionTruncation(t *testing.T) {
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
		`CREATE TABLE teams (
    id String, name String, members Array(String), repo_patterns Array(String), org_id String
) ENGINE = ReplacingMergeTree ORDER BY (id)`,
		`CREATE TABLE repos (
    id UUID, repo String, org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (id)`,
		`CREATE TABLE git_commits (
    repo_id UUID, hash String, author_name Nullable(String), author_email Nullable(String),
    committer_when DateTime64(3, 'UTC'), org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, hash)`,
		// Exact production schema (001_metrics_v2.sql + 024_add_org_id.sql +
		// 027_add_org_id_to_sorting_keys.py): computed_at is `DateTime('UTC')`
		// -- second precision -- this is the column under test.
		`CREATE TABLE team_metrics_daily (
    day Date, team_id LowCardinality(String), team_name String,
    commits_count UInt32, after_hours_commits_count UInt32, weekend_commits_count UInt32,
    after_hours_commit_ratio Float64, weekend_commit_ratio Float64,
    computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, team_id, day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const orgID = "00000000-0000-4000-8000-000000000009"
	repoA := "00000000-0000-4000-8000-0000000000a1"
	repoB := "00000000-0000-4000-8000-0000000000b2"

	// One team, repo-pattern owning BOTH repos -- both repos' commits
	// resolve to the SAME team, exactly the shape that produces two rows
	// for one (org_id, team_id, day) key.
	if err := conn.Exec(ctx, `
INSERT INTO teams (id, name, members, repo_patterns, org_id) VALUES
('shared-team', 'Shared Team', [], ['org/repo-a', 'org/repo-b'], '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'org/repo-a', '`+orgID+`', now64(3)),
(toUUID('`+repoB+`'), 'org/repo-b', '`+orgID+`', now64(3))`); err != nil {
		t.Fatal(err)
	}
	// repo-a: 5 commits. repo-b: 3 commits. Deliberately different counts
	// so the readback below can tell WHICH repo's row won the tie
	// unambiguously, not just that a row exists.
	if err := conn.Exec(ctx, `
INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'a1', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoA+`'), 'a2', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:01:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoA+`'), 'a3', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:02:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoA+`'), 'a4', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:03:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoA+`'), 'a5', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:04:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoB+`'), 'b1', 'Dev B', 'dev-b@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoB+`'), 'b2', 'Dev B', 'dev-b@example.com', toDateTime64('2026-08-24 12:01:00', 3, 'UTC'), '`+orgID+`', now64(3)),
(toUUID('`+repoB+`'), 'b3', 'Dev B', 'dev-b@example.com', toDateTime64('2026-08-24 12:02:00', 3, 'UTC'), '`+orgID+`', now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewTeamWellbeingExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)}
	partition := Partition{
		ID:      "00000000-0000-4000-8000-000000000121",
		RunID:   "00000000-0000-4000-8000-000000000120",
		RepoIDs: []RepositoryID{RepositoryID(repoA), RepositoryID(repoB)},
	}

	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatal(err)
	}
	if written != 2 {
		t.Fatalf("written=%d, want 2 (one row per repo for the one shared team)", written)
	}

	// Read back BOTH raw rows to prove ClickHouse actually persisted two
	// DISTINCT computed_at values -- the exact storage-boundary property
	// codex round 3 flagged as unverified.
	rows, err := conn.Query(ctx, `
SELECT commits_count, computed_at FROM team_metrics_daily
WHERE org_id = ? AND team_id = 'shared-team'
ORDER BY computed_at`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type persistedRow struct {
		commitsCount uint32
		computedAt   time.Time
	}
	var persisted []persistedRow
	for rows.Next() {
		var row persistedRow
		if err := rows.Scan(&row.commitsCount, &row.computedAt); err != nil {
			t.Fatal(err)
		}
		persisted = append(persisted, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 2 {
		t.Fatalf("persisted rows=%d, want 2: %#v", len(persisted), persisted)
	}
	if persisted[0].computedAt.Equal(persisted[1].computedAt) {
		t.Fatalf(
			"both rows persisted with the IDENTICAL computed_at %v -- ClickHouse's "+
				"second-precision DateTime column truncated the per-repo timestamps "+
				"to the same value, reopening the argMax tie codex round 3 flagged",
			persisted[0].computedAt,
		)
	}
	if !persisted[1].computedAt.After(persisted[0].computedAt) {
		t.Fatalf("expected computed_at to be strictly increasing by repo order, got %v then %v",
			persisted[0].computedAt, persisted[1].computedAt)
	}

	// Now the real consumer-facing query: argMax(commits_count, computed_at)
	// per (org_id, team_id, day), the SAME dedup pattern
	// cognitive_load.py/clickhouse_dedup.py use. It must deterministically
	// resolve to repo-b's row (the later, index-1 timestamp) -- the
	// production analogue of "whichever repo was processed last wins",
	// matching Python's own real per-repo_id call cadence.
	dedupedRows, err := conn.Query(ctx, `
SELECT argMax(commits_count, computed_at) AS commits_count
FROM team_metrics_daily
WHERE org_id = ? AND team_id = 'shared-team'
GROUP BY org_id, team_id, day`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer dedupedRows.Close()
	if !dedupedRows.Next() {
		t.Fatal("expected one deduped row, got none")
	}
	var dedupedCommitsCount uint32
	if err := dedupedRows.Scan(&dedupedCommitsCount); err != nil {
		t.Fatal(err)
	}
	if dedupedCommitsCount != 3 {
		t.Fatalf("argMax-deduped commits_count=%d, want 3 (repo-b's count, the later-timestamped row) -- "+
			"got %d instead, which means the tie-break did not resolve deterministically",
			dedupedCommitsCount, dedupedCommitsCount)
	}
}
