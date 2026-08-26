//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestComputeFamilyWritesOneRowPerRepoForAMultiRepoTeam is a real-ClickHouse
// proof that TeamWellbeingExecutor.ComputeFamily writes team_metrics_daily's
// per-repo row SHAPE correctly (CHAOS-4276 codex round-1 finding 2): a team
// whose commits span two repos in one partition must land as two separate
// rows, each counting only its own repo's commits -- never one row
// aggregating both. This drives the real production entry point
// (ComputeFamily), not a direct numerical/computeWellbeingPerRepo call, so
// it also proves the ClickHouse write side (WriteTeamMetricsDailyPerRepo)
// round-trips correctly.
//
// This does NOT assert anything about computed_at ordering or distinctness
// (codex round-3 raised that; chris/team-lead's ruling rejected fabricating
// timestamps to force it -- see ComputeFamily's doc comment and this PR's
// RISK-NOTES: team_metrics_daily has no repo_id column at all, so which
// repo's row a reader's argMax(computed_at) picks for a multi-repo team is
// a pre-existing, cross-language property, not something this test's scope
// covers or something a Go-side timestamp trick should paper over).
func TestComputeFamilyWritesOneRowPerRepoForAMultiRepoTeam(t *testing.T) {
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
		// 027_add_org_id_to_sorting_keys.py).
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
	// so the readback below can tell WHICH repo's row is WHICH unambiguously.
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

	rows, err := conn.Query(ctx, `
SELECT commits_count FROM team_metrics_daily
WHERE org_id = ? AND team_id = 'shared-team'
ORDER BY commits_count`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var commitCounts []uint32
	for rows.Next() {
		var count uint32
		if err := rows.Scan(&count); err != nil {
			t.Fatal(err)
		}
		commitCounts = append(commitCounts, count)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(commitCounts) != 2 {
		t.Fatalf("persisted rows=%d, want 2: %#v", len(commitCounts), commitCounts)
	}
	// repo-b's row (3 commits) and repo-a's row (5 commits), never a single
	// aggregated 8 -- the round-1 regression this test guards against.
	if commitCounts[0] != 3 || commitCounts[1] != 5 {
		t.Fatalf("commits_count values=%v, want [3, 5] (repo-b's and repo-a's counts kept separate, never summed)", commitCounts)
	}
}
