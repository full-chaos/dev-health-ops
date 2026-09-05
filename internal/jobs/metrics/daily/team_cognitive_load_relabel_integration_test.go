//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestLoadTeamMetricsCognitiveLoadInputsForDayCollapsesARelabeledRepoToItsLatestGeneration
// is the red-first proof for CHAOS-5141, #2255 r1 finding 1: a repo relabeled
// from one team_id to another during the day keeps BOTH team_metrics_daily
// generations in storage -- each is its own physical row, and a query that
// dedupes per (team_id, repo_id) rather than per repo_id across every
// team_id it has ever been under returns BOTH. resolveDailyFinalizeRepoToTeam
// then maps both rows to whichever team repo_to_team currently says the repo
// belongs to, and buildTeamCognitiveLoadRows sums both -- double-counting
// every relabeled repo's commits.
//
// The fix dedupes to the single LATEST generation (by computed_at) across
// every team_id FIRST, mirroring job_daily.py's team_metrics_query (:2377)
// exactly -- team_metrics_daily's own team_id is read only to know a legacy
// bucket exists (CHAOS-4396), never to resolve a team, so this loader never
// groups by it.
func TestLoadTeamMetricsCognitiveLoadInputsForDayCollapsesARelabeledRepoToItsLatestGeneration(t *testing.T) {
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

	if err := conn.Exec(ctx, `
CREATE TABLE team_metrics_daily (
    org_id String DEFAULT 'default',
    day Date,
    team_id LowCardinality(String),
    team_name String,
    repo_id String DEFAULT '',
    commits_count UInt32,
    after_hours_commits_count UInt32,
    weekend_commits_count UInt32,
    after_hours_commit_ratio Float64,
    weekend_commit_ratio Float64,
    computed_at DateTime64(6, 'UTC')
) ENGINE = MergeTree ORDER BY (team_id, day)`); err != nil {
		t.Fatal(err)
	}

	day := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	repoID := uuid.New()
	older := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 28, 18, 0, 0, 0, time.UTC)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO team_metrics_daily "+
		"(org_id, day, team_id, team_name, repo_id, commits_count, "+
		"after_hours_commits_count, weekend_commits_count, "+
		"after_hours_commit_ratio, weekend_commit_ratio, computed_at)")
	if err != nil {
		t.Fatal(err)
	}
	// Generation 1, under the OLD team: 10 commits, earlier computed_at.
	if err := batch.Append(
		"acme", day, "team-old", "Old Team", repoID.String(),
		uint32(10), uint32(2), uint32(1), 0.2, 0.1, older,
	); err != nil {
		t.Fatal(err)
	}
	// Generation 2, under the NEW team (a mid-day relabel): 20 commits, later
	// computed_at -- the TRUE current generation.
	if err := batch.Append(
		"acme", day, "team-new", "New Team", repoID.String(),
		uint32(20), uint32(4), uint32(2), 0.2, 0.1, newer,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}

	rows, err := loadTeamMetricsCognitiveLoadInputsForDay(ctx, conn, "acme", day)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1 (one row per repo, latest generation only, "+
			"across every team_id it has been under): %#v", len(rows), rows)
	}
	if rows[0].RepoID != repoID {
		t.Fatalf("repo_id=%s, want %s", rows[0].RepoID, repoID)
	}
	if rows[0].CommitsCount != 20 {
		t.Fatalf("commits_count=%d, want 20 (the latest generation's count "+
			"only -- 30 would be both generations double-counted, CHAOS-5141 "+
			"finding 1)", rows[0].CommitsCount)
	}
	if rows[0].AfterHoursCommitsCount != 4 || rows[0].WeekendCommitsCount != 2 {
		t.Fatalf("after_hours=%d weekend=%d, want 4/2 (latest generation only)",
			rows[0].AfterHoursCommitsCount, rows[0].WeekendCommitsCount)
	}
}
