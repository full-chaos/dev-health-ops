//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestARepositoryOwnerTeamWithNoWorkItemsIsEvaluated: a team that owns a
// repository with merged, reviewed pull requests but has no work item rows is
// evaluated (it gets recommendation rows), and a team with work items that
// also owns a repository is evaluated once.
func TestARepositoryOwnerTeamWithNoWorkItemsIsEvaluated(t *testing.T) {
	const (
		org           = "org-discover-owner-teams"
		ownerOnlyTeam = "team-owner-only"
		bothTeam      = "team-work-items-and-owner"
		ownerOnlyRepo = "a1111111-1111-1111-1111-111111111111"
		bothRepo      = "a2222222-2222-2222-2222-222222222222"
	)
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed failed: %v\nquery: %s", err, query)
		}
	}

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	since := mustTimestamp(t, "2026-01-01 00:00:00")

	for _, seed := range []struct{ team, repo, name string }{
		{ownerOnlyTeam, ownerOnlyRepo, "acme/owner-only"},
		{bothTeam, bothRepo, "acme/both"},
	} {
		exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
			VALUES (?, ?, ?, ?, ?, 'github')`, seed.repo, seed.name, since, since, org)
		exec(`INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type,
			 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 1, 0, ?, NULL, ?)`,
			org, seed.team, seed.repo, seed.name, since, since)
		exec(`INSERT INTO repo_metrics_daily
			(repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
			 large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours,
			 pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio,
			 change_failure_rate, computed_at, org_id, prs_merged_reviewed, prs_merged_rework)
			VALUES (?, ?, 3, 120, 40, 0, 4, 20, 30, 40, 4, 0, 0.25, 0, ?, ?, 4, 1)`,
			seed.repo, today.AddDate(0, 0, -2), today.AddDate(0, 0, -1), org)
	}
	exec(`INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
		 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
		 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
		 story_points_completed, computed_at, org_id)
		VALUES (?, 'github', 'acme/both', ?, '', 1, 1, 0, 0, 1, 0, 10, 0, 0, ?, ?)`,
		today.AddDate(0, 0, -1), bothTeam, today, org)

	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	outcome, err := executor.ComputeOrg(ctx, org, now, 30, "v1", "")
	if err != nil {
		t.Fatalf("compute org: %v", err)
	}
	if outcome.Teams != 2 {
		t.Errorf("evaluated %d teams, want 2 (the owner-only team, and the team in both sets once)", outcome.Teams)
	}

	rows, err := conn.Query(ctx, `
        SELECT team_id, count()
        FROM recommendations_daily FINAL
        WHERE org_id = ?
        GROUP BY team_id`, org)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = rows.Close() }()
	written := map[string]uint64{}
	for rows.Next() {
		var teamID string
		var count uint64
		if err := rows.Scan(&teamID, &count); err != nil {
			t.Fatalf("scan: %v", err)
		}
		written[teamID] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if written[ownerOnlyTeam] == 0 {
		t.Errorf("team %s owns %s and has no work items: no recommendation rows, want its rules evaluated (rows by team: %v)",
			ownerOnlyTeam, ownerOnlyRepo, written)
	}
	if written[bothTeam] == 0 || written[bothTeam] != written[ownerOnlyTeam] {
		t.Errorf("rows by team = %v, want one evaluation each (the same rule count for both teams)", written)
	}
}
