//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAProjectOwnerTeamWithNoWorkItemsIsEvaluated: a team that owns only a
// project (jira, github, gitlab, linear) and has no work item rows is
// evaluated, a co-owner-only team too (a co-owner is an owner, as in
// OwnersByRepo); a team that owns a repository and a project is evaluated once;
// an inactive project owner and a project ownership that ended before the
// window end are not.
func TestAProjectOwnerTeamWithNoWorkItemsIsEvaluated(t *testing.T) {
	const (
		org          = "org-discover-project-owner-teams"
		jiraTeam     = "jira:team-project-only"
		githubTeam   = "gh:team-project-only"
		coOwnerTeam  = "jira:team-co-owner-only"
		gitlabTeam   = "gl:team-project-only"
		linearTeam   = "linear:team-project-only"
		bothTeam     = "gl:team-repo-and-project"
		inactiveTeam = "jira:team-inactive-project-owner"
		endedTeam    = "linear:team-ended-project-owner"
		bothRepo     = "a3333333-3333-3333-3333-333333333333"
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

	project := func(provider, team, projectID, projectKey string, isPrimary uint8, validTo any) {
		exec(`INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority,
			 valid_from, valid_to, updated_at, last_synced)
			VALUES (?, ?, ?, ?, ?, 'native', ?, 1, 0, ?, ?, ?, ?)`,
			org, provider, team, projectID, projectKey, isPrimary, since, validTo, since, since)
	}
	project("jira", jiraTeam, "10001", "PROJ", 1, nil)
	project("jira", coOwnerTeam, "10001", "PROJ", 0, nil)
	project("github", githubTeam, "PVT_project-only", "", 1, nil)
	project("gitlab", gitlabTeam, "20001", "group/project-only", 1, nil)
	project("linear", linearTeam, "3a4b5c6d-0000-4000-8000-000000000001", "LIN", 1, nil)
	project("gitlab", bothTeam, "20002", "group/both", 1, nil)
	project("jira", inactiveTeam, "10002", "OLD", 1, nil)
	project("linear", endedTeam, "3a4b5c6d-0000-4000-8000-000000000002", "END", 1, today.AddDate(0, 0, -20))

	exec(`INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES (?, 'inactive', ?, 'jira', 0, ?, ?)`, inactiveTeam, org, since, since)

	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES (?, 'group/both', ?, ?, ?, 'gitlab')`, bothRepo, since, since, org)
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'gitlab', ?, ?, 'group/both', 'exact', 'inferred', 1, 1, 0, ?, NULL, ?)`,
		org, bothTeam, bothRepo, since, since)

	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	outcome, err := executor.ComputeOrg(ctx, org, now, 30, "v1", "")
	if err != nil {
		t.Fatalf("compute org: %v", err)
	}
	if outcome.Teams != 6 {
		t.Errorf("evaluated %d teams, want 6 (four project-only owners, one co-owner, and the repository+project owner once)", outcome.Teams)
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
	for _, team := range []string{jiraTeam, githubTeam, gitlabTeam, linearTeam, coOwnerTeam} {
		if written[team] == 0 {
			t.Errorf("team %s owns or co-owns only a project and has no work items: no recommendation rows, want its rules evaluated (rows by team: %v)",
				team, written)
		}
	}
	if written[bothTeam] == 0 || written[bothTeam] != written[jiraTeam] {
		t.Errorf("rows by team = %v, want one evaluation each (the same rule count for %s and %s)", written, bothTeam, jiraTeam)
	}
	for _, team := range []string{inactiveTeam, endedTeam} {
		if written[team] != 0 {
			t.Errorf("team %s (inactive, or its ownership ended before the window end) has %d rows, want none", team, written[team])
		}
	}
}
