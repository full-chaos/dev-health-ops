//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The kept side of a run of the whole organization, for each provider of the
// matrix. Each provider has one CURRENT item in a work scope of its own form
// (jira: project key; gitlab: project id; github: project name; linear: team
// key only; and one item with no scope value at all), a row of an earlier
// compute under that current scope and another team id, and the stored rows
// of a scope with no item. After the run each current scope holds its item
// under its present team only, and each scope with no item holds nothing.
func TestARunOfTheWholeOrganizationKeepsTheCurrentScopesOfEveryProvider(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	float := func(query string, args ...any) float64 {
		t.Helper()
		var value float64
		if err := conn.QueryRow(ctx, query, args...).Scan(&value); err != nil {
			t.Fatalf("read: %v\n%s", err, query)
		}
		return value
	}
	const org = "00000000-0000-4000-8000-0000006e0090"
	day := sharedScopeDay
	stored := day.Add(30 * time.Hour)
	clock := stored.Add(10 * time.Hour)
	seedSharedScopeTeams(t, ctx, conn, org, day.Add(-72*time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
		staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
	)
	seedOrgDay(t, ctx, exec, org, stored)
	items := []struct {
		provider, id, projectKey, projectID, projectName, teamKey, scope string
		repo                                                             uuid.UUID
	}{
		{"jira", "J-1", "LIVEJ", "", "", "", "LIVEJ", uuid.Nil},
		{"gitlab", "G-1", "", "live-gitlab", "", "", "live-gitlab", sharedScopeRepoAPI},
		{"github", "H-1", "", "", "live-github", "", "live-github", sharedScopeRepoWeb},
		{"linear", "L-1", "", "", "", "ENG", "ENG", uuid.Nil},
		{"github", "H-2", "", "", "", "", "", sharedScopeRepoWeb},
	}
	for _, item := range items {
		exec("current item "+item.id, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_key, project_id, project_name, native_team_key, created_at, started_at, story_points, org_id, last_synced)
    VALUES (?, ?, ?, 'story', 'in_progress', ?, ?, ?, ?, ?, ?, 2, ?, ?)`,
			item.repo, item.id, item.provider, item.projectKey, item.projectID, item.projectName, item.teamKey,
			day.Add(-48*time.Hour), day.Add(-24*time.Hour), org, day.Add(12*time.Hour))
		exec("earlier row of the current scope", `INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, ?, ?, 'old-team', 'old', 10, 9, 5, ?, ?)`, day, item.provider, item.scope, stored, org)
	}
	for _, repo := range []uuid.UUID{sharedScopeRepoAPI, sharedScopeRepoWeb} {
		exec("insert repo", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)",
			repo, "acme/"+repo.String()[30:], org, stored)
	}
	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := discoverer.RepositoryIDs(ctx, org)
	if err != nil {
		t.Fatal(err)
	}
	// The two repositories and the partition of the items with no repository.
	if len(listed) != 3 {
		t.Fatalf("the discovery lists %v, want two repositories and the no-repository partition: the case is not set", listed)
	}
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: listed}
	for _, repo := range listed {
		for _, family := range sharedScopeFamilies {
			runSharedScopeFamily(t, ctx, conn, family, run, uuid.MustParse(string(repo)), clock)
		}
	}
	endStaleKeyRun(t, ctx, conn, run, clock)

	const wip = `SELECT toFloat64(sum(wip_count_end_of_day)) FROM work_item_metrics_daily FINAL WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ?`
	const backlog = `SELECT toFloat64(sum(backlog_size)) FROM estimate_coverage_metrics_daily FINAL WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ?`
	for _, item := range items {
		gotWIP := float(wip, org, day, item.provider, item.scope)
		gotBacklog := float(backlog, org, day, item.provider, item.scope)
		old := float(wip+" AND team_id = 'old-team'", org, day, item.provider, item.scope)
		if gotWIP != 1 || gotBacklog != 1 || old != 0 {
			t.Errorf("%s scope %q holds work in progress %v and backlog %v (want 1 and 1) and %v under the earlier team id (want 0)",
				item.provider, item.scope, gotWIP, gotBacklog, old)
		}
	}
	for _, provider := range orgDayProviders {
		if got := float(wip, org, day, provider, "gone-"+provider); got != 0 {
			t.Errorf("%s: the scope with no item still holds %v", provider, got)
		}
	}
}
