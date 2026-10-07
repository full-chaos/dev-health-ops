//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// ownedWorkItems is the question a team answer asks of the store: follow the
// team's project ownership, as the ownership reader of record returns it, to
// the work items of those projects by (provider, project_id). Nothing here
// joins by project key.
func ownedWorkItems(t *testing.T, ctx context.Context, conn driver.Conn, orgID, provider, teamID string, asOf time.Time) uint64 {
	t.Helper()
	links, err := loadTeamRepoOwnershipProjectLinks(ctx, conn, orgID, asOf)
	if err != nil {
		t.Fatalf("load project links: %v", err)
	}
	var total uint64
	for _, link := range links {
		if link.Provider != provider || link.TeamID != teamID {
			continue
		}
		var items uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_items FINAL WHERE org_id = ? AND provider = ? AND project_id = ?`,
			orgID, provider, link.ProjectID).Scan(&items); err != nil {
			t.Fatalf("count work items: %v", err)
		}
		total += items
	}
	return total
}

func countRows(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

const openJiraOwnershipCount = `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND provider = 'jira' AND team_id = ? AND project_id = ? AND valid_to IS NULL`

// One Jira project is one project: the team catalog and the work-items route
// name it by the same native id, so the team that owns the project reaches the
// project's work items through ownership. Both rows come from the real
// producers -- the catalog collector against a provider answer, and the
// work-item normalizer with the route's own `projects` row builder.
//
// The store starts in the state the old catalog left: an open ownership row
// and a `projects` row on an id built from the project key. The first sync
// closes that ownership row; the operator cleanup removes the `projects` row.
func TestTeamReachesItsJiraProjectsWorkItemsThroughOwnership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := uuid.NewString()
	keyBuiltID := orgID + ":jira:OPS"
	atlassianTeam := "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b"
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	firstSync := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	insertOwnership := `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', ?, ?, 'OPS', 'native', 1, 100, 10, ?, ?)`
	for _, validFrom := range []time.Time{old, old.Add(time.Hour)} {
		if err := conn.Exec(ctx, insertOwnership, orgID, "OPS", keyBuiltID, validFrom, validFrom); err != nil {
			t.Fatal(err)
		}
	}
	// An Atlassian Teams link shares provider and source with the catalog's
	// rows. It is not the catalog's to close.
	if err := conn.Exec(ctx, insertOwnership, orgID, atlassianTeam, keyBuiltID, old, old); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, updated_at, last_synced) VALUES (?, ?, 'jira', 'OPS', 'Ops Project', 1, ?, ?)`,
		keyBuiltID, orgID, old, old); err != nil {
		t.Fatal(err)
	}
	// The third writer: the admin-curated project -> ops-team links, which
	// hold project keys only. OPS is a project the provider returns; GONE is
	// one it does not. The old catalog wrote the OPS link on the key-built id.
	const opsTeam, goneTeam = "ops-team-1", "ops-team-2"
	for key, team := range map[string]string{"OPS": opsTeam, "GONE": goneTeam} {
		if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
			orgID, key, team, key, team, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', ?, ?, 'OPS', ?, 1, ?, ?, ?, ?)`,
		orgID, opsTeam, keyBuiltID, jiraTeamCatalogLegacySource, uint16(jiraTeamCatalogLegacySpecificity), int32(jiraTeamCatalogLegacyPriority), old, old); err != nil {
		t.Fatal(err)
	}

	// The work-items route: one issue of project OPS.
	var issue map[string]any
	if err := json.Unmarshal([]byte(`{"key":"OPS-1","self":"https://jira.example.com/rest/api/3/issue/1","fields":{
		"summary":"Fix the pager","issuetype":{"name":"Task"},"status":{"name":"To Do","statusCategory":{"key":"new"}},
		"project":{"id":"10001","key":"OPS","name":"Ops Project"},"created":"2026-09-20T10:00:00.000+0000","updated":"2026-09-21T10:00:00.000+0000"}}`), &issue); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = orgID
	item, _, err := normalizeJiraWorkItem(claim, jiraWorkItemFixtureInput{Raw: issue}, loadRealStatusMapping(t),
		func(string, string, string) string { return "" }, firstSync)
	if err != nil {
		t.Fatalf("normalize work item: %v", err)
	}
	if item.ProjectID == nil || item.ProjectKey == nil {
		t.Fatalf("work item carries no project: %+v", item)
	}
	seedWorkItem(t, ctx, conn, orgID, item.WorkItemID, "jira", uuid.Nil, *item.ProjectID, firstSync)
	ensured := projectmembership.EnsureProjectsRow(orgID, "jira", *item.ProjectID, *item.ProjectKey, "Ops Project", firstSync)
	if err := conn.Exec(ctx, projectmembership.ProjectsInsert+` VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ensured.ID, ensured.OrgID, ensured.Provider, ensured.ProjectKey, ensured.Name, ensured.IsActive, ensured.UpdatedAt, ensured.LastSynced); err != nil {
		t.Fatal(err)
	}

	// The team catalog: the provider returns the same project.
	sync := func(at time.Time) TeamCatalogResult {
		t.Helper()
		doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
			jiraTeamCatalogProjectSearchURI:                                     {body: `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}]}`},
			"/rest/api/3/project/OPS":                                           {body: `{"projectTypeKey":"software"}`},
			"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {body: `{"values":[],"isLast":true}`},
		}}
		result, err := JiraTeamCatalogCollector{
			Handler: JiraTeamCatalogRouteHandler{},
			Sink:    JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
		}.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			providerfoundation.Credential{Provider: "jira"}, jiraTeamCatalogTestClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Projects: true}, at)
		if err != nil {
			t.Fatalf("team catalog sync: %v", err)
		}
		return result
	}
	first := sync(firstSync)

	if got := ownedWorkItems(t, ctx, conn, orgID, "jira", "OPS", firstSync.Add(time.Second)); got != 1 {
		t.Fatalf("team OPS reaches %d work items of its project through ownership, want 1", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM projects FINAL WHERE org_id = ? AND provider = 'jira' AND project_key = 'OPS' AND id != ?`, orgID, keyBuiltID); got != 1 {
		t.Fatalf("the two producers wrote %d project rows for one project, want 1", got)
	}
	if got := countRows(t, ctx, conn, openJiraOwnershipCount, orgID, "OPS", keyBuiltID); got != 0 {
		t.Fatalf("%d ownership rows on the key-built project id are still open after the sync, want 0", got)
	}
	if first.OwnershipRetracted != 3 || first.OwnershipWritten != 2 {
		t.Fatalf("result = %+v, want 2 ownership rows written (the project team and the linked ops team) and the 3 key-built rows retracted", first)
	}
	// The linked ops team owns the project by the same native id, so it
	// reaches the same work item; its row on the key-built id is closed.
	if got := ownedWorkItems(t, ctx, conn, orgID, "jira", opsTeam, firstSync.Add(time.Second)); got != 1 {
		t.Fatalf("the linked ops team reaches %d work items of project OPS through ownership, want 1", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND provider = 'jira' AND team_id = ? AND source = 'jira_legacy' AND valid_to IS NULL`, orgID, opsTeam); got != 1 {
		t.Fatalf("the linked ops team has %d open ownership rows, want 1 (the native id; the key-built one closed)", got)
	}
	// A link whose project the provider did not return has no identity to
	// write: no row under any id, never one built from the key.
	if got := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = ?`, orgID, goneTeam); got != 0 {
		t.Fatalf("%d ownership rows for a link whose project the provider did not return, want 0", got)
	}
	if got := countRows(t, ctx, conn, openJiraOwnershipCount, orgID, atlassianTeam, keyBuiltID); got != 1 {
		t.Fatalf("the catalog sync left %d open Atlassian Teams links, want 1 (not its row to close)", got)
	}

	// The same provider answer a day later is the same fact: no new open row.
	second := sync(firstSync.Add(24 * time.Hour))
	if got := countRows(t, ctx, conn, openJiraOwnershipCount, orgID, "OPS", "10001"); got != 1 {
		t.Fatalf("%d open ownership rows after two syncs of the same data, want 1", got)
	}
	if second.OwnershipRetracted != 0 {
		t.Fatalf("second sync retracted %d rows, want 0", second.OwnershipRetracted)
	}
	if got := countRows(t, ctx, conn, `SELECT toUInt64(toUnixTimestamp64Milli(min(valid_from))) FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = 'OPS' AND project_id = '10001' AND valid_to IS NULL`, orgID); int64(got) != firstSync.UnixMilli() {
		t.Fatalf("valid_from = %d, want the time the fact was first seen %d", got, firstSync.UnixMilli())
	}

	// The operator cleanup. Another organization still has only a key-built
	// row: nothing of it may go.
	otherOrg := uuid.NewString()
	if err := conn.Exec(ctx, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, updated_at, last_synced) VALUES (?, ?, 'jira', 'OPS', 'Other', 1, ?, ?)`,
		otherOrg+":jira:OPS", otherOrg, old, old); err != nil {
		t.Fatal(err)
	}
	// A row of this organization whose id holds the marker under ANOTHER
	// prefix is not the retired form: the form is tied to the row's own org.
	foreignShaped := "elsewhere:jira:OPS"
	if err := conn.Exec(ctx, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, updated_at, last_synced) VALUES (?, ?, 'jira', 'OPS', 'Ops Project', 1, ?, ?)`,
		foreignShaped, orgID, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := RetireJiraKeyProjectRows(ctx, conn, otherOrg, false); !errors.Is(err, ErrJiraKeyProjectCleanupNoNativeRows) {
		t.Fatalf("cleanup of an organization with no native-id row: err = %v, want the refusal", err)
	}
	dry, err := RetireJiraKeyProjectRows(ctx, conn, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if dry.EligibleRows != 1 || dry.HeldRows != 1 || dry.DeletedRows != 0 {
		t.Fatalf("dry run = %+v, want 1 eligible, 1 held, 0 deleted", dry)
	}
	keyBuilt := `SELECT count() FROM projects FINAL WHERE provider = 'jira' AND org_id = ? AND startsWith(id, concat(org_id, ':jira:'))`
	if got := countRows(t, ctx, conn, keyBuilt, orgID); got != 1 {
		t.Fatalf("a dry run left %d key-built rows, want 1", got)
	}
	real, err := RetireJiraKeyProjectRows(ctx, conn, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if real.DeletedRows != 1 {
		t.Fatalf("cleanup = %+v, want 1 deleted", real)
	}
	if got := countRows(t, ctx, conn, keyBuilt, orgID); got != 0 {
		t.Fatalf("%d key-built rows left after the cleanup, want 0", got)
	}
	if got := countRows(t, ctx, conn, keyBuilt, otherOrg); got != 1 {
		t.Fatalf("the cleanup removed the only project row of another organization (%d left)", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM projects FINAL WHERE org_id = ? AND provider = 'jira' AND project_key = 'OPS' AND id IN ('10001', ?)`, orgID, foreignShaped); got != 2 {
		t.Fatalf("%d of the native-id row and the row under another prefix are left after the cleanup, want both", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM projects FINAL WHERE org_id = ? AND provider = 'jira' AND project_key = 'OPS'`, orgID); got != 2 {
		t.Fatalf("project OPS has %d rows after the cleanup, want 2 (its one native-id row and the row this test put under another prefix)", got)
	}
	again, err := RetireJiraKeyProjectRows(ctx, conn, "", false)
	if err != nil || again.EligibleRows != 0 || again.DeletedRows != 0 {
		t.Fatalf("second cleanup = %+v, %v; want nothing to do", again, err)
	}
}

// The pattern Jira follows, one row per provider. A project entity has one
// id: the catalog, the ownership row and the work item name it by the same
// value, so ownership reaches the items by (provider, project_id).
func TestProjectIdentityIsOneIDAcrossCatalogOwnershipAndWorkItems(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := uuid.NewString()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	t.Run("linear", func(t *testing.T) {
		projectID := "6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f"
		if err := (LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}).writeOwnership(ctx, []linearReferenceOwnershipRow{{
			OrgID: orgID, Provider: "linear", TeamID: "ENG", ProjectID: projectID,
			Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: now, UpdatedAt: now,
		}}); err != nil {
			t.Fatal(err)
		}
		seedWorkItem(t, ctx, conn, orgID, "linear:ENG-1", "linear", uuid.Nil, projectID, now)
		if got := ownedWorkItems(t, ctx, conn, orgID, "linear", "ENG", now.Add(time.Second)); got != 1 {
			t.Fatalf("team ENG reaches %d work items through ownership, want 1", got)
		}
	})

	// GitHub has no project ownership table: a team owns repositories
	// (team_repo_ownership). Its one project entity, a Projects V2 board, is
	// named by one id in `projects` and on the work item.
	t.Run("github", func(t *testing.T) {
		boardID := "ghprojv2:acme#3"
		ensured := projectmembership.EnsureProjectsRow(orgID, "github", boardID, "", "Board", now)
		if err := conn.Exec(ctx, projectmembership.ProjectsInsert+` VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			ensured.ID, ensured.OrgID, ensured.Provider, ensured.ProjectKey, ensured.Name, ensured.IsActive, ensured.UpdatedAt, ensured.LastSynced); err != nil {
			t.Fatal(err)
		}
		seedWorkItem(t, ctx, conn, orgID, "gh:acme/api#7", "github", uuid.New(), boardID, now)
		if got := countRows(t, ctx, conn, `SELECT count() FROM work_items AS w FINAL INNER JOIN (SELECT org_id, provider, id FROM projects FINAL) AS p ON p.org_id = w.org_id AND p.provider = w.provider AND p.id = w.project_id WHERE w.org_id = ? AND w.provider = 'github'`, orgID); got != 1 {
			t.Fatalf("%d github work items resolve to their project row by id, want 1", got)
		}
	})

	// KNOWN RED, not fixed here (tracked as the GitLab project identity
	// ticket): GitLab ownership names a project by its PATH and the catalog
	// names the same project "{org}:gitlab:<native id>". The two meet only
	// through project_key. No GitLab work item hangs on a project (a GitLab
	// project is this schema's repository), so no team answer is empty
	// because of it. This case pins the gap as it is: when the GitLab
	// writers get one id, it fails and this row of the table is rewritten.
	t.Run("gitlab known red", func(t *testing.T) {
		project, ok := normalizeGitLabProjectCatalogRow(orgID, gitlabTeamCatalogProjectPayload{ID: "42", PathWithNamespace: "acme/api", Name: "api"}, now)
		if !ok {
			t.Fatal("gitlab project row was not built")
		}
		ownership := normalizeGitLabOwnershipRow(orgID, "gl:acme", "acme/api", gitlabTeamCatalogBaseSpecificity, now)
		if ownership.ProjectID == project.ID {
			t.Fatalf("GitLab ownership and catalog now share the id %q: this is no longer a known red, move the row to the green cases", project.ID)
		}
		if ownership.ProjectKey == nil || project.ProjectKey == nil || *ownership.ProjectKey != *project.ProjectKey {
			t.Fatalf("GitLab ownership and catalog no longer meet through project_key: ownership=%+v project=%+v", ownership, project)
		}
	})
}
