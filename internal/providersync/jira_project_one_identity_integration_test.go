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
// name it by the same native id, so a team that owns the project reaches the
// project's work items through ownership. Both rows come from the real
// producers -- the catalog collector against a provider answer, and the
// work-item normalizer with the route's own `projects` row builder.
//
// The store starts in the state the old catalog left: a team made out of the
// project (a project-as-team) with its lead, open ownership rows and a
// `projects` row on an id built from the project key. The first sync retires
// the project-as-team with every row of it and writes none again: the project
// is a project, and the teams that own it are the linked ops team and the
// Atlassian team. The operator cleanup removes the key-built `projects` row.
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
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('OPS', ?, 'Ops Project', [], [], ['OPS'], [], 1, ?, ?, 'jira', 'OPS')`,
		uuid.New(), old, orgID); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', 'OPS', 'jira:lead-1', [], 'native', 1, 100, 10, ?, ?)`,
		orgID, old, old); err != nil {
		t.Fatal(err)
	}
	// An Atlassian Teams link shares provider and source with the
	// project-as-team rows. It is not a row of that class: it stays.
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

	if got := ownedWorkItems(t, ctx, conn, orgID, "jira", "OPS", firstSync.Add(time.Second)); got != 0 {
		t.Fatalf("the retired project-as-team OPS reaches %d work items through ownership, want 0", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'OPS' AND is_active = 1`, orgID); got != 0 {
		t.Fatalf("%d active project-as-team rows after the sync, want 0", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'OPS' AND is_active = 0`, orgID); got != 1 {
		t.Fatalf("%d inactive project-as-team rows after the sync, want 1 (retired, not removed, not written again)", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND provider = 'jira' AND team_id = 'OPS' AND valid_to IS NULL`, orgID); got != 0 {
		t.Fatalf("%d open ownership rows of the project-as-team after the sync, want 0 on any project id", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = 'jira' AND team_id = 'OPS' AND valid_to IS NULL`, orgID); got != 0 {
		t.Fatalf("%d open membership rows of the project-as-team after the sync, want 0", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM projects FINAL WHERE org_id = ? AND provider = 'jira' AND project_key = 'OPS' AND id != ?`, orgID, keyBuiltID); got != 1 {
		t.Fatalf("the two producers wrote %d project rows for one project, want 1", got)
	}
	if got := countRows(t, ctx, conn, openJiraOwnershipCount, orgID, "OPS", keyBuiltID); got != 0 {
		t.Fatalf("%d ownership rows on the key-built project id are still open after the sync, want 0", got)
	}
	if first.ProjectAsTeamRetired != 4 || first.OwnershipRetracted != 1 || first.OwnershipWritten != 1 || first.TeamsWritten != 0 || first.MembershipsWritten != 0 {
		t.Fatalf("result = %+v, want 4 project-as-team rows retired (the team, its two ownership rows, its lead), 1 ownership row "+
			"written (the linked ops team), its key-built row retracted, and no team or membership row written", first)
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
	if got := countRows(t, ctx, conn, openJiraOwnershipCount, orgID, opsTeam, "10001"); got != 1 {
		t.Fatalf("%d open ownership rows of the linked ops team after two syncs of the same data, want 1", got)
	}
	if got := countRows(t, ctx, conn, openJiraOwnershipCount, orgID, "OPS", "10001"); got != 0 {
		t.Fatalf("%d open ownership rows of the project-as-team after two syncs, want 0: no sync writes one again", got)
	}
	if second.OwnershipRetracted != 0 || second.ProjectAsTeamRetired != 0 {
		t.Fatalf("second sync = %+v, want nothing retracted and nothing retired: the class was retired by the first", second)
	}
	if got := countRows(t, ctx, conn, `SELECT toUInt64(toUnixTimestamp64Milli(min(valid_from))) FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = ? AND project_id = '10001' AND valid_to IS NULL`, orgID, opsTeam); int64(got) != firstSync.UnixMilli() {
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

// The catalog closes an ownership row only on a COMPLETE snapshot. A project
// search that stopped before the provider's last page, or a legacy links
// table that could not be read, is a part of what this writer owns: the rows
// it found are written and no open row is closed. The same store on a
// complete read closes the row of the project the provider no longer has.
// The rows this rule is about are the legacy links (an ops team owns a
// project). A project-as-team row is not under it: the retire step closes it
// at the first run, whatever the snapshot holds.
func TestAPartialJiraSnapshotClosesNoOwnership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := uuid.NewString()
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// Project ZZZ is owned by an ops team through a legacy link, and by the
	// team the old catalog made out of it. It is not on the first page.
	if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', 'ops-team-z', '20001', 'ZZZ', 'jira_legacy', 1, 90, 20, ?, ?), (?, 'jira', 'ZZZ', '20001', 'ZZZ', 'native', 1, 100, 10, ?, ?)`,
		orgID, old, old, orgID, old, old); err != nil {
		t.Fatal(err)
	}
	for key, team := range map[string]string{"ZZZ": "ops-team-z", "OPS": "ops-team-o"} {
		if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
			orgID, key, team, key, team, old); err != nil {
			t.Fatal(err)
		}
	}
	const page2 = "/rest/api/3/project/search?maxResults=100&startAt=1"
	ops := `{"id":"10001","key":"OPS","name":"Ops Project"}`
	sync := func(at time.Time, search map[string]jiraTeamCatalogFixtureResponse) TeamCatalogResult {
		t.Helper()
		byURI := map[string]jiraTeamCatalogFixtureResponse{
			"/rest/api/3/project/OPS": {body: `{"projectTypeKey":"business"}`},
			"/rest/api/3/project/ZZZ": {body: `{"projectTypeKey":"business"}`},
		}
		for uri, response := range search {
			byURI[uri] = response
		}
		result, err := JiraTeamCatalogCollector{
			Handler: JiraTeamCatalogRouteHandler{},
			Sink:    JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
		}.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			providerfoundation.Credential{Provider: "jira"},
			jiraTeamCatalogTestClient(t, fakehttp.Client(&jiraTeamCatalogFixtureDoer{t: t, byURI: byURI})),
			TeamCatalogSelections{Projects: true}, at)
		if err != nil {
			t.Fatalf("team catalog sync: %v", err)
		}
		return result
	}
	open := func(team, project string) uint64 {
		t.Helper()
		return countRows(t, ctx, conn, openJiraOwnershipCount, orgID, team, project)
	}

	// 1. The first page is not the last one and the second page fails.
	result := sync(at, map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[` + ops + `],"isLast":false,"total":2}`},
		page2:                           {status: 403, body: `{}`},
	})
	if got := open("ops-team-z", "20001"); got != 1 {
		t.Fatalf("a search that stopped after its first page left %d open ownership rows for a project on a later page, want 1", got)
	}
	// The retire does not wait for a complete snapshot.
	if result.ProjectAsTeamRetired != 1 || open("ZZZ", "20001") != 0 {
		t.Fatalf("result = %+v, open project-as-team rows = %d; want the project-as-team row retired on a partial snapshot too", result, open("ZZZ", "20001"))
	}
	if result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete || result.OwnershipWritten != 1 || open("ops-team-o", "10001") != 1 {
		t.Fatalf("result = %+v, want the page's one row written, nothing retracted, the snapshot reported as not complete", result)
	}

	// 2. Both pages come: both projects are written, nothing is closed, and
	// ZZZ keeps the valid_from it was first seen with.
	result = sync(at.Add(time.Hour), map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[` + ops + `],"isLast":false,"total":2}`},
		page2:                           {body: `{"values":[{"id":"20001","key":"ZZZ","name":"Zed"}],"isLast":true,"total":2}`},
	})
	if result.OwnershipRetracted != 0 || result.OwnershipSnapshotIncomplete || result.OwnershipWritten != 2 || open("ops-team-z", "20001") != 1 || open("ZZZ", "20001") != 0 {
		t.Fatalf("result = %+v, want both pages' rows written and nothing retracted", result)
	}
	if got := countRows(t, ctx, conn, `SELECT toUInt64(toUnixTimestamp64Milli(min(valid_from))) FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = 'ops-team-z' AND valid_to IS NULL`, orgID); int64(got) != old.UnixMilli() {
		t.Fatalf("ZZZ valid_from = %d, want the first-seen %d", got, old.UnixMilli())
	}

	// 3. The search is complete and no longer has ZZZ, but the legacy links
	// table cannot be read: still a part, nothing closed.
	complete := map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[` + ops + `],"isLast":true,"total":1}`},
	}
	if err := conn.Exec(ctx, `RENAME TABLE jira_project_ops_team_links TO jira_project_ops_team_links_away`); err != nil {
		t.Fatal(err)
	}
	result = sync(at.Add(2*time.Hour), complete)
	if result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete || open("ops-team-z", "20001") != 1 {
		t.Fatalf("result = %+v, open ZZZ rows = %d; want nothing closed while the legacy links cannot be read", result, open("ops-team-z", "20001"))
	}
	if err := conn.Exec(ctx, `RENAME TABLE jira_project_ops_team_links_away TO jira_project_ops_team_links`); err != nil {
		t.Fatal(err)
	}

	// 4. Every read reaches its end: now the provider's answer is the whole
	// truth, and the project it no longer has loses its ownership row.
	result = sync(at.Add(3*time.Hour), complete)
	if result.OwnershipRetracted != 1 || result.OwnershipSnapshotIncomplete || open("ops-team-z", "20001") != 0 || open("ops-team-o", "10001") != 1 {
		t.Fatalf("result = %+v, open ZZZ = %d, open OPS = %d; want ZZZ closed and OPS open on the complete snapshot",
			result, open("ops-team-z", "20001"), open("ops-team-o", "10001"))
	}
	if result.ProjectAsTeamRetired != 0 {
		t.Fatalf("result = %+v, want nothing left to retire after the first run", result)
	}
}

// Archiving a project in Jira does not end its ownership. The project search
// returns live projects only, so the walk also reads the archived ones and
// keeps their open legacy links open. The project-as-team row of an archived
// project is not held: that class is retired as a whole. An archived project
// nothing owned gets no row. When the archived read does
// not reach its end nothing is closed, and when the provider has the project
// in neither answer its rows are closed.
func TestAnArchivedJiraProjectKeepsItsOwnership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := uuid.NewString()
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// ZZZ was synced while it was live: an ops team owns it through a legacy
	// link, and the old catalog made a team out of it. The live project OPS
	// has a legacy link too.
	if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', 'ZZZ', '20001', 'ZZZ', 'native', 1, 100, 10, ?, ?), (?, 'jira', 'ops-team-1', '20001', 'ZZZ', 'jira_legacy', 1, 90, 20, ?, ?)`,
		orgID, old, old, orgID, old, old); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		orgID, "ZZZ", "ops-team-1", "Zed", "Ops Team", old); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		orgID, "OPS", "ops-team-2", "Ops Project", "Ops Team 2", old); err != nil {
		t.Fatal(err)
	}
	live := `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}],"isLast":true,"total":1}`
	sync := func(at time.Time, archived jiraTeamCatalogFixtureResponse) TeamCatalogResult {
		t.Helper()
		result, err := JiraTeamCatalogCollector{
			Handler: JiraTeamCatalogRouteHandler{},
			Sink:    JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
		}.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			providerfoundation.Credential{Provider: "jira"},
			jiraTeamCatalogTestClient(t, fakehttp.Client(&jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
				"/rest/api/3/project/OPS":               {body: `{"projectTypeKey":"business"}`},
				jiraTeamCatalogProjectSearchURI:         {body: live},
				jiraTeamCatalogArchivedProjectSearchURI: archived,
			}})),
			TeamCatalogSelections{Projects: true}, at)
		if err != nil {
			t.Fatalf("team catalog sync: %v", err)
		}
		return result
	}
	open := func(team, project string) uint64 {
		t.Helper()
		return countRows(t, ctx, conn, openJiraOwnershipCount, orgID, team, project)
	}
	firstSeen := func(team string) int64 {
		t.Helper()
		return int64(countRows(t, ctx, conn, `SELECT toUInt64(toUnixTimestamp64Milli(min(valid_from))) FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = ? AND valid_to IS NULL`, orgID, team))
	}

	// 1. ZZZ is archived, with another archived project nothing owns.
	result := sync(at, jiraTeamCatalogFixtureResponse{
		body: `{"values":[{"id":"20001","key":"ZZZ","name":"Zed"},{"id":"30001","key":"NEVER","name":"Never"}],"isLast":true}`})
	if result.OwnershipRetracted != 0 || result.OwnershipSnapshotIncomplete || open("ops-team-1", "20001") != 1 || open("ops-team-2", "10001") != 1 {
		t.Fatalf("result = %+v, open legacy = %d, open OPS = %d; want a complete snapshot that closes no legacy link of the archived project",
			result, open("ops-team-1", "20001"), open("ops-team-2", "10001"))
	}
	if result.ProjectAsTeamRetired != 1 || open("ZZZ", "20001") != 0 {
		t.Fatalf("result = %+v, open project-as-team rows = %d; want the project-as-team row of the archived project retired", result, open("ZZZ", "20001"))
	}
	if firstSeen("ops-team-1") != old.UnixMilli() {
		t.Fatalf("valid_from of the archived project's legacy link = %d; want the first-seen %d", firstSeen("ops-team-1"), old.UnixMilli())
	}
	if open("NEVER", "30001") != 0 {
		t.Fatal("an archived project that had no ownership row got one")
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND id IN ('ZZZ', 'NEVER', 'OPS')`, orgID); got != 0 {
		t.Fatalf("%d team rows for projects, want none: a project is not a team, live or archived", got)
	}
	if got := countRows(t, ctx, conn, `SELECT count() FROM projects FINAL WHERE org_id = ? AND id IN ('20001', '30001')`, orgID); got != 0 {
		t.Fatalf("%d project rows for archived projects, want none", got)
	}

	// 2. The archived read fails: the live answer has no ZZZ, and nothing
	// says ZZZ is gone. Nothing is closed.
	result = sync(at.Add(time.Hour), jiraTeamCatalogFixtureResponse{status: 400, body: `{}`})
	if result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete || open("ops-team-1", "20001") != 1 {
		t.Fatalf("result = %+v, open legacy = %d; want nothing closed and the snapshot reported as not complete when the archived read fails",
			result, open("ops-team-1", "20001"))
	}

	// 3. Both reads reach their end and neither has ZZZ: the project is
	// gone, and its legacy link is closed.
	result = sync(at.Add(2*time.Hour), jiraTeamCatalogFixtureResponse{body: `{"values":[],"isLast":true}`})
	if result.OwnershipRetracted != 1 || result.OwnershipSnapshotIncomplete || open("ops-team-1", "20001") != 0 || open("ops-team-2", "10001") != 1 {
		t.Fatalf("result = %+v, open legacy = %d, open OPS = %d; want the legacy link of the deleted project closed",
			result, open("ops-team-1", "20001"), open("ops-team-2", "10001"))
	}
}

// A store written before the one-id rule names a project by the id built from
// its key. The first sync after the change closes those rows for LIVE
// projects and writes the native ones. For an ARCHIVED project no native row
// is written, so its key-built rows are left open as they are; in a store
// that has both forms for it, both stay. And an answer with no live project
// closes nothing, whatever the archived read holds. All of this is the rule
// of the legacy links. The project-as-team rows of the same store, on either
// id form and for a live or an archived project, are retired by the first run.
func TestAnArchivedJiraProjectKeepsItsKeyBuiltOwnership(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	const insert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', ?, ?, ?, ?, 1, ?, ?, ?, ?)`
	seed := func(orgID, team, project, key, source string) {
		t.Helper()
		specificity, priority := 100, 10
		if source == "jira_legacy" {
			specificity, priority = 90, 20
		}
		if err := conn.Exec(ctx, insert, orgID, team, project, key, source, specificity, priority, old, old); err != nil {
			t.Fatal(err)
		}
	}
	sync := func(orgID string, at time.Time, live, archived string) TeamCatalogResult {
		t.Helper()
		result, err := JiraTeamCatalogCollector{
			Handler: JiraTeamCatalogRouteHandler{},
			Sink:    JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
		}.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			providerfoundation.Credential{Provider: "jira"},
			jiraTeamCatalogTestClient(t, fakehttp.Client(&jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
				"/rest/api/3/project/YAK":                                   {body: `{"projectTypeKey":"business"}`},
				jiraTeamCatalogProjectSearchURI:                             {body: live},
				"/rest/api/3/project/search?maxResults=100&status=archived": {body: archived},
			}})),
			TeamCatalogSelections{Projects: true}, at)
		if err != nil {
			t.Fatalf("team catalog sync: %v", err)
		}
		return result
	}
	const openOf = `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND provider = 'jira' AND team_id = ? AND project_id = ? AND valid_to IS NULL`
	const yak = `{"values":[{"id":"20001","key":"YAK","name":"Yak"}],"isLast":true,"total":1}`
	const oldArchived = `{"values":[{"id":"20002","key":"OLD","name":"Old"}],"isLast":true,"total":1}`
	const none = `{"values":[],"isLast":true,"total":0}`
	prodShaped := func(orgID string) {
		t.Helper()
		seed(orgID, "YAK", orgID+":jira:YAK", "YAK", "native")
		seed(orgID, "OLD", orgID+":jira:OLD", "OLD", "native")
		seed(orgID, "ops-team-1", orgID+":jira:OLD", "OLD", "jira_legacy")
		seed(orgID, "ops-team-2", orgID+":jira:YAK", "YAK", "jira_legacy")
		for key, team := range map[string]string{"OLD": "ops-team-1", "YAK": "ops-team-2"} {
			if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
				orgID, key, team, key, team, old); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("key-built rows only", func(t *testing.T) {
		orgID := uuid.NewString()
		prodShaped(orgID)
		open := func(team, project string) uint64 {
			t.Helper()
			return countRows(t, ctx, conn, openOf, orgID, team, project)
		}
		for run := 1; run <= 2; run++ {
			result := sync(orgID, at.Add(time.Duration(run)*time.Hour), yak, oldArchived)
			if result.OwnershipSnapshotIncomplete || open("ops-team-2", orgID+":jira:YAK") != 0 || open("ops-team-2", "20001") != 1 {
				t.Fatalf("run %d: result = %+v, open key-built YAK = %d, open native YAK = %d; want the legacy link of the live project moved to its native id",
					run, result, open("ops-team-2", orgID+":jira:YAK"), open("ops-team-2", "20001"))
			}
			if open("ops-team-1", orgID+":jira:OLD") != 1 {
				t.Fatalf("run %d: open key-built legacy links of the archived project = %d; want it left open", run, open("ops-team-1", orgID+":jira:OLD"))
			}
			if open("YAK", orgID+":jira:YAK") != 0 || open("YAK", "20001") != 0 || open("OLD", orgID+":jira:OLD") != 0 {
				t.Fatalf("run %d: open project-as-team rows: YAK key-built %d, YAK native %d, OLD key-built %d; want all retired and none written",
					run, open("YAK", orgID+":jira:YAK"), open("YAK", "20001"), open("OLD", orgID+":jira:OLD"))
			}
			if want := map[int]int{1: 2, 2: 0}[run]; result.ProjectAsTeamRetired != want {
				t.Fatalf("run %d: project-as-team rows retired = %d, want %d (both key-built rows, once)", run, result.ProjectAsTeamRetired, want)
			}
			if got := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND project_id = '20002'`, orgID); got != 0 {
				t.Fatalf("run %d: %d rows written on the native id of the archived project, want none", run, got)
			}
			if want := map[int]int{1: 1, 2: 0}[run]; result.OwnershipRetracted != want {
				t.Fatalf("run %d: retracted = %d, want %d (the key-built legacy link of the live project, once)", run, result.OwnershipRetracted, want)
			}
		}
		if got := countRows(t, ctx, conn, `SELECT toUInt64(toUnixTimestamp64Milli(min(valid_from))) FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = 'ops-team-1' AND valid_to IS NULL`, orgID); int64(got) != old.UnixMilli() {
			t.Fatalf("valid_from of the held row = %d, want the stored %d", got, old.UnixMilli())
		}
	})

	t.Run("both id forms for the archived project", func(t *testing.T) {
		orgID := uuid.NewString()
		prodShaped(orgID)
		seed(orgID, "OLD", "20002", "OLD", "native")
		seed(orgID, "ops-team-1", "20002", "OLD", "jira_legacy")
		result := sync(orgID, at.Add(time.Hour), yak, oldArchived)
		for _, row := range [][2]string{{"ops-team-1", orgID + ":jira:OLD"}, {"ops-team-1", "20002"}} {
			if got := countRows(t, ctx, conn, openOf, orgID, row[0], row[1]); got != 1 {
				t.Fatalf("open legacy links of %v = %d, want 1; result = %+v", row, got, result)
			}
		}
		for _, row := range [][2]string{{"OLD", orgID + ":jira:OLD"}, {"OLD", "20002"}} {
			if got := countRows(t, ctx, conn, openOf, orgID, row[0], row[1]); got != 0 {
				t.Fatalf("open project-as-team rows of %v = %d, want 0; result = %+v", row, got, result)
			}
		}
		if result.OwnershipRetracted != 1 || result.OwnershipSnapshotIncomplete || result.ProjectAsTeamRetired != 3 {
			t.Fatalf("result = %+v, want only the key-built legacy link of the live project closed by the snapshot rule and the 3 project-as-team rows retired", result)
		}
	})

	t.Run("no live project closes nothing", func(t *testing.T) {
		orgID := uuid.NewString()
		prodShaped(orgID)
		seed(orgID, "OLD", "20002", "OLD", "native")
		seed(orgID, "YAK", "20001", "YAK", "native")
		result := sync(orgID, at.Add(time.Hour), none, oldArchived)
		closed := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND source = 'jira_legacy' AND valid_to IS NOT NULL`, orgID)
		if closed != 0 || result.OwnershipRetracted != 0 || !result.OwnershipSnapshotIncomplete {
			t.Fatalf("result = %+v, closed legacy links = %d; want no legacy link closed and the snapshot reported as not complete when the live answer is empty", result, closed)
		}
		// The retire reads no provider answer: an empty one does not hold it.
		openProjectAsTeam := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND source = 'native' AND valid_to IS NULL`, orgID)
		if result.ProjectAsTeamRetired != 4 || openProjectAsTeam != 0 {
			t.Fatalf("result = %+v, open project-as-team rows = %d; want the 4 project-as-team rows retired on an empty live answer too", result, openProjectAsTeam)
		}
		// An organization with no project and no open row has nothing to
		// keep: its empty answer is complete.
		if empty := sync(uuid.NewString(), at.Add(time.Hour), none, none); empty.OwnershipSnapshotIncomplete || empty.OwnershipRetracted != 0 {
			t.Fatalf("result = %+v, want a complete snapshot for an organization with no project and no open row", empty)
		}
	})
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
