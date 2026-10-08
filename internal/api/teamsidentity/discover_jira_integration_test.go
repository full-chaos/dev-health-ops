//go:build integration

package teamsidentity

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// projectAsTeamIDs lists the `teams` rows of the retired project-as-team
// class (provider 'jira', native_team_key = id, no team ARI), the shape
// internal/providersync.RetireJiraProjectAsTeamRows retires.
func projectAsTeamIDs(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND provider = 'jira' "+
		"AND id != '' AND ifNull(native_team_key, '') = id AND NOT startsWith(ifNull(native_team_key, ''), 'ari:cloud:identity::team/') ORDER BY id",
		clickhouse.Named("org_id", orgID))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// TestDiscoverJiraListsOnlyStoredActiveAtlassianTeams: Jira team discovery
// lists the active Atlassian teams of the organization from the catalog
// (no provider request), never a Jira project, an inactive team, an
// admin team or another organization's team. An import of that list, with
// on_conflict skip and with merge, writes no project-as-team row.
func TestDiscoverJiraListsOnlyStoredActiveAtlassianTeams(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"
	doer := &jiraStubDoer{status: 200, body: `{"values": [{"key": "ENG", "name": "Engineering"}, {"key": "OPS", "name": "Operations"}]}`}
	oldClient := fakehttp.Client(discoveryHTTPClient)
	discoveryHTTPClient = fakehttp.Client(doer)
	defer func() { discoveryHTTPClient = fakehttp.Client(oldClient) }()

	now := time.Now().UTC()
	ari := func(id string) *string { value := "ari:cloud:identity::team/" + id; return &value }
	native := func(key string) *string { return &key }
	description := "platform squad"
	seed := []teamInsertRow{
		// An active Atlassian team: the only kind discovery lists.
		{ID: "0a1b2c3d-platform", Name: "Platform", Description: &description, Members: []string{"a@acme.test", "b@acme.test"},
			ProjectKeys: []string{"ENG", "OPS"}, IsActive: true, OrgID: orgID, Provider: "jira", NativeTeamKey: ari("0a1b2c3d-platform")},
		// An archived Atlassian team.
		{ID: "9f8e7d6c-archived", Name: "Archived", IsActive: false, OrgID: orgID, Provider: "jira", NativeTeamKey: ari("9f8e7d6c-archived")},
		// A project-as-team row (the retired class), still active in this store.
		{ID: "ENG", Name: "Engineering", ProjectKeys: []string{"ENG"}, IsActive: true, OrgID: orgID, Provider: "jira", NativeTeamKey: native("ENG")},
		// An admin team whose id equals a project key: provider "" and no native key.
		{ID: "OPS", Name: "Operations", IsActive: true, OrgID: orgID, Provider: ""},
		// Another organization's Atlassian team.
		{ID: "5e5e5e5e-other", Name: "Other", IsActive: true, OrgID: "org-2", Provider: "jira", NativeTeamKey: ari("5e5e5e5e-other")},
	}
	for _, row := range seed {
		row.TeamUUID = teamUUID(row.OrgID, row.ID)
		row.UpdatedAt = now
		if err := store.insertTeamRow(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	teams, err := discoverJira(ctx, store.Conn, orgID, jiraTestCredential())
	if err != nil {
		t.Fatalf("discoverJira: %v", err)
	}
	if doer.request != nil {
		t.Errorf("discoverJira sent %s %s, want no provider request", doer.request.Method, doer.request.URL.Path)
	}
	if len(teams) != 1 {
		t.Fatalf("discoverJira = %+v, want exactly the active Atlassian team of org-1", teams)
	}
	team := teams[0]
	if team.ProviderType != "jira" || team.ProviderTeamID != "0a1b2c3d-platform" || team.Name != "Platform" ||
		team.Description == nil || *team.Description != description || team.MemberCount == nil || *team.MemberCount != 2 {
		t.Errorf("discovered team = %+v, want jira/0a1b2c3d-platform/Platform/platform squad/2 members", team)
	}
	projectKeys, _ := team.Associations.Get("project_keys")
	if keys, ok := projectKeys.([]string); !ok || len(keys) != 2 || keys[0] != "ENG" || keys[1] != "OPS" {
		t.Errorf("associations.project_keys = %v, want [ENG OPS] (the team's stored projects)", projectKeys)
	}
	if providerOrg, _ := team.Associations.Get("provider_org"); providerOrg != "https://acme.atlassian.net" {
		t.Errorf("associations.provider_org = %v, want the credential's base_url", providerOrg)
	}

	before := projectAsTeamIDs(t, ctx, store.Conn, orgID)
	if len(before) != 1 || before[0] != "ENG" {
		t.Fatalf("seeded project-as-team rows = %v, want [ENG]", before)
	}
	for _, onConflict := range []string{"skip", "merge"} {
		for _, discovered := range teams {
			if _, err := store.projectTeam(ctx, orgID, discovered, onConflict); err != nil {
				t.Fatalf("import %s (%s): %v", discovered.ProviderTeamID, onConflict, err)
			}
		}
		after := projectAsTeamIDs(t, ctx, store.Conn, orgID)
		sort.Strings(after)
		if len(after) != 1 || after[0] != "ENG" {
			t.Errorf("project-as-team rows after an import of the discovered list (%s) = %v, want only the seeded [ENG]", onConflict, after)
		}
	}
}
