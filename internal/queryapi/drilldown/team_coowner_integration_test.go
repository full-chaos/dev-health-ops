//go:build integration

package drilldown

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

var coOwnerItems = map[string]string{
	"github:acme/api#1": "github",
	"gitlab:acme/api#2": "gitlab",
	"jira:OPS-3":        "jira",
	"linear:ENG-4":      "linear",
}

// seedCoOwnedItems writes one completed item per provider and the attribution
// rows the cascade writes for a project of teams A and B: A primary (1), B
// co-owner (2) when withCoOwner, and both ownership rows (0). An inactive
// team C has no row.
func seedCoOwnedItems(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, withCoOwner bool) {
	t.Helper()
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	computed := day.Add(2 * time.Hour)
	repoID := uuid.New()
	attribution := func(item, provider, team, source string, isPrimary uint8) {
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'high', ?, ?)`,
			org, repoID, item, provider, team, "Team "+team, source, isPrimary, source+"=KEY", computed)
	}
	for item, provider := range coOwnerItems {
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, ?, ?, 'KEY', 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`,
			item, provider, day, day, day, day.Add(time.Hour), computed, org)
		attribution(item, provider, "team-a", "issue_project", 1)
		attribution(item, provider, "team-a", "project_ownership", 0)
		attribution(item, provider, "team-b", "project_ownership", 0)
		if withCoOwner {
			attribution(item, provider, "team-b", "issue_project", 2)
		}
	}
}

func issueTeams(t *testing.T, response *IssuesResponse) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, item := range response.Items {
		if _, duplicate := result[item.WorkItemID]; duplicate {
			t.Fatalf("item %s listed twice: %+v", item.WorkItemID, response.Items)
		}
		team := ""
		if item.TeamID != nil {
			team = *item.TeamID
		}
		result[item.WorkItemID] = team
	}
	return result
}

// The issues of a project of teams A and B are in team A's view and in team
// B's view, each time under that team; not in the inactive team C's view;
// and once in the organization view. The organization view with the
// co-owner rows equals the view of the same store without them.
func TestAnIssueOfAProjectOfTwoTeamsIsInEachTeamsViewAndOnceInTheOrgView(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	seedCoOwnedItems(ctx, t, admin, f.OrgA, true)
	seedCoOwnedItems(ctx, t, admin, f.OrgB, false)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	read := func(org, scope string, teams ...string) map[string]string {
		t.Helper()
		response, err := BuildIssuesResponse(ctx, reader, org, IssueParams{
			StartDay: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), EndDay: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC),
			ScopeLevel: scope, ScopeIDs: teams, Limit: 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		return issueTeams(t, response)
	}
	every := func(team string) map[string]string {
		result := map[string]string{}
		for item := range coOwnerItems {
			result[item] = team
		}
		return result
	}
	for _, team := range []string{"team-a", "team-b"} {
		if got := read(f.OrgA, "team", team); !reflect.DeepEqual(got, every(team)) {
			t.Errorf("%s view = %v, want every item under %s", team, got, team)
		}
	}
	if got := read(f.OrgA, "team", "team-c"); len(got) != 0 {
		t.Errorf("inactive team-c view = %v, want empty", got)
	}
	orgWith := read(f.OrgA, "org")
	if !reflect.DeepEqual(orgWith, every("team-a")) {
		t.Errorf("org view = %v, want every item once, under its primary team", orgWith)
	}
	if orgWithout := read(f.OrgB, "org"); !reflect.DeepEqual(orgWith, orgWithout) {
		t.Errorf("org view with co-owner rows = %v, without = %v: a co-owner row changed the org view", orgWith, orgWithout)
	}
	if got := read(f.OrgB, "team", "team-b"); len(got) != 0 {
		t.Errorf("team-b view without a co-owner row = %v, want empty (an ownership row alone is not attribution)", got)
	}
	// A team(s) view of A and B shows each item once under EACH team.
	response, err := BuildIssuesResponse(ctx, reader, f.OrgA, IssueParams{
		StartDay: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), EndDay: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC),
		ScopeLevel: "team", ScopeIDs: []string{"team-a", "team-b"}, Limit: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	pairs := []string{}
	for _, item := range response.Items {
		pairs = append(pairs, item.WorkItemID+"@"+*item.TeamID)
	}
	sort.Strings(pairs)
	want := []string{}
	for item := range coOwnerItems {
		want = append(want, item+"@team-a", item+"@team-b")
	}
	sort.Strings(want)
	if !reflect.DeepEqual(pairs, want) {
		t.Errorf("team(s) view a+b = %v, want %v", pairs, want)
	}
}

// Team B was a co-owner (2) in an older attribution run and then left the
// project; the newer run wrote only team A. B's old row keeps its own key
// (team-b, issue_project) and stays stored until a merge, so only the newest
// computed_at fence of the team-scoped read keeps the item out of team B's
// view. Every provider.
func TestAStaleCoOwnerRowIsNotInTheTeamView(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	org := crossorg.Default().OrgA
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	repoID := uuid.New()
	first, second := day.Add(2*time.Hour), day.Add(26*time.Hour)
	for item, provider := range coOwnerItems {
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, ?, ?, 'KEY', 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`,
			item, provider, day, day, day, day.Add(time.Hour), first, org)
		attribution := func(team string, isPrimary uint8, computed time.Time) {
			crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, ?, ?, ?, 'issue_project', ?, 'high', 'issue_project_key=KEY', ?)`,
				org, repoID, item, provider, team, "Team "+team, isPrimary, computed)
		}
		attribution("team-a", 1, first)
		attribution("team-b", 2, first)
		attribution("team-a", 1, second)
	}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	view := func(team string) *IssuesResponse {
		t.Helper()
		response, err := BuildIssuesResponse(ctx, reader, org, IssueParams{
			StartDay: day, EndDay: day.Add(48 * time.Hour), ScopeLevel: "team", ScopeIDs: []string{team}, Limit: 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	if got := view("team-b"); len(got.Items) != 0 {
		t.Errorf("team-b view after team-b left = %+v, want empty", got.Items)
	}
	if got := view("team-a"); len(got.Items) != len(coOwnerItems) {
		t.Errorf("team-a view = %d items, want %d", len(got.Items), len(coOwnerItems))
	}
}
