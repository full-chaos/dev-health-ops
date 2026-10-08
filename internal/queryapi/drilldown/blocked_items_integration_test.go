//go:build integration

package drilldown

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestBuildIssuesResponseBlockedOnlyReadsTheLatestPositiveSnapshot is the
// real-ClickHouse proof of the CHAOS-8489/CHAOS-8106 reader contract. It
// seeds all supported work-item providers, a same-id second organization,
// team scope and an old positive superseded by zero. The production query must
// group its latest daily snapshot before filtering, then count the distinct
// work items in the requested window before applying the page limit.
func TestBuildIssuesResponseBlockedOnlyReadsTheLatestPositiveSnapshot(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()

	day := func(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }
	computed := func(d, hour int) time.Time { return time.Date(2026, 10, d, hour, 0, 0, 0, time.UTC) }
	insert := func(org string, d time.Time, provider, scope, teamID, teamName, item string, duration float64, at time.Time) {
		t.Helper()
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_blocked_durations_daily
    (day, provider, work_scope_id, team_id, team_name, work_item_id, duration_hours, computed_at, org_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			d, provider, scope, teamID, teamName, item, duration, at, org)
	}

	// This old positive must disappear. The later zero is the latest snapshot
	// for exactly the same migration-104 identity.
	insert(f.OrgA, day(1), "github", "acme/api", "team-a", "Team A", "github:acme/api#stale", 4, computed(1, 1))
	insert(f.OrgA, day(1), "github", "moved/api", "team-a", "Team A", "github:acme/api#stale", 0, computed(1, 2))

	// One supported provider each. The GitHub item appears on two days but is
	// one work item in the window count and result list.
	insert(f.OrgA, day(1), "github", "acme/api", "team-a", "Team A", "github:acme/api#1", 2, computed(1, 1))
	insert(f.OrgA, day(2), "github", "acme/api", "team-a", "Team A", "github:acme/api#1", 3, computed(2, 1))
	insert(f.OrgA, day(1), "gitlab", "acme/api", "team-a", "Team A", "gitlab:acme/api#2", 1, computed(1, 1))
	insert(f.OrgA, day(1), "jira", "OPS", "team-a", "Team A", "jira:OPS-3", 1, computed(1, 1))
	insert(f.OrgA, day(1), "linear", "ENG", "team-a", "Team A", "linear:ENG-4", 1, computed(1, 1))
	insert(f.OrgA, day(1), "github", "other/api", "team-b", "Team B", "github:other/api#5", 1, computed(1, 1))

	// Same provider/id in another organization must never be admitted by the
	// source's raw-table org predicate.
	insert(f.OrgB, day(1), "github", "acme/api", "team-a", "Other Team", "github:acme/api#1", 9, computed(1, 3))

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	params := IssueParams{
		StartDay: day(1), EndDay: day(3), ScopeLevel: "team", ScopeIDs: []string{"team-a"}, Limit: 50, BlockedOnly: true,
	}
	response, err := BuildIssuesResponse(ctx, reader, f.OrgA, params)
	if err != nil {
		t.Fatal(err)
	}
	if response.Count == nil || *response.Count != 4 {
		t.Fatalf("team-a count = %v, want 4", response.Count)
	}
	got := map[string]string{}
	for _, item := range response.Items {
		got[item.WorkItemID] = item.Provider
		if item.Status != "blocked" || item.TeamID == nil || *item.TeamID != "team-a" {
			t.Fatalf("team-a item = %+v, want a blocked team-a item", item)
		}
		if item.TeamName == nil || *item.TeamName != "Team A" {
			t.Fatalf("team-a item team_name = %v, want Team A", item.TeamName)
		}
	}
	want := map[string]string{
		"github:acme/api#1": "github",
		"gitlab:acme/api#2": "gitlab",
		"jira:OPS-3":        "jira",
		"linear:ENG-4":      "linear",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("team-a blocked items = %v, want %v", got, want)
	}

	params.Limit = 2
	response, err = BuildIssuesResponse(ctx, reader, f.OrgA, params)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 2 || response.Count == nil || *response.Count != 4 {
		t.Fatalf("limited response = %+v, want two items with count 4", response)
	}

	params.Limit = 50
	params.ScopeLevel, params.ScopeIDs = "org", nil
	response, err = BuildIssuesResponse(ctx, reader, f.OrgA, params)
	if err != nil {
		t.Fatal(err)
	}
	if response.Count == nil || *response.Count != 5 {
		t.Fatalf("org-a count = %v, want 5 (the team-b row included, stale/org-b excluded)", response.Count)
	}
}
