package workitemmetrics

import (
	"reflect"
	"testing"
	"time"
)

// A person's row keeps one team label, and its team split holds the completed
// and open items of each team of the items (CHAOS-12535): the primary team of
// each item (unassigned for an item with none) and each co-owner team. The
// primary entries add up to the row's counts; an item counts once per team.
func TestTheUserRowSplitAddsUpToTheRow(t *testing.T) {
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	person := "ana@example.test"
	started := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	startedToday := day.Add(2 * time.Hour)
	item := func(index int, id string, completed *time.Time, start time.Time) Item {
		return Item{WorkItemID: id, Provider: "jira", Type: "task", Status: "in_progress", Assignee: &person,
			SourceIndex: index, CreatedAt: start.Add(-time.Hour), StartedAt: &start, CompletedAt: completed}
	}
	items := []Item{
		item(0, "A-1", &done, started),    // done, team-b
		item(1, "A-2", nil, started),      // open, team-b
		item(2, "A-3", nil, started),      // open, no team
		item(3, "A-4", &done, started),    // done, team-a, co-owned by team-b (and team-a again)
		item(4, "A-5", nil, startedToday), // open, team-a, co-owned by team-c
		item(5, "A-6", nil, started),      // open, team-b
	}
	teams := []Attribution{
		{WorkScopeID: "PAY", TeamID: "team-b", TeamName: "Team B"},
		{WorkScopeID: "PAY", TeamID: "team-b", TeamName: "Team B"},
		{WorkScopeID: "PAY", TeamID: UnassignedTeamID, TeamName: UnassignedTeamName},
		{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A", CoOwnerTeamIDs: []string{"team-b", "team-a", " ", "team-b"}},
		{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A", CoOwnerTeamIDs: []string{" team-c "}},
		{WorkScopeID: "PAY", TeamID: "team-b", TeamName: "Team B"},
	}
	result := ComputeDailyTriplet(day, items, nil, func(index int) Attribution { return teams[index] })
	if len(result.UserMetricsDaily) != 1 {
		t.Fatalf("rows = %+v, want one row for the person", result.UserMetricsDaily)
	}
	row := result.UserMetricsDaily[0]
	if row.TeamID != "team-b" || row.ItemsCompleted != 2 || row.WIPCountEndOfDay != 4 {
		t.Fatalf("row = %+v, want label team-b (3 of 6 items), 2 done, 4 open", row)
	}
	want := []UserTeamCount{
		{TeamID: "team-a", IsPrimary: 1, ItemsCompleted: 1, WIPCountEndOfDay: 1},
		{TeamID: "team-b", IsPrimary: 1, ItemsCompleted: 1, WIPCountEndOfDay: 2},
		{TeamID: "team-b", IsPrimary: 2, ItemsCompleted: 1},
		{TeamID: "team-c", IsPrimary: 2, WIPCountEndOfDay: 1},
		{TeamID: UnassignedTeamID, IsPrimary: 1, WIPCountEndOfDay: 1},
	}
	if !reflect.DeepEqual(row.TeamSplit, want) {
		t.Errorf("split = %+v,\n want %+v", row.TeamSplit, want)
	}
	var completed, wip int
	for _, entry := range row.TeamSplit {
		if entry.IsPrimary == 1 {
			completed += entry.ItemsCompleted
			wip += entry.WIPCountEndOfDay
		}
	}
	if completed != row.ItemsCompleted || wip != row.WIPCountEndOfDay {
		t.Errorf("the primary entries hold %d done and %d open, the row %d and %d", completed, wip, row.ItemsCompleted, row.WIPCountEndOfDay)
	}
	// The rollups keep the primary team only: no co-owner row.
	for _, metrics := range result.MetricsDaily {
		if metrics.TeamID == "team-c" {
			t.Errorf("work_item_metrics_daily has a team-c row %+v: the rollups keep the primary team", metrics)
		}
	}
}

// An item that is only created, or only started and done before the day, is in
// no entry: the split holds done and open items, as the row's counts do.
func TestTheUserRowSplitHoldsNoEntryWithoutADoneOrOpenItem(t *testing.T) {
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	person := "ana@example.test"
	created := day.Add(3 * time.Hour)
	items := []Item{{WorkItemID: "A-1", Provider: "jira", Type: "task", Status: "todo", Assignee: &person,
		SourceIndex: 0, CreatedAt: created}}
	result := ComputeDailyTriplet(day, items, nil, func(int) Attribution {
		return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A", CoOwnerTeamIDs: []string{"team-b"}}
	})
	if len(result.UserMetricsDaily) != 1 || len(result.UserMetricsDaily[0].TeamSplit) != 0 {
		t.Errorf("rows = %+v, want one row with an empty split", result.UserMetricsDaily)
	}
}
