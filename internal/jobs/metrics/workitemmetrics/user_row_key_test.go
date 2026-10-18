package workitemmetrics

import (
	"testing"
	"time"
)

// work_item_user_metrics_daily is ordered by (org_id, provider, work_scope_id,
// user_identity, day): the team of a person is a value column, not part of the
// key (migration 116). The compute must therefore give one row for each
// (provider, work scope, person) and day; two rows with one key leave whichever
// is written last.
func TestUserMetricsDailyHasOneRowForEachStoredKey(t *testing.T) {
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	done := day.Add(10 * time.Hour)
	started := day.Add(-48 * time.Hour)
	person := "ana@example.test"
	items := []Item{
		{WorkItemID: "A-1", Provider: "jira", Type: "task", Status: "done", Assignee: &person, SourceIndex: 0,
			CreatedAt: started, StartedAt: &started, CompletedAt: &done},
		{WorkItemID: "A-2", Provider: "jira", Type: "task", Status: "done", Assignee: &person, SourceIndex: 1,
			CreatedAt: started, StartedAt: &started, CompletedAt: &done},
	}
	teams := []Attribution{
		{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"},
		{WorkScopeID: "PAY", TeamID: "team-b", TeamName: "Team B"},
	}
	result := ComputeDailyTriplet(day, items, nil, func(index int) Attribution { return teams[index] })

	seen := map[[3]string]int{}
	for _, row := range result.UserMetricsDaily {
		seen[[3]string{row.Provider, row.WorkScopeID, row.UserIdentity}]++
	}
	for key, rows := range seen {
		if rows > 1 {
			t.Errorf("key %v has %d rows (teams %v): the table stores one", key, rows, userRowTeams(result.UserMetricsDaily))
		}
	}
	completed := 0
	for _, row := range result.UserMetricsDaily {
		completed += row.ItemsCompleted
	}
	if completed != 2 {
		t.Errorf("items_completed over the person's rows = %d, want 2", completed)
	}
}

func userRowTeams(rows []UserMetricsDailyRow) []string {
	teams := make([]string, 0, len(rows))
	for _, row := range rows {
		teams = append(teams, row.TeamID)
	}
	return teams
}

func personItem(index int, id string, person *string, started, completed time.Time) Item {
	return Item{WorkItemID: id, Provider: "jira", Type: "task", Status: "done", Assignee: person, SourceIndex: index,
		CreatedAt: started.Add(-time.Hour), StartedAt: &started, CompletedAt: &completed}
}

// The one row of a person in two teams sums the person's items, takes its cycle
// percentiles over all of them, and is labelled by the team with the most items
// that day (the lowest team id on a tie).
func TestUserMetricsDailyRowOfAPersonInTwoTeamsSumsAndIsLabelledByTheTeamWithTheMostItems(t *testing.T) {
	day := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	person := "ana@example.test"
	begin := day.Add(-48 * time.Hour)
	items := []Item{
		personItem(0, "A-1", &person, begin, day.Add(2*time.Hour)),
		personItem(1, "A-2", &person, begin, day.Add(4*time.Hour)),
		personItem(2, "A-3", &person, begin, day.Add(6*time.Hour)),
	}
	for _, tc := range []struct {
		name             string
		teams            []string
		wantID, wantName string
	}{
		{"the team with the most items", []string{"team-b", "team-a", "team-b"}, "team-b", "Team team-b"},
		{"a tie goes to the lowest team id", []string{"team-b", "team-a", "team-c"}, "team-a", "Team team-a"},
	} {
		result := ComputeDailyTriplet(day, items, nil, func(index int) Attribution {
			return Attribution{WorkScopeID: "PAY", TeamID: tc.teams[index], TeamName: "Team " + tc.teams[index]}
		})
		if len(result.UserMetricsDaily) != 1 {
			t.Fatalf("%s: %d rows, want 1: %+v", tc.name, len(result.UserMetricsDaily), result.UserMetricsDaily)
		}
		if result.UserRowsSpanningTeams != 1 || result.SpanningTeamsByProvider["jira"] != 1 {
			t.Errorf("%s: spanning persons = %d (%v), want 1 for jira", tc.name, result.UserRowsSpanningTeams, result.SpanningTeamsByProvider)
		}
		row := result.UserMetricsDaily[0]
		if row.TeamID != tc.wantID || row.TeamName != tc.wantName || row.ItemsCompleted != 3 || row.ItemsStarted != 0 {
			t.Errorf("%s: row = team %q (%q), completed %d, started %d; want team %q (%q), completed 3, started 0",
				tc.name, row.TeamID, row.TeamName, row.ItemsCompleted, row.ItemsStarted, tc.wantID, tc.wantName)
		}
		if row.CycleTimeP50Hours == nil {
			t.Errorf("%s: no cycle time over the person's three completed items", tc.name)
		}
	}
}
