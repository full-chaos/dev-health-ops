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
