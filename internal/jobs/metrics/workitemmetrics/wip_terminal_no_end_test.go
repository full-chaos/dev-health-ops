package workitemmetrics

import (
	"testing"
	"time"
)

// A finished item (done or canceled) that the provider stored with no completed_at and no closed_at has no end. It is
// not in progress at the end of any day.
func TestAFinishedItemWithNoEndIsNotInProgress(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	started := created.Add(time.Hour)
	for _, status := range []string{"done", "canceled"} {
		item := Item{
			WorkItemID: "A-1", Provider: "jira", Type: "story", Status: status, CreatedAt: created, StartedAt: &started,
		}
		result := ComputeDailyTriplet(day, []Item{item}, nil, func(int) Attribution {
			return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
		})
		var wip, personWIP int
		for _, row := range result.MetricsDaily {
			wip += row.WIPCountEndOfDay
		}
		for _, row := range result.UserMetricsDaily {
			personWIP += row.WIPCountEndOfDay
		}
		if wip != 0 || personWIP != 0 {
			t.Errorf("status %s, no completed_at, no closed_at: wip %d, person wip %d; want 0, 0", status, wip, personWIP)
		}
	}
}

// An open item with no end, and a finished item whose end is after the day, are still in progress at the end of the day.
func TestAnItemWithNoEndYetStaysInProgress(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	started := created.Add(time.Hour)
	after := day.Add(30 * time.Hour)
	for _, tc := range []struct {
		name              string
		status            string
		completed, closed *time.Time
	}{
		{"open, no end", "in_progress", nil, nil},
		{"done, completed_at after the day", "done", &after, nil},
		{"canceled, closed_at after the day", "canceled", nil, &after},
	} {
		item := Item{
			WorkItemID: "A-1", Provider: "jira", Type: "story", Status: tc.status, CreatedAt: created, StartedAt: &started,
			CompletedAt: tc.completed, ClosedAt: tc.closed,
		}
		result := ComputeDailyTriplet(day, []Item{item}, nil, func(int) Attribution {
			return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
		})
		var wip, personWIP int
		for _, row := range result.MetricsDaily {
			wip += row.WIPCountEndOfDay
		}
		for _, row := range result.UserMetricsDaily {
			personWIP += row.WIPCountEndOfDay
		}
		if wip != 1 || personWIP != 1 {
			t.Errorf("%s: wip %d, person wip %d; want 1, 1", tc.name, wip, personWIP)
		}
	}
}
