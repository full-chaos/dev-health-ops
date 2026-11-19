package workitemmetrics

import (
	"testing"
	"time"
)

// A finished item (done or canceled) that the provider stored with no completed_at and no closed_at has no end, so
// main keeps it in progress at the end of every day after its start. It is not in progress.
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
