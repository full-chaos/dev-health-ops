package workitemmetrics

import (
	"testing"
	"time"
)

// An item's work in progress ends at its completed_at or closed_at only when its status is terminal (done or
// canceled, IsTerminalStatus, the rule of the cycle rows and of items_completed). A provider can reopen an item and
// keep its completed_at or closed_at: that open item is still in progress at the end of every later day.
func TestAnItemLeavesWIPAtItsEndOnlyWithATerminalStatus(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	started := created.Add(time.Hour)
	endedBefore := day.Add(-14 * time.Hour)
	for _, ends := range []struct {
		name              string
		completed, closed *time.Time
	}{
		{"completed_at", &endedBefore, nil},
		{"closed_at", nil, &endedBefore},
		{"both", &endedBefore, &endedBefore},
	} {
		for _, tc := range []struct {
			status string
			wip    bool
		}{
			{"in_progress", true}, {"todo", true}, {"backlog", true}, {"blocked", true}, {"in_review", true},
			{"unknown", true}, {"done", false}, {"canceled", false},
		} {
			item := Item{
				WorkItemID: "A-1", Provider: "jira", Type: "story", Status: tc.status, CreatedAt: created,
				StartedAt: &started, CompletedAt: ends.completed, ClosedAt: ends.closed,
			}
			result := ComputeDailyTriplet(day, []Item{item}, nil, func(int) Attribution {
				return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
			})
			want := 0
			if tc.wip {
				want = 1
			}
			var wip, unassigned, personWIP, splitWIP, completed int
			var ageP50 *float64
			for _, row := range result.MetricsDaily {
				wip += row.WIPCountEndOfDay
				unassigned += row.WIPUnassignedEndOfDay
				completed += row.ItemsCompleted
				ageP50 = row.WIPAgeP50Hours
			}
			for _, row := range result.UserMetricsDaily {
				personWIP += row.WIPCountEndOfDay
				for _, entry := range row.TeamSplit {
					splitWIP += entry.WIPCountEndOfDay
				}
			}
			if wip != want || unassigned != want || personWIP != want || splitWIP != want || completed != 0 {
				t.Errorf("%s %s: wip %d, wip unassigned %d, person wip %d, team split wip %d, completed %d; want %d, %d, %d, %d, 0",
					ends.name, tc.status, wip, unassigned, personWIP, splitWIP, completed, want, want, want, want)
			}
			if tc.wip && (ageP50 == nil || *ageP50 != Hours(day.AddDate(0, 0, 1).Sub(started))) {
				t.Errorf("%s %s: wip age p50 %v, want %v", ends.name, tc.status, ageP50, Hours(day.AddDate(0, 0, 1).Sub(started)))
			}
		}
	}
}
