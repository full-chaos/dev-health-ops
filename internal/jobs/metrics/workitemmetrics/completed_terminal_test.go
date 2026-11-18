package workitemmetrics

import (
	"testing"
	"time"
)

// An item completes on a day only when its status is terminal (done or canceled, IsTerminalStatus, the rule of the
// cycle rows) and its completed_at is on the day. A provider can reopen an item and keep its completed_at: that open
// item completes on no day, and none of the completion columns count it.
func TestAnItemCompletesOnTheDayOnlyWithATerminalStatus(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	started := created.Add(time.Hour)
	completed := day.Add(10 * time.Hour)
	points := 3.0
	for _, tc := range []struct {
		status    string
		completes bool
	}{
		{"in_progress", false}, {"todo", false}, {"backlog", false}, {"blocked", false}, {"in_review", false},
		{"unknown", false}, {"done", true}, {"canceled", true},
	} {
		item := Item{
			WorkItemID: "A-1", Provider: "jira", Type: "bug", Status: tc.status, CreatedAt: created,
			StartedAt: &started, CompletedAt: &completed, StoryPoints: &points,
		}
		result := ComputeDailyTriplet(day, []Item{item}, nil, func(int) Attribution {
			return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
		})
		want := 0
		if tc.completes {
			want = 1
		}
		var completedRows, unassigned, personCompleted, splitCompleted int
		var storyPoints, bugRatio float64
		var cycleP50, leadP50, personCycleP50 *float64
		for _, row := range result.MetricsDaily {
			completedRows += row.ItemsCompleted
			unassigned += row.ItemsCompletedUnassigned
			storyPoints += row.StoryPointsCompleted
			bugRatio += row.BugCompletedRatio
			cycleP50, leadP50 = row.CycleTimeP50Hours, row.LeadTimeP50Hours
		}
		for _, row := range result.UserMetricsDaily {
			personCompleted += row.ItemsCompleted
			personCycleP50 = row.CycleTimeP50Hours
			for _, entry := range row.TeamSplit {
				splitCompleted += entry.ItemsCompleted
			}
		}
		if completedRows != want || unassigned != want || personCompleted != want || splitCompleted != want ||
			len(result.CycleTimes) != want {
			t.Errorf("status %s: items_completed %d, unassigned %d, person %d, team split %d, cycle rows %d; want %d each",
				tc.status, completedRows, unassigned, personCompleted, splitCompleted, len(result.CycleTimes), want)
		}
		if tc.completes {
			if storyPoints != points || bugRatio != 1 || cycleP50 == nil || leadP50 == nil || personCycleP50 == nil {
				t.Errorf("status %s: story points %v, bug ratio %v, cycle p50 %v, lead p50 %v, person cycle p50 %v; want 3, 1 and set p50s",
					tc.status, storyPoints, bugRatio, cycleP50, leadP50, personCycleP50)
			}
			continue
		}
		if storyPoints != 0 || bugRatio != 0 || cycleP50 != nil || leadP50 != nil || personCycleP50 != nil {
			t.Errorf("status %s: story points %v, bug ratio %v, cycle p50 %v, lead p50 %v, person cycle p50 %v; want 0, 0 and no p50",
				tc.status, storyPoints, bugRatio, cycleP50, leadP50, personCycleP50)
		}
	}
}
