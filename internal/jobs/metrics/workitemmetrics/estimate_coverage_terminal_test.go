package workitemmetrics

import (
	"testing"
	"time"
)

// An item leaves the backlog of estimate coverage at its completed_at or closed_at only when its status is terminal
// (done or canceled, IsTerminalStatus). A provider can reopen an item and keep its completed_at or closed_at: that
// open item is still in the backlog of every later day.
func TestAnItemLeavesTheEstimateBacklogAtItsEndOnlyWithATerminalStatus(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	endedBefore := day.Add(-14 * time.Hour)
	points := 3.0
	for _, ends := range []struct {
		name              string
		completed, closed *time.Time
	}{
		{"completed_at", &endedBefore, nil},
		{"closed_at", nil, &endedBefore},
		{"both", &endedBefore, &endedBefore},
	} {
		for _, estimate := range []struct {
			name   string
			points *float64
		}{{"estimated", &points}, {"unestimated", nil}} {
			for _, tc := range []struct {
				status    string
				inBacklog bool
			}{
				{"in_progress", true}, {"todo", true}, {"backlog", true}, {"blocked", true}, {"in_review", true},
				{"unknown", true}, {"done", false}, {"canceled", false},
			} {
				item := Item{
					WorkItemID: "A-1", Provider: "jira", Type: "story", Status: tc.status, CreatedAt: created,
					CompletedAt: ends.completed, ClosedAt: ends.closed, StoryPoints: estimate.points,
				}
				rows := ComputeEstimateCoverage(day, []Item{item}, func(int) Attribution {
					return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
				})
				if len(rows) != 1 {
					t.Fatalf("%s %s %s: %d rows, want 1", ends.name, estimate.name, tc.status, len(rows))
				}
				row := rows[0]
				wantEstimated, wantUnestimated := 0, 0
				if tc.inBacklog && estimate.points != nil {
					wantEstimated = 1
				}
				if tc.inBacklog && estimate.points == nil {
					wantUnestimated = 1
				}
				wantBacklog := wantEstimated + wantUnestimated
				if row.EstimatedCount != wantEstimated || row.UnestimatedCount != wantUnestimated || row.BacklogSize != wantBacklog ||
					(wantBacklog == 0) != (row.Ratio == nil) {
					t.Errorf("%s %s %s: estimated %d, unestimated %d, backlog %d, ratio %v; want %d, %d, %d",
						ends.name, estimate.name, tc.status, row.EstimatedCount, row.UnestimatedCount, row.BacklogSize, row.Ratio,
						wantEstimated, wantUnestimated, wantBacklog)
				}
			}
		}
	}
}

// An item whose end is on or after the end of the day is in that day's backlog, whatever its status.
func TestAnItemEndingAfterTheDayStaysInThatDaysEstimateBacklog(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	endedAfter := day.Add(30 * time.Hour)
	for _, status := range []string{"in_progress", "done", "canceled"} {
		item := Item{WorkItemID: "A-1", Provider: "jira", Type: "story", Status: status, CreatedAt: created, CompletedAt: &endedAfter}
		rows := ComputeEstimateCoverage(day, []Item{item}, func(int) Attribution {
			return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
		})
		if len(rows) != 1 || rows[0].BacklogSize != 1 || rows[0].UnestimatedCount != 1 {
			t.Errorf("%s ended after the day: %+v, want one row with backlog 1, unestimated 1", status, rows)
		}
	}
}
