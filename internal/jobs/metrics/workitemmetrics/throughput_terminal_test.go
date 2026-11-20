package workitemmetrics

import (
	"testing"
	"time"
)

// The weekly throughput under wip_congestion_ratio counts an item only when its status is terminal (done or canceled,
// IsTerminalStatus) and its completed_at is in the week. A provider can reopen an item and keep its completed_at: that
// open item is no throughput. The group also holds one plain item in progress, so the ratio is wip / max(1, throughput).
func TestWeeklyThroughputCountsAnItemOnlyWithATerminalStatus(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	inProgressStart := day.Add(-95 * time.Hour)
	inProgress := Item{
		WorkItemID: "W-1", Provider: "jira", Type: "story", Status: "in_progress",
		CreatedAt: day.Add(-96 * time.Hour), StartedAt: &inProgressStart,
	}
	at := func(value time.Time) *time.Time { return &value }
	for _, shape := range []struct {
		name               string
		created            time.Time
		started, completed *time.Time
		terminalThroughput int
	}{
		{"created and completed on the day", day.Add(2 * time.Hour), nil, at(day.Add(5 * time.Hour)), 2},
		{"started again on the day, completed 3 days before", day.AddDate(0, 0, -10), at(day.Add(3 * time.Hour)), at(day.AddDate(0, 0, -3).Add(10 * time.Hour)), 2},
		{"started 9 days before, completed 2 days before", day.AddDate(0, 0, -10), at(day.AddDate(0, 0, -9)), at(day.AddDate(0, 0, -2).Add(10 * time.Hour)), 0},
	} {
		for _, tc := range []struct {
			status   string
			terminal bool
		}{
			{"in_progress", false}, {"todo", false}, {"backlog", false}, {"blocked", false}, {"in_review", false},
			{"unknown", false}, {"done", true}, {"canceled", true},
		} {
			items := []Item{inProgress}
			for _, id := range []string{"A-1", "A-2"} {
				items = append(items, Item{
					WorkItemID: id, Provider: "jira", Type: "story", Status: tc.status, CreatedAt: shape.created,
					StartedAt: shape.started, CompletedAt: shape.completed,
				})
			}
			result := ComputeDailyTriplet(day, items, nil, func(int) Attribution {
				return Attribution{WorkScopeID: "PAY", TeamID: "team-a", TeamName: "Team A"}
			})
			throughput := 0
			if tc.terminal {
				throughput = shape.terminalThroughput
			}
			if len(result.MetricsDaily) != 1 {
				t.Fatalf("%s %s: %d metrics rows, want 1", shape.name, tc.status, len(result.MetricsDaily))
			}
			row := result.MetricsDaily[0]
			want := float64(row.WIPCountEndOfDay) / maxFloat(1, float64(throughput))
			if row.WIPCountEndOfDay == 0 || row.WIPCongestionRatio != want {
				t.Errorf("%s %s: wip_congestion_ratio %v with wip %d; want %v (weekly throughput %d)",
					shape.name, tc.status, row.WIPCongestionRatio, row.WIPCountEndOfDay, want, throughput)
			}
		}
	}
}
