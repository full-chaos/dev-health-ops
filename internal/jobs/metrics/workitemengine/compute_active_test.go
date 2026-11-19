package workitemengine

import (
	"testing"
	"time"
)

// An item ends on its completed_at only when its status is terminal. A provider can reopen an item and keep its
// completed_at: that open item is active on a day after the old completed_at. A done or canceled item is not.
func TestComputeIssueTypeMetricsDailyCountsAnOpenItemWithACompletedAtBeforeTheDayAsActive(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	for _, tc := range []struct {
		status string
		active int
	}{
		{"in_progress", 1},
		{"todo", 1},
		{"unknown", 1},
		{"", 1},
		{"done", 0},
		{"canceled", 0},
	} {
		items := []Item{{WorkItemID: "1", Provider: "github", Type: "bug", Status: tc.status,
			CreatedAt: day.AddDate(0, 0, -10), CompletedAt: timePointer(day.AddDate(0, 0, -3))}}
		rows := ComputeIssueTypeMetricsDaily(items, day, end, teams(""), labelNormalizer{})
		if len(rows) != 1 || rows[0].ActiveCount != tc.active || rows[0].CompletedCount != 0 {
			t.Errorf("status %q, completed_at before the day: rows %+v, want active=%d completed=0",
				tc.status, rows, tc.active)
		}
	}
}
