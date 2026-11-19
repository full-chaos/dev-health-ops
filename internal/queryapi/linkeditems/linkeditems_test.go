package linkeditems

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

const (
	testRepo1 = "00000000-0000-4000-8000-0000000000a1"
	testRepo2 = "00000000-0000-4000-8000-0000000000a2"
)

func testCopy(repo, status string, synced time.Time, completed *time.Time) Version {
	created := time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)
	started := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	return Version{
		Item: workitemmetrics.StoredItem{
			WorkItemID: "gh:a/r#1", Provider: "github", Status: status, ProjectID: "a/r", Type: "task",
			CreatedAt: created, StartedAt: &started, CompletedAt: completed, Assignees: []string{"ann"},
		},
		RepoID: repo,
		Synced: synced,
	}
}

func completedByDay(rows map[time.Time][]workitemmetrics.MetricsDailyRow) map[string]int {
	out := map[string]int{}
	for day, dayRows := range rows {
		for _, row := range dayRows {
			if row.ItemsCompleted != 0 {
				out[day.Format("2006-01-02")] += row.ItemsCompleted
			}
		}
	}
	return out
}

// An item stored under two repository ids counts on a day only when its current copy (the newest last_synced over
// every copy; on a tie, the lower repository id) passes the day, as the daily work item tables decide.
func TestRowsByDayCountsAnItemOnItsCurrentCopy(t *testing.T) {
	from, to := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	older, newer := time.Date(2026, 8, 25, 20, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 22, 0, 0, 0, time.UTC)
	at := func(day, hour int) *time.Time {
		value := time.Date(2026, 8, day, hour, 0, 0, 0, time.UTC)
		return &value
	}
	cases := []struct {
		name     string
		versions []Version
		want     map[string]int
		wantRows bool
	}{
		{"newest copy done with no completed_at", []Version{
			testCopy(testRepo1, "done", older, at(20, 10)), testCopy(testRepo2, "done", newer, nil),
		}, map[string]int{}, false},
		{"newest copy done on an earlier day", []Version{
			testCopy(testRepo1, "done", older, at(20, 10)), testCopy(testRepo2, "done", newer, at(18, 10)),
		}, map[string]int{"2026-08-18": 1}, true},
		{"tie on last_synced: the lower repository id is the current copy", []Version{
			testCopy(testRepo2, "done", newer, at(20, 10)), testCopy(testRepo1, "done", newer, nil),
		}, map[string]int{}, false},
		{"single copy", []Version{
			testCopy(testRepo1, "done", newer, at(20, 10)),
		}, map[string]int{"2026-08-20": 1}, true},
		{"newest copy done before the window", []Version{
			testCopy(testRepo1, "done", older, at(20, 10)), testCopy(testRepo2, "done", newer, at(5, 10)),
		}, map[string]int{}, false},
		{"newest copy open, an older copy done in the window", []Version{
			testCopy(testRepo1, "done", older, at(20, 10)), testCopy(testRepo2, "in_progress", newer, nil),
		}, map[string]int{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows := RowsByDay(from, to, c.versions, nil)
			got := completedByDay(rows)
			if len(got) != len(c.want) {
				t.Fatalf("items_completed by day = %v, want %v", got, c.want)
			}
			for day, n := range c.want {
				if got[day] != n {
					t.Fatalf("items_completed by day = %v, want %v", got, c.want)
				}
			}
			if (len(rows) > 0) != c.wantRows {
				t.Fatalf("days with rows = %d, want rows %v", len(rows), c.wantRows)
			}
		})
	}
}

// The request-time read follows the daily rule: a canceled item with started_at and no completed_at or closed_at is
// not in progress at the end of any day; an open item with no end is.
func TestRowsByDayLeavesAFinishedItemWithNoEndOutOfWIP(t *testing.T) {
	from, to := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 25, 22, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		status string
		want   int
	}{
		{"canceled", 0},
		{"in_progress", 1},
	} {
		rows := RowsByDay(from, to, []Version{testCopy(testRepo1, tc.status, synced, nil)}, nil)
		for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
			wip := 0
			for _, row := range rows[day] {
				wip += row.WIPCountEndOfDay
			}
			if wip != tc.want {
				t.Errorf("%s, no completed_at, no closed_at, day %s: wip %d; want %d", tc.status, day.Format("2006-01-02"), wip, tc.want)
			}
		}
	}
}
