package daily

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// issueTypeMetricsRowsToWrite decides what one (repository, day) gets. The
// rule has three cases and each one is a different stored result.
func TestIssueTypeMetricsRowsToWriteNeverFillsADayWithNoData(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	zero := func(issueType string) workitemengine.IssueTypeMetricsDailyRow {
		return workitemengine.IssueTypeMetricsDailyRow{
			RepoID: &repoID, Provider: "github", TeamID: "unassigned", IssueTypeNorm: issueType,
		}
	}
	keysOf := func(rows []workitemengine.IssueTypeMetricsDailyRow) map[string][3]int {
		result := map[string][3]int{}
		for _, row := range rows {
			if _, twice := result[row.IssueTypeNorm]; twice {
				t.Fatalf("the key %q is written twice", row.IssueTypeNorm)
			}
			result[row.IssueTypeNorm] = [3]int{row.CreatedCount, row.CompletedCount, row.ActiveCount}
		}
		return result
	}

	t.Run("no count and no live key: nothing is written", func(t *testing.T) {
		rows := issueTypeMetricsRowsToWrite(
			[]workitemengine.IssueTypeMetricsDailyRow{zero("bug"), zero("story")}, nil, repoID)
		if len(rows) != 0 {
			t.Fatalf("a day with no data got %d rows (%v), want 0", len(rows), keysOf(rows))
		}
	})

	t.Run("no count and a live key: only that key gets its row of zeros", func(t *testing.T) {
		// "bug" is a key the compute still produces (with zeros); "gone" is a
		// key the compute no longer produces. Both hold a count in their newest
		// stored row. "story" does not.
		rows := issueTypeMetricsRowsToWrite(
			[]workitemengine.IssueTypeMetricsDailyRow{zero("bug"), zero("story")},
			[]issueTypeMetricsKey{{"github", "unassigned", "bug"}, {"github", "unassigned", "gone"}}, repoID)
		got := keysOf(rows)
		if len(got) != 2 || got["bug"] != [3]int{} || got["gone"] != [3]int{} {
			t.Fatalf("rows = %v, want a row of zeros for bug and for gone, and nothing for story", got)
		}
		if _, held := got["bug"]; !held {
			t.Fatal("the live key bug got no row of zeros: its older row stays the newest one")
		}
		if _, held := got["story"]; held {
			t.Fatal("the key story got a row on a day with no data")
		}
	})

	for _, counted := range []struct {
		name string
		row  workitemengine.IssueTypeMetricsDailyRow
	}{
		{"an item created on the day", workitemengine.IssueTypeMetricsDailyRow{CreatedCount: 1}},
		{"an item completed on the day", workitemengine.IssueTypeMetricsDailyRow{CompletedCount: 1}},
		{"an item active on the day", workitemengine.IssueTypeMetricsDailyRow{ActiveCount: 1}},
	} {
		t.Run(counted.name+": every computed row is written", func(t *testing.T) {
			row := counted.row
			row.RepoID, row.Provider, row.TeamID, row.IssueTypeNorm = &repoID, "github", "unassigned", "bug"
			rows := issueTypeMetricsRowsToWrite(
				[]workitemengine.IssueTypeMetricsDailyRow{row, zero("story")},
				[]issueTypeMetricsKey{{"github", "unassigned", "gone"}}, repoID)
			got := keysOf(rows)
			want := [3]int{counted.row.CreatedCount, counted.row.CompletedCount, counted.row.ActiveCount}
			if len(got) != 3 || got["bug"] != want || got["story"] != [3]int{} || got["gone"] != [3]int{} {
				t.Fatalf("rows = %v, want bug %v, a row of zeros for story (the day has data) and for the live key gone", got, want)
			}
		})
	}
}
