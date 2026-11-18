//go:build integration

package daily

import (
	"context"
	"testing"
)

// A work item stored once, under one repository, whose provider reopened it and kept its completed_at: the day of that
// completed_at counts no completion in the org view, the scope view or the person rows. The same item done counts one.
func TestDailyCountsDoNotCompleteASingleCopyOpenItemWithACompletedAt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		status string
		want   float64
	}{
		{"in_progress", 0},
		{"done", 1},
	} {
		c := newCurrentCopyCase(t, ctx, "org-14138-"+tc.status+"-")
		storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "one", tc.status, "proj-a", c.created, &c.doneD, c.olderSync)
		c.compute(t, ctx, c.day)
		got := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a")
		if got.orgCompleted != tc.want || got.scopeCompleted != tc.want || got.userCompleted != tc.want {
			t.Errorf("status %s, day %s: %+v, want %v completed in the org view, the scope view and the person rows",
				tc.status, c.day.Format("2006-01-02"), got, tc.want)
		}
	}
}
