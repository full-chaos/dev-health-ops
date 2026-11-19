//go:build integration

package daily

import (
	"context"
	"testing"
	"time"
)

// A work item stored once, started before the day, whose provider reopened it and kept the completed_at or closed_at
// of the day before: at the end of the day it is still in progress in the org view, the scope view and the person rows.
// The same item done or canceled is not.
func TestDailyWIPKeepsAnOpenItemWithAKeptEndBeforeTheDay(t *testing.T) {
	ctx := context.Background()
	for _, ends := range []struct{ name, column string }{{"completed", "completed_at"}, {"closed", "closed_at"}} {
		for _, tc := range []struct {
			status string
			want   float64
		}{
			{"in_progress", 1},
			{"done", 0},
			{"canceled", 0},
		} {
			c := newCurrentCopyCase(t, ctx, "org-14150-"+ends.name+"-"+tc.status+"-")
			endedBefore := c.day.Add(-14 * time.Hour)
			storeWorkItemCopyEndedAt(t, ctx, c, "one", tc.status, ends.column, endedBefore)
			c.compute(t, ctx, c.day)
			var orgWIP, scopeWIP, userWIP float64
			if err := c.conn.QueryRow(ctx, `
SELECT toFloat64(sumIf(wip_count_end_of_day, row_view = 'org')), toFloat64(sumIf(wip_count_end_of_day, row_view = 'scope'))
FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = 'proj-a'`, c.orgID, c.day,
			).Scan(&orgWIP, &scopeWIP); err != nil {
				t.Fatalf("read work_item_metrics_daily: %v", err)
			}
			if err := c.conn.QueryRow(ctx, `
SELECT toFloat64(sum(wip_count_end_of_day))
FROM work_item_user_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = 'proj-a' AND row_view = 'org'`, c.orgID, c.day,
			).Scan(&userWIP); err != nil {
				t.Fatalf("read work_item_user_metrics_daily: %v", err)
			}
			if orgWIP != tc.want || scopeWIP != tc.want || userWIP != tc.want {
				t.Errorf("%s %s, day %s: wip org %v, scope %v, person %v; want %v each",
					ends.column, tc.status, c.day.Format("2006-01-02"), orgWIP, scopeWIP, userWIP, tc.want)
			}
		}
	}
}

func storeWorkItemCopyEndedAt(t *testing.T, ctx context.Context, c currentCopyCase, workItemID, status, column string, endedAt time.Time) {
	t.Helper()
	var completedAt, closedAt *time.Time
	if column == "completed_at" {
		completedAt = &endedAt
	} else {
		closedAt = &endedAt
	}
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, project_key, created_at, started_at, completed_at, closed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	started := c.created.Add(time.Hour)
	if err := batch.Append(
		c.olderRepo, workItemID, "github", "Item "+workItemID, "issue", status, "proj-a", c.created, &started,
		completedAt, closedAt, []string{"quality"}, nil, c.orgID, c.olderSync,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}
