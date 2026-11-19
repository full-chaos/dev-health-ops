//go:build integration

package daily

import (
	"context"
	"testing"
	"time"
)

// A work item stored once, started before the day, finished (done or canceled) with no completed_at and no closed_at:
// at the end of the day it is not in progress. The daily loader already skips the done item (status != 'done' OR
// completed_at >= start); the canceled one is read and stays in progress.
func TestDailyWIPSkipsAFinishedItemWithNoEnd(t *testing.T) {
	ctx := context.Background()
	for _, status := range []string{"done", "canceled"} {
		c := newCurrentCopyCase(t, ctx, "org-d2-"+status+"-")
		batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, project_key, created_at, started_at, completed_at, closed_at,
			labels, story_points, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		started := c.created.Add(time.Hour)
		if err := batch.Append(
			c.olderRepo, "one", "github", "Item one", "issue", status, "proj-a", c.created, &started,
			nil, nil, []string{"quality"}, nil, c.orgID, c.olderSync,
		); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
		c.compute(t, ctx, c.day)
		var orgWIP, scopeWIP float64
		if err := c.conn.QueryRow(ctx, `
SELECT toFloat64(sumIf(wip_count_end_of_day, row_view = 'org')), toFloat64(sumIf(wip_count_end_of_day, row_view = 'scope'))
FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = 'proj-a'`, c.orgID, c.day,
		).Scan(&orgWIP, &scopeWIP); err != nil {
			t.Fatalf("read work_item_metrics_daily: %v", err)
		}
		if orgWIP != 0 || scopeWIP != 0 {
			t.Errorf("%s, no completed_at, no closed_at, day %s: wip org %v, scope %v; want 0, 0",
				status, c.day.Format("2006-01-02"), orgWIP, scopeWIP)
		}
	}
}
