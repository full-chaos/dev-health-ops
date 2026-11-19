//go:build integration

package daily

import (
	"context"
	"testing"
	"time"
)

// A day with one plain item in progress and two items whose provider kept a completed_at in the week: the weekly
// throughput under wip_congestion_ratio counts the two only when their status is terminal (done or canceled), in the
// org view and the scope view.
func TestDailyWeeklyThroughputSkipsAnOpenItemWithAKeptCompletedAt(t *testing.T) {
	ctx := context.Background()
	at := func(value time.Time) *time.Time { return &value }
	for _, tc := range []struct {
		shape, status string
		throughput    float64
	}{
		{"today", "in_progress", 0},
		{"today", "todo", 0},
		{"today", "done", 2},
		{"today", "canceled", 2},
		{"restarted", "in_progress", 0},
		{"restarted", "canceled", 2},
	} {
		c := newCurrentCopyCase(t, ctx, "org-14151-"+tc.shape+"-"+tc.status+"-")
		storeThroughputItem(t, ctx, c, "wip", "in_progress", c.created, at(c.created.Add(time.Hour)), nil)
		for _, id := range []string{"one", "two"} {
			if tc.shape == "today" {
				storeThroughputItem(t, ctx, c, id, tc.status, c.day.Add(2*time.Hour), nil, at(c.day.Add(5*time.Hour)))
			} else {
				storeThroughputItem(t, ctx, c, id, tc.status, c.day.AddDate(0, 0, -10), at(c.day.Add(3*time.Hour)),
					at(c.day.AddDate(0, 0, -3).Add(10*time.Hour)))
			}
		}
		c.compute(t, ctx, c.day)
		for _, view := range []string{"org", "scope"} {
			var rows uint64
			var wip, ratio float64
			if err := c.conn.QueryRow(ctx, `
SELECT count(), toFloat64(sum(wip_count_end_of_day)), toFloat64(max(wip_congestion_ratio))
FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = 'proj-a' AND row_view = ?`, c.orgID, c.day, view,
			).Scan(&rows, &wip, &ratio); err != nil {
				t.Fatalf("read work_item_metrics_daily: %v", err)
			}
			want := wip / max(1, tc.throughput)
			if rows != 1 || wip == 0 || ratio != want {
				t.Errorf("%s %s, %s view, day %s: %d rows, wip %v, wip_congestion_ratio %v; want 1 row, ratio %v (weekly throughput %v)",
					tc.shape, tc.status, view, c.day.Format("2006-01-02"), rows, wip, ratio, want, tc.throughput)
			}
		}
	}
}

func storeThroughputItem(
	t *testing.T, ctx context.Context, c currentCopyCase, workItemID, status string, created time.Time, started, completed *time.Time,
) {
	t.Helper()
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, project_key, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(
		c.olderRepo, workItemID, "github", "Item "+workItemID, "issue", status, "proj-a", created, started, completed,
		[]string{"quality"}, nil, c.orgID, c.olderSync,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}
