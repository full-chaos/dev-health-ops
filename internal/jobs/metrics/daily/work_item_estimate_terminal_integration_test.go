//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A work item stored once, created before the day, whose provider reopened it and kept the completed_at or closed_at
// of the day before: it is still in the day's backlog of estimate_coverage_metrics_daily. The same item done or
// canceled is not. A done item that ended before the day is not read at all (the items query keeps a done item only
// when its completed_at is in or after the day), so its scope has no row.
func TestDailyEstimateCoverageKeepsAnOpenItemWithAKeptEndInTheBacklog(t *testing.T) {
	ctx := context.Background()
	for _, ends := range []struct{ name, column string }{{"completed", "completed_at"}, {"closed", "closed_at"}} {
		for _, tc := range []struct {
			status      string
			points      *float64
			wantBacklog float64
			wantEst     float64
			wantRows    uint64
		}{
			{"in_progress", nil, 1, 0, 1},
			{"todo", ptrFloat64(2), 1, 1, 1},
			{"done", nil, 0, 0, 0},
			{"canceled", ptrFloat64(2), 0, 0, 1},
		} {
			c := newCurrentCopyCase(t, ctx, "org-14152-"+ends.name+"-"+tc.status+"-")
			storeEstimateCoverageItemEndedAt(t, ctx, c, "one", tc.status, ends.column, c.day.Add(-14*time.Hour), tc.points)
			computeEstimateCoverageDay(t, ctx, c, c.day)
			var rows uint64
			var backlog, estimated, unestimated float64
			if err := c.conn.QueryRow(ctx, `
SELECT count(), toFloat64(sum(backlog_size)), toFloat64(sum(estimated_count)), toFloat64(sum(unestimated_count))
FROM estimate_coverage_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = 'proj-a'`, c.orgID, c.day,
			).Scan(&rows, &backlog, &estimated, &unestimated); err != nil {
				t.Fatalf("read estimate_coverage_metrics_daily: %v", err)
			}
			if rows != tc.wantRows || backlog != tc.wantBacklog || estimated != tc.wantEst || unestimated != tc.wantBacklog-tc.wantEst {
				t.Errorf("%s %s, day %s: rows %d, backlog %v, estimated %v, unestimated %v; want %d, %v, %v, %v",
					ends.column, tc.status, c.day.Format("2006-01-02"), rows, backlog, estimated, unestimated,
					tc.wantRows, tc.wantBacklog, tc.wantEst, tc.wantBacklog-tc.wantEst)
			}
		}
	}
}

func ptrFloat64(value float64) *float64 { return &value }

func computeEstimateCoverageDay(t *testing.T, ctx context.Context, c currentCopyCase, day time.Time) {
	t.Helper()
	executor, err := NewWorkItemEstimateExecutor(c.conn)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return c.computeAt }
	run := Run{ID: uuid.NewString(), OrganizationID: c.orgID, TargetDay: day}
	partition := Partition{ID: "p", RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(c.olderRepo.String())}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
}

func storeEstimateCoverageItemEndedAt(
	t *testing.T, ctx context.Context, c currentCopyCase, workItemID, status, column string, endedAt time.Time, points *float64,
) {
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
		completedAt, closedAt, []string{"quality"}, points, c.orgID, c.olderSync,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}
