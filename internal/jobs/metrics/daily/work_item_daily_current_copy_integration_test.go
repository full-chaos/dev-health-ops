//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The day's work_item rows decide on the item's current copy, the copy of the cycle rows (CHAOS-14003): the newest
// last_synced over every repository copy, on a tie the lower repository id. An item whose current copy does not pass
// the day's predicate is not an item of that day, whatever an older copy says. Each case stores every copy first and
// computes each day once, listing both repositories.

type currentCopyDaySums struct {
	orgCompleted, scopeCompleted, userCompleted, orgWIP float64
	orgRows                                             uint64
}

func readCurrentCopyDaySums(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, scope string) currentCopyDaySums {
	t.Helper()
	var sums currentCopyDaySums
	if err := conn.QueryRow(ctx, `
SELECT toFloat64(sumIf(items_completed, row_view = 'org')), toFloat64(sumIf(items_completed, row_view = 'scope')),
       toFloat64(sumIf(wip_count_end_of_day, row_view = 'org')), countIf(row_view = 'org')
FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = ?`, orgID, day, scope,
	).Scan(&sums.orgCompleted, &sums.scopeCompleted, &sums.orgWIP, &sums.orgRows); err != nil {
		t.Fatalf("read work_item_metrics_daily: %v", err)
	}
	if err := conn.QueryRow(ctx, `
SELECT toFloat64(sum(items_completed))
FROM work_item_user_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = ? AND row_view = 'org'`, orgID, day, scope,
	).Scan(&sums.userCompleted); err != nil {
		t.Fatalf("read work_item_user_metrics_daily: %v", err)
	}
	return sums
}

type currentCopyCase struct {
	conn                driver.Conn
	orgID               string
	olderRepo, newRepo  uuid.UUID
	olderSync, newSync  time.Time
	computeAt           time.Time
	day, created, doneD time.Time
}

func newCurrentCopyCase(t *testing.T, ctx context.Context, prefix string) currentCopyCase {
	t.Helper()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC().Truncate(time.Second)
	return currentCopyCase{
		conn: workItemAttributionLinkedIssueMigratedClickHouse(t, ctx), orgID: prefix + uuid.NewString(),
		olderRepo: uuid.New(), newRepo: uuid.New(), olderSync: now.Add(-4 * time.Hour), newSync: now.Add(-2 * time.Hour),
		computeAt: now.Add(-time.Hour), day: day, created: day.Add(-96 * time.Hour), doneD: day.Add(10 * time.Hour),
	}
}

func (c currentCopyCase) compute(t *testing.T, ctx context.Context, day time.Time) {
	t.Helper()
	computeWorkItemDay(t, ctx, executorAt(t, c.conn, c.computeAt), c.orgID, day, c.olderRepo, c.newRepo)
}

// R1: the current copy is done with no completed_at; an older copy of the same work scope completed on the day. The
// item is not completed on the day in either view, nor in the person rows, and no org row of the scope is left.
func TestDailyCountsSkipAnOlderCopyWhenTheCurrentCopyIsDoneWithoutCompletedAt(t *testing.T) {
	ctx := context.Background()
	c := newCurrentCopyCase(t, ctx, "org-14128-r1-")
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "r1", "done", "proj-a", c.created, &c.doneD, c.olderSync)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.newRepo, "r1", "done", "proj-a", c.created, nil, c.newSync)
	c.compute(t, ctx, c.day)
	got := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a")
	if got.orgCompleted != 0 || got.scopeCompleted != 0 || got.userCompleted != 0 || got.orgRows != 0 {
		t.Fatalf("day %s: %+v, want no completion in the org view, the scope view or the person rows, and no org row",
			c.day.Format("2006-01-02"), got)
	}
}

// R2: as R1, with the two copies in two work scopes. The org view does not count the item. The scope view counts it
// in the older copy's scope: in that scope the older copy is the newest copy (CHAOS-10797, D5204).
func TestDailyCountsSkipAnOlderCopyOfAnotherScopeWhenTheCurrentCopyIsDoneWithoutCompletedAt(t *testing.T) {
	ctx := context.Background()
	c := newCurrentCopyCase(t, ctx, "org-14128-r2-")
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "r2", "done", "proj-a", c.created, &c.doneD, c.olderSync)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.newRepo, "r2", "done", "proj-b", c.created, nil, c.newSync)
	c.compute(t, ctx, c.day)
	got := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a")
	if got.orgCompleted != 0 {
		t.Fatalf("org view of proj-a on %s: %v completed, want 0", c.day.Format("2006-01-02"), got.orgCompleted)
	}
	if got.scopeCompleted != 1 {
		t.Fatalf("scope view of proj-a on %s: %v completed, want 1 (the newest copy of that scope)", c.day.Format("2006-01-02"), got.scopeCompleted)
	}
}

// R3: the current copy completed the day before; an older copy completed on the day. The item completes once, on the
// current copy's day.
func TestDailyCountsCompleteAnItemOnlyOnItsCurrentCopysDay(t *testing.T) {
	ctx := context.Background()
	c := newCurrentCopyCase(t, ctx, "org-14128-r3-")
	before := c.day.AddDate(0, 0, -1)
	doneBefore := before.Add(10 * time.Hour)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "r3", "done", "proj-a", c.created, &c.doneD, c.olderSync)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.newRepo, "r3", "done", "proj-a", c.created, &doneBefore, c.newSync)
	c.compute(t, ctx, before)
	c.compute(t, ctx, c.day)
	onBefore := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, before, "proj-a")
	onDay := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a")
	if onBefore.orgCompleted != 1 || onBefore.scopeCompleted != 1 {
		t.Fatalf("day %s: %+v, want 1 completed in each view", before.Format("2006-01-02"), onBefore)
	}
	if onDay.orgCompleted != 0 || onDay.scopeCompleted != 0 || onDay.userCompleted != 0 {
		t.Fatalf("day %s: %+v, want 0 completed (the current copy completed the day before)", c.day.Format("2006-01-02"), onDay)
	}
}

// R4: the current copy completed two days before; an older copy is still in progress. The item is not work in
// progress at the end of the day.
func TestDailyCountsDoNotKeepAnItemInProgressFromAnOlderOpenCopy(t *testing.T) {
	ctx := context.Background()
	c := newCurrentCopyCase(t, ctx, "org-14128-r4-")
	doneEarlier := c.day.AddDate(0, 0, -2).Add(10 * time.Hour)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "r4", "in_progress", "proj-a", c.created, nil, c.olderSync)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.newRepo, "r4", "done", "proj-a", c.created, &doneEarlier, c.newSync)
	c.compute(t, ctx, c.day)
	if got := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a"); got.orgWIP != 0 || got.orgRows != 0 {
		t.Fatalf("day %s: %+v, want no work in progress and no org row", c.day.Format("2006-01-02"), got)
	}
}

// P1: the current copy is open with no completed_at; it is the day's item on main too, so nothing completes.
func TestDailyCountsDoNotCompleteAnItemWhoseCurrentCopyIsOpen(t *testing.T) {
	ctx := context.Background()
	c := newCurrentCopyCase(t, ctx, "org-14128-p1-")
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "p1", "done", "proj-a", c.created, &c.doneD, c.olderSync)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.newRepo, "p1", "in_progress", "proj-a", c.created, nil, c.newSync)
	c.compute(t, ctx, c.day)
	if got := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a"); got.orgCompleted != 0 || got.scopeCompleted != 0 {
		t.Fatalf("day %s: %+v, want 0 completed", c.day.Format("2006-01-02"), got)
	}
}

// P2: the current copy is open and kept its completed_at on the day. The current copy is the day's item, and an open
// item completes on no day whatever completed_at its provider kept (CHAOS-14138), so nothing completes.
func TestDailyCountsDoNotCompleteTheCurrentOpenCopyWithACompletedAt(t *testing.T) {
	ctx := context.Background()
	c := newCurrentCopyCase(t, ctx, "org-14128-p2-")
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.olderRepo, "p2", "done", "proj-a", c.created, &c.doneD, c.olderSync)
	storeWorkItemCopy(t, ctx, c.conn, c.orgID, c.newRepo, "p2", "in_progress", "proj-a", c.created, &c.doneD, c.newSync)
	c.compute(t, ctx, c.day)
	if got := readCurrentCopyDaySums(t, ctx, c.conn, c.orgID, c.day, "proj-a"); got.orgCompleted != 0 || got.scopeCompleted != 0 || got.userCompleted != 0 {
		t.Fatalf("day %s: %+v, want 0 completed in each view and the person rows", c.day.Format("2006-01-02"), got)
	}
}
