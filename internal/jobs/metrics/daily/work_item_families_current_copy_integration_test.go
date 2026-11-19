//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The state and estimate families and the capability window decide on the item's current copy, as the work_item
// family does (CHAOS-14128): the newest last_synced over every repository copy, on a tie the lower repository id.
// The current copy and the copy a day reads differ only when the current copy does not pass the day; such an item is
// not an item of that day, whatever an older copy says. Each case stores x under two repositories (the older copy
// passes the day, the current copy does not) and y, a single-copy open item of the same work scope.

type familyCopy struct {
	repo                     uuid.UUID
	id, status, typ, project string
	created                  time.Time
	completed                *time.Time
	points                   *float64
	synced                   time.Time
}

func storeFamilyCopy(t *testing.T, ctx context.Context, conn driver.Conn, org string, stored familyCopy) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, project_key, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	started := stored.created.Add(time.Hour)
	if err := batch.Append(stored.repo, stored.id, "github", "Item "+stored.id, stored.typ, stored.status, stored.project,
		stored.created, &started, stored.completed, []string{}, stored.points, org, stored.synced); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func storeFamilyTransition(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, id, from, to string, at, synced time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status,
		from_status_raw, to_status_raw, actor, org_id, last_synced) VALUES (?, ?, ?, 'github', ?, ?, ?, ?, '', ?, ?)`,
		repo, id, at, from, to, from, to, org, synced); err != nil {
		t.Fatal(err)
	}
}

type familyCopyCase struct {
	conn                      driver.Conn
	org                       string
	olderRepo, newRepo        uuid.UUID
	olderSync, newSync, clock time.Time
	day, created              time.Time
}

func newFamilyCopyCase(t *testing.T, ctx context.Context, prefix string) familyCopyCase {
	t.Helper()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC().Truncate(time.Second)
	return familyCopyCase{
		conn: workItemAttributionLinkedIssueMigratedClickHouse(t, ctx), org: prefix + uuid.NewString(),
		olderRepo: uuid.New(), newRepo: uuid.New(), olderSync: now.Add(-4 * time.Hour), newSync: now.Add(-2 * time.Hour),
		clock: now.Add(-time.Hour), day: day, created: day.AddDate(0, 0, -4),
	}
}

// runDay runs the work_item, work_item_estimate and work_item_state families on one partition of both repositories,
// then the end of the run (RunStaleKeyRetractor), as a daily run does.
func (c familyCopyCase) runDay(t *testing.T, ctx context.Context) {
	t.Helper()
	run := Run{ID: uuid.NewString(), OrganizationID: c.org, TargetDay: c.day}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID,
		RepoIDs: []RepositoryID{RepositoryID(c.olderRepo.String()), RepositoryID(c.newRepo.String())}}
	now := func() time.Time { return c.clock }
	metrics, err := NewWorkItemExecutor(c.conn)
	if err != nil {
		t.Fatal(err)
	}
	metrics.nowUTC = now
	estimate, err := NewWorkItemEstimateExecutor(c.conn)
	if err != nil {
		t.Fatal(err)
	}
	estimate.nowUTC = now
	state, err := NewWorkItemStateExecutor(c.conn)
	if err != nil {
		t.Fatal(err)
	}
	state.nowUTC = now
	for _, family := range []NativeFamilyExecutor{metrics, estimate, state} {
		if _, err := family.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatal(err)
		}
	}
	run.DiscoveredRepoIDs = partition.RepoIDs
	endStaleKeyRun(t, ctx, c.conn, run, c.clock.Add(time.Minute))
}

// liveStateHours is the newest row of each key of work_item_state_durations_daily, summed.
func (c familyCopyCase) liveStateHours(t *testing.T, ctx context.Context, status, view, scope string) float64 {
	t.Helper()
	var hours float64
	if err := c.conn.QueryRow(ctx, `
SELECT toFloat64(sum(d)) FROM (
  SELECT argMax(duration_hours, computed_at) AS d
  FROM work_item_state_durations_daily
  WHERE org_id = ? AND day = ? AND status = ? AND row_view = ? AND work_scope_id = ?
  GROUP BY provider, work_scope_id, team_id, status)`, c.org, c.day, status, view, scope).Scan(&hours); err != nil {
		t.Fatalf("read work_item_state_durations_daily: %v", err)
	}
	return hours
}

// blockedSnapshot is the newest blocked snapshot of the item on the day and the number of stored snapshots.
func (c familyCopyCase) blockedSnapshot(t *testing.T, ctx context.Context, id string) (hours float64, rows uint64) {
	t.Helper()
	if err := c.conn.QueryRow(ctx, `
SELECT toFloat64(argMax(duration_hours, computed_at)), count() FROM work_item_blocked_durations_daily
WHERE org_id = ? AND day = ? AND work_item_id = ?`, c.org, c.day, id).Scan(&hours, &rows); err != nil {
		t.Fatalf("read work_item_blocked_durations_daily: %v", err)
	}
	return hours, rows
}

// The older copy of x is blocked and open; its current copy completed two days before the day. An earlier compute
// stored a blocked snapshot of x for the day. The work_item family has x off the day; the state family has it off
// too, in both views, and the stored snapshot is superseded by a 0.
func TestStateFamilyLeavesAnItemOffADayItsCurrentCopyDoesNotPass(t *testing.T) {
	ctx := context.Background()
	c := newFamilyCopyCase(t, ctx, "org-14137-s1-")
	doneBefore := c.day.AddDate(0, 0, -2).Add(10 * time.Hour)
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "x", "blocked", "issue", "proj-a", c.created, nil, nil, c.olderSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.newRepo, "x", "done", "issue", "proj-a", c.created, &doneBefore, nil, c.newSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "y", "in_progress", "issue", "proj-a", c.created, nil, nil, c.olderSync})
	storeFamilyTransition(t, ctx, c.conn, c.org, c.olderRepo, "x", "todo", "blocked", c.day.AddDate(0, 0, -3), c.olderSync)
	storeFamilyTransition(t, ctx, c.conn, c.org, c.olderRepo, "y", "todo", "in_progress", c.day.AddDate(0, 0, -3), c.olderSync)
	if err := c.conn.Exec(ctx, `INSERT INTO work_item_blocked_durations_daily
		(day, provider, work_scope_id, team_id, team_name, work_item_id, duration_hours, computed_at, org_id)
		VALUES (?, 'github', 'proj-a', 'unassigned', 'Unassigned', 'x', 24, ?, ?)`, c.day, c.clock.Add(-24*time.Hour), c.org); err != nil {
		t.Fatal(err)
	}
	c.runDay(t, ctx)

	if got := readCurrentCopyDaySums(t, ctx, c.conn, c.org, c.day, "proj-a"); got.orgWIP != 1 {
		t.Errorf("work_item org wip = %v, want 1 (y only)", got.orgWIP)
	}
	if got := c.liveStateHours(t, ctx, "blocked", "org", "proj-a"); got != 0 {
		t.Errorf("state org view blocked hours = %v, want 0 (the current copy is done before the day)", got)
	}
	if got := c.liveStateHours(t, ctx, "blocked", "scope", "proj-a"); got != 0 {
		t.Errorf("state scope view blocked hours = %v, want 0 (same scope: the current copy of the scope is done)", got)
	}
	if got, _ := c.blockedSnapshot(t, ctx, "x"); got != 0 {
		t.Errorf("blocked snapshot of x = %v, want 0 (the stored positive snapshot is superseded)", got)
	}
	if got := c.liveStateHours(t, ctx, "in_progress", "org", "proj-a"); got != 24 {
		t.Errorf("state org view in_progress hours = %v, want 24 (y, single copy, unchanged)", got)
	}
}

// As above with no stored blocked snapshot of x: nothing is written for x (a 0 goes only over a live positive
// snapshot).
func TestStateFamilyWritesNoBlockedSnapshotForAnItemOffTheDayWithNoLiveSnapshot(t *testing.T) {
	ctx := context.Background()
	c := newFamilyCopyCase(t, ctx, "org-14137-s3-")
	doneBefore := c.day.AddDate(0, 0, -2).Add(10 * time.Hour)
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "x", "blocked", "issue", "proj-a", c.created, nil, nil, c.olderSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.newRepo, "x", "done", "issue", "proj-a", c.created, &doneBefore, nil, c.newSync})
	storeFamilyTransition(t, ctx, c.conn, c.org, c.olderRepo, "x", "todo", "blocked", c.day.AddDate(0, 0, -3), c.olderSync)
	c.runDay(t, ctx)
	if hours, rows := c.blockedSnapshot(t, ctx, "x"); rows != 0 {
		t.Errorf("x has %d blocked snapshot(s) (newest %v), want none", rows, hours)
	}
}

// As the first case with the current copy in proj-b. The org view has x off the day; the scope view of proj-a keeps
// the older copy: in proj-a it is the newest copy (CHAOS-10797, D5204), as in the work_item family.
func TestStateFamilyScopeViewKeepsTheOlderCopyOfItsOwnScope(t *testing.T) {
	ctx := context.Background()
	c := newFamilyCopyCase(t, ctx, "org-14137-s2-")
	doneBefore := c.day.AddDate(0, 0, -2).Add(10 * time.Hour)
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "x", "blocked", "issue", "proj-a", c.created, nil, nil, c.olderSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.newRepo, "x", "done", "issue", "proj-b", c.created, &doneBefore, nil, c.newSync})
	storeFamilyTransition(t, ctx, c.conn, c.org, c.olderRepo, "x", "todo", "blocked", c.day.AddDate(0, 0, -3), c.olderSync)
	c.runDay(t, ctx)
	if got := c.liveStateHours(t, ctx, "blocked", "org", "proj-a"); got != 0 {
		t.Errorf("state org view blocked hours of proj-a = %v, want 0", got)
	}
	if got := c.liveStateHours(t, ctx, "blocked", "scope", "proj-a"); got != 24 {
		t.Errorf("state scope view blocked hours of proj-a = %v, want 24 (the older copy is the newest copy of proj-a)", got)
	}
}

// The older copy of x is open with story points; its current copy is done with no completed_at.
func TestEstimateFamilyLeavesAnItemOffADayItsCurrentCopyDoesNotPass(t *testing.T) {
	ctx := context.Background()
	c := newFamilyCopyCase(t, ctx, "org-14137-e1-")
	three := 3.0
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "x", "in_progress", "issue", "proj-a", c.created, nil, &three, c.olderSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.newRepo, "x", "done", "issue", "proj-a", c.created, nil, &three, c.newSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "y", "in_progress", "issue", "proj-a", c.created, nil, nil, c.olderSync})
	c.runDay(t, ctx)

	if got := readCurrentCopyDaySums(t, ctx, c.conn, c.org, c.day, "proj-a"); got.orgWIP != 1 {
		t.Errorf("work_item org wip = %v, want 1 (y only)", got.orgWIP)
	}
	var estimated, unestimated, backlog uint32
	if err := c.conn.QueryRow(ctx, `
SELECT estimated_count, unestimated_count, backlog_size FROM estimate_coverage_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = 'proj-a'`, c.org, c.day).Scan(&estimated, &unestimated, &backlog); err != nil {
		t.Fatalf("read estimate_coverage_metrics_daily: %v", err)
	}
	if estimated != 0 || unestimated != 1 || backlog != 1 {
		t.Errorf("estimate proj-a: estimated=%d unestimated=%d backlog=%d, want 0/1/1 (x's current copy is done)",
			estimated, unestimated, backlog)
	}
}

// The older copy of x is an open bug with story points; its current copy is an issue done 120 days before the day,
// before the capability window.
func TestCapabilityWindowLeavesAnItemWhoseCurrentCopyIsOutsideIt(t *testing.T) {
	ctx := context.Background()
	c := newFamilyCopyCase(t, ctx, "org-14137-c1-")
	two := 2.0
	longBefore := c.day.AddDate(0, 0, -120)
	created := c.day.AddDate(0, 0, -150)
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "x", "in_progress", "bug", "proj-a", created, nil, &two, c.olderSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.newRepo, "x", "done", "issue", "proj-a", created, &longBefore, nil, c.newSync})
	storeFamilyCopy(t, ctx, c.conn, c.org, familyCopy{c.olderRepo, "y", "in_progress", "issue", "proj-a", created, nil, nil, c.olderSync})
	if _, err := computeWorkItemMeasureCapability(ctx, c.conn, c.org, c.day, c.clock); err != nil {
		t.Fatal(err)
	}
	answers := capabilityAnswers(t, ctx, c.conn, c.org, c.day)
	want := capabilityAnswer{tracked: 0, evidence: 0, items: 1}
	if got := answers["github/bug_completed_ratio"]; got != want {
		t.Errorf("bug_completed_ratio = %+v, want %+v (x's current copy left the window)", got, want)
	}
	if got := answers["github/story_points_completed"]; got != want {
		t.Errorf("story_points_completed = %+v, want %+v", got, want)
	}
}
