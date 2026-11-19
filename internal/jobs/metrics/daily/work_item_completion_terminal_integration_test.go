//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func seedWorkItemCompletionStatusItem(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID, status string, createdAt, startedAt, completedAt, lastSynced time.Time, storyPoints float64,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(
		repoID, workItemID, "github", "Item "+workItemID, "issue", status, createdAt, &startedAt, &completedAt,
		[]string{"quality"}, &storyPoints, orgID, lastSynced,
	); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

type workItemCompletionDayNewest struct {
	completed, units, churn uint64
	cycleP50                float64
}

// readWorkItemCompletionDayNewest sums the newest row of each (repository, team) key of the day, the reader rule. A
// NULL churn_loc (no sized linked pull request) reads as 0.
func readWorkItemCompletionDayNewest(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time,
) workItemCompletionDayNewest {
	t.Helper()
	var got workItemCompletionDayNewest
	if err := conn.QueryRow(ctx, `
SELECT toUInt64(sum(completed)), toUInt64(sum(units)), toUInt64(sum(churn)), toFloat64(max(p50)) FROM (
  SELECT argMax(work_items_completed, computed_at) AS completed,
         argMax(delivery_units, computed_at) AS units,
         ifNull(tupleElement(argMax(tuple(churn_loc), computed_at), 1), 0) AS churn,
         argMax(cycle_p50_hours, computed_at) AS p50
  FROM work_item_completion_daily WHERE org_id = ? AND day = ?
  GROUP BY day, repo_id, team_id)`, orgID, day).Scan(&got.completed, &got.units, &got.churn, &got.cycleP50); err != nil {
		t.Fatalf("read work_item_completion_daily day: %v", err)
	}
	return got
}

func runWorkItemCompletionFamilies(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID, day, at time.Time,
) {
	t.Helper()
	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	investment, err := NewWorkItemInvestmentExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	attribution.nowUTC = func() time.Time { return at }
	investment.nowUTC = func() time.Time { return at }
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	for _, executor := range []NativeFamilyExecutor{attribution, investment} {
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("compute at %s: %v", at, err)
		}
	}
}

// seedWorkItemCompletionLinkedPR links the item to one merged pull request of 10 + 1 lines: churn_loc 11 when the
// item completes on the day.
func seedWorkItemCompletionLinkedPR(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID, workItemID string, at time.Time,
) {
	t.Helper()
	additions, deletions := uint32(10), uint32(1)
	seedChurnPullRequest(t, ctx, conn, orgID, repoID, 1, &additions, &deletions, at)
	seedChurnLink(t, ctx, conn, orgID, repoID, workItemID, 1, "native", at)
}

// An item whose provider reopened it and kept its completed_at completes on no day in work_item_completion_daily:
// not in work_items_completed, delivery_units, churn_loc or the cycle p50. A done or canceled item with the same
// dates counts.
func TestWorkItemCompletionDailyCountsAnItemOnlyWithATerminalStatus(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, started, completed := day.AddDate(0, 0, -2), day.AddDate(0, 0, -1), day.Add(10*time.Hour)
	for _, tc := range []struct {
		status string
		want   workItemCompletionDayNewest
	}{
		{"in_progress", workItemCompletionDayNewest{}},
		{"todo", workItemCompletionDayNewest{}},
		{"backlog", workItemCompletionDayNewest{}},
		{"blocked", workItemCompletionDayNewest{}},
		{"in_review", workItemCompletionDayNewest{}},
		{"unknown", workItemCompletionDayNewest{}},
		{"done", workItemCompletionDayNewest{1, 3, 11, 34}},
		{"canceled", workItemCompletionDayNewest{1, 3, 11, 34}},
	} {
		orgID := "org-14153-" + tc.status + "-" + uuid.NewString()
		repoID := uuid.New()
		seedWorkItemCompletionStatusItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", tc.status,
			created, started, completed, created, 3)
		seedWorkItemCompletionLinkedPR(t, ctx, conn, orgID, repoID, "gh:acme/api#1", created)
		runWorkItemCompletionFamilies(t, ctx, conn, orgID, repoID, day, day.AddDate(0, 0, 1))
		if got := readWorkItemCompletionDayNewest(t, ctx, conn, orgID, day); got != tc.want {
			t.Errorf("status %s, completed_at on %s: newest per key %+v, want %+v",
				tc.status, day.Format("2006-01-02"), got, tc.want)
		}
	}
}

// The reopen on a day already computed: the item counted as done, then the provider reopened it and kept its
// completed_at. The recompute retracts the key with a row of zeros.
func TestWorkItemCompletionDailyRetractsAReopenedItemThatKeptItsCompletedAt(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, started, completed := day.AddDate(0, 0, -2), day.AddDate(0, 0, -1), day.Add(10*time.Hour)
	orgID := "org-14153-reopen-" + uuid.NewString()
	repoID := uuid.New()

	seedWorkItemCompletionStatusItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "done",
		created, started, completed, created, 3)
	seedWorkItemCompletionLinkedPR(t, ctx, conn, orgID, repoID, "gh:acme/api#1", created)
	first := day.AddDate(0, 0, 1)
	runWorkItemCompletionFamilies(t, ctx, conn, orgID, repoID, day, first)
	if got, want := readWorkItemCompletionDayNewest(t, ctx, conn, orgID, day), (workItemCompletionDayNewest{1, 3, 11, 34}); got != want {
		t.Fatalf("done: newest per key %+v, want %+v", got, want)
	}

	seedWorkItemCompletionStatusItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "in_progress",
		created, started, completed, first, 3)
	runWorkItemCompletionFamilies(t, ctx, conn, orgID, repoID, day, first.Add(time.Hour))
	if got := readWorkItemCompletionDayNewest(t, ctx, conn, orgID, day); got != (workItemCompletionDayNewest{}) {
		t.Errorf("reopened with completed_at kept: newest per key %+v, want all zero", got)
	}
}
