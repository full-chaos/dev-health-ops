//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func reopenWorkItem(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID string, createdAt, startedAt, syncedAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(
		repoID, workItemID, "github", "Item "+workItemID, "issue", "in_progress", createdAt, &startedAt, nil,
		[]string{"quality"}, nil, orgID, syncedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

type cycleChartRead struct {
	rows          uint64
	avgCycleHours float64
	avgLeadHours  float64
	completedRows uint64
}

// readCycleChart is the shape of the people flow-breakdown and metric readers: FINAL, a day window,
// avg over cycle_time_hours / lead_time_hours.
func readCycleChart(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, from, to time.Time) cycleChartRead {
	t.Helper()
	var out cycleChartRead
	if err := conn.QueryRow(ctx, `
SELECT count(), coalesce(toFloat64(avg(cycle_time_hours)), 0), coalesce(toFloat64(avg(lead_time_hours)), 0),
       countIf(completed_at IS NOT NULL)
FROM work_item_cycle_times FINAL
WHERE org_id = ? AND day >= ? AND day < ?`, orgID, from, to).Scan(
		&out.rows, &out.avgCycleHours, &out.avgLeadHours, &out.completedRows); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReopenedWorkItemLeavesNoCycleRowBehind(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-reopen-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	started := day.Add(-24 * time.Hour)
	done := day.Add(10 * time.Hour)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "keep-1", created, &done)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "reopen-1", created, &done)

	executor, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	before := readCycleChart(t, ctx, conn, orgID, day, day.AddDate(0, 0, 1))
	if before.completedRows != 2 {
		t.Fatalf("after the first compute: %d completed cycle rows, want 2", before.completedRows)
	}

	reopenWorkItem(t, ctx, conn, orgID, repoID, "reopen-1", created, started, time.Now().UTC().Add(time.Hour))
	executor.nowUTC = func() time.Time { return time.Now().UTC().Add(2 * time.Hour) }
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	after := readCycleChart(t, ctx, conn, orgID, day, day.AddDate(0, 0, 1))
	if after.completedRows != 1 {
		t.Fatalf("after the reopen and a recompute: %d completed cycle rows, want 1 (the reopened item is not completed)", after.completedRows)
	}
	wantLead := 58.0
	if after.avgLeadHours != wantLead {
		t.Fatalf("avg lead_time_hours = %v, want %v (completed items only)", after.avgLeadHours, wantLead)
	}
}

func reopenedWorkItemRow(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID, status string, createdAt time.Time, completedAt *time.Time, syncedAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	started := createdAt.Add(time.Hour)
	if err := batch.Append(
		repoID, workItemID, "github", "Item "+workItemID, "issue", status, createdAt, &started, completedAt,
		[]string{"quality"}, nil, orgID, syncedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func completedWorkItemCycleRows(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) uint64 {
	t.Helper()
	return readCycleChart(t, ctx, conn, orgID, day, day.AddDate(0, 0, 1)).completedRows
}

// A provider keeps its historical completed_at when an item is reopened. The item is open now: recomputing the
// day it was completed on must not write the completion back.
func TestReopenedItemWithAKeptCompletedAtGetsNoCompletionRowOnItsOriginalDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-reopen-kept-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "kept-1", created, &done)
	executor, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 1 {
		t.Fatalf("after the first compute: %d completed cycle rows, want 1", got)
	}
	reopenedWorkItemRow(t, ctx, conn, orgID, repoID, "kept-1", "in_progress", created, &done, time.Now().UTC().Add(time.Hour))
	executor.nowUTC = func() time.Time { return time.Now().UTC().Add(2 * time.Hour) }
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 0 {
		t.Fatalf("after the reopen and a recompute of the original day: %d completed cycle rows, want 0 (the item is open now)", got)
	}
}

// The rollup's day predicate never loads an item whose stored status is still done and whose completed_at is
// gone: it is not completed (no completed_at), and its old completion row must still be retracted.
func TestReopenedItemTheDayLoaderSkipsIsRetracted(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-reopen-skip-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "skip-1", created, &done)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "other-1", created, &done)
	executor, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	reopenedWorkItemRow(t, ctx, conn, orgID, repoID, "skip-1", "done", created, nil, time.Now().UTC().Add(time.Hour))
	nextDay := day.AddDate(0, 0, 1)
	executor.nowUTC = func() time.Time { return time.Now().UTC().Add(2 * time.Hour) }
	if _, err := executor.ComputeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: nextDay}, partition); err != nil {
		t.Fatal(err)
	}
	if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 1 {
		t.Fatalf("after the reopen and a compute of the next day: %d completed cycle rows, want 1 (skip-1 retracted, other-1 kept)", got)
	}
}

// The current copy of an item across repositories decides its retraction: the newest copy by last_synced.
func TestRetractionDecidesOnTheNewestRepositoryCopy(t *testing.T) {
	day := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	now := time.Now().UTC()
	for _, tc := range []struct {
		name             string
		olderStatus      string
		olderCompleted   *time.Time
		newestStatus     string
		newestCompleted  *time.Time
		wantCompletedRow uint64
	}{
		{"older copy open, newest copy done: the completion stays", "in_progress", nil, "done", &done, 1},
		{"older copy done, newest copy open: the completion is retracted", "done", &done, "in_progress", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
			orgID := "org-copies-" + uuid.NewString()
			olderRepo, newestRepo := uuid.New(), uuid.New()
			partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(olderRepo.String()), RepositoryID(newestRepo.String())}}
			executor, err := NewWorkItemExecutor(conn)
			if err != nil {
				t.Fatal(err)
			}
			reopenedWorkItemRow(t, ctx, conn, orgID, olderRepo, "dup-1", tc.olderStatus, created, tc.olderCompleted, now.Add(-4*time.Hour))
			executor.nowUTC = func() time.Time { return now.Add(-3 * time.Hour) }
			if _, err := executor.ComputeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}, partition); err != nil {
				t.Fatal(err)
			}
			reopenedWorkItemRow(t, ctx, conn, orgID, newestRepo, "dup-1", tc.newestStatus, created, tc.newestCompleted, now.Add(-2*time.Hour))
			if tc.newestStatus == "done" {
				executor.nowUTC = func() time.Time { return now.Add(-time.Hour) }
				if _, err := executor.ComputeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}, partition); err != nil {
					t.Fatal(err)
				}
			}
			executor.nowUTC = func() time.Time { return now }
			if _, err := executor.ComputeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day.AddDate(0, 0, 1)}, partition); err != nil {
				t.Fatal(err)
			}
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != tc.wantCompletedRow {
				t.Fatalf("%d completed cycle rows, want %d", got, tc.wantCompletedRow)
			}
		})
	}
}
