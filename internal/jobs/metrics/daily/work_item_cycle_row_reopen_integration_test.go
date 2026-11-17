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
