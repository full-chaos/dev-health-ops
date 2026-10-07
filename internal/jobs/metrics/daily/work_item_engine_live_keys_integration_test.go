//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// TestLoadIssueTypeMetricsLiveKeysHoldsEachCountOfTheTargetDayOnly holds the
// live-key read of issue_type_metrics_daily clause by clause, on the schema of
// the migration chain: a key is live when the newest row of the TARGET day
// holds a created, a completed or an active count. A key whose newest row is
// all zeros is not live, and a key that holds a count on another day only is
// not live on the target day.
func TestLoadIssueTypeMetricsLiveKeysHoldsEachCountOfTheTargetDayOnly(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-live-keys-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	write := func(targetDay, at time.Time, rows ...workitemengine.IssueTypeMetricsDailyRow) {
		t.Helper()
		for index := range rows {
			rows[index].RepoID, rows[index].Provider, rows[index].TeamID = &repoID, "github", "unassigned"
		}
		if _, err := WriteIssueTypeMetricsDaily(ctx, conn, orgID, targetDay, rows, at); err != nil {
			t.Fatal(err)
		}
	}
	write(day, day.Add(10*time.Hour),
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "created-only", CreatedCount: 1},
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "completed-only", CompletedCount: 1},
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "active-only", ActiveCount: 1},
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "zero-from-the-start"},
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "zero-now", CreatedCount: 1, CompletedCount: 1, ActiveCount: 1},
	)
	write(day, day.Add(11*time.Hour), workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "zero-now"})
	write(day.AddDate(0, 0, -1), day.Add(10*time.Hour),
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "the-day-before", CreatedCount: 1, CompletedCount: 1, ActiveCount: 1})
	write(day.AddDate(0, 0, 1), day.Add(10*time.Hour),
		workitemengine.IssueTypeMetricsDailyRow{IssueTypeNorm: "the-day-after", CreatedCount: 1, CompletedCount: 1, ActiveCount: 1})

	live, err := LoadIssueTypeMetricsLiveKeys(ctx, conn, orgID, repoID, day)
	if err != nil {
		t.Fatal(err)
	}
	want := []issueTypeMetricsKey{
		{"github", "unassigned", "active-only"},
		{"github", "unassigned", "completed-only"},
		{"github", "unassigned", "created-only"},
	}
	if !reflect.DeepEqual(live, want) {
		t.Fatalf("live keys of the day = %v, want %v", live, want)
	}
}

// TestWorkItemEngineFamiliesWriteZeroForALiveKeyOfARepositoryWithNoItem holds
// the case "no item is loaded, but the day holds a live key", for both
// families, on the schema of the migration chain. It is a production case of
// the investment family: the only completion of the day moved away and no item
// is active on that day. The key keeps a count that is no longer true unless
// the family writes its row of zeros.
func TestWorkItemEngineFamiliesWriteZeroForALiveKeyOfARepositoryWithNoItem(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-no-item-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	clock := day.Add(30 * time.Hour)
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}

	// No work item of the repository is stored. A writer left one row with a
	// count in each table.
	if _, err := WriteIssueTypeMetricsDaily(ctx, conn, orgID, day,
		[]workitemengine.IssueTypeMetricsDailyRow{{
			RepoID: &repoID, Provider: "github", TeamID: "unassigned", IssueTypeNorm: "quality", CompletedCount: 1,
		}}, clock); err != nil {
		t.Fatal(err)
	}
	area := "quality"
	if _, err := WriteInvestmentMetricsDaily(ctx, conn, orgID, day,
		[]workitemengine.InvestmentMetricsDailyRow{{
			RepoID: &repoID, InvestmentArea: &area, ProjectStream: "general",
			DeliveryUnits: 1, WorkItemsCompleted: 1, CycleP50Hours: 20,
		}}, clock); err != nil {
		t.Fatal(err)
	}

	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	issueType.nowUTC = func() time.Time { return clock.Add(time.Hour) }
	if written, err := issueType.ComputeFamily(ctx, run, partition); err != nil || written != 1 {
		t.Fatalf("issue type family, no stored item and one live key: written=%d err=%v, want 1 (the row of zeros)", written, err)
	}
	if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, day, "unassigned", "quality"); versions != 2 || completed != 0 || active != 0 {
		t.Fatalf("issue type key: versions=%d completed=%d active=%d, want 2 versions whose newest is all zeros", versions, completed, active)
	}

	investment, err := NewWorkItemInvestmentExecutor(conn, workItemEngineTestClassifier{})
	if err != nil {
		t.Fatal(err)
	}
	investment.nowUTC = func() time.Time { return clock.Add(time.Hour) }
	if written, err := investment.ComputeFamily(ctx, run, partition); err != nil || written != 1 {
		t.Fatalf("investment family, no stored item and one live key: written=%d err=%v, want 1 (the row of zeros)", written, err)
	}
	var versions uint64
	var completed uint32
	if err := conn.QueryRow(ctx, `
SELECT count(), argMax(work_items_completed, computed_at)
FROM investment_metrics_daily WHERE org_id = ? AND day = ? AND investment_area = 'quality'`,
		orgID, day).Scan(&versions, &completed); err != nil {
		t.Fatal(err)
	}
	if versions != 2 || completed != 0 {
		t.Fatalf("investment key: versions=%d completed=%d, want 2 versions whose newest holds 0 completed", versions, completed)
	}
}
