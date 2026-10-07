//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// issueTypeDayRows is the stored state of one (org, day) of
// issue_type_metrics_daily: every version of every key.
func issueTypeDayRows(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) uint64 {
	t.Helper()
	var rows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?`, orgID, day).Scan(&rows); err != nil {
		t.Fatalf("count issue_type_metrics_daily rows: %v", err)
	}
	return rows
}

// The issue-type family on the real schema, for the rule "a day with no data
// is never filled". The compute returns a row of zeros for every key of the
// repository's stored items whatever the day; the family must not store them
// for a (repository, day) on which no item was created, completed or active.
// It must still store the row of zeros that replaces an older row with a
// count, and on a day WITH data it must store every key.
func TestWorkItemIssueTypeFamilyWritesNoRowForADayWithNoData(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-issue-type-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	timePtr := func(value time.Time) *time.Time { return &value }
	// "quality": created two days before the day, completed on the day.
	// "security": created on the day, open.
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "quality",
		day.AddDate(0, 0, -2), timePtr(day.AddDate(0, 0, -1)), timePtr(day.Add(10*time.Hour)), nil)
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, "gh:acme/api#2", "security",
		day.Add(8*time.Hour), nil, nil, nil)

	family, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	runAt := func(targetDay, at time.Time) int {
		t.Helper()
		family.nowUTC = func() time.Time { return at }
		written, err := family.ComputeFamily(ctx,
			Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: targetDay}, partition)
		if err != nil {
			t.Fatalf("issue type family for %s at %s: %v", targetDay.Format(time.DateOnly), at, err)
		}
		return written
	}
	clock := day.AddDate(0, 0, 5)

	// --- a day before the first item exists: no data, no row ----------------
	emptyDay := day.AddDate(0, 0, -40)
	if written := runAt(emptyDay, clock); written != 0 {
		t.Fatalf("a day with no data: the family reported %d rows, want 0", written)
	}
	if rows := issueTypeDayRows(t, ctx, conn, orgID, emptyDay); rows != 0 {
		t.Fatalf("a day with no data holds %d issue_type_metrics_daily rows, want 0: a missing day must stay missing, not read as zero", rows)
	}

	// --- the same day, after a writer stored a row with a count for one key --
	// (what an hourly sync unit leaves behind). The count is not true for the
	// stored items, so that key gets a row of zeros. The other key gets nothing.
	if _, err := WriteIssueTypeMetricsDaily(ctx, conn, orgID, emptyDay,
		[]workitemengine.IssueTypeMetricsDailyRow{{
			RepoID: &repoID, Provider: "github", TeamID: "unassigned", IssueTypeNorm: "quality", CompletedCount: 1,
		}}, clock.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if written := runAt(emptyDay, clock.Add(2*time.Hour)); written != 1 {
		t.Fatalf("a day with no data and one key with a stored count: the family reported %d rows, want 1 (the row of zeros of that key)", written)
	}
	if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, emptyDay, "unassigned", "quality"); versions != 2 || completed != 0 || active != 0 {
		t.Fatalf("key (unassigned, quality) of the day with no data: versions=%d completed=%d active=%d, want 2 versions whose newest is all zeros", versions, completed, active)
	}
	if versions, _, _ := readIssueTypeKey(t, ctx, conn, orgID, emptyDay, "unassigned", "security"); versions != 0 {
		t.Fatalf("key (unassigned, security) of the day with no data has %d rows, want 0", versions)
	}
	// A key whose newest row is zero is left alone: the next run writes nothing.
	if written := runAt(emptyDay, clock.Add(3*time.Hour)); written != 0 {
		t.Fatalf("a day with no data whose keys are all zero: the family reported %d rows, want 0", written)
	}
	if rows := issueTypeDayRows(t, ctx, conn, orgID, emptyDay); rows != 2 {
		t.Fatalf("the day with no data holds %d rows after the third run, want still 2", rows)
	}

	// --- a day with data keeps every key -----------------------------------
	// The day before `day`: the quality item is active, the security item does
	// not exist yet. The day has data, so the security key is stored with zeros
	// (the same rows the sync deriver gives for the same items).
	dayBefore := day.AddDate(0, 0, -1)
	if written := runAt(dayBefore, clock.Add(4*time.Hour)); written != 2 {
		t.Fatalf("a day with data: the family reported %d rows, want 2 (one per key)", written)
	}
	if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, dayBefore, "unassigned", "quality"); versions != 1 || completed != 0 || active != 1 {
		t.Fatalf("key (unassigned, quality) of the day before: versions=%d completed=%d active=%d, want 1, 0, 1", versions, completed, active)
	}
	if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, dayBefore, "unassigned", "security"); versions != 1 || completed != 0 || active != 0 {
		t.Fatalf("key (unassigned, security) of the day before: versions=%d completed=%d active=%d, want 1 version of zeros", versions, completed, active)
	}
	if written := runAt(day, clock.Add(5*time.Hour)); written != 2 {
		t.Fatalf("the day itself: the family reported %d rows, want 2", written)
	}
	if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, day, "unassigned", "quality"); versions != 1 || completed != 1 || active != 1 {
		t.Fatalf("key (unassigned, quality) of the day: versions=%d completed=%d active=%d, want 1, 1, 1", versions, completed, active)
	}
}

// The partition of the nil repository id holds the work items that have no
// repository. The three tables store "no repository" as NULL, and the live-key
// read compares on that. A row of zeros written with the nil UUID instead
// would be a second key: the older row with the count would stay the newest
// row of the NULL key for ever, and every run would add one more row.
func TestWorkItemEngineZeroRowsOfTheNoRepositoryPartitionHoldANullRepository(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-no-repository-" + uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	// One open item with no repository, so the day has data.
	seedWorkItemEngineItem(t, ctx, conn, orgID, uuid.Nil, "linear:ENG-1", "security",
		day.Add(8*time.Hour), nil, nil, nil)

	// An older row with a count, for a key no stored item produces.
	stored := day.AddDate(0, 0, 1)
	area := "gone"
	if _, err := WriteIssueTypeMetricsDaily(ctx, conn, orgID, day,
		[]workitemengine.IssueTypeMetricsDailyRow{{
			Provider: "github", TeamID: "unassigned", IssueTypeNorm: "gone", CompletedCount: 1,
		}}, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteInvestmentMetricsDaily(ctx, conn, orgID, day,
		[]workitemengine.InvestmentMetricsDailyRow{{
			InvestmentArea: &area, ProjectStream: "general", DeliveryUnits: 1, WorkItemsCompleted: 1,
		}}, stored); err != nil {
		t.Fatal(err)
	}

	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	investment, err := NewWorkItemInvestmentExecutor(conn, workItemEngineTestClassifier{})
	if err != nil {
		t.Fatal(err)
	}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(uuid.Nil.String())}}
	for run := 1; run <= 2; run++ {
		at := stored.Add(time.Duration(run) * time.Hour)
		issueType.nowUTC = func() time.Time { return at }
		investment.nowUTC = func() time.Time { return at }
		for name, executor := range map[string]NativeFamilyExecutor{
			WorkItemIssueTypeFamilyName: issueType, WorkItemInvestmentFamilyName: investment,
		} {
			if _, err := executor.ComputeFamily(ctx,
				Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}, partition); err != nil {
				t.Fatalf("%s, run %d: %v", name, run, err)
			}
		}
	}

	for _, table := range []struct {
		name, keyPredicate, newestCount string
	}{
		{"issue_type_metrics_daily", "issue_type_norm = 'gone'", "argMax(completed_count, computed_at)"},
		{"investment_metrics_daily", "investment_area = 'gone'", "argMax(work_items_completed, computed_at)"},
	} {
		var versions, nullRepository, newest uint64
		if err := conn.QueryRow(ctx, `
SELECT count(), countIf(repo_id IS NULL), toUInt64(`+table.newestCount+`)
FROM `+table.name+` WHERE org_id = ? AND day = ? AND `+table.keyPredicate, orgID, day,
		).Scan(&versions, &nullRepository, &newest); err != nil {
			t.Fatalf("read %s: %v", table.name, err)
		}
		// Two runs, ONE row of zeros: the second run finds the key already zero.
		if versions != 2 || nullRepository != 2 || newest != 0 {
			t.Fatalf("%s key gone after two runs: versions=%d, with a NULL repository=%d, newest count=%d; want 2 versions, both NULL, newest 0",
				table.name, versions, nullRepository, newest)
		}
		var total, withRepository uint64
		if err := conn.QueryRow(ctx, `
SELECT count(), countIf(repo_id IS NOT NULL) FROM `+table.name+` WHERE org_id = ?`, orgID,
		).Scan(&total, &withRepository); err != nil {
			t.Fatalf("read %s: %v", table.name, err)
		}
		if total == 0 || withRepository != 0 {
			t.Fatalf("%s holds %d rows of the organization, %d of them with a repository id; want every row of the no-repository partition to hold NULL",
				table.name, total, withRepository)
		}
	}
}
