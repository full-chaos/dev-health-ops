//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func seedIssueTypeStatusItem(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID, status string, createdAt, startedAt, completedAt, lastSynced time.Time,
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
		[]string{"quality"}, nil, orgID, lastSynced,
	); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

type issueTypeDayNewest struct {
	created, completed, active uint64
	cycleP50, cycleP90         float64
	leadP50                    *float64
}

func (row issueTypeDayNewest) equal(other issueTypeDayNewest) bool {
	if (row.leadP50 == nil) != (other.leadP50 == nil) || (row.leadP50 != nil && *row.leadP50 != *other.leadP50) {
		return false
	}
	return row.created == other.created && row.completed == other.completed && row.active == other.active &&
		row.cycleP50 == other.cycleP50 && row.cycleP90 == other.cycleP90
}

func (row issueTypeDayNewest) String() string {
	lead := "NULL"
	if row.leadP50 != nil {
		lead = fmt.Sprint(*row.leadP50)
	}
	return fmt.Sprintf("created=%d completed=%d active=%d cycle_p50=%v cycle_p90=%v lead_p50=%s",
		row.created, row.completed, row.active, row.cycleP50, row.cycleP90, lead)
}

// readIssueTypeDayNewest reads the newest row of the day's one key with the report reader's rule (dedupFromSource:
// ORDER BY computed_at DESC LIMIT 1 BY key). Not argMax: argMax skips a NULL lead_p50_hours and returns an older row's.
func readIssueTypeDayNewest(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time,
) issueTypeDayNewest {
	t.Helper()
	var got issueTypeDayNewest
	var keys uint64
	if err := conn.QueryRow(ctx, `
SELECT count(), toUInt64(any(created_count)), toUInt64(any(completed_count)), toUInt64(any(active_count)),
       toFloat64(any(cycle_p50_hours)), toFloat64(any(cycle_p90_hours)), anyRespectNulls(lead_p50_hours)
FROM (
  SELECT * FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?
  ORDER BY computed_at DESC LIMIT 1 BY repo_id, provider, team_id, issue_type_norm)`, orgID, day).Scan(
		&keys, &got.created, &got.completed, &got.active, &got.cycleP50, &got.cycleP90, &got.leadP50); err != nil {
		t.Fatalf("read issue_type_metrics_daily day: %v", err)
	}
	if keys != 1 {
		t.Fatalf("issue_type_metrics_daily %s: %d keys, want 1", day.Format("2006-01-02"), keys)
	}
	return got
}

func runIssueTypeFamilies(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID, day, at time.Time,
) {
	t.Helper()
	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	attribution.nowUTC = func() time.Time { return at }
	issueType.nowUTC = func() time.Time { return at }
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	for _, executor := range []NativeFamilyExecutor{attribution, issueType} {
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("compute at %s: %v", at, err)
		}
	}
}

// An item whose provider reopened it and kept its completed_at completes on no day in issue_type_metrics_daily:
// not in completed_count, the cycle p50/p90 or the lead p50. It is still active on the day. A done or canceled
// item with the same dates counts.
func TestIssueTypeFamilyCountsACompletionOnlyWithATerminalStatus(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, started, completed := day.AddDate(0, 0, -2), day.AddDate(0, 0, -1), day.Add(10*time.Hour)
	lead := 58.0
	open := issueTypeDayNewest{active: 1}
	finished := issueTypeDayNewest{completed: 1, active: 1, cycleP50: 34, cycleP90: 34, leadP50: &lead}
	for _, tc := range []struct {
		status string
		want   issueTypeDayNewest
	}{
		{"in_progress", open},
		{"todo", open},
		{"backlog", open},
		{"blocked", open},
		{"in_review", open},
		{"unknown", open},
		{"done", finished},
		{"canceled", finished},
	} {
		orgID := "org-14166-" + tc.status + "-" + uuid.NewString()
		repoID := uuid.New()
		seedIssueTypeStatusItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", tc.status,
			created, started, completed, created)
		runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, day.AddDate(0, 0, 1))
		if got := readIssueTypeDayNewest(t, ctx, conn, orgID, day); !got.equal(tc.want) {
			t.Errorf("status %s, completed_at on %s: newest row %s, want %s",
				tc.status, day.Format("2006-01-02"), got, tc.want)
		}
	}
}

// The reopen on a day already computed: the item counted as done, then the provider reopened it and kept its
// completed_at. The recompute appends a newer row with no completion, cycle or lead time; the item stays active.
func TestIssueTypeFamilyRetractsAReopenedItemThatKeptItsCompletedAt(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, started, completed := day.AddDate(0, 0, -2), day.AddDate(0, 0, -1), day.Add(10*time.Hour)
	orgID := "org-14166-reopen-" + uuid.NewString()
	repoID := uuid.New()
	lead := 58.0

	seedIssueTypeStatusItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "done",
		created, started, completed, created)
	first := day.AddDate(0, 0, 1)
	runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, first)
	want := issueTypeDayNewest{completed: 1, active: 1, cycleP50: 34, cycleP90: 34, leadP50: &lead}
	if got := readIssueTypeDayNewest(t, ctx, conn, orgID, day); !got.equal(want) {
		t.Fatalf("done: newest row %s, want %s", got, want)
	}

	seedIssueTypeStatusItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "in_progress",
		created, started, completed, first)
	runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, first.Add(time.Hour))
	if got, want := readIssueTypeDayNewest(t, ctx, conn, orgID, day), (issueTypeDayNewest{active: 1}); !got.equal(want) {
		t.Errorf("reopened with completed_at kept: newest row %s, want %s", got, want)
	}
}
