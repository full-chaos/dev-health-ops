//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// seedWorkItemWithProvider stores one work item of the given provider. The engine seed helper of
// work_item_engine_native_integration_test.go is github only.
func seedWorkItemWithProvider(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	provider, workItemID string, createdAt time.Time, completedAt *time.Time,
) {
	t.Helper()
	status := "in_progress"
	if completedAt != nil {
		status = "done"
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(
		repoID, workItemID, provider, "Item "+workItemID, "issue", status, createdAt, nil, completedAt,
		[]string{"quality"}, nil, orgID, createdAt,
	); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

// The state the fix exists to reach: issue_type_metrics_daily.lead_p50_hours holds the upper median of
// (completed_at - created_at) over the items completed on the day, for every provider, read back from a real
// ClickHouse. Python never computed this column (it wrote 0.0), so there is no oracle; the expected values are
// worked by hand from the seeded timestamps.
func TestIssueTypeFamilyWritesLeadTimeForEveryProvider(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-lead-" + uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	timePtr := func(value time.Time) *time.Time { return &value }

	providers := []string{"jira", "gitlab", "github", "linear"}
	repoIDs := make([]RepositoryID, 0, len(providers))
	wantLead := map[string]float64{}
	for index, provider := range providers {
		repoID := uuid.New()
		repoIDs = append(repoIDs, RepositoryID(repoID.String()))
		extra := time.Duration(index) * time.Hour
		// Completed on the day: 58h+extra and 4h after creation; the upper median of the two is 58h+extra.
		seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, provider, provider+"-1",
			day.Add(-48*time.Hour-extra), timePtr(day.Add(10*time.Hour)))
		seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, provider, provider+"-2",
			day.Add(2*time.Hour), timePtr(day.Add(6*time.Hour)))
		// Completed the next day with a very long lead time, and still open: neither is in the day's set.
		seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, provider, provider+"-3",
			day.Add(-1000*time.Hour), timePtr(day.Add(30*time.Hour)))
		seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, provider, provider+"-4",
			day.Add(-300*time.Hour), nil)
		// Completed the day before, twice, with very long lead times: another day's set.
		for _, suffix := range []string{"-5", "-6"} {
			seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, provider, provider+suffix,
				day.Add(-2000*time.Hour), timePtr(day.Add(-2*time.Hour)))
		}
		wantLead[provider] = 58 + float64(index)
	}

	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: repoIDs}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	if _, err := attribution.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	if _, err := issueType.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}

	for _, provider := range providers {
		var lead float64
		var completed uint64
		if err := conn.QueryRow(ctx, `
SELECT argMax(lead_p50_hours, computed_at), toUInt64(argMax(completed_count, computed_at))
FROM issue_type_metrics_daily
WHERE org_id = ? AND day = ? AND provider = ?
GROUP BY provider, team_id, issue_type_norm, repo_id`, orgID, day, provider).Scan(&lead, &completed); err != nil {
			t.Fatalf("read %s: %v", provider, err)
		}
		if completed != 2 {
			t.Fatalf("%s: completed_count = %d, want 2", provider, completed)
		}
		if lead != wantLead[provider] {
			t.Fatalf("%s: lead_p50_hours = %v, want %v (upper median of completed_at - created_at over the 2 items completed on the day)",
				provider, lead, wantLead[provider])
		}
	}
}

// work_items.created_at is not nullable: a missing creation time is stored, and read back, as the Unix epoch. That
// is not a lead time; the item is left out of the median.
func TestIssueTypeFamilyLeavesAnEpochCreatedAtOutOfTheLeadTime(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-lead-epoch-" + uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	repoID := uuid.New()
	done := day.Add(10 * time.Hour)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "epoch-1", time.Unix(0, 0).UTC(), &done)
	seedWorkItemWithProvider(t, ctx, conn, orgID, repoID, "github", "real-1", day.Add(-14*time.Hour), &done)

	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	if _, err := attribution.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	if _, err := issueType.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatal(err)
	}
	var lead float64
	var completed uint64
	if err := conn.QueryRow(ctx, `
SELECT argMax(lead_p50_hours, computed_at), toUInt64(argMax(completed_count, computed_at))
FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?
GROUP BY provider, team_id, issue_type_norm, repo_id`, orgID, day).Scan(&lead, &completed); err != nil {
		t.Fatal(err)
	}
	if completed != 2 || lead != 24 {
		t.Fatalf("completed=%d lead_p50_hours=%v, want 2 completed and 24 (only the item with a real created_at has a lead time)", completed, lead)
	}
}
