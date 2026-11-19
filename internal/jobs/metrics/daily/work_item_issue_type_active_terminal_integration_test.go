//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func seedIssueTypeActiveItem(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID, status string, createdAt time.Time, completedAt *time.Time, lastSynced time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(
		repoID, workItemID, "github", "Item "+workItemID, "issue", status, createdAt, nil, completedAt,
		[]string{"quality"}, nil, orgID, lastSynced,
	); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

// An item ends on its completed_at only when its status is terminal (done or canceled). A provider can reopen an item
// and keep its completed_at: that open item is active on every day after the old completed_at. The open anchor item
// (no completed_at) keeps the key live in every case.
func TestIssueTypeFamilyCountsAnOpenItemWithACompletedAtBeforeTheDayAsActive(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, completed := day.AddDate(0, 0, -10), day.AddDate(0, 0, -3).Add(10*time.Hour)
	for _, tc := range []struct {
		status string
		active uint64
	}{
		{"in_progress", 2},
		{"todo", 2},
		{"backlog", 2},
		{"blocked", 2},
		{"in_review", 2},
		{"unknown", 2},
		{"done", 1},
		{"canceled", 1},
	} {
		orgID := "org-14176-" + tc.status + "-" + uuid.NewString()
		repoID := uuid.New()
		seedIssueTypeActiveItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "todo", created, nil, created)
		seedIssueTypeActiveItem(t, ctx, conn, orgID, repoID, "gh:acme/api#2", tc.status, created, &completed, created)
		runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, day.AddDate(0, 0, 1))
		want := issueTypeDayNewest{active: tc.active}
		if got := readIssueTypeDayNewest(t, ctx, conn, orgID, day); !got.equal(want) {
			t.Errorf("status %s, completed_at %s before %s: newest row %s, want %s",
				tc.status, completed.Format(time.RFC3339), day.Format("2006-01-02"), got, want)
		}
	}
}

// The reopen on a day already computed: the item ended before the day (done), then the provider reopened it and kept
// its completed_at. The recompute appends a newer row that counts it as active.
func TestIssueTypeFamilyCountsAReopenedItemAsActiveOnARecomputedDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, completed := day.AddDate(0, 0, -10), day.AddDate(0, 0, -3).Add(10*time.Hour)
	orgID := "org-14176-reopen-" + uuid.NewString()
	repoID := uuid.New()

	seedIssueTypeActiveItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "todo", created, nil, created)
	seedIssueTypeActiveItem(t, ctx, conn, orgID, repoID, "gh:acme/api#2", "done", created, &completed, created)
	first := day.AddDate(0, 0, 1)
	runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, first)
	if got, want := readIssueTypeDayNewest(t, ctx, conn, orgID, day), (issueTypeDayNewest{active: 1}); !got.equal(want) {
		t.Fatalf("done: newest row %s, want %s", got, want)
	}

	seedIssueTypeActiveItem(t, ctx, conn, orgID, repoID, "gh:acme/api#2", "in_progress", created, &completed, first)
	runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, first.Add(time.Hour))
	if got, want := readIssueTypeDayNewest(t, ctx, conn, orgID, day), (issueTypeDayNewest{active: 2}); !got.equal(want) {
		t.Errorf("reopened with completed_at %s kept: newest row %s, want %s",
			completed.Format(time.RFC3339), got, want)
	}
}

// A repository whose only item is a reopened one that kept a completed_at before the day has data on the day: the
// item is active, so the day gets a row, not "no data".
func TestIssueTypeFamilyWritesADayOnWhichOnlyAReopenedItemIsActive(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created, completed := day.AddDate(0, 0, -10), day.AddDate(0, 0, -3).Add(10*time.Hour)
	orgID := "org-14176-only-" + uuid.NewString()
	repoID := uuid.New()

	seedIssueTypeActiveItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "in_progress", created, &completed, created)
	runIssueTypeFamilies(t, ctx, conn, orgID, repoID, day, day.AddDate(0, 0, 1))
	if got, want := readIssueTypeDayNewest(t, ctx, conn, orgID, day), (issueTypeDayNewest{active: 1}); !got.equal(want) {
		t.Errorf("only item in_progress with completed_at %s kept: newest row %s, want %s",
			completed.Format(time.RFC3339), got, want)
	}
}
