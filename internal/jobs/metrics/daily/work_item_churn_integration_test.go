//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func seedChurnPullRequest(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID, number uint32,
	additions, deletions *uint32, at time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO git_pull_requests (
		repo_id, number, title, state, created_at, additions, deletions, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare git_pull_requests batch: %v", err)
	}
	if err := batch.Append(repoID, number, "PR", "merged", at, additions, deletions, orgID, at); err != nil {
		t.Fatalf("append git_pull_requests row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send git_pull_requests batch: %v", err)
	}
}

func seedChurnLink(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, prRepoID uuid.UUID,
	workItemID string, number uint32, provenance string, at time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_issue_pr (
		repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)`)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	if err := batch.Append(prRepoID, workItemID, number, float32(0.9), provenance, "fixture", at, orgID); err != nil {
		t.Fatalf("append work_graph_issue_pr row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}
}

// The state the fix exists to reach: work_item_completion_daily.churn_loc holds the additions plus deletions of the
// pull requests linked (work_graph_issue_pr, all three tiers) to the items completed on the day, read back from a
// real ClickHouse for every provider. Python wrote 0, so there is no oracle; the expected values are worked by
// hand from the seeded rows.
//
// The data holds the wrong answers a bad join would give: an item stored under the same repository as a pull
// request with the same number (the link's repo_id is the pull request's repository, not the item's), and a pull
// request linked to an item that did not complete on the day.
func TestCompletionFamilyWritesChurnFromLinkedPullRequestsForEveryProvider(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-churn-" + uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := day.Add(-72 * time.Hour)
	timePtr := func(value time.Time) *time.Time { return &value }
	lines := func(value uint32) *uint32 { return &value }

	providers := []string{"jira", "gitlab", "github", "linear"}
	repoIDs := make([]RepositoryID, 0, len(providers)+1)
	wantChurn := map[string]uint64{}
	for index, provider := range providers {
		itemRepo, prRepo := uuid.New(), uuid.New()
		repoIDs = append(repoIDs, RepositoryID(itemRepo.String()))
		extra := uint32(index)
		completed := timePtr(day.Add(10 * time.Hour))

		seedWorkItemWithProvider(t, ctx, conn, orgID, itemRepo, provider, provider+"-1", day.Add(-48*time.Hour), completed)
		seedWorkItemWithProvider(t, ctx, conn, orgID, itemRepo, provider, provider+"-2", day.Add(-48*time.Hour), completed)
		seedWorkItemWithProvider(t, ctx, conn, orgID, itemRepo, provider, provider+"-3", day.Add(-48*time.Hour), completed)
		seedWorkItemWithProvider(t, ctx, conn, orgID, itemRepo, provider, provider+"-open", day.Add(-48*time.Hour), nil)

		// Item 1: PR 1 by a native link (100+extra, 20) and PR 2 by a text link (30, 10).
		seedChurnPullRequest(t, ctx, conn, orgID, prRepo, 1, lines(100+extra), lines(20), at)
		seedChurnPullRequest(t, ctx, conn, orgID, prRepo, 2, lines(30), lines(10), at)
		seedChurnLink(t, ctx, conn, orgID, prRepo, provider+"-1", 1, "native", at)
		seedChurnLink(t, ctx, conn, orgID, prRepo, provider+"-1", 2, "explicit_text", at)
		// Item 2: the same PR 1 again (once per row) and a heuristic PR 3 whose size is unknown (adds 0).
		seedChurnLink(t, ctx, conn, orgID, prRepo, provider+"-2", 1, "native", at)
		seedChurnPullRequest(t, ctx, conn, orgID, prRepo, 3, nil, nil, at)
		seedChurnLink(t, ctx, conn, orgID, prRepo, provider+"-2", 3, "heuristic", at)
		// Item 3 has no link. Its repository holds a PR 1 of 9999 lines that no link points at: the item's own
		// repository is not a relation.
		seedChurnPullRequest(t, ctx, conn, orgID, itemRepo, 1, lines(9999), lines(0), at)
		// The open item is linked to a huge PR: it did not complete on the day.
		seedChurnPullRequest(t, ctx, conn, orgID, prRepo, 4, lines(50000), lines(50000), at)
		seedChurnLink(t, ctx, conn, orgID, prRepo, provider+"-open", 4, "native", at)

		wantChurn[provider] = uint64(100+extra) + 20 + 30 + 10
	}
	// A repository with completed items and no linked pull request at all writes 0.
	bareRepo := uuid.New()
	repoIDs = append(repoIDs, RepositoryID(bareRepo.String()))
	seedWorkItemWithProvider(t, ctx, conn, orgID, bareRepo, "github", "bare-1", day.Add(-48*time.Hour), timePtr(day.Add(9*time.Hour)))

	completion, err := NewWorkItemInvestmentExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	if _, err := completion.ComputeFamily(ctx, run, Partition{ID: "p1", RunID: "r1", RepoIDs: repoIDs}); err != nil {
		t.Fatal(err)
	}

	for index, provider := range providers {
		var churn, completed uint64
		if err := conn.QueryRow(ctx, `
SELECT toUInt64(argMax(churn_loc, computed_at)), toUInt64(argMax(work_items_completed, computed_at))
FROM work_item_completion_daily
WHERE org_id = ? AND day = ? AND toString(repo_id) = ?
GROUP BY repo_id, team_id`, orgID, day, string(repoIDs[index])).Scan(&churn, &completed); err != nil {
			t.Fatalf("read %s: %v", provider, err)
		}
		if completed != 3 {
			t.Fatalf("%s: work_items_completed = %d, want 3", provider, completed)
		}
		if churn != wantChurn[provider] {
			t.Fatalf("%s: churn_loc = %d, want %d (PR 1 once, PR 2, PR 3 unknown size; not the item repository's PR 1, not the open item's PR)",
				provider, churn, wantChurn[provider])
		}
	}
	var bareChurn, bareCompleted uint64
	if err := conn.QueryRow(ctx, `
SELECT toUInt64(argMax(churn_loc, computed_at)), toUInt64(argMax(work_items_completed, computed_at))
FROM work_item_completion_daily
WHERE org_id = ? AND day = ? AND toString(repo_id) = ?
GROUP BY repo_id, team_id`, orgID, day, bareRepo.String()).Scan(&bareChurn, &bareCompleted); err != nil {
		t.Fatalf("read the repository with no linked pull request: %v", err)
	}
	if bareCompleted != 1 || bareChurn != 0 {
		t.Fatalf("no linked pull request: completed=%d churn_loc=%d, want 1 completed and churn_loc 0", bareCompleted, bareChurn)
	}
}
