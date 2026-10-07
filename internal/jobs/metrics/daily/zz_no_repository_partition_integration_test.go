//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A work item of a provider with no repository (linear, jira) is stored under
// the nil repository id. The daily job computes the repositories that the
// discoverer returns. This test asks: does the discovered set hold the nil
// repository id when the store holds such items?
func TestDailyJobVisitsWorkItemsThatHaveNoRepository(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	repoID := uuid.New()
	completed := day.Add(10 * time.Hour)
	insertItem := func(repo uuid.UUID, id, provider string) {
		t.Helper()
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
			repo_id, work_item_id, provider, type, status, created_at, completed_at, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(repo, id, provider, "issue", "done", day.AddDate(0, 0, -2), &completed, orgID, day); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	insertItem(repoID, "gh:acme/api#1", "github")
	insertItem(uuid.Nil, "linear:OPS-1", "linear")
	if err := conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/api', 'github', ?, ?)`,
		repoID, orgID, day, day); err != nil {
		t.Fatal(err)
	}

	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := discoverer.RepositoryIDs(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	completedByProvider := func() map[string]uint64 {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT provider, sum(items_completed) FROM work_item_metrics_daily FINAL WHERE org_id = ? AND day = ? GROUP BY provider`, orgID, day)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		result := map[string]uint64{}
		for rows.Next() {
			var provider string
			var count uint64
			if err := rows.Scan(&provider, &count); err != nil {
				t.Fatal(err)
			}
			result[provider] = count
		}
		return result
	}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p1", RunID: run.ID, RepoIDs: discovered}); err != nil {
		t.Fatal(err)
	}
	afterDiscovered := completedByProvider()
	t.Logf("discovered repositories: %v; items completed by provider after the family ran over them: %v", discovered, afterDiscovered)

	// Control: the family CAN compute them when it is given the nil id.
	if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p2", RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(uuid.Nil.String())}}); err != nil {
		t.Fatal(err)
	}
	t.Logf("control, after the family ran over the nil repository id: %v", completedByProvider())

	if afterDiscovered["github"] != 1 {
		t.Fatalf("github items completed = %d, want 1", afterDiscovered["github"])
	}
	if afterDiscovered["linear"] != 1 {
		t.Fatalf("linear items completed = %d, want 1: the discovered repository set %v does not hold the nil repository id, so no partition of the daily job visits a work item that has no repository",
			afterDiscovered["linear"], discovered)
	}
}
