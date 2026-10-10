//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The count of repositories a whole-organization run cannot discover, on real
// ClickHouse with the schema of the migration chain: stored rows of the
// organization under a repository id with no repos row of exactly that
// organization. Rows of another organization, a repos row under another form
// of the organization id, and the nil repository id are each a case.
func TestRepositoryDiscovererCountsStoredRowsItCannotDiscover(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	repos := func(org string, id uuid.UUID) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/api', 'github', ?, ?)`,
			id, org, day, day); err != nil {
			t.Fatal(err)
		}
	}
	pullRequest := func(org string, repo uuid.UUID, number int) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO git_pull_requests (repo_id, number, state, created_at, org_id, last_synced) VALUES (?, ?, 'merged', ?, ?, ?)`,
			repo, number, day, org, day); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(org string, repo uuid.UUID, hash string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO git_commits (repo_id, hash, author_when, committer_when, org_id, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			repo, hash, day, day, org, day); err != nil {
			t.Fatal(err)
		}
	}
	workItem := func(org string, repo uuid.UUID, id string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, status, created_at, org_id, last_synced) VALUES (?, ?, 'github', 'issue', 'open', ?, ?, ?)`,
			repo, id, day, org, day); err != nil {
			t.Fatal(err)
		}
	}
	count := func(org string) map[string]uint64 {
		t.Helper()
		counts, err := discoverer.repositoriesNotDiscovered(ctx, org)
		if err != nil {
			t.Fatal(err)
		}
		return counts
	}
	want := func(t *testing.T, got map[string]uint64, pullRequests, commits, workItems uint64) {
		t.Helper()
		if got[notDiscoveredPullRequests] != pullRequests || got[notDiscoveredCommits] != commits || got[notDiscoveredWorkItems] != workItems {
			t.Errorf("not discovered = %v, want pull requests %d, commits %d, work items %d", got, pullRequests, commits, workItems)
		}
	}

	t.Run("every stored row is under a discovered repository", func(t *testing.T) {
		org, repo := uuid.NewString(), uuid.New()
		repos(org, repo)
		pullRequest(org, repo, 1)
		commit(org, repo, "a1")
		workItem(org, repo, "gh:1")
		workItem(org, uuid.Nil, "linear:1")
		want(t, count(org), 0, 0, 0)
	})
	t.Run("rows under repository ids with no repos row", func(t *testing.T) {
		org, known := uuid.NewString(), uuid.New()
		repos(org, known)
		pullRequest(org, known, 1)
		lostA, lostB, lostC := uuid.New(), uuid.New(), uuid.New()
		pullRequest(org, lostA, 1)
		pullRequest(org, lostA, 2) // two rows, one repository id
		pullRequest(org, lostB, 1)
		commit(org, lostA, "a1")
		workItem(org, lostC, "gh:1")
		workItem(org, uuid.Nil, "linear:1") // the nil id has its own rule
		want(t, count(org), 2, 1, 1)
		discovered, err := discoverer.RepositoryIDs(ctx, org)
		if err != nil {
			t.Fatal(err)
		}
		if len(discovered) != 2 {
			t.Errorf("discovered %v: the count must not add a repository to the run (want the one repos row and the nil id)", discovered)
		}
	})
	t.Run("the repos row is of another organization", func(t *testing.T) {
		org, other, repo := uuid.NewString(), uuid.NewString(), uuid.New()
		repos(other, repo)
		pullRequest(org, repo, 1)
		pullRequest(other, repo, 1)
		want(t, count(org), 1, 0, 0)
		want(t, count(other), 0, 0, 0)
	})
	t.Run("the repos row has another form of the organization id", func(t *testing.T) {
		org, repoDefault := uuid.NewString(), uuid.New()
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, provider, created_at, last_synced) VALUES (?, 'acme/api', 'github', ?, ?)`,
			repoDefault, day, day); err != nil {
			t.Fatal(err)
		}
		commit(org, repoDefault, "a1")
		want(t, count(org), 0, 1, 0)
	})
	t.Run("an organization with nothing stored", func(t *testing.T) {
		want(t, count(uuid.NewString()), 0, 0, 0)
	})
}
