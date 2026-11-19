//go:build integration

package streamhandlers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// keptHookConn runs hook once, before the external sink's git_pull_requests batch: after its held-version read.
type keptHookConn struct {
	driver.Conn
	hook  func()
	fired bool
}

func (c *keptHookConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if !c.fired && strings.Contains(query, "INSERT INTO git_pull_requests") {
		c.fired = true
		c.hook()
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

// CHAOS-6014 C11: external pull_request.v1 against another writer of the same key (a whole-row statement as provider
// sync writes it), both orders. The other writer's statement has the older version.
//   - interleaved: the other writer states the merge and the first review inside the external write's window; the
//     external write then inserts its stale NULLs at the newer version, and its own repair restores the statement.
//   - after: the other writer's newer row holds NULL leads; the external write states them later at an older version,
//     and its own repair fills the newer row.
func TestExternalPullRequestKeepsTheOtherWritersStatement(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	repo := externalRepoUUID("github", "acme/api", "acme/api")
	created := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	merged := time.Date(2026, 6, 3, 9, 0, 0, 0, time.UTC)
	reviewed := time.Date(2026, 6, 2, 9, 0, 0, 0, time.UTC)
	state := func(number uint32, version time.Time) {
		establishRow(ctx, t, conn, establishPullRequest, storedVersionOrg, repo, number, "T", "b", "merged", "Ada",
			created, merged, merged, uint32(1), reviewed, nil, uint32(0), uint32(2), uint32(0), version)
	}
	open := func(number uint32, version time.Time) {
		establishRow(ctx, t, conn, establishPullRequest, storedVersionOrg, repo, number, "T", "b", "open", "Ada",
			created, nil, nil, uint32(1), nil, nil, uint32(0), uint32(0), uint32(0), version)
	}
	want := map[string]string{"merged_at": "2026-06-03 09:00:00.000", "first_review_at": "2026-06-02 09:00:00.000", "reviews_count": "2"}

	t.Run("interleaved", func(t *testing.T) {
		open(21, establishedAt)
		hooked := &keptHookConn{Conn: conn, hook: func() { state(21, firstWriteAt) }}
		externalWrite(ctx, t, hooked, secondWriteAt, "github", "acme/api", "pull_request.v1",
			`{"repositoryExternalId":"acme/api","number":21,"state":"open","createdAt":"2026-06-01T09:00:00Z","title":"T","mergedAt":null,"firstReviewAt":null,"reviewsCount":0}`)
		if !hooked.fired {
			t.Fatal("the other writer did not write inside the external write's window")
		}
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, repo.String(), uint32(21)), want)
	})

	t.Run("after", func(t *testing.T) {
		open(22, secondWriteAt)
		externalWrite(ctx, t, conn, firstWriteAt, "github", "acme/api", "pull_request.v1",
			`{"repositoryExternalId":"acme/api","number":22,"state":"merged","createdAt":"2026-06-01T09:00:00Z","title":"T","mergedAt":"2026-06-03T09:00:00Z","firstReviewAt":"2026-06-02T09:00:00Z","reviewsCount":2}`)
		expectColumns(t, finalColumns(ctx, t, conn, "git_pull_requests", "repo_id = ? AND number = ?", prColumns, repo.String(), uint32(22)), want)
	})
}
