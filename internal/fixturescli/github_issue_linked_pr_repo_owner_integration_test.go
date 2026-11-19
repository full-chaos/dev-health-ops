//go:build integration

package fixturescli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// On the real GitHub world (the team catalog's grants are keyed by repository name, repo_id NULL), every GitHub
// issue linked to a pull request of an owned repository takes that repository's owner as its primary team, by
// repo_ownership through the link (team-attribution.md: an issue takes repo_ownership from the repositories of its
// linked pull requests). Nothing is seeded: the production verbs run over what the producers wrote.
func TestGitHubIssueTakesItsLinkedPullRequestsRepoOwnerInTheRealWorld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	now := time.Now().UTC()
	frozen, err := LoadFrozenWorld(githubWorld.Params)
	if err != nil {
		t.Fatal(err)
	}
	org := githubWorld.OrgID
	if _, err := LoadWorld(ctx, conn, frozen, org, now); err != nil {
		t.Fatal(err)
	}
	var first time.Time
	matrixQuery(ctx, t, conn, `SELECT min(created_at) FROM work_items WHERE org_id = ?`, []any{org}, func(rows driver.Rows) error {
		return rows.Scan(&first)
	})
	runAttributionVerbs(ctx, t, conn, org, first.UTC().Truncate(24*time.Hour), now.Truncate(24*time.Hour))
	f := loadMatrixFacts(ctx, t, conn, org, now)
	everOwners := map[string]map[string]bool{}
	for _, row := range f.repoOwners {
		if everOwners[row.RepoName] == nil {
			everOwners[row.RepoName] = map[string]bool{}
		}
		everOwners[row.RepoName][row.TeamID] = true
	}
	linked := 0
	for _, link := range f.links {
		issue, ok := f.items[link.WorkItemID]
		if !ok || issue.Provider != "github" || isPullRequestItem(issue) {
			continue
		}
		repo := f.repos[link.RepoID].Name
		if len(everOwners[repo]) == 0 {
			continue
		}
		linked++
		primary, ok := f.primary(link.WorkItemID)
		if !ok || primary.Source != "repo_ownership" || !strings.Contains(primary.Evidence, "work_graph_issue_pr=") || !everOwners[repo][primary.TeamID] {
			t.Errorf("%s (linked to %s#%d) primary = %+v, want a repo_ownership row of an owner of %s through the link", link.WorkItemID, repo, link.PRNumber, primary, repo)
		}
	}
	if linked == 0 {
		t.Fatal("the github world holds no issue linked to a pull request of an owned repository")
	}
}
