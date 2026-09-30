//go:build integration

// CHAOS-7239: two orgs share one repo_id (see internal/testsupport/crossorg).
// The PR drilldown must return only the calling org's pull requests.
package drilldown

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

func TestBuildPRsResponseReturnsOnlyTheCallingOrgsPullRequestsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)

	created := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	for _, pr := range []struct {
		org    string
		number uint32
		title  string
	}{
		{f.OrgA, 1, "org-a pull request"},
		{f.OrgB, 2, "org-b pull request"},
	} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO git_pull_requests (org_id, repo_id, number, title, author_email, created_at, last_synced)
            VALUES (?, ?, ?, ?, ?, ?, now64(3))`,
			pr.org, f.RepoID, pr.number, pr.title, f.Identity, created)
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	base := PRParams{
		StartDay: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		EndDay:   time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
		Limit:    50,
	}
	cases := map[string]PRParams{
		"org scope": base,
		"repo scope by id": func() PRParams {
			p := base
			p.ScopeLevel, p.ScopeIDs = "repo", []string{f.RepoID}
			return p
		}(),
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			resp, err := BuildPRsResponse(ctx, reader, f.OrgA, params)
			if err != nil {
				t.Fatalf("BuildPRsResponse: %v", err)
			}
			var titles []string
			for _, item := range resp.Items {
				title := "<nil>"
				if item.Title != nil {
					title = *item.Title
				}
				titles = append(titles, title)
			}
			if len(titles) != 1 || titles[0] != "org-a pull request" {
				t.Fatalf("org A read %q; want exactly [\"org-a pull request\"] (org B's row under the shared repo_id must not appear)", titles)
			}
		})
	}
}
