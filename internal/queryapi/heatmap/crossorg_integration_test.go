//go:build integration

// CHAOS-7239: two orgs share one repo_id (see internal/testsupport/crossorg).
// Every heatmap metric that joins git_pull_requests or git_commits to repos
// must read only the calling org's rows.
package heatmap

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

func TestHeatmapReadersReturnOnlyTheCallingOrgsRowsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)

	// Same weekday and hour in both orgs, so a leaked row lands in org A's
	// cell. 2026-09-15 is a Tuesday.
	at := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		org       string
		pr        uint32
		title     string
		waitHours int
		commits   int
	}{
		{f.OrgA, 1, "org-a pull request", 2, 1},
		{f.OrgB, 2, "org-b pull request", 10, 3},
	} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO git_pull_requests (org_id, repo_id, number, title, author_email, created_at, first_review_at, last_synced)
            VALUES (?, ?, ?, ?, ?, ?, ?, now64(3))`,
			row.org, f.RepoID, row.pr, row.title, f.Identity, at, at.Add(time.Duration(row.waitHours)*time.Hour))
		for i := 0; i < row.commits; i++ {
			crossorg.Exec(ctx, t, admin, `
                INSERT INTO git_commits (org_id, repo_id, hash, message, author_name, author_email, author_when, committer_when, last_synced)
                VALUES (?, ?, ?, ?, 'Dev', ?, ?, ?, now64(3))`,
				row.org, f.RepoID, row.org+"-"+strconv.Itoa(i), row.org+" commit", f.Identity, at, at)
		}
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, computed_at)
            VALUES (?, ?, toDate(?), ?, ?, now())`,
			row.org, f.RepoID, at, f.Identity, f.Identity)
	}

	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	sum := func(resp *Response) float64 {
		total := 0.0
		for _, c := range resp.Cells {
			total += c.Value
		}
		return total
	}

	t.Run("review_wait_density", func(t *testing.T) {
		resp, err := BuildResponse(ctx, client, f.OrgA, Params{
			Type: "temporal_load", Metric: "review_wait_density", StartDate: &start, EndDate: &end,
			X: "9", Y: weekdayLabels[1], Limit: 50,
		})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got := sum(resp); got != 2 {
			t.Errorf("org A review wait hours = %v; want 2 (org B's 10h PR must not count)", got)
		}
		items, ok := resp.Evidence.([]ReviewWaitEvidenceItem)
		if !ok || len(items) != 1 || items[0].Number != 1 {
			t.Fatalf("org A evidence = %#v; want exactly PR #1", resp.Evidence)
		}
	})

	t.Run("repo_touchpoints", func(t *testing.T) {
		resp, err := BuildResponse(ctx, client, f.OrgA, Params{
			Type: "context_switch", Metric: "repo_touchpoints", StartDate: &start, EndDate: &end,
		})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got := sum(resp); got != 1 {
			t.Fatalf("org A commits = %v; want 1 (org B's 3 commits must not count)", got)
		}
	})

	// The touchpoints TOP-N query picks which repos the series covers. With
	// one repo, a leak there only changes a total nobody returns, so it
	// needs its own orgs and more repos than the limit (20): org C owns 20
	// repos at 2 commits each plus the shared repo at 1; org D adds 3
	// commits under the shared id. Org C's true top 20 excludes the shared
	// repo; a leaked count (4) would pull it in.
	t.Run("repo_touchpoints top-N", func(t *testing.T) {
		g := crossorg.Fixture{
			OrgA: "org-c-chaos-7239", OrgB: "org-d-chaos-7239",
			RepoID: "72390000-0000-4000-8000-000000000002", RepoName: "acme/topn-shared", RepoNameB: "Acme/TopN-Shared",
			Identity: f.Identity,
		}
		crossorg.SeedRepos(ctx, t, admin, g)
		commit := func(org, repoID, hash string) {
			crossorg.Exec(ctx, t, admin, `
                INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, author_when, committer_when, last_synced)
                VALUES (?, ?, ?, 'Dev', ?, ?, ?, now64(3))`,
				org, repoID, hash, g.Identity, at, at)
		}
		commit(g.OrgA, g.RepoID, "c-shared-0")
		for i := 0; i < 3; i++ {
			commit(g.OrgB, g.RepoID, "d-shared-"+strconv.Itoa(i))
		}
		for r := 0; r < 20; r++ {
			repoID := fmt.Sprintf("72390000-0000-4000-8000-1000000000%02d", r)
			crossorg.Exec(ctx, t, admin, `
                INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
                VALUES (?, ?, 'github', ?, now64(3), now64(3))`,
				repoID, fmt.Sprintf("acme/c-only-%02d", r), g.OrgA)
			commit(g.OrgA, repoID, fmt.Sprintf("c-%02d-0", r))
			commit(g.OrgA, repoID, fmt.Sprintf("c-%02d-1", r))
		}

		resp, err := BuildResponse(ctx, client, g.OrgA, Params{
			Type: "context_switch", Metric: "repo_touchpoints", StartDate: &start, EndDate: &end,
		})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		for _, c := range resp.Cells {
			if c.Y == g.RepoName {
				t.Fatalf("org C cells include %s; want it outside the top 20 (it has 1 org C commit, every other repo 2)", g.RepoName)
			}
		}
		if got := sum(resp); got != 40 {
			t.Fatalf("org C commits = %v; want 40", got)
		}
	})

	t.Run("active_hours", func(t *testing.T) {
		resp, err := BuildResponse(ctx, client, f.OrgA, Params{
			Type: "individual", Metric: "active_hours", ScopeType: "developer", ScopeID: f.PersonID(ctx, t, admin),
			StartDate: &start, EndDate: &end, X: "9", Y: weekdayLabels[1], Limit: 50,
		})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got := sum(resp); got != 1 {
			t.Errorf("org A active commits = %v; want 1 (org B's 3 commits must not count)", got)
		}
		items, ok := resp.Evidence.([]IndividualActiveEvidenceItem)
		if !ok || len(items) != 1 || items[0].CommitHash != f.OrgA+"-0" {
			t.Fatalf("org A evidence = %#v; want exactly commit %s-0", resp.Evidence, f.OrgA)
		}
	})
}
