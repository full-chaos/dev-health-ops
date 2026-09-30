//go:build integration

// CHAOS-7239: two orgs share one repo_id (see internal/testsupport/crossorg).
// Every heatmap metric that joins git_pull_requests or git_commits to repos
// must read only the calling org's rows.
package heatmap

import (
	"context"
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

	t.Run("active_hours", func(t *testing.T) {
		resp, err := BuildResponse(ctx, client, f.OrgA, Params{
			Type: "individual", Metric: "active_hours", ScopeType: "developer", ScopeID: f.PersonID(),
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
