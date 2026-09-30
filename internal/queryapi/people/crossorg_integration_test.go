//go:build integration

// CHAOS-7239: two orgs share one repo_id (see internal/testsupport/crossorg).
// The person PR drilldown and the per-repo metric breakdown must read only
// the calling org's rows.
package people

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

var crossOrgNow = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

func seedCrossOrgPeople(ctx context.Context, t *testing.T) (*Reader, crossorg.Fixture) {
	t.Helper()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)

	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	created := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		org          string
		loc          uint32
		firstReviewH float64
		pr           uint32
		title        string
	}{
		{f.OrgA, 10, 4, 1, "org-a pull request"},
		{f.OrgB, 1000, 40, 2, "org-b pull request"},
	} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, loc_touched, pr_first_review_p50_hours, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, now())`,
			row.org, f.RepoID, day, f.Identity, f.Identity, row.loc, row.firstReviewH)
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO git_pull_requests (org_id, repo_id, number, title, author_email, created_at, last_synced)
            VALUES (?, ?, ?, ?, ?, ?, now64(3))`,
			row.org, f.RepoID, row.pr, row.title, f.Identity, created)
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return reader, f
}

func TestPeopleReadersReturnOnlyTheCallingOrgsRowsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	reader, f := seedCrossOrgPeople(ctx, t)

	t.Run("drilldown prs", func(t *testing.T) {
		resp, err := BuildDrilldownPRsResponse(ctx, reader, f.OrgA, DrilldownPRsParams{
			PersonID: f.PersonID(), RangeDays: 30, Limit: 50, Now: crossOrgNow,
		})
		if err != nil {
			t.Fatalf("BuildDrilldownPRsResponse: %v", err)
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
			t.Fatalf("org A read %q; want exactly [\"org-a pull request\"]", titles)
		}
	})

	// Both metrics whose config carries a by_repo repos join.
	for metric, want := range map[string]float64{"churn": 10, "review_latency": 4} {
		t.Run("metric by_repo breakdown "+metric, func(t *testing.T) {
			resp, err := BuildMetricResponse(ctx, reader, f.OrgA, MetricParams{
				PersonID: f.PersonID(), Metric: metric, RangeDays: 30, CompareDays: 30, Now: crossOrgNow,
			})
			if err != nil {
				t.Fatalf("BuildMetricResponse: %v", err)
			}
			got := resp.Breakdowns.ByRepo
			if len(got) != 1 || got[0].Label != f.RepoName || got[0].Value != want {
				t.Fatalf("org A by_repo = %+v; want exactly [{%s %v}] (org A's rows only, no org B repos row joined)", got, f.RepoName, want)
			}
		})
	}
}
