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

func seedCrossOrgPeople(ctx context.Context, t *testing.T) (*Reader, crossorg.Fixture, string) {
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
	return reader, f, f.PersonID(ctx, t, admin)
}

func TestPeopleReadersReturnOnlyTheCallingOrgsRowsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	reader, f, personID := seedCrossOrgPeople(ctx, t)

	t.Run("drilldown prs", func(t *testing.T) {
		resp, err := BuildDrilldownPRsResponse(ctx, reader, f.OrgA, DrilldownPRsParams{
			PersonID: personID, RangeDays: 30, Limit: 50, Now: crossOrgNow,
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
				PersonID: personID, Metric: metric, RangeDays: 30, CompareDays: 30, Now: crossOrgNow,
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

// CHAOS-8955: the served repository name belongs to the calling org even when
// another org stores the same repo_id.
func TestPersonDrilldownRepoNameIsTheCallingOrgsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	reader, f, personID := seedCrossOrgPeople(ctx, t)
	for org, want := range map[string]string{f.OrgA: f.RepoName, f.OrgB: f.RepoNameB} {
		resp, err := BuildDrilldownPRsResponse(ctx, reader, org, DrilldownPRsParams{
			PersonID: personID, RangeDays: 30, Limit: 50, Now: crossOrgNow,
		})
		if err != nil {
			t.Fatalf("BuildDrilldownPRsResponse(%s): %v", org, err)
		}
		if len(resp.Items) != 1 {
			t.Fatalf("org %s: %d items, want 1", org, len(resp.Items))
		}
		if got := resp.Items[0].RepoName; got == nil || *got != want {
			t.Errorf("org %s: RepoName = %v, want %q", org, got, want)
		}
	}
}

// CHAOS-8955: the served work item title belongs to the calling org even when
// another org stores the same work_item_id.
func TestWorkItemTitleIsTheCallingOrgsForASharedWorkItemID(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	for org, title := range map[string]string{f.OrgA: "org-a title", f.OrgB: "org-b title"} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO work_items (repo_id, work_item_id, provider, title, status, created_at, updated_at, last_synced, org_id)
            VALUES (?, 'shared-wi', 'github', ?, 'done', now64(3), now64(3), now64(3), ?)`,
			f.RepoID, title, org)
	}
	got, err := fetchWorkItemTitles(ctx, client, f.OrgA, []string{"shared-wi", "absent-wi"})
	if err != nil {
		t.Fatalf("fetchWorkItemTitles: %v", err)
	}
	if len(got) != 1 || got["shared-wi"] != "org-a title" {
		t.Fatalf("titles = %v, want only shared-wi = org-a title", got)
	}
}

// CHAOS-8955: the repositories of an issue are those of its linked pull
// requests in the calling org only, even when another org links the same
// work item to the same repo_id.
func TestIssueLinkedRepoNamesAreTheCallingOrgsForASharedKey(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	for _, org := range []string{f.OrgA, f.OrgB} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO work_graph_issue_pr (org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced)
            VALUES (?, ?, 'shared-wi', 1, 1.0, 'native', 'test', now64(3))`,
			org, f.RepoID)
	}
	crossorg.Exec(ctx, t, admin, `
            INSERT INTO work_graph_issue_pr (org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced)
            VALUES (?, ?, 'org-b-only-wi', 2, 1.0, 'native', 'test', now64(3))`,
		f.OrgB, f.RepoID)
	got, err := fetchLinkedRepoNames(ctx, client, f.OrgA, []string{"shared-wi", "unlinked-wi", "org-b-only-wi"})
	if err != nil {
		t.Fatalf("fetchLinkedRepoNames: %v", err)
	}
	if len(got) != 1 || len(got["shared-wi"]) != 1 || got["shared-wi"][0] != f.RepoName {
		t.Fatalf("linked repo names = %v, want only shared-wi = [%s]", got, f.RepoName)
	}
}
