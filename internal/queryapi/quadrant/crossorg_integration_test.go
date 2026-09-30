//go:build integration

// CHAOS-7239: two orgs share one repo_id (see internal/testsupport/crossorg).
// Every repo-grain quadrant metric joins its fact table to repos on repo_id;
// each must read only the calling org's metric rows and repos row.
//
// Per spec, through fetchQuadrantMetric, not only through BuildResponse: a
// quadrant point needs BOTH axes, so a leak on one axis alone yields an
// entity the other axis lacks and the point is dropped -- the public
// response hides it. Reading each spec directly makes every join clause
// observable on its own.
package quadrant

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

func TestRepoGrainQuadrantMetricsReadOnlyTheCallingOrgsRowsForASharedRepoID(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)

	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		org          string
		loc          uint32
		prsMerged    uint32
		cycleHours   float64
		reviews      uint32
		firstReviewH float64
	}{
		{f.OrgA, 10, 2, 48, 6, 4},
		{f.OrgB, 1000, 50, 480, 60, 40},
	} {
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, prs_merged, median_pr_cycle_hours, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, now())`,
			row.org, f.RepoID, day, row.loc, row.prsMerged, row.cycleHours)
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, computed_at)
            VALUES (?, ?, ?, ?, ?, 'team-1', ?, ?, now())`,
			row.org, f.RepoID, day, f.Identity, f.Identity, row.reviews, row.firstReviewH)
	}

	start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	want := map[string]float64{
		"churn":          10,
		"throughput":     2,
		"cycle_time":     48, // raw hours; the days transform is applied later
		"review_load":    6,
		"review_latency": 4,
	}
	// wip joins repos on repos.repo = work_scope_id (a NAME), not repo_id:
	// outside the shared-repo_id class this test pins. It is NOT covered here.
	if len(RepoMetrics) != len(want)+1 {
		t.Fatalf("RepoMetrics has %d specs; this test covers %d plus wip -- cover the new spec", len(RepoMetrics), len(want))
	}
	for metric, value := range want {
		t.Run(metric, func(t *testing.T) {
			spec, ok := RepoMetrics[metric]
			if !ok {
				t.Fatalf("RepoMetrics has no %q spec", metric)
			}
			rows, err := fetchQuadrantMetric(ctx, client, spec, start, end, "week", f.OrgA, "")
			if err != nil {
				t.Fatalf("fetchQuadrantMetric: %v", err)
			}
			if len(rows) != 1 || rows[0].EntityLabel != f.RepoName || rows[0].Value != value {
				t.Fatalf("org A %s rows = %+v; want one %s row with value %v (org A's rows only, no org B repos row joined)", metric, rows, f.RepoName, value)
			}
		})
	}
}
