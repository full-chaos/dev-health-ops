//go:build integration

// CHAOS-5867: the repos table is ReplacingMergeTree(last_synced). While a repo
// has two physical versions that are not yet merged, a join without FINAL
// reads the row twice and doubles every summed metric. Every repo-grain
// quadrant reader must still return the single-row truth, with a same-id row
// of a second org present as well (crossorg fixture).
package quadrant

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

func TestRepoGrainQuadrantMetricsIgnoreUnmergedDuplicateReposVersions(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()

	crossorg.Exec(ctx, t, admin, `SYSTEM STOP MERGES repos`)
	// Three separate inserts for org A (separate parts, never merged) plus
	// org B's row under the same repo id.
	for i, org := range []string{f.OrgA, f.OrgA, f.OrgA, f.OrgB} {
		name := f.RepoName
		if org == f.OrgB {
			name = f.RepoNameB
		}
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
            VALUES (?, ?, 'github', ?, now64(3), now64(3) + toIntervalSecond(?))`,
			f.RepoID, name, org, i)
	}
	var physical uint64
	if err := admin.QueryRow(ctx, `SELECT count() FROM repos WHERE id = ? AND org_id = ?`, f.RepoID, f.OrgA).Scan(&physical); err != nil {
		t.Fatalf("count physical repos rows: %v", err)
	}
	if physical != 3 {
		t.Fatalf("seed precondition: %d physical org A repos rows; want 3 unmerged versions", physical)
	}

	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	for _, org := range []string{f.OrgA, f.OrgB} {
		loc, merged, reviews, hours, first := uint32(10), uint32(2), uint32(6), 48.0, 4.0
		if org == f.OrgB {
			loc, merged, reviews, hours, first = 1000, 50, 60, 480, 40
		}
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, prs_merged, median_pr_cycle_hours, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, now())`,
			org, f.RepoID, day, loc, merged, hours)
		crossorg.Exec(ctx, t, admin, `
            INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, computed_at)
            VALUES (?, ?, ?, ?, ?, 'team-1', ?, ?, now())`,
			org, f.RepoID, day, f.Identity, f.Identity, reviews, first)
	}

	start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	want := map[string]float64{"churn": 10, "throughput": 2, "review_load": 6, "review_latency": 4}
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
				t.Fatalf("%s rows = %+v; want one %s row valued %v (single repos version, own org only)", metric, rows, f.RepoName, value)
			}
		})
	}
}
