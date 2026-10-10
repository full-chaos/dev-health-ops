//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
)

// The coverage of the pull request rework ratio says what share of the merged
// pull requests of the WINDOW the ratio speaks for. A window can hold days
// that were stored before the review counts existed (and not computed again):
// their rows hold prs_merged and no counts. Those merged pull requests are in
// the denominator, so the part of the window that is not counted is visible.
// The ratio and its state are of the counted days only.
//
// Counted days come from the daily job's own compute and writer; the rows with
// no counts are written with the columns the earlier release wrote.
func TestHomePRReworkRatio_CoverageIsOverEveryStoredDayOfTheWindow(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	counted := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	older := counted.AddDate(0, 0, -3)
	uncounted := func(orgID string, repo uuid.UUID, merged int) {
		t.Helper()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`INSERT INTO repo_metrics_daily (repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc, large_commit_ratio, prs_merged, large_pr_ratio, pr_rework_ratio, computed_at, org_id)
			 VALUES ('%s', '%s', 0, 0, 0, 0, %d, 0, 0, toDateTime64('%s', 6, 'UTC'), '%s')`,
			repo, older.Format(time.DateOnly), merged, older.Add(30*time.Hour).Format("2006-01-02 15:04:05"), orgID)); err != nil {
			t.Fatal(err)
		}
	}
	many := func(reviewed, rework, unreviewed, open int) []prreworktest.PullRequest {
		var out []prreworktest.PullRequest
		for i := 0; i < reviewed; i++ {
			out = append(out, prreworktest.PullRequest{Reviews: 2})
		}
		for i := 0; i < rework; i++ {
			out = append(out, prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1})
		}
		for i := 0; i < unreviewed; i++ {
			out = append(out, prreworktest.PullRequest{})
		}
		for i := 0; i < open; i++ {
			out = append(out, prreworktest.PullRequest{Open: true})
		}
		return out
	}
	var spec metricSpec
	for _, candidate := range metrics {
		if candidate.Metric == "pr_rework_ratio" {
			spec = candidate
		}
	}
	if spec.Metric == "" {
		t.Fatal("Home has no pr_rework_ratio metric")
	}
	start, end := counted.AddDate(0, 0, -6), counted.AddDate(0, 0, 1)
	none := -1.0
	for _, test := range []struct {
		name, org string
		// the counted day (nil: no counted day) and the merged pull requests
		// of the row with no counts (0: no such row)
		day             []prreworktest.PullRequest
		uncountedMerged int
		value           float64
		hasData         bool
		state           string // "": null
		coverage        float64
	}{
		{"every stored day is counted", "cov-all-counted", many(4, 1, 7, 0), 0, 20, true, "measured", 5.0 / 12},
		{"one counted day and an older day with 88 merged pull requests and no counts", "cov-part-counted", many(4, 1, 7, 0), 88, 20, true, "measured", 0.05},
		{"no review data on the counted day, and an older day with no counts", "cov-unknown", many(0, 0, 3, 0), 27, 0, false, "unknown_no_review_evidence", 0},
		{"no merged pull request on the counted day, and an older day with no counts", "cov-none-merged", many(0, 0, 0, 1), 30, 0, false, "not_applicable_no_merged_pull_requests", 0},
		{"no merged pull request in any stored day", "cov-nothing-merged", many(0, 0, 0, 1), 0, 0, false, "not_applicable_no_merged_pull_requests", none},
		{"only days with no counts", "cov-no-counts", nil, 40, 0, false, "", none},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := uuid.New()
			if test.day != nil {
				prreworktest.WriteDay(ctx, t, admin, test.org, repo, "github", counted, counted.Add(30*time.Hour), test.day)
			}
			if test.uncountedMerged > 0 {
				uncounted(test.org, repo, test.uncountedMerged)
			}
			got, err := computeMetricDelta(ctx, client, spec, start, end, start.AddDate(0, 0, -7), start, Filters{}, test.org, teamScopeReadAsOf)
			if err != nil {
				t.Fatal(err)
			}
			state := ""
			if got.RateState != nil {
				state = *got.RateState
			}
			if state != test.state || math.Abs(got.Value-test.value) > 1e-9 || got.HasData != test.hasData {
				t.Errorf("state %q value %v has_data %v, want %q %v %v: the ratio and its state are of the counted days only",
					state, got.Value, got.HasData, test.state, test.value, test.hasData)
			}
			switch {
			case test.coverage == none && got.RateCoverage != nil:
				t.Errorf("coverage %v, want none", *got.RateCoverage)
			case test.coverage != none && (got.RateCoverage == nil || math.Abs(*got.RateCoverage-test.coverage) > 1e-12):
				shown := "none"
				if got.RateCoverage != nil {
					shown = fmt.Sprint(*got.RateCoverage)
				}
				t.Errorf("coverage %s, want %v: reviewed pull requests over the merged pull requests of every stored day of the window", shown, test.coverage)
			}
		})
	}
}
