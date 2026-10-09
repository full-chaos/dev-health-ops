//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
)

// The pull request rework ratio counts reviewed pull requests only, over the
// view's window and subject. A pull request with no review data says nothing
// about rework: it is not a "no rework" pull request. The rows are computed
// and written by the daily job's own compute and writer (prreworktest) and
// read through the Home metric reader.
//
//	repo N (github)  day 1: 3 merged, no review data
//	repo Z (github)  day 1: 2 merged, both reviewed, no changes requested
//	repo R (github)  day 1: 4 merged: 2 reviewed (1 with changes requested), 2 with no review data
//	                 day 2: 8 merged: 3 reviewed (no changes requested), 5 with no review data
//	repo G (gitlab)  day 1: 3 merged, each with reviews (the provider has no changes-requested event)
//	repo E (github)  day 1: 1 pull request opened, none merged
//	repo O (github)  day 1: a row written before the counts existed: 5 merged, stored ratio 0
//	repo V (github)  day 1: an older version with counts (2 reviewed, 2 rework); the newest
//	                        version is a row with no counts (a worker of the older release)
//	other organization, repo R's id: 10 merged, all reviewed, all with changes requested
func TestHomePRReworkRatio_CountsReviewedPullRequestsOnly(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-pr-rework"
	const teamNR, teamNG, teamZ = "team-nr", "team-ng", "team-z"
	repoN, repoZ, repoR, repoG := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	repoE, repoO, repoV := uuid.New(), uuid.New(), uuid.New()
	day1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	older := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)

	// Two versions of a day are stored below. A background merge would keep
	// only the newest one and hide a reader that does not select it.
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES repo_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	unreviewed := prreworktest.PullRequest{}
	reviewed := prreworktest.PullRequest{Reviews: 2}
	rework := prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1}
	repeat := func(pullRequest prreworktest.PullRequest, count int) []prreworktest.PullRequest {
		out := make([]prreworktest.PullRequest, count)
		for i := range out {
			out[i] = pullRequest
		}
		return out
	}
	write := func(org string, repo uuid.UUID, provider string, day, computedAt time.Time, pullRequests ...[]prreworktest.PullRequest) {
		t.Helper()
		var all []prreworktest.PullRequest
		for _, group := range pullRequests {
			all = append(all, group...)
		}
		prreworktest.WriteDay(ctx, t, admin, org, repo, provider, day, computedAt, all)
	}
	write(orgID, repoN, "github", day1, newer, repeat(unreviewed, 3))
	write(orgID, repoZ, "github", day1, newer, repeat(reviewed, 2))
	write(orgID, repoR, "github", day1, newer, repeat(reviewed, 1), repeat(rework, 1), repeat(unreviewed, 2))
	write(orgID, repoR, "github", day2, newer, repeat(reviewed, 3), repeat(unreviewed, 5))
	write(orgID, repoG, "gitlab", day1, newer, repeat(reviewed, 3))
	write(orgID, repoE, "github", day1, newer, []prreworktest.PullRequest{{Open: true}})
	write(orgID, repoV, "github", day1, older, repeat(rework, 2))
	write("home-org-pr-rework-other", repoR, "github", day1, newer, repeat(rework, 10))
	// Rows as a worker of the release before the counts writes them: the
	// columns it knows, and the stored ratio over ALL merged pull requests.
	for repo, computedAt := range map[uuid.UUID]time.Time{repoO: newer, repoV: newer} {
		if err := admin.Exec(ctx, fmt.Sprintf(
			`INSERT INTO repo_metrics_daily (repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc, large_commit_ratio, prs_merged, large_pr_ratio, pr_rework_ratio, computed_at, org_id)
			 VALUES ('%s', '%s', 0, 0, 0, 0, 5, 0, 0, toDateTime64('%s', 6, 'UTC'), '%s')`,
			repo, day1.Format(time.DateOnly), computedAt.Format("2006-01-02 15:04:05"), orgID)); err != nil {
			t.Fatal(err)
		}
	}

	names := map[uuid.UUID]string{repoN: "acme/rw-n", repoZ: "acme/rw-z", repoR: "acme/rw-r", repoG: "acme/rw-g", repoE: "acme/rw-e", repoO: "acme/rw-o", repoV: "acme/rw-v"}
	for repo, name := range names {
		provider := "github"
		if repo == repoG {
			provider = "gitlab"
		}
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', '%s', '%s', '%s', now64(3), now64(3))",
			repo, name, provider, orgID)); err != nil {
			t.Fatal(err)
		}
	}
	own := func(team string, repo uuid.UUID) {
		t.Helper()
		provider := "github"
		if repo == repoG {
			provider = "gitlab"
		}
		if err := admin.Exec(ctx, fmt.Sprintf(
			`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			 VALUES ('%s', '%s', '%s', '%s', '%s', 'exact', 'inferred', 0, 0, 0, toDateTime64('2026-01-01 00:00:00', 3), NULL, toDateTime64('2026-01-01 00:00:00', 3))`,
			orgID, provider, team, repo, names[repo])); err != nil {
			t.Fatal(err)
		}
	}
	own(teamNR, repoN)
	own(teamNR, repoR)
	own(teamNG, repoN)
	own(teamNG, repoG)
	own(teamZ, repoZ)

	spec := metricSpec{}
	for _, candidate := range metrics {
		if candidate.Metric == "pr_rework_ratio" {
			spec = candidate
		}
	}
	if spec.Metric == "" {
		t.Fatal("Home has no pr_rework_ratio metric")
	}
	// The production path of the Home delta. The unit is percent.
	delta := func(scope ScopeFilter, start, end time.Time) MetricDelta {
		t.Helper()
		got, err := computeMetricDelta(ctx, client, spec, start, end, start.AddDate(0, 0, -7), start, Filters{Scope: scope}, orgID, teamScopeReadAsOf)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	repoScope := func(repo uuid.UUID) ScopeFilter { return ScopeFilter{Level: "repo", IDs: []string{repo.String()}} }
	teamScope := func(team string) ScopeFilter { return ScopeFilter{Level: "team", IDs: []string{team}} }
	after := day2.AddDate(0, 0, 1)
	const (
		measured   = "measured"
		unknown    = "unknown_no_review_evidence"
		noSignal   = "not_applicable_no_rework_signal"
		noneMerged = "not_applicable_no_merged_pull_requests"
		noState    = "null"
	)

	for _, tc := range []struct {
		name       string
		scope      ScopeFilter
		start, end time.Time
		want       float64
		hasData    bool
		state      string
	}{
		{"no review evidence: no value, unknown, never a measured 0", repoScope(repoN), day1, day2, 0, false, unknown},
		{"reviewed with zero rework: a measured 0", repoScope(repoZ), day1, day2, 0, true, measured},
		{"reviewed with rework: 1 of 2 reviewed, not 1 of 4 merged", repoScope(repoR), day1, day2, 50, true, measured},
		{"a day with no changes requested among its reviewed pull requests", repoScope(repoR), day2, after, 0, true, measured},
		// 1 rework of 5 reviewed. The mean of the two daily ratios is 25 %, and
		// the old read (weighted by all merged pull requests) is 8.3 %.
		{"mixed window: the ratio of the sums", repoScope(repoR), day1, after, 20, true, measured},
		{"a provider with no changes-requested event: not applicable", repoScope(repoG), day1, day2, 0, false, noSignal},
		{"no merged pull request: not applicable", repoScope(repoE), day1, day2, 0, false, noneMerged},
		{"a row written before the counts: nothing stored, no state", repoScope(repoO), day1, day2, 0, false, noState},
		{"the newest version of a day has no counts: the older counts are not read", repoScope(repoV), day1, day2, 0, false, noState},
		{"a team: the sum over its repositories", teamScope(teamNR), day1, day2, 50, true, measured},
		{"a team with a repository of each kind and no reviewed pull request: unknown", teamScope(teamNG), day1, day2, 0, false, unknown},
		{"a team whose reviewed pull requests have no rework", teamScope(teamZ), day1, day2, 0, true, measured},
		// N 0/0, Z 0/2, R 1/2, G -, E -, O -, V -: 1 of 4 reviewed.
		{"the organization, day 1", ScopeFilter{Level: "org"}, day1, day2, 25, true, measured},
		{"a window with no stored row", repoScope(repoR), day1.AddDate(0, 0, -30), day1.AddDate(0, 0, -20), 0, false, noState},
	} {
		got := delta(tc.scope, tc.start, tc.end)
		state := noState
		if got.RateState != nil {
			state = *got.RateState
		}
		if got.HasData != tc.hasData || math.Abs(got.Value-tc.want) > 1e-9 || state != tc.state {
			t.Errorf("%s: value %v has_data %v state %s, want %v %v %s", tc.name, got.Value, got.HasData, state, tc.want, tc.hasData, tc.state)
		}
	}

	// The series draws a point only for a day that is measured.
	series, err := fetchMetricSeries(ctx, client, spec.Table, spec.Column, day1, after, "", nil, spec.Aggregator, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 2 || math.Abs(series[0].Value-0.25) > 1e-9 || series[1].Value != 0 {
		t.Errorf("the organization's day series = %+v, want day 1 at 0.25 and day 2 at 0", series)
	}
	unreviewedSeries, err := fetchMetricSeries(ctx, client, spec.Table, spec.Column, day1, after,
		" AND repo_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{{Name: "scope_ids", Value: []string{repoN.String()}}}, spec.Aggregator, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if len(unreviewedSeries) != 0 {
		t.Errorf("a repository with no review data has %d point(s) in its series, want none: %+v", len(unreviewedSeries), unreviewedSeries)
	}
}
