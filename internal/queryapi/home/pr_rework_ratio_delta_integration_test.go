//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
)

// The delta of the pull request rework ratio and its "driven by" lookup follow
// the one delta rule (internal/queryapi/deltarule), over the ratio of REVIEWED
// pull requests. Rows computed and written by the daily job's own compute and
// writer; each repository has one day in the comparison window and one in the
// current window:
//
//	repo P   prior: 2 merged, 2 reviewed, 0 with changes requested (a measured 0)
//	         current: 2 merged, 2 reviewed, 1 with changes requested (50 %)
//	repo D1  prior: 4 merged, 4 reviewed, 1 (25 %); current: 4 merged, 4 reviewed, 2 (50 %): +100 %
//	repo D2  prior: 10 merged, 2 reviewed, 1 (50 % of the reviewed; 10 % of the merged)
//	         current: 10 merged, 10 reviewed, 3 (30 %): -40 % over the reviewed
//	         (over ALL merged pull requests it would be +200 %, and D2 the first driver)
//	repo N   prior and current: 3 merged, no review data
func TestHomePRReworkRatio_DeltaAndDriversFollowTheDeltaRule(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-pr-rework-delta"
	repoP, repoD1, repoD2, repoN := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	current := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -7)
	computedAt := time.Date(2026, 9, 10, 6, 0, 0, 0, time.UTC)
	unreviewed := prreworktest.PullRequest{}
	reviewed := prreworktest.PullRequest{Reviews: 2}
	rework := prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1}
	day := func(repo uuid.UUID, on time.Time, groups ...any) {
		t.Helper()
		var pullRequests []prreworktest.PullRequest
		for i := 0; i < len(groups); i += 2 {
			for n := 0; n < groups[i+1].(int); n++ {
				pullRequests = append(pullRequests, groups[i].(prreworktest.PullRequest))
			}
		}
		prreworktest.WriteDay(ctx, t, admin, orgID, repo, "github", on, computedAt, pullRequests)
	}
	day(repoP, prior, reviewed, 2)
	day(repoP, current, reviewed, 1, rework, 1)
	day(repoD1, prior, reviewed, 3, rework, 1)
	day(repoD1, current, reviewed, 2, rework, 2)
	day(repoD2, prior, reviewed, 1, rework, 1, unreviewed, 8)
	day(repoD2, current, reviewed, 7, rework, 3)
	day(repoN, prior, unreviewed, 3)
	day(repoN, current, unreviewed, 3)
	// A repository scope is resolved through the repos table.
	for repo, name := range map[uuid.UUID]string{repoP: "acme/rwd-p", repoD1: "acme/rwd-d1", repoD2: "acme/rwd-d2", repoN: "acme/rwd-n"} {
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', '%s', 'github', '%s', now64(3), now64(3))",
			repo, name, orgID)); err != nil {
			t.Fatal(err)
		}
	}

	spec := metricSpec{}
	for _, candidate := range metrics {
		if candidate.Metric == "pr_rework_ratio" {
			spec = candidate
		}
	}
	if spec.Metric == "" {
		t.Fatal("Home has no pr_rework_ratio metric")
	}
	end := current.AddDate(0, 0, 1)
	compareStart, compareEnd := prior, prior.AddDate(0, 0, 1)
	delta := func(repo uuid.UUID) MetricDelta {
		t.Helper()
		got, err := computeMetricDelta(ctx, client, spec, current, end, compareStart, compareEnd,
			Filters{Scope: ScopeFilter{Level: "repo", IDs: []string{repo.String()}}}, orgID, teamScopeReadAsOf)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	// show prints a percent that can be absent.
	show := func(percent *float64) string {
		if percent == nil {
			return "none"
		}
		return fmt.Sprint(*percent)
	}
	// A measured 0 before and 50 % now: two measured values and no percent.
	// It is not 0 % ("no change"), and it is not a number.
	fromZero := delta(repoP)
	if !fromZero.HasData || !fromZero.HasPriorData || fromZero.Value != 50 {
		t.Fatalf("repo P: value %v has_data %v has_prior_data %v, want 50 with both windows measured: the case is not set",
			fromZero.Value, fromZero.HasData, fromZero.HasPriorData)
	}
	if fromZero.DeltaPct != nil {
		t.Errorf("repo P: a measured 0 before and 50 %% now serves delta_pct = %v, want none (null): a percent of 0 does not exist", *fromZero.DeltaPct)
	}
	// Two measured values: the percent of the ratio over reviewed pull requests.
	if got := delta(repoD1); got.DeltaPct == nil || *got.DeltaPct != 100 {
		t.Errorf("repo D1: delta_pct = %s, want 100 (25 %% to 50 %%)", show(got.DeltaPct))
	}
	if got := delta(repoD2); got.DeltaPct == nil || *got.DeltaPct < -40.0001 || *got.DeltaPct > -39.9999 {
		t.Errorf("repo D2: delta_pct = %s, want -40 (50 %% of 2 reviewed to 30 %% of 10 reviewed)", show(got.DeltaPct))
	}
	// No review data in a window: no value, and the percent is the 0 of "no
	// delta", with the flags saying so.
	if got := delta(repoN); got.HasData || got.HasPriorData || got.DeltaPct == nil || *got.DeltaPct != 0 {
		t.Errorf("repo N: has_data %v has_prior_data %v delta_pct %s, want no data in both windows and the 0 of no delta",
			got.HasData, got.HasPriorData, show(got.DeltaPct))
	}

	// The "driven by" lookup: the groups ranked by the percent of their own
	// ratio over reviewed pull requests. D1 (+100 %) is before D2 (-40 %). P
	// has no percent and N has no value: neither is a driver.
	drivers, err := fetchMetricDriverDelta(ctx, client, spec.Table, spec.Column, metricGroup(spec.Metric),
		current, end, compareStart, compareEnd, "", nil, orgID, 10)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, driver := range drivers {
		ids = append(ids, driver.ID)
	}
	if len(ids) != 2 || ids[0] != repoD1.String() || ids[1] != repoD2.String() {
		names := map[string]string{repoP.String(): "P", repoD1.String(): "D1", repoD2.String(): "D2", repoN.String(): "N"}
		var got []string
		for _, id := range ids {
			got = append(got, names[id])
		}
		t.Errorf("the drivers of the rework ratio are %v, want [D1 D2]: ranked by the ratio over reviewed pull requests, "+
			"with no driver that has no percent or no value", got)
	}
}
