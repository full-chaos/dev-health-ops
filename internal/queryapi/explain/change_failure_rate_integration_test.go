//go:build integration

package explain

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// /explain's change failure rate is the window ratio of the summed counts
// that the real writer stores (CHAOS-8981): undefined without a deployment or
// without incident evidence, and an undefined repository is not a contributor
// shown as 0%. Revert rate is total reverted / total merged pull requests
// over the days that hold a stored revert rate; a day with merges and no
// stored rate is unknown, never 0%.
//
//	repo A  day 1: 1 deployment, 1 failed, 1 incident; day 2: 9 deployments, 1 incident
//	repo B  day 1: 4 deployments, no incident evidence (unknown)
func TestExplainChangeFailureRateAndRevertRate_LiveEngine(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)

	const orgID = "org-explain-change-failure"
	repoA, repoB := uuid.New(), uuid.New()
	day1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	computedAt := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)

	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteChangeFailure(ctx, []repouser.ChangeFailureDaily{
		{RepoID: repoA, Day: day1, ComputedAt: computedAt, Counts: changefailure.Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
		{RepoID: repoA, Day: day2, ComputedAt: computedAt, Counts: changefailure.Counts{Deployments: 9, IncidentsDirect: 1}},
		{RepoID: repoB, Day: day1, ComputedAt: computedAt, Counts: changefailure.Counts{Deployments: 4}},
	}, orgID); err != nil {
		t.Fatal(err)
	}
	// Revert rate through the real repo_metrics_daily writer. Repo A holds a
	// stored revert rate (the rows a revert detector will write): 4 merged with
	// 1 reverted on day 1, 6 merged with none on day 2. Repo B holds the rows
	// the writer stores today: no revert rate, on a day with no merge and on a
	// day with 90 merges whose deprecated column says 0.2. Total reverted /
	// total merged over the days with a rate is 1 / 10: not 0.19 (the
	// deprecated column read as a rate), not 0.01 (repo B's merges counted as
	// 90 pull requests with no revert), not the 0.125 average of the two rates.
	rate := func(v float64) *float64 { return &v }
	if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{
		{RepoID: repoA, Day: day1, PRsMerged: 4, ChangeFailureRate: 0.25, RevertRate: rate(0.25), ComputedAt: computedAt},
		{RepoID: repoA, Day: day2, PRsMerged: 6, RevertRate: rate(0), ComputedAt: computedAt},
		{RepoID: repoB, Day: day1, CommitsCount: 1, ComputedAt: computedAt},
		{RepoID: repoB, Day: day2, PRsMerged: 90, ChangeFailureRate: 0.2, ComputedAt: computedAt},
	}}, orgID); err != nil {
		t.Fatal(err)
	}
	for i, repo := range []uuid.UUID{repoA, repoB} {
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', 'acme/explain-%d', 'github', '%s', now64(3), now64(3))",
			repo, i, orgID)); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	explain := func(metric string) *Response {
		t.Helper()
		got, err := BuildExplainResponse(ctx, reader, orgID, Params{
			Metric: metric, StartDay: day1, EndDay: day2.AddDate(0, 0, 1),
			CompareStart: day1.AddDate(0, 0, -2), CompareEnd: day1,
		})
		if err != nil {
			t.Fatalf("explain %s: %v", metric, err)
		}
		return got
	}

	cfr := explain("change_failure_rate")
	// 1 failed of 14 deployments in the organization; the unit is percent.
	if want := 100.0 / 14.0; !cfr.HasData || cfr.HasPriorData || !near(cfr.Value, want) {
		t.Errorf("change_failure_rate = %v (has_data %v, has_prior_data %v), want %v with data and no prior data", cfr.Value, cfr.HasData, cfr.HasPriorData, want)
	}
	if len(cfr.Contributors) != 1 || cfr.Contributors[0].ID != repoA.String() || !near(cfr.Contributors[0].Value, 10) {
		t.Errorf("change_failure_rate contributors = %+v, want repo A alone at 10%% (repo B is unknown, not 0%%)", cfr.Contributors)
	}
	// Every failed deployment of the window counts through a heuristic link.
	if cfr.LinkTier == nil || *cfr.LinkTier != changefailure.TierHeuristic {
		t.Errorf("change_failure_rate link_tier = %v, want heuristic", cfr.LinkTier)
	}
	for _, driver := range cfr.Drivers {
		if driver.ID == repoB.String() {
			t.Errorf("repo B (no incident evidence) is a change_failure_rate driver: %+v", driver)
		}
	}

	// Repo B alone: deployments and no incident evidence. No value, no tier.
	unknown, err := BuildExplainResponse(ctx, reader, orgID, Params{
		Metric: "change_failure_rate", StartDay: day1, EndDay: day2.AddDate(0, 0, 1),
		CompareStart: day1.AddDate(0, 0, -2), CompareEnd: day1,
		ScopeLevel: "repo", ScopeIDs: []string{repoB.String()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if unknown.HasData || unknown.Value != 0 || unknown.LinkTier != nil || len(unknown.Contributors) != 0 {
		t.Errorf("repo B change_failure_rate = %v (has_data %v, link_tier %v, contributors %+v), want no data", unknown.Value, unknown.HasData, unknown.LinkTier, unknown.Contributors)
	}

	revert := explain("revert_rate")
	if !revert.HasData || !near(revert.Value, 10) || revert.Label != "Revert Rate" || revert.LinkTier != nil || revert.RateState != nil {
		t.Errorf("revert_rate = %v %q (has_data %v), want 10%% Revert Rate with data, no tier and no state", revert.Value, revert.Label, revert.HasData)
	}
	// Repo B has merges and no measured revert rate: it is not a contributor
	// at 0%.
	if len(revert.Contributors) != 1 || revert.Contributors[0].ID != repoA.String() || !near(revert.Contributors[0].Value, 10) {
		t.Errorf("revert_rate contributors = %+v, want repo A alone at 10%%", revert.Contributors)
	}
	for _, driver := range revert.Drivers {
		if driver.ID == repoB.String() {
			t.Errorf("repo B (no measured revert rate) is a revert_rate driver: %+v", driver)
		}
	}
	// Repo B alone, over a day with 90 merged pull requests: no data. This is
	// what every repository reads today, because no writer measures a revert
	// rate yet.
	unmeasured, err := BuildExplainResponse(ctx, reader, orgID, Params{
		Metric: "revert_rate", StartDay: day1, EndDay: day2.AddDate(0, 0, 1),
		CompareStart: day1.AddDate(0, 0, -2), CompareEnd: day1,
		ScopeLevel: "repo", ScopeIDs: []string{repoB.String()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if unmeasured.HasData || unmeasured.Value != 0 || len(unmeasured.Contributors) != 0 {
		t.Errorf("revert_rate of a repository with 90 merged pull requests and no measured rate = %v (has_data %v, contributors %+v), want no data",
			unmeasured.Value, unmeasured.HasData, unmeasured.Contributors)
	}
}

func near(got, want float64) bool {
	diff := got - want
	return diff < 1e-9 && diff > -1e-9
}
