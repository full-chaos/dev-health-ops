//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CI Success Rate is the window's successes over its runs, never a mean of the stored daily rates (pm ruling A,
// 2026-11-15): cicd_metrics_daily holds a day's run count and its success rate (stored counts: none), so a row's
// successes are rate x runs and the window's rate is the sum of those over the sum of runs.
//
//	repo A  day 1: 1 run, rate 1.0          day 2: 9 runs, rate 1.0
//	repo B  day 1: 99 runs, rate 0.0        day 2: 1 run, rate 0.0 (an older version of the row said 100 runs at 1.0)
//	repo C  day 1: 0 runs (no row sample)    other organization, repo A's id: 500 runs at 0.0
func TestHomeCISuccess_IsTheWeightedRateOfTheWindow(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-ci-success-weighted"
	const teamAB, teamB = "team-ab", "team-b"
	repoA, repoB, repoC := uuid.New(), uuid.New(), uuid.New()
	day1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	after := day2.AddDate(0, 0, 1)
	older := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)

	write := func(org string, repo uuid.UUID, day, computedAt time.Time, runs int, rate float64) {
		t.Helper()
		if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO cicd_metrics_daily (repo_id, day, pipelines_count, success_rate, computed_at, org_id) VALUES ('%s', '%s', %d, %v, '%s', '%s')`,
			repo, day.Format("2006-01-02"), runs, rate, computedAt.Format("2006-01-02 15:04:05"), org)); err != nil {
			t.Fatal(err)
		}
	}
	write(orgID, repoA, day1, newer, 1, 1.0)
	write(orgID, repoA, day2, newer, 9, 1.0)
	write(orgID, repoB, day1, older, 100, 1.0)
	write(orgID, repoB, day1, newer, 99, 0.0)
	write(orgID, repoB, day2, newer, 1, 0.0)
	write("home-org-ci-success-weighted-other", repoA, day1, newer, 500, 0.0)

	names := map[uuid.UUID]string{repoA: "acme/ci-a", repoB: "acme/ci-b", repoC: "acme/ci-c"}
	for repo, name := range names {
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', '%s', 'github', '%s', now64(3), now64(3))",
			repo, name, orgID)); err != nil {
			t.Fatal(err)
		}
	}
	own := func(team string, repo uuid.UUID) {
		t.Helper()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			 VALUES ('%s', 'github', '%s', '%s', '%s', 'exact', 'inferred', 0, 0, 0, toDateTime64('2026-01-01 00:00:00', 3), NULL, toDateTime64('2026-01-01 00:00:00', 3))`,
			orgID, team, repo, names[repo])); err != nil {
			t.Fatal(err)
		}
	}
	own(teamAB, repoA)
	own(teamAB, repoB)
	own(teamB, repoB)

	spec := metricSpec{}
	for _, candidate := range metrics {
		if candidate.Metric == "ci_success" {
			spec = candidate
		}
	}
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

	for _, tc := range []struct {
		name       string
		scope      ScopeFilter
		start, end time.Time
		want       float64
		hasData    bool
	}{
		// 10 successes of 110 runs over both days and both repositories, the newest version of repo B day 1
		// (99 runs, none succeeded): the ratio of sums, not the mean of the four stored rates (50).
		{"organization, both days", ScopeFilter{Level: "org"}, day1, after, 10.0 / 110.0 * 100, true},
		// 1 of 100 on day 1.
		{"organization, day 1", ScopeFilter{Level: "org"}, day1, day2, 1.0 / 100.0 * 100, true},
		{"repo A", repoScope(repoA), day1, after, 100, true},
		{"repo B", repoScope(repoB), day1, after, 0, true},
		// A team is its owned repositories: A and B.
		{"team owning A and B", teamScope(teamAB), day1, after, 10.0 / 110.0 * 100, true},
		{"team owning B", teamScope(teamB), day1, after, 0, true},
		// repo C has no row: no data, never 0 or 100.
		{"repo C, no row", repoScope(repoC), day1, after, 0, false},
		{"a window with no row", ScopeFilter{Level: "org"}, after, after.AddDate(0, 0, 7), 0, false},
	} {
		got := delta(tc.scope, tc.start, tc.end)
		if got.HasData != tc.hasData || (tc.hasData && !closeTo(got.Value, tc.want)) || got.Unit != "%" {
			t.Errorf("%s: value %v %s hasData %v; want %v %%, %v", tc.name, got.Value, got.Unit, got.HasData, tc.want, tc.hasData)
		}
	}
	// The prior window is the same rule: the week before day 2 holds day 1 (1 of 100).
	if got := delta(ScopeFilter{Level: "org"}, day2, after); !got.HasPriorData || got.PriorValue == nil || !closeTo(*got.PriorValue, 1) {
		t.Errorf("organization, day 2: prior %+v, want a measured 1%%", got)
	}
	// The sparkline of the window is the weighted rate of each day.
	got := delta(ScopeFilter{Level: "org"}, day1, after)
	if len(got.Spark) != 2 || !closeTo(got.Spark[0].Value, 1) || !closeTo(got.Spark[1].Value, 9.0/10.0*100) {
		t.Errorf("organization spark %+v, want day 1 = 1%% and day 2 = 90%%", got.Spark)
	}
}
