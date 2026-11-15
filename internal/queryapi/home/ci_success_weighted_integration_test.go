//go:build integration

package home

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CI Success Rate is the window's successes over its COMPLETED runs (success plus failure; cancelled, skipped and
// running runs are out), never a mean of stored daily rates (pm rulings, 2026-11-15). It reads
// testops_pipeline_metrics_daily, the table of the TestOps Pipelines series and of the Delivery Risk factor.
func seedPipelineDay(ctx context.Context, t *testing.T, admin interface {
	Exec(context.Context, string, ...any) error
}, org string, repo uuid.UUID, day, computedAt time.Time, success, failure, cancelled int) {
	t.Helper()
	runs := success + failure + cancelled
	rate := func(n int) float64 {
		if runs == 0 {
			return 0
		}
		return float64(n) / float64(runs)
	}
	if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, failure_count, cancelled_count, success_rate, failure_rate, cancel_rate, rerun_rate, computed_at, org_id) VALUES ('%s', '%s', %d, %d, %d, %d, %v, %v, %v, 0, '%s', '%s')`,
		repo, day.Format("2006-01-02"), runs, success, failure, cancelled, rate(success), rate(failure), rate(cancelled), computedAt.Format("2006-01-02 15:04:05"), org)); err != nil {
		t.Fatal(err)
	}
}

// repo A  day 1: 1 success                 day 2: 9 success
// repo B  day 1: 99 failures + 5 cancelled day 2: 1 failure   (an older version of day 1 said 100 successes)
// repo C  day 1: 3 cancelled, nothing completed
// other organization, repo A's id: 500 failures
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

	seedPipelineDay(ctx, t, admin, orgID, repoA, day1, newer, 1, 0, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoA, day2, newer, 9, 0, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoB, day1, older, 100, 0, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoB, day1, newer, 0, 99, 5)
	seedPipelineDay(ctx, t, admin, orgID, repoB, day2, newer, 0, 1, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoC, day1, newer, 0, 0, 3)
	seedPipelineDay(ctx, t, admin, "home-org-ci-success-weighted-other", repoA, day1, newer, 0, 500, 0)

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
	if spec.Table != "testops_pipeline_metrics_daily" {
		t.Fatalf("ci_success reads %q, want testops_pipeline_metrics_daily", spec.Table)
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
		// 10 successes of 110 completed runs (the 5 cancelled are out; the newest version of repo B day 1):
		// the ratio of sums, not the mean of the stored rates.
		{"organization, both days", ScopeFilter{Level: "org"}, day1, after, 10.0 / 110.0 * 100, true},
		{"organization, day 1", ScopeFilter{Level: "org"}, day1, day2, 1.0 / 100.0 * 100, true},
		{"repo A", repoScope(repoA), day1, after, 100, true},
		{"repo B", repoScope(repoB), day1, after, 0, true},
		{"team owning A and B", teamScope(teamAB), day1, after, 10.0 / 110.0 * 100, true},
		{"team owning B", teamScope(teamB), day1, after, 0, true},
		// repo C ran 3 pipelines, all cancelled: no completed run, no value (never 0 or 100).
		{"repo C, nothing completed", repoScope(repoC), day1, after, 0, false},
		{"a window with no row", ScopeFilter{Level: "org"}, after, after.AddDate(0, 0, 7), 0, false},
	} {
		got := delta(tc.scope, tc.start, tc.end)
		if got.HasData != tc.hasData || (tc.hasData && !closeTo(got.Value, tc.want)) || got.Unit != "%" {
			t.Errorf("%s: value %v %s hasData %v; want %v %%, %v", tc.name, got.Value, got.Unit, got.HasData, tc.want, tc.hasData)
		}
	}
	if got := delta(ScopeFilter{Level: "org"}, day2, after); !got.HasPriorData || got.PriorValue == nil || !closeTo(*got.PriorValue, 1) {
		t.Errorf("organization, day 2: prior %+v, want a measured 1%%", got)
	}
	got := delta(ScopeFilter{Level: "org"}, day1, after)
	if len(got.Spark) != 2 || !closeTo(got.Spark[0].Value, 1) || !closeTo(got.Spark[1].Value, 9.0/10.0*100) {
		t.Errorf("organization spark %+v, want day 1 = 1%% and day 2 = 90%%", got.Spark)
	}
}

// The "driven by" reads (the single-metric read and the batch) rank repositories by the change of the same weighted
// rate: Z 90 of 100 completed runs against 50% before is +80%, Y +20%, X 1 of 100 against 50% is -98%. A mean of the
// stored daily rates would put X and Z at 50% and rank Y first.
func TestHomeCISuccessDriversRankByTheWeightedRate(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	current, endDay, prior, priorEnd, err := TimeWindow(DefaultFilters(), now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "home-org-ci-success-drivers"
	computedAt := now.Add(-time.Hour)
	repoX, repoY, repoZ := uuid.New(), uuid.New(), uuid.New()
	seedPipelineDay(ctx, t, admin, org, repoX, current, computedAt, 1, 0, 0)
	seedPipelineDay(ctx, t, admin, org, repoX, current.AddDate(0, 0, 1), computedAt, 0, 99, 0)
	seedPipelineDay(ctx, t, admin, org, repoX, prior, computedAt, 50, 50, 0)
	seedPipelineDay(ctx, t, admin, org, repoY, current, computedAt, 60, 40, 0)
	seedPipelineDay(ctx, t, admin, org, repoY, prior, computedAt, 50, 50, 0)
	seedPipelineDay(ctx, t, admin, org, repoZ, current, computedAt, 90, 0, 0)
	seedPipelineDay(ctx, t, admin, org, repoZ, current.AddDate(0, 0, 1), computedAt, 0, 10, 0)
	seedPipelineDay(ctx, t, admin, org, repoZ, prior, computedAt, 50, 50, 0)
	want := []string{repoZ.String(), repoY.String(), repoX.String()}

	single, err := fetchMetricDriverDelta(ctx, client, metricTable("ci_success"), metricColumn("ci_success"), metricGroup("ci_success"), current, endDay, prior, priorEnd, "", nil, org, 3)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := fetchMetricDriversBatch(ctx, client, []driverBatchMetric{{Metric: "ci_success"}}, current, endDay, prior, priorEnd, nil, org, 3)
	if err != nil {
		t.Fatal(err)
	}
	ids := func(rows []driverRow) []string {
		var out []string
		for _, r := range rows {
			out = append(out, r.ID)
		}
		return out
	}
	if got := ids(single); !slices.Equal(got, want) {
		t.Errorf("single read ranks %v, want %v", got, want)
	}
	if got := ids(batch["ci_success"]); !slices.Equal(got, want) {
		t.Errorf("batch ranks %v, want %v", got, want)
	}
}

// Home and the TestOps Pipelines series are one number for one idea: for the same repositories and days (no run is
// cancelled, skipped or running here, where the series' own denominator rule is not the question) the Home tile
// equals the series of the same repository on every day.
func TestHomeCISuccessEqualsTheTestOpsPipelineSeries(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	const orgID = "home-org-ci-success-equals-series"
	repoA, repoB := uuid.New(), uuid.New()
	day1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	after := day2.AddDate(0, 0, 1)
	computedAt := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	seedPipelineDay(ctx, t, admin, orgID, repoA, day1, computedAt, 7, 3, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoA, day2, computedAt, 1, 9, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoB, day1, computedAt, 40, 10, 0)
	seedPipelineDay(ctx, t, admin, orgID, repoB, day2, computedAt, 0, 5, 0)
	for repo, name := range map[uuid.UUID]string{repoA: "acme/eq-a", repoB: "acme/eq-b"} {
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', '%s', 'github', '%s', now64(3), now64(3))",
			repo, name, orgID)); err != nil {
			t.Fatal(err)
		}
	}
	spec := metricSpec{}
	for _, candidate := range metrics {
		if candidate.Metric == "ci_success" {
			spec = candidate
		}
	}
	got, err := analytics.Resolve(ctx, client, orgID, model.AnalyticsRequestInput{Timeseries: []model.TimeseriesRequestInput{{
		Dimension: model.DimensionInputRepo, Measure: model.MeasureInputPipelineSuccessRate, Interval: model.BucketIntervalInputDay,
		DateRange: &model.DateRangeInput{StartDate: graphqldate.New(day1), EndDate: graphqldate.New(day2)},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	series := map[string]map[string]float64{}
	for _, s := range got.Timeseries {
		if s.Scope == model.TimeseriesScopeOrg {
			continue
		}
		series[s.DimensionValue] = map[string]float64{}
		for _, b := range s.Buckets {
			if b.Value != nil {
				series[s.DimensionValue][b.Date.String()] = *b.Value
			}
		}
	}
	if len(series) == 0 {
		t.Fatalf("the TestOps pipeline series returned nothing: %+v", got.Timeseries)
	}
	for repo, name := range map[uuid.UUID]string{repoA: "acme/eq-a", repoB: "acme/eq-b"} {
		for _, day := range []time.Time{day1, day2} {
			home, err := computeMetricDelta(ctx, client, spec, day, day.AddDate(0, 0, 1), day.AddDate(0, 0, -7), day, Filters{Scope: ScopeFilter{Level: "repo", IDs: []string{repo.String()}}}, orgID, teamScopeReadAsOf)
			if err != nil {
				t.Fatal(err)
			}
			want, ok := series[repo.String()][day.Format("2006-01-02")]
			if !ok {
				want, ok = series[name][day.Format("2006-01-02")]
			}
			if !ok || !home.HasData || !closeTo(home.Value, want) {
				t.Errorf("%s %s: Home %v (data %v), TestOps series %v (found %v)", name, day.Format("2006-01-02"), home.Value, home.HasData, want, ok)
			}
		}
	}
	_ = after
}
