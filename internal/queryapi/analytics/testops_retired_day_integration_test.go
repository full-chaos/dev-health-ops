//go:build integration

package analytics

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// A day whose newest testops_test_metrics_daily row retires it (retracted = 1, as the daily writer stores it over a day that
// counts no kept suite any more) has no value in a TestOps test series: the stored rates and counts of its older row are not
// read, the marker row is no sample of 0, and the pooled suite duration does not pool the suites of a retired day.
func TestTestopsTestSeriesReadARetiredDayAsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)

	const orgID = "seeded-retired-day"
	const repo = "13928000-0000-4000-8000-0000000000e1"
	retired := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	live := retired.AddDate(0, 0, 1)
	stored := time.Date(2026, 1, 4, 10, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES testops_test_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(repo)
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
 flake_rate, total_suites, team_id, org_id, computed_at, flake_cases, distinct_cases)
VALUES (?, ?, 4, 2, 2, 0.5, 0.5, 0.5, 1, 'team-1', ?, ?, 1, 2)`, id, retired, orgID, stored)
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, team_id, org_id, computed_at, retracted) VALUES (?, ?, 'team-1', ?, ?, 1)`,
		id, retired, orgID, stored.Add(time.Hour))
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
 flake_rate, total_suites, team_id, org_id, computed_at, flake_cases, distinct_cases)
VALUES (?, ?, 2, 2, 0, 1, 0, 0.25, 1, 'team-1', ?, ?, 1, 4)`, id, live, orgID, stored)
	for _, suite := range []struct {
		run      string
		day      time.Time
		duration float64
	}{{"r-retired", retired, 10}, {"r-live", live, 20}} {
		exec(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count,
 duration_seconds, started_at, finished_at, org_id, last_synced) VALUES (?, ?, ?, 'pkg', 1, 1, 0, 0, ?, ?, ?, ?, ?)`,
			id, suite.run, suite.run+"-s", suite.duration, suite.day.Add(10*time.Hour), suite.day.Add(11*time.Hour), orgID, stored)
	}
	for _, q := range ownerSeedSQL(orgID, repo, "team-1") {
		exec(q)
	}

	for _, measure := range []model.MeasureInput{
		model.MeasureInputTestPassRate, model.MeasureInputTestFailureRate, model.MeasureInputTestFlakeRate,
		model.MeasureInputTestSuiteDurationP95,
	} {
		req, err := TimeseriesRequestFromInput(model.TimeseriesRequestInput{
			Dimension: model.DimensionInputTeam, Measure: measure, Interval: model.BucketIntervalInputDay,
			DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
		})
		if err != nil {
			t.Fatal(err)
		}
		query, err := CompileTimeseries(req, orgID, 120, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		results, err := ExecuteTimeseries(ctx, client, query, "team", strings.ToLower(string(measure)))
		if err != nil {
			t.Fatalf("%s: %v", measure, err)
		}
		byDay := map[string]*float64{}
		for _, result := range results {
			if result.DimensionValue != "team-1" {
				continue
			}
			for _, bucket := range result.Buckets {
				byDay[time.Time(bucket.Date).UTC().Format(time.DateOnly)] = bucket.Value
			}
		}
		if v := byDay[retired.Format(time.DateOnly)]; v != nil {
			t.Errorf("%s: the retired day has the value %v, want none (never 0, never its older value)", measure, *v)
		}
		v := byDay[live.Format(time.DateOnly)]
		if v == nil {
			t.Errorf("%s: the live day has no value", measure)
			continue
		}
		if measure == model.MeasureInputTestSuiteDurationP95 && math.Abs(*v-20) > 1e-9 {
			t.Errorf("%s: the live day = %v, want 20 (the live day's suite alone)", measure, *v)
		}
	}
}
