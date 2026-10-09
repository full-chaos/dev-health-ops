//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A report chart of change failure rate is the shared window rule over the
// counts of each bucket (CHAOS-8981): the number Home, /explain and the
// operating review give for the same window. It is not the average of the
// stored one-day values, and never the deprecated
// repo_metrics_daily.change_failure_rate column, which still holds the legacy
// revert ratio. Rows written by the real writers on the migrated schema:
//
//	repo A  day 1: 1 deployment, 1 failed, 1 incident (an older version of the
//	               day says 1 deployment, none failed, no incident)
//	        day 2: 9 deployments, 1 incident, none failed
//	repo B  day 1: 4 deployments, no incident evidence
//	        day 3: 2 deployments, no incident evidence
//	repo C  day 1: an older version says 100 deployments, all failed; the
//	               newest version is a retraction row of zeros
//	repo_metrics_daily, repo A day 1: legacy ratio 0.9, one-day value 0.7
func TestChangeFailureRateChartIsTheWindowRuleOverTheCounts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	const org = "org-1"
	repoA, repoB, repoC := uuid.New(), uuid.New(), uuid.New()
	day1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day2, day3 := day1.AddDate(0, 0, 1), day1.AddDate(0, 0, 2)
	older := day1.Add(80 * time.Hour)
	newer := older.Add(time.Hour)
	// Each version of a key in its own insert: both versions stay stored
	// until a merge, and the chart must read the newest one.
	write := func(rows ...repouser.ChangeFailureDaily) {
		t.Helper()
		if _, err := writer.WriteChangeFailure(ctx, rows, org); err != nil {
			t.Fatal(err)
		}
	}
	write(
		repouser.ChangeFailureDaily{RepoID: repoA, Day: day1, ComputedAt: older, Counts: changefailure.Counts{Deployments: 1}},
		repouser.ChangeFailureDaily{RepoID: repoC, Day: day1, ComputedAt: older, Counts: changefailure.Counts{Deployments: 100, FailedNative: 100, IncidentsDirect: 1}},
	)
	write(
		repouser.ChangeFailureDaily{RepoID: repoA, Day: day1, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1}},
		repouser.ChangeFailureDaily{RepoID: repoA, Day: day2, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 9, IncidentsDirect: 1}},
		repouser.ChangeFailureDaily{RepoID: repoB, Day: day1, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 4}},
		repouser.ChangeFailureDaily{RepoID: repoB, Day: day3, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 2}},
		repouser.ChangeFailureDaily{RepoID: repoC, Day: day1, ComputedAt: newer, Counts: changefailure.Counts{}},
	)
	// Another organization's counts for the same repository id are not read.
	if _, err := writer.WriteChangeFailure(ctx, []repouser.ChangeFailureDaily{
		{RepoID: repoA, Day: day1, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 50, FailedNative: 50, IncidentsDirect: 1}},
	}, "org-2"); err != nil {
		t.Fatal(err)
	}
	oneDay := 0.7
	if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{
		{RepoID: repoA, Day: day1, CommitsCount: 1, ChangeFailureRate: 0.9, ChangeFailureRateIncident: &oneDay, ComputedAt: newer},
	}}, org); err != nil {
		t.Fatal(err)
	}

	chart := func(id, chartType, groupBy, start, end string) ChartSpec {
		return ChartSpec{
			ChartID: id, PlanID: "plan-1", ChartType: chartType, Metric: "change_failure_rate", GroupBy: groupBy,
			TimeRangeStart: start, TimeRangeEnd: end, OrganizationID: org,
		}
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
			Charts: []ChartSpec{
				chart("by-day", "line", "day", "2026-01-01", "2026-01-03"),
				chart("total", "scorecard", "", "2026-01-01", "2026-01-03"),
				chart("by-repo", "bar", "repo", "2026-01-01", "2026-01-03"),
				chart("day-three", "scorecard", "", "2026-01-03", "2026-01-03"),
			},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	points := map[string]map[string]float64{}
	for _, chart := range result.Charts {
		if chart.SourceTable != changefailure.Table {
			t.Errorf("chart %s names source table %q, want %q", chart.Spec.ChartID, chart.SourceTable, changefailure.Table)
		}
		points[chart.Spec.ChartID] = map[string]float64{}
		for _, point := range chart.DataPoints {
			points[chart.Spec.ChartID][point.X] = point.Y
		}
	}
	want := map[string]map[string]float64{
		// Day 1: 1 failed of A's 1 and B's 4 deployments, with A's incident as
		// the evidence. Day 2: a measured 0. Day 3 has deployments and no
		// incident evidence: no point, not a 0.
		"by-day": {"2026-01-01": 0.2, "2026-01-02": 0},
		// 1 failed of 16 deployments in the three days: not the 0.1 mean of
		// the two day values.
		"total": {"total": 1.0 / 16.0},
		// A: 1 of 10. B is unknown and C is retracted: no point for either.
		"by-repo": {repoA.String(): 0.1},
		// A window of day 3 alone is unknown: the scorecard has no value.
		"day-three": {},
	}
	if len(result.Charts) != len(want) {
		t.Fatalf("%d charts, want %d", len(result.Charts), len(want))
	}
	for id, wantPoints := range want {
		got := points[id]
		if len(got) != len(wantPoints) {
			t.Errorf("chart %s points = %v, want %v", id, got, wantPoints)
			continue
		}
		for x, y := range wantPoints {
			if diff := got[x] - y; diff > 1e-12 || diff < -1e-12 {
				t.Errorf("chart %s point %s = %v, want %v (all points %v)", id, x, got[x], y, got)
			}
		}
	}
}

// A report chart of revert rate draws only a measured rate. The writer stores
// none today (nothing detects a reverted pull request), so a day with merged
// pull requests has no point: it is not 0%, and the deprecated
// change_failure_rate column is not read for it. The second organization
// holds a stored rate, as a revert detector will write it: the chart can draw.
func TestRevertRateChartHasNoPointWhenNoRateIsMeasured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	computedAt := day.Add(26 * time.Hour)
	stored := 0.25
	for org, row := range map[string]repouser.RepoMetric{
		"org-unmeasured": {RepoID: uuid.New(), Day: day, CommitsCount: 1, PRsMerged: 5, ChangeFailureRate: 0.4, ComputedAt: computedAt},
		"org-measured":   {RepoID: uuid.New(), Day: day, CommitsCount: 1, PRsMerged: 4, RevertRate: &stored, ComputedAt: computedAt},
	} {
		if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{row}}, org); err != nil {
			t.Fatal(err)
		}
	}
	points := func(org string) map[string][]DataPoint {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			chart := func(id, chartType, groupBy string) ChartSpec {
				return ChartSpec{
					ChartID: id, PlanID: "plan-1", ChartType: chartType, Metric: "revert_rate", GroupBy: groupBy,
					TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-01", OrganizationID: org,
				}
			}
			return ReportDefinition{
				Plan:   Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{chart("by-day", "line", "day"), chart("total", "scorecard", ""), chart("by-repo", "bar", "repo")},
			}, nil
		})
		adapter, err := NewClickHouseQueryAdapter(loader, conn)
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
		if err != nil {
			t.Fatal(err)
		}
		out := map[string][]DataPoint{}
		for _, chart := range result.Charts {
			out[chart.Spec.ChartID] = chart.DataPoints
		}
		if len(out) != 3 {
			t.Fatalf("%d charts, want 3", len(out))
		}
		return out
	}
	for id, got := range points("org-unmeasured") {
		if len(got) != 0 {
			t.Errorf("chart %s of a day with 5 merged pull requests and no measured revert rate = %+v, want no point", id, got)
		}
	}
	for id, got := range points("org-measured") {
		if len(got) != 1 || got[0].Y != 0.25 {
			t.Errorf("chart %s of a stored revert rate = %+v, want one point at 0.25", id, got)
		}
	}
}
