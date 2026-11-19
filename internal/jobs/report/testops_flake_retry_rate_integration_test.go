//go:build integration

package report

import (
	"context"
	"math"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The flake rate and retry dependency rate of a window are the sums of the flake (retry-dependent) cases over the sums of
// the distinct cases of its repository-days, never the mean of their stored rates. A repository-day with a NULL rate or
// NULL counts is in neither sum. A window with no measured repository-day has no point, not 0 and not NaN.
func TestFlakeAndRetryRateChartsAreCaseWeightedOverRepositoryDays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	}()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const (
		org = "org-flake-retry"
		at  = "2026-03-10 10:00:00"
	)
	repo := func(n string) string { return "00000000-0000-4000-8000-0000000000" + n }
	row := func(repoID, day string, flake, retry *float64, flakeCases, distinct *uint32) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at,
     flake_cases, distinct_cases)
VALUES (toUUID(?), ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, 1, 0, 'team-1', ?, ?, ?, ?)`,
			repoID, day, flake, retry, org, at, flakeCases, distinct); err != nil {
			t.Fatal(err)
		}
	}
	f := func(v float64) *float64 { return &v }
	u := func(v uint32) *uint32 { return &v }

	// Two repository-days of 1 and 99 distinct cases: the stored mean is 0.5, the weighted value 0.01.
	row(repo("a1"), "2026-01-01", f(1), f(1), u(1), u(1))
	row(repo("b1"), "2026-01-01", f(0), f(0), u(0), u(99))
	// A measured day beside rows that must drop out: NULL rate, NULL distinct_cases, NULL flake_cases.
	row(repo("a1"), "2026-02-01", f(0.5), f(0.25), u(5), u(10))
	row(repo("b1"), "2026-02-01", nil, nil, u(0), u(0))
	row(repo("c1"), "2026-02-01", f(1), f(1), u(7), nil)
	row(repo("d1"), "2026-02-01", f(1), f(0.5), nil, u(40))
	// Only rows that drop out: no point.
	row(repo("a1"), "2026-03-01", nil, nil, u(0), u(0))
	row(repo("b1"), "2026-03-01", f(0.5), f(0.5), u(3), nil)

	point := func(metric, from, to string) (float64, bool) {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "scorecard", Metric: metric,
					TimeRangeStart: from, TimeRangeEnd: to, OrganizationID: org,
				}},
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
		if len(result.Charts) != 1 {
			t.Fatalf("%s %s..%s: charts = %#v", metric, from, to, result.Charts)
		}
		points := result.Charts[0].DataPoints
		if len(points) == 0 {
			return 0, false
		}
		if len(points) != 1 {
			t.Fatalf("%s %s..%s: points = %#v, want at most one", metric, from, to, points)
		}
		return points[0].Y, true
	}
	for _, c := range []struct {
		name, metric, from, to string
		want                   float64
	}{
		{"flake, 1 and 99 cases (stored mean 0.5)", "flake_rate", "2026-01-01", "2026-01-01", 0.01},
		{"retry, 1 and 99 cases (stored mean 0.5)", "retry_dependency_rate", "2026-01-01", "2026-01-01", 0.01},
		{"flake, NULL rows drop out", "flake_rate", "2026-02-01", "2026-02-01", 0.5},
		{"retry, NULL rate and NULL distinct drop out, NULL flake_cases does not", "retry_dependency_rate", "2026-02-01", "2026-02-01", (2.5 + 20) / 50},
	} {
		got, ok := point(c.metric, c.from, c.to)
		if !ok || math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: got %v (point %v), want %v", c.name, got, ok, c.want)
		}
	}
	for _, metric := range []string{"flake_rate", "retry_dependency_rate"} {
		for _, window := range [][2]string{{"2026-04-01", "2026-04-07"}, {"2026-03-01", "2026-03-01"}} {
			if got, ok := point(metric, window[0], window[1]); ok {
				t.Errorf("%s %s..%s: point %v, want none", metric, window[0], window[1], got)
			}
		}
	}
}
