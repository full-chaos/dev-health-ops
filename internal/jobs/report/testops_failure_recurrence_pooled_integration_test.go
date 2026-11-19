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

// The failure recurrence score of a window is the sum of the recurrent failed names over the sum of the distinct failed
// names of its repository-days, never the mean of their stored scores. A repository-day without both counts (a row
// stored before the counts existed) is in neither sum. A window whose denominator sum is 0 (no row, only rows without
// counts, or only days with no failed name) has no point, not 0 and not NaN.
func TestFailureRecurrenceChartIsPooledOverRepositoryDays(t *testing.T) {
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
		org = "org-failure-recurrence"
		at  = "2026-03-10 10:00:00"
	)
	repo := func(n string) string { return "00000000-0000-4000-8000-0000000000" + n }
	row := func(repoID, day string, score *float64, recurrent, distinct *uint32) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at,
     flake_cases, distinct_cases, recurrent_failed_names, distinct_failed_names)
VALUES (toUUID(?), ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, ?, 'team-1', ?, ?, 0, 1, ?, ?)`,
			repoID, day, score, org, at, recurrent, distinct); err != nil {
			t.Fatal(err)
		}
	}
	f := func(v float64) *float64 { return &v }
	u := func(v uint32) *uint32 { return &v }

	// Two repository-days of 1 and 50 distinct failed names: the stored mean is 0.5, the pooled value 1/51.
	row(repo("a1"), "2026-01-01", f(1), u(1), u(1))
	row(repo("b1"), "2026-01-01", f(0), u(0), u(50))
	// A counted day beside rows that must drop out: a legacy row (no counts), a row with one count NULL.
	row(repo("a1"), "2026-02-01", f(0.5), u(2), u(4))
	row(repo("b1"), "2026-02-01", f(1), nil, nil)
	row(repo("c1"), "2026-02-01", f(1), nil, u(5))
	// Only legacy rows: no point.
	row(repo("a1"), "2026-03-01", f(0.25), nil, nil)
	row(repo("b1"), "2026-03-01", f(1), nil, nil)
	// Only days with no failed name (counts 0 and 0, stored score 0): no point.
	row(repo("a1"), "2026-03-02", f(0), u(0), u(0))

	point := func(from, to string) (float64, bool) {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "scorecard", Metric: "failure_recurrence_score",
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
			t.Fatalf("%s..%s: charts = %#v", from, to, result.Charts)
		}
		points := result.Charts[0].DataPoints
		if len(points) == 0 {
			return 0, false
		}
		if len(points) != 1 {
			t.Fatalf("%s..%s: points = %#v, want at most one", from, to, points)
		}
		return points[0].Y, true
	}
	for _, c := range []struct {
		name, from, to string
		want           float64
	}{
		{"1 and 50 failed names (stored mean 0.5)", "2026-01-01", "2026-01-01", 1.0 / 51},
		{"legacy row and half-counted row drop out (stored mean 0.833)", "2026-02-01", "2026-02-01", 0.5},
		{"whole window pools every counted day", "2026-01-01", "2026-03-31", 3.0 / 55},
	} {
		got, ok := point(c.from, c.to)
		if !ok || math.Abs(got-c.want) > 1e-9 {
			t.Errorf("FAIL %s: got %v (point %v), want %v", c.name, got, ok, c.want)
		}
	}
	for _, c := range []struct{ name, from, to string }{
		{"no row", "2026-04-01", "2026-04-07"},
		{"only legacy rows", "2026-03-01", "2026-03-01"},
		{"only days with no failed name", "2026-03-02", "2026-03-02"},
	} {
		if got, ok := point(c.from, c.to); ok {
			t.Errorf("FAIL %s %s..%s: point %v, want none", c.name, c.from, c.to, got)
		}
	}
}
