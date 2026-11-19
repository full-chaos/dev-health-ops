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

// The cancel and rerun rate of a window are the sum of the cancelled runs (reruns) over the sum of the runs of its
// repository-days, never the mean of their stored rates. rerun_rate is a non-Nullable Float64 column (chmigrate
// baseline head.json), so no row reads NULL there. A window with no run has no point, not 0 and not NaN.
func TestCancelAndRerunRateChartsAreRunWeightedOverRepositoryDays(t *testing.T) {
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
		org = "org-cancel-rerun"
		day = "2026-01-01"
		at  = "2026-01-05 10:00:00"
	)
	repos := map[string]string{
		"a1": "00000000-0000-4000-8000-0000000000a1", "b1": "00000000-0000-4000-8000-0000000000b1",
		"c1": "00000000-0000-4000-8000-0000000000c1", "d1": "00000000-0000-4000-8000-0000000000d1",
		"e1": "00000000-0000-4000-8000-0000000000e1", "f1": "00000000-0000-4000-8000-0000000000f1",
	}
	row := func(repo, day string, runs, cancelled uint32, storedCancel, storedRerun float64, team string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_pipeline_metrics_daily
    (repo_id, day, pipelines_count, success_count, failure_count, cancelled_count, success_rate, failure_rate, cancel_rate,
     rerun_rate, team_id, org_id, computed_at)
VALUES (toUUID(?), ?, ?, 0, 0, ?, 0, 0, ?, ?, ?, ?, ?)`,
			repos[repo], day, runs, cancelled, storedCancel, storedRerun, team, org, at); err != nil {
			t.Fatal(err)
		}
	}
	// The 14004 fixture: runs 5, 4, 2, 4; cancelled 1, 0, 0, 2; reruns 1, 1, 0, 0.
	row("a1", day, 5, 1, 0.2, 0.2, "team-1")
	row("b1", day, 4, 0, 0, 0.25, "team-1")
	row("c1", day, 2, 0, 0, 0, "team-3")
	row("d1", day, 4, 2, 0.5, 0, "team-3")
	// Two repository-days of 1 and 99 runs: the mean of the stored rates is 0.5, the weighted rate 0.01.
	row("e1", "2026-02-01", 1, 1, 1, 1, "team-4")
	row("f1", "2026-02-01", 99, 0, 0, 0, "team-4")
	// A day with a row but no run.
	row("a1", "2026-03-01", 0, 0, 0, 0, "team-1")

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
		{"org cancel (stored mean 0.175)", "cancel_rate", day, day, 3.0 / 15},
		{"org rerun (stored mean 0.1125)", "rerun_rate", day, day, 2.0 / 15},
		{"1 run and 99 runs cancel (stored mean 0.5)", "cancel_rate", "2026-02-01", "2026-02-01", 0.01},
		{"1 run and 99 runs rerun (stored mean 0.5)", "rerun_rate", "2026-02-01", "2026-02-01", 0.01},
	} {
		got, ok := point(c.metric, c.from, c.to)
		if !ok || math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: got %v (point %v), want %v", c.name, got, ok, c.want)
		}
	}
	// The rerun value is the query-api term SUM(rerun_rate * pipelines_count) / SUM(pipelines_count) on the same rows.
	var terms *float64
	if err := conn.QueryRow(ctx, `SELECT SUM(rerun_rate * pipelines_count) / NULLIF(SUM(pipelines_count), 0)
FROM testops_pipeline_metrics_daily FINAL WHERE org_id = ? AND day = ?`, org, day).Scan(&terms); err != nil {
		t.Fatal(err)
	}
	if got, _ := point("rerun_rate", day, day); terms == nil || math.Abs(got-*terms) > 1e-9 {
		t.Errorf("rerun_rate chart = %v, query-api terms = %v", got, *terms)
	}
	// No run: a window with no row, and a window whose only row has no run, give no point.
	for _, metric := range []string{"cancel_rate", "rerun_rate"} {
		for _, window := range [][2]string{{"2026-04-01", "2026-04-07"}, {"2026-03-01", "2026-03-01"}} {
			if got, ok := point(metric, window[0], window[1]); ok {
				t.Errorf("%s %s..%s: point %v, want none", metric, window[0], window[1], got)
			}
		}
	}
}
