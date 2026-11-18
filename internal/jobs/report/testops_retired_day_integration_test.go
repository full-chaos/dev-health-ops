//go:build integration

package report

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A day whose newest testops_test_metrics_daily row retires it (retracted = 1, as the daily writer stores it over a day that
// counts no kept suite any more) has no point in a TestOps test chart: not its older value, and never the 0 of the marker row.
func TestATestOpsChartHasNoPointOnARetiredDay(t *testing.T) {
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
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES testops_test_metrics_daily"); err != nil {
		t.Fatal(err)
	}

	const (
		org     = "org-testops-retired"
		retired = "2026-01-01"
		live    = "2026-01-02"
		repo    = "13928000-0000-4000-8000-0000000000d1"
	)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
  flake_rate, total_suites, flake_cases, distinct_cases, team_id, org_id, computed_at)
VALUES (toUUID(?), ?, 4, 2, 2, 0.5, 0.5, 0.5, 1, 1, 2, 'team-1', ?, '2026-01-03 10:00:00')`, repo, retired, org)
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, team_id, org_id, computed_at, retracted)
VALUES (toUUID(?), ?, 'team-1', ?, '2026-01-03 11:00:00', 1)`, repo, retired, org)
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
  flake_rate, total_suites, flake_cases, distinct_cases, team_id, org_id, computed_at)
VALUES (toUUID(?), ?, 2, 2, 0, 1, 0, 0.25, 1, 1, 4, 'team-1', ?, '2026-01-03 10:00:00')`, repo, live, org)

	for metric, want := range map[string]float64{"pass_rate": 1, "flake_rate": 0.25} {
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: metric, GroupBy: "day",
					TimeRangeStart: retired, TimeRangeEnd: live, OrganizationID: org,
				}},
			}, nil
		})
		adapter, err := NewClickHouseQueryAdapter(loader, conn)
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
		if err != nil {
			t.Fatalf("%s: %v", metric, err)
		}
		if len(result.Charts) != 1 {
			t.Fatalf("%s: %d chart(s)", metric, len(result.Charts))
		}
		points := map[string]float64{}
		for _, point := range result.Charts[0].DataPoints {
			points[point.X] = point.Y
		}
		var onLive []float64
		for x, y := range points {
			if strings.HasPrefix(x, retired) {
				t.Errorf("%s: a point on the retired day %q = %v, want none (never the marker's 0 or the older value)", metric, x, y)
				continue
			}
			onLive = append(onLive, y)
		}
		if len(onLive) != 1 || onLive[0] != want {
			t.Errorf("%s: %v, want only %s = %v", metric, points, live, want)
		}
	}
}
