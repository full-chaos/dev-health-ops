//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestClickHouseQueryAdapterChartsKeepEachRepositoryOfOneDay holds the report
// charts of issue_type_metrics_daily and investment_metrics_daily against
// three repository partitions of one day whose other key columns are equal:
// two repositories and the nil repository. The daily job computes a day one
// repository partition at a time (CHAOS-8813), so each partition has its own
// newest row and none of them replaces another.
//
//	repository A    10:00 100   11:05   1
//	repository B    10:00 200   11:06  20
//	nil repository  10:00 400   11:07 300
//
// Both charts are a mean: 107 is the newest row of each repository (1, 20,
// 300), 300 is a key with no repo_id, 170.17 is every row.
func TestClickHouseQueryAdapterChartsKeepEachRepositoryOfOneDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	const repoA = "11111111-1111-4111-8111-111111111111"
	const repoB = "22222222-2222-4222-8222-222222222222"
	const noRepo = "00000000-0000-0000-0000-000000000000"
	type version struct {
		repo       string
		value      int
		computedAt string
	}
	versions := []version{
		{repoA, 100, "2026-01-03 10:00:00"}, {repoA, 1, "2026-01-03 11:05:00"},
		{repoB, 200, "2026-01-03 10:00:00"}, {repoB, 20, "2026-01-03 11:06:00"},
		{noRepo, 400, "2026-01-03 10:00:00"}, {noRepo, 300, "2026-01-03 11:07:00"},
	}
	for _, row := range versions {
		if err := conn.Exec(ctx, `
INSERT INTO issue_type_metrics_daily
(repo_id, day, provider, team_id, issue_type_norm, created_count, completed_count, active_count,
 cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at, org_id)
VALUES (?, '2026-01-03', 'github', '', 'bug', 0, 1, 0, 0, 0, ?, ?, 'org-1')`, row.repo, row.value, row.computedAt); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, `
INSERT INTO investment_metrics_daily
(repo_id, day, team_id, investment_area, project_stream, delivery_units,
 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
VALUES (?, '2026-01-03', '', 'quality', 'general', 0, 0, 0, ?, 0, ?, 'org-1')`, row.repo, row.value, row.computedAt); err != nil {
			t.Fatal(err)
		}
	}
	chart := func(metric string) float64 {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
					Metric: metric, GroupBy: "day",
					TimeRangeStart: "2026-01-03", TimeRangeEnd: "2026-01-03",
					OrganizationID: "org-1",
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
		if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
			t.Fatalf("%s chart result = %#v, want one data point", metric, result.Charts)
		}
		return result.Charts[0].DataPoints[0].Y
	}
	if got := chart("lead_p50_hours"); got != 107 {
		t.Errorf("lead_p50_hours = %v, want 107: the mean of the newest row of each repository (1, 20, 300)", got)
	}
	if got := chart("churn_loc"); got != 107 {
		t.Errorf("churn_loc = %v, want 107: the mean of the newest row of each repository (1, 20, 300)", got)
	}
}
