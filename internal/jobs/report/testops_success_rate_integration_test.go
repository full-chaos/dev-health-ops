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

// A repo-day with cancelled and running runs: the report's success rate is its successes over its completed runs
// (succeeded or failed), the value the Pipelines series gives for the same rows, never the stored success_rate over
// every run. A repo-day with no completed run has no point.
func TestSuccessRateChartIsTheCompletedRunRateOfThePipelinesSeries(t *testing.T) {
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
		org     = "org-success-rate"
		day     = "2026-01-01"
		noneDay = "2026-01-02"
		repo    = "00000000-0000-4000-8000-0000000000b1"
		at      = "2026-01-03 10:00:00"
	)
	row := func(day string, runs, success, failure, cancelled uint32, storedRate float64) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_pipeline_metrics_daily
    (repo_id, day, pipelines_count, success_count, failure_count, cancelled_count, success_rate, failure_rate, cancel_rate,
     rerun_rate, team_id, org_id, computed_at)
VALUES (toUUID(?), ?, ?, ?, ?, ?, ?, 0, 0, 0, 'team-1', ?, ?)`,
			repo, day, runs, success, failure, cancelled, storedRate, org, at); err != nil {
			t.Fatal(err)
		}
	}
	// 10 runs: 6 succeeded, 2 failed, 2 cancelled; running runs count in the 10 only. Stored rate over all runs: 0.6.
	row(day, 10, 6, 2, 2, 0.6)
	// 3 runs, none completed.
	row(noneDay, 3, 0, 0, 1, 0)

	var pipelines *float64
	if err := conn.QueryRow(ctx, `SELECT SUM(success_count) / NULLIF(SUM(success_count) + SUM(failure_count), 0)
FROM testops_pipeline_metrics_daily FINAL WHERE org_id = ? AND day = ?`, org, day).Scan(&pipelines); err != nil {
		t.Fatal(err)
	}
	if pipelines == nil || math.Abs(*pipelines-0.75) > 1e-9 {
		t.Fatalf("fixture drift: the Pipelines rate = %v, want 0.75", pipelines)
	}

	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: "success_rate", GroupBy: "day",
				TimeRangeStart: day, TimeRangeEnd: noneDay, OrganizationID: org,
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
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
		t.Fatalf("chart result = %#v, want one point (the day with no completed run has none)", result.Charts)
	}
	point := result.Charts[0].DataPoints[0]
	if point.X != day || math.Abs(point.Y-*pipelines) > 1e-9 {
		t.Fatalf("success_rate point = %+v, want %s = %v (successes over completed runs)", point, day, *pipelines)
	}
}
