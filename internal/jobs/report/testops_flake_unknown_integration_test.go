//go:build integration

package report

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A TestOps test repo-day with no measured case stores NULL in flake_rate,
// retry_dependency_rate and failure_recurrence_score: a day whose case rows are
// all of runs left out of the flake measures (unknown), and a day with no case
// row at all (a rate over 0 cases). A chart of the three reads such a row as no
// sample: the day's value is the measured repos' mean, and a day on which no
// repo measured a case has no point, never a 0.
func TestATestOpsRateChartLeavesOutARepoDayWithNoMeasuredCase(t *testing.T) {
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
		org      = "org-testops-unknown"
		measured = "2026-01-01"
		noneDay  = "2026-01-02"
		known    = "00000000-0000-4000-8000-0000000000a1"
		unknown  = "00000000-0000-4000-8000-0000000000a2"
		noCases  = "00000000-0000-4000-8000-0000000000a3"
		at       = "2026-01-03 10:00:00"
	)
	row := func(repo, day string, totalCases uint32, flake, retry, recurrence *float64, distinct uint32) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at,
     flake_cases, distinct_cases)
VALUES (toUUID(?), ?, ?, ?, 0, 0, 0, 1, 0, ?, ?, 1, ?, 'team-1', ?, ?, 0, ?)`,
			repo, day, totalCases, totalCases, flake, retry, recurrence, org, at, distinct); err != nil {
			t.Fatal(err)
		}
	}
	value := func(v float64) *float64 { return &v }
	row(known, measured, 10, value(0.2), value(0.1), value(0.5), 10)
	// Unknown: the day's cases are all of pull-request runs; suites still count.
	row(unknown, measured, 10, nil, nil, nil, 0)
	// A day with suites and no case row: nothing was measured.
	row(noCases, measured, 0, nil, nil, nil, 0)
	row(unknown, noneDay, 10, nil, nil, nil, 0)
	row(noCases, noneDay, 0, nil, nil, nil, 0)

	chart := func(metric string) map[string]float64 {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: metric, GroupBy: "day",
					TimeRangeStart: measured, TimeRangeEnd: noneDay, OrganizationID: org,
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
		for _, dataPoint := range result.Charts[0].DataPoints {
			points[dataPoint.X] = dataPoint.Y
		}
		return points
	}
	for metric, want := range map[string]float64{
		"flake_rate": 0.2, "retry_dependency_rate": 0.1, "failure_recurrence_score": 0.5,
	} {
		points := chart(metric)
		var got []float64
		for x, y := range points {
			if !strings.HasPrefix(x, measured) {
				t.Errorf("%s: a point on %q = %v, want none (no repo measured a case that day)", metric, x, y)
				continue
			}
			got = append(got, y)
		}
		if len(got) != 1 || math.Abs(got[0]-want) > 1e-9 {
			t.Errorf("%s: %v, want only %s = %v (the measured repo alone)", metric, points, measured, want)
		}
	}
}
