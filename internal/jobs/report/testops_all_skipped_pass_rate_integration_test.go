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

// A pass rate chart bucket is its passed cases over its passed and failed cases. A repo-day where every case was
// skipped stores pass_rate 0; the chart never reads that column, so such a day adds nothing to its bucket, a bucket
// with no passed or failed case has no point, and a newest all-skipped row is not replaced by an older measured row.
func TestReportPassRateChartLeavesOutAnAllSkippedDay(t *testing.T) {
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
		org        = "org-testops-all-skipped"
		allSkipped = "2026-01-01"
		mixed      = "2026-01-02"
		recomputed = "2026-01-03"
		repoA      = "00000000-0000-4000-8000-0000000000d1"
		repoB      = "00000000-0000-4000-8000-0000000000d2"
		older      = "2026-01-05 10:00:00"
		newer      = "2026-01-05 11:00:00"
	)
	// storedRate is the writer's passed / total_cases; the chart must not read it.
	row := func(repo, day string, passed, failed, skipped uint32, storedRate float64, computedAt string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count, pass_rate, failure_rate,
     flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, org_id, computed_at,
     flake_cases, distinct_cases)
VALUES (toUUID(?), ?, ?, ?, ?, ?, 0, ?, 0, 0, 0, 1, 0, 'team-1', ?, ?, 0, ?)`,
			repo, day, passed+failed+skipped, passed, failed, skipped, storedRate, org, computedAt, passed+failed+skipped); err != nil {
			t.Fatal(err)
		}
	}
	row(repoA, allSkipped, 0, 0, 2, 0, older)
	row(repoA, mixed, 3, 1, 4, 0.375, older)
	row(repoB, mixed, 1, 1, 0, 0.5, older)
	row(repoA, recomputed, 4, 0, 0, 1, older)
	row(repoA, recomputed, 0, 0, 3, 0, newer)

	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: org},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: "pass_rate", GroupBy: "day",
				TimeRangeStart: allSkipped, TimeRangeEnd: recomputed, OrganizationID: org,
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
		t.Fatalf("%d chart(s), want 1", len(result.Charts))
	}
	points := map[string]float64{}
	for _, dataPoint := range result.Charts[0].DataPoints {
		points[dataPoint.X[:len(mixed)]] = dataPoint.Y
	}
	t.Logf("pass_rate points=%v", points)
	want := 4.0 / 6.0
	if len(points) != 1 || math.Abs(points[mixed]-want) > 1e-9 {
		t.Errorf("pass_rate points %v, want only %s = %.4f (4 passed of 6 passed or failed; no point on the all-skipped days %s and %s)",
			points, mixed, want, allSkipped, recomputed)
	}
}
