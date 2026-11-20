//go:build integration

package report

import (
	"testing"
	"time"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL. A chart grouped by repository reads issue_type_metrics_daily.repo_id,
// a Nullable(UUID): the row with no repository is the v2.49 empty x.
func TestChartGroupedByANullableColumnReadsNullAsEmpty(t *testing.T) {
	e := startPooledEnv(t)
	const org = "00000000-0000-4000-8000-000000014331"
	const repo = "00000000-0000-4000-8000-000000014332"
	e.exec(`INSERT INTO issue_type_metrics_daily (org_id, repo_id, day, created_count, computed_at) VALUES (?, ?, '2026-02-10', 2, '2026-02-11 00:00:00')`, org, repo)
	e.exec(`INSERT INTO issue_type_metrics_daily (org_id, repo_id, day, created_count, computed_at) VALUES (?, NULL, '2026-02-10', 3, '2026-02-11 00:00:00')`, org)
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: "bar", Metric: "created_count", GroupBy: "repo", OrganizationID: org,
		TimeRangeStart: "2026-02-01", TimeRangeEnd: "2026-02-28"})
	if err != nil {
		t.Fatal(err)
	}
	points := pointsOf(result)
	if _, ok := points[""]; !ok || len(points) != 2 {
		t.Fatalf("points = %v, want the repository and the empty x of the row with no repository", points)
	}
}
