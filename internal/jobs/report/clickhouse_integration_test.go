//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestClickHouseQueryAdapterExecutesBoundedPythonParityQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE cicd_metrics_daily (
	org_id String,
	repo_id String,
	day Date,
	success_rate Nullable(Float64)
) ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO cicd_metrics_daily VALUES
('org-1', 'repo-a', '2026-01-01', 0.91),
('org-1', 'repo-a', '2026-01-07', 0.95),
('other-org', 'repo-a', '2026-01-07', 0.10)`); err != nil {
		t.Fatal(err)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "success_rate", GroupBy: "day", FilterRepos: []string{"repo-a"},
				TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07",
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
		t.Fatal(err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 2 ||
		result.Charts[0].DataPoints[0].Y != 0.91 || result.Charts[0].DataPoints[1].Y != 0.95 {
		t.Fatalf("chart result = %#v", result.Charts)
	}
}

type reportLoaderFunc func(context.Context, QueryInput) (ReportDefinition, error)

func (function reportLoaderFunc) Load(ctx context.Context, input QueryInput) (ReportDefinition, error) {
	return function(ctx, input)
}
