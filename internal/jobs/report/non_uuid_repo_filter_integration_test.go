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

// A non-UUID repository filter on a non-pooled chart must match no rows and
// leave the run successful (CHAOS-14130); repo_id is UUID on the real chain.
func TestNonPooledChartNonUUIDRepoFilterMatchesNoRows(t *testing.T) {
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
	chschema.CreateTables(ctx, t, conn, "testops_pipeline_metrics_daily")
	if err := conn.Exec(ctx, `
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, success_count, failure_count, computed_at) VALUES
('org-1', '`+reportRepoID+`', '2026-01-01', 91, 9, '2026-01-01 01:00:00')`); err != nil {
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
		t.Fatalf("WANT run succeeds with no rows; run failed: %v", err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 0 {
		t.Fatalf("WANT no data points for an unknown repository; got %#v", result.Charts)
	}
}

// A valid UUID beside a non-UUID value serves the valid repository rows unchanged (CHAOS-14130).
func TestNonPooledChartMixedRepoFilterServesValidRepoRowsUnchanged(t *testing.T) {
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
	chschema.CreateTables(ctx, t, conn, "testops_pipeline_metrics_daily")
	if err := conn.Exec(ctx, `
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, success_count, failure_count, computed_at) VALUES
('org-1', '`+reportRepoID+`', '2026-01-01', 91, 9, '2026-01-01 01:00:00')`); err != nil {
		t.Fatal(err)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "success_rate", GroupBy: "day", FilterRepos: []string{reportRepoID, "repo-a"},
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
		t.Fatalf("WANT run succeeds; run failed: %v", err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 || result.Charts[0].DataPoints[0].Y != 0.91 {
		t.Fatalf("WANT one data point y=0.91 for the valid repository; got %#v", result.Charts)
	}
}
