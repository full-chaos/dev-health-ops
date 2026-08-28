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
	success_rate Nullable(Float64),
	computed_at DateTime('UTC')
) ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`); err != nil {
		t.Fatal(err)
	}
	// computed_at is required: CHAOS-4246 wraps this table in a
	// latest-generation-by-computed_at dedup subquery (dedupFromSource).
	if err := conn.Exec(ctx, `
INSERT INTO cicd_metrics_daily VALUES
('org-1', 'repo-a', '2026-01-01', 0.91, '2026-01-01 01:00:00'),
('org-1', 'repo-a', '2026-01-07', 0.95, '2026-01-07 01:00:00'),
('other-org', 'repo-a', '2026-01-07', 0.10, '2026-01-07 01:00:00')`); err != nil {
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

// TestClickHouseQueryAdapterDedupsDoraMetricsDailyOnPartitionRetry is the
// CHAOS-4140 DB-backed proof (real ClickHouse, not a query-string assertion):
// a DORA partition retry writes a fresh computed_at generation for the same
// (org_id, repo_id, day, metric_name) key without deleting the prior one
// (job_dora.py / dora_native.go, by design). Before this change,
// buildChartQuery read dora_metrics_daily raw and averaged both generations
// together; this test seeds exactly that shape -- two generations of one key,
// 5.0 then a retry's 8.0 -- and asserts the chart reads ONLY the latest
// generation (8.0), not avg(5.0, 8.0)=6.5. It also asserts the wired dedup
// guard observer sees the guard actually discard the stale generation
// (observed=2, skipped=1), proving the telemetry measures the real shape, not
// a call count.
func TestClickHouseQueryAdapterDedupsDoraMetricsDailyOnPartitionRetry(t *testing.T) {
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
	// Schema mirrors migrations 023b (base table) + 024 (org_id) exactly.
	if err := conn.Exec(ctx, `
CREATE TABLE dora_metrics_daily (
	repo_id UUID,
	day Date,
	metric_name String,
	value Float64,
	computed_at DateTime('UTC'),
	org_id String DEFAULT 'default'
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, metric_name)`); err != nil {
		t.Fatal(err)
	}
	const repoID = "11111111-1111-1111-1111-111111111111"
	// Two generations of the SAME (org_id, repo_id, day, metric_name) key:
	// the original partition run (value=5, computed_at=t1), then a retry
	// (value=8, computed_at=t2, strictly later) that neither deleted nor
	// replaced the first row -- job_dora.py's write shape exactly.
	if err := conn.Exec(ctx, `
INSERT INTO dora_metrics_daily (repo_id, day, metric_name, value, computed_at, org_id) VALUES
('`+repoID+`', '2026-01-03', 'deployment_frequency', 5.0, '2026-01-03 01:00:00', 'org-1'),
('`+repoID+`', '2026-01-03', 'deployment_frequency', 8.0, '2026-01-03 02:00:00', 'org-1')`); err != nil {
		t.Fatal(err)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "value", GroupBy: "day", FilterRepos: []string{repoID},
				TimeRangeStart: "2026-01-03", TimeRangeEnd: "2026-01-03",
				OrganizationID: "org-1",
			}},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	observer := &fakeDedupGuardObserver{}
	adapter.SetDedupObserver(observer)

	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
		t.Fatalf("chart result = %#v", result.Charts)
	}
	got := result.Charts[0].DataPoints[0].Y
	if got != 8.0 {
		t.Fatalf("dora chart y = %v, want 8 (latest generation only, not avg(5,8)=6.5 -- the pre-fix defect)", got)
	}

	if len(observer.calls) != 1 {
		t.Fatalf("dedup guard observer calls = %#v, want exactly 1", observer.calls)
	}
	call := observer.calls[0]
	if call.table != "dora_metrics_daily" || call.observed != 2 || call.skipped != 1 {
		t.Fatalf("dedup guard observation = %#v, want {dora_metrics_daily 2 1}", call)
	}
}

type dedupGuardCall struct {
	table, reason     string
	observed, skipped int
}

type fakeDedupGuardObserver struct {
	calls []dedupGuardCall
}

func (observer *fakeDedupGuardObserver) ObserveReportDedupGuard(table, reason string, observedRows, skippedRows int) error {
	observer.calls = append(observer.calls, dedupGuardCall{table: table, reason: reason, observed: observedRows, skipped: skippedRows})
	return nil
}

type reportLoaderFunc func(context.Context, QueryInput) (ReportDefinition, error)

func (function reportLoaderFunc) Load(ctx context.Context, input QueryInput) (ReportDefinition, error) {
	return function(ctx, input)
}
