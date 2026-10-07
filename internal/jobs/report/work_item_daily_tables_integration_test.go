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

// TestClickHouseQueryAdapterChartsLeadTimeByNewestRowAndSample holds the
// report chart of lead_p50_hours (issue_type_metrics_daily) against the rows the daily
// family work_item_issue_type appends, on the schema of the migration chain.
//
// The store holds, for one day:
//
//	key (repo, github, no team, bug)
//	  10:00  1 completed, lead p50 34 h
//	key (repo, github, team-x, bug)
//	  10:00  1 completed, lead p50 20 h   the sync
//	  11:05  0 completed, lead p50 0 h    the daily job: the completion left this key
//
// One item is completed, with a lead time of 34 hours. A raw read gives a
// lead time of 18 (the mean of 34, 20 and 0).
func TestClickHouseQueryAdapterChartsLeadTimeByNewestRowAndSample(t *testing.T) {
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

	const repoID = "11111111-1111-1111-1111-111111111111"
	if err := conn.Exec(ctx, `
INSERT INTO issue_type_metrics_daily
(repo_id, day, provider, team_id, issue_type_norm, created_count, completed_count, active_count,
 cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at, org_id) VALUES
('`+repoID+`', '2026-01-03', 'github', '',       'bug', 0, 1, 0, 0, 0, 34, '2026-01-03 10:00:00', 'org-1'),
('`+repoID+`', '2026-01-03', 'github', 'team-x', 'bug', 0, 1, 0, 0, 0, 20, '2026-01-03 10:00:00', 'org-1'),
('`+repoID+`', '2026-01-03', 'github', 'team-x', 'bug', 0, 0, 0, 0, 0, 0,  '2026-01-03 11:05:00', 'org-1')`); err != nil {
		t.Fatal(err)
	}
	chart := func(metric string) float64 {
		t.Helper()
		loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
			return ReportDefinition{
				Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
				Charts: []ChartSpec{{
					ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
					Metric: metric, GroupBy: "day", FilterRepos: []string{repoID},
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
	if got := chart("lead_p50_hours"); got != 34 {
		t.Errorf("lead_p50_hours = %v, want 34: the one key with a completed item. "+
			"17 counts the row of zeros as a measured 0; 27 counts the older row of the key that lost its completion; "+
			"18 is every row", got)
	}
}
