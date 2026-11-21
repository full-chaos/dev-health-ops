//go:build integration

package report

import (
	"context"
	"fmt"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The report chart (one-team form) of a computed_at tie
// of two versions of ONE key, against the version the served page picks
// (teamkeytables.TeamMetricsDailyNewestOrder). Want: equal on every pair.
func TestReportChartPicksThePageVersionOnATeamMetricsTie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE team_metrics_daily (
    day Date, team_id LowCardinality(String), team_name String,
    commits_count UInt32, after_hours_commits_count UInt32, weekend_commits_count UInt32,
    after_hours_commit_ratio Float64, weekend_commit_ratio Float64,
    computed_at DateTime64(6, 'UTC'), org_id String, repo_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, team_id, day)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES team_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	insert := func(commits, afterHours, weekend int, ratio, wratio float64) string {
		return fmt.Sprintf(`INSERT INTO team_metrics_daily (day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count, after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
VALUES ('2026-01-01', 'core', 'Core', %d, %d, %d, %g, %g, '2026-01-02 00:00:00.000000', 'org-1', 'repo-a')`, commits, afterHours, weekend, ratio, wratio)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: "after_hours_commit_ratio", GroupBy: "day",
				FilterTeams: []string{"core"}, TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-01", OrganizationID: "org-1"}},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	disagree := 0
	for k := 0; k < 12; k++ {
		for order := 0; order < 2; order++ {
			versions := []string{insert(10+k, 1, 2, 0.1, 0.2), insert(20+k, 15, 3, 0.75, 0.15)}
			if order == 1 {
				versions[0], versions[1] = versions[1], versions[0]
			}
			if err := conn.Exec(ctx, "TRUNCATE TABLE team_metrics_daily"); err != nil {
				t.Fatal(err)
			}
			for _, v := range versions {
				if err := conn.Exec(ctx, v); err != nil {
					t.Fatal(err)
				}
			}
			result, err := adapter.Query(ctx, QueryInput{ReportID: "r", RunID: "run"})
			if err != nil {
				t.Fatal(err)
			}
			y := result.Charts[0].DataPoints[0].Y
			var after, commits float64
			rows, err := conn.Query(ctx, `SELECT toFloat64(p.2), toFloat64(p.1) FROM (SELECT argMax(`+teamkeytables.TeamMetricsDailyPick+`, `+teamkeytables.TeamMetricsDailyNewestOrder+`) AS p FROM team_metrics_daily) `)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				if err := rows.Scan(&after, &commits); err != nil {
					t.Fatal(err)
				}
			}
			rows.Close()
			want := after / commits
			if y < want-1e-9 || y > want+1e-9 {
				disagree++
				t.Logf("k=%d order=%d: report %.9f, page version %.9f", k, order, y, want)
			}
		}
	}
	if disagree > 0 {
		t.Errorf("the report chart serves another version than the page on %d of 24 ties", disagree)
	}
}
