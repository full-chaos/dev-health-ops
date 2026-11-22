package report

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// A chart of the cancel or rerun rate is the bucket's cancelled runs (reruns) over all its runs, from the newest counts
// of each repository and day: never an average of the stored per-repository-day rates.
func TestCancelAndRerunRateChartsAreTheRunWeightedRuleOverTheCounts(t *testing.T) {
	t.Parallel()
	const source = "FROM (SELECT * FROM testops_pipeline_metrics_daily ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC LIMIT 1 BY org_id, repo_id, day) AS testops_pipeline_metrics_daily"
	for metric, expression := range map[string]string{
		"cancel_rate": testops.PipelineCancelRateWindowSQL,
		"rerun_rate":  testops.PipelineRerunRateWindowSQL,
	} {
		registered, ok := supportedMetrics[metric]
		if !ok || registered.SourceTable != "testops_pipeline_metrics_daily" {
			t.Fatalf("fixture drift: %s is not a testops_pipeline_metrics_daily registry metric (got %+v)", metric, registered)
		}
		definition := withChartRule(registered)
		if definition.SourceTable != "testops_pipeline_metrics_daily" || definition.rule != expression {
			t.Fatalf("%s chart definition = %+v, want the run-weighted rule", metric, definition)
		}
		rule := strings.Join(strings.Fields(expression), " ")
		for _, shape := range []struct{ chartType, groupBy string }{{"line", "day"}, {"bar", "repo"}, {"scorecard", ""}, {"line", "week"}} {
			query, _, err := buildChartQuery(ChartSpec{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: shape.chartType, Metric: metric, GroupBy: shape.groupBy,
				TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", OrganizationID: "org-1",
			}, definition)
			if err != nil {
				t.Fatal(err)
			}
			query = strings.Join(strings.Fields(query), " ")
			if !strings.Contains(query, rule+" AS y") || !strings.Contains(query, source) {
				t.Errorf("%s %s by %q does not apply the run-weighted rule to the newest counts:\n%s", metric, shape.chartType, shape.groupBy, query)
			}
			for _, banned := range []string{"avg(", metric + " IS NOT NULL", "sum(" + metric + ")"} {
				if strings.Contains(query, banned) {
					t.Errorf("%s %s by %q reads %q:\n%s", metric, shape.chartType, shape.groupBy, banned, query)
				}
			}
		}
	}
}
