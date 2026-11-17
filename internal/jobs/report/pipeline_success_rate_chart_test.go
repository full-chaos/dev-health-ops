package report

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// A chart of the success rate is the bucket's successes over its completed runs (succeeded or failed), from the
// newest counts of each repository and day: never the stored success_rate of cicd_metrics_daily, which divides by
// every run (cancelled, skipped and running included) and holds 0 on a day with no run counted. It is the rule
// Pipelines, Home and Risk read.
func TestSuccessRateChartIsTheCompletedRunRuleOverTheCounts(t *testing.T) {
	t.Parallel()
	registered, ok := supportedMetrics["success_rate"]
	if !ok || registered.SourceTable != "cicd_metrics_daily" {
		t.Fatalf("fixture drift: success_rate is not a cicd_metrics_daily registry metric (got %+v)", registered)
	}
	definition := withChartRule(registered)
	if definition.SourceTable != "testops_pipeline_metrics_daily" || definition.rule != testops.PipelineSuccessRateWindowSQL {
		t.Fatalf("success_rate chart definition = %+v, want the completed-run rule on the TestOps pipeline counts", definition)
	}
	const source = "FROM (SELECT * FROM testops_pipeline_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_pipeline_metrics_daily"
	rule := strings.Join(strings.Fields(testops.PipelineSuccessRateWindowSQL), " ")
	for _, shape := range []struct{ chartType, groupBy string }{{"line", "day"}, {"bar", "repo"}, {"scorecard", ""}, {"line", "week"}} {
		query, _, err := buildChartQuery(ChartSpec{
			ChartID: "chart-1", PlanID: "plan-1", ChartType: shape.chartType, Metric: "success_rate", GroupBy: shape.groupBy,
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", OrganizationID: "org-1",
		}, definition)
		if err != nil {
			t.Fatal(err)
		}
		query = strings.Join(strings.Fields(query), " ")
		if !strings.Contains(query, rule+" AS y") || !strings.Contains(query, source) {
			t.Errorf("%s by %q does not apply the completed-run rule to the newest counts:\n%s", shape.chartType, shape.groupBy, query)
		}
		for _, banned := range []string{"avg(", "success_rate IS NOT NULL", "sum(success_rate", "cicd_metrics_daily"} {
			if strings.Contains(query, banned) {
				t.Errorf("%s by %q reads %q:\n%s", shape.chartType, shape.groupBy, banned, query)
			}
		}
	}
	// The other CI/CD columns of cicd_metrics_daily keep their own table.
	if got := withChartRule(supportedMetrics["pipelines_count"]); got.rule != "" || got.SourceTable != "cicd_metrics_daily" {
		t.Errorf("pipelines_count got a chart rule: %+v", got)
	}
}
