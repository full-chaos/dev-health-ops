package report

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// A chart of the pipeline failure rate is the bucket's failures over its completed runs, from the newest counts of
// each repository and day: never an average of the stored failure_rate column, which holds 0 on a day with no
// completed run.
func TestPipelineFailureRateChartIsTheCompletedRunRuleOverTheCounts(t *testing.T) {
	t.Parallel()
	registered, ok := supportedMetrics["failure_rate"]
	if !ok || registered.SourceTable != "testops_pipeline_metrics_daily" {
		t.Fatalf("fixture drift: failure_rate is not a testops_pipeline_metrics_daily registry metric (got %+v)", registered)
	}
	definition := withChartRule(registered)
	if definition.SourceTable != "testops_pipeline_metrics_daily" || definition.rule != testops.PipelineFailureRateWindowSQL {
		t.Fatalf("failure_rate chart definition = %+v, want the completed-run rule on its own table", definition)
	}
	const source = "FROM (SELECT * FROM testops_pipeline_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_pipeline_metrics_daily"
	rule := strings.Join(strings.Fields(testops.PipelineFailureRateWindowSQL), " ")
	for _, shape := range []struct{ chartType, groupBy string }{{"line", "day"}, {"bar", "repo"}, {"scorecard", ""}, {"line", "week"}} {
		query, _, err := buildChartQuery(ChartSpec{
			ChartID: "chart-1", PlanID: "plan-1", ChartType: shape.chartType, Metric: "failure_rate", GroupBy: shape.groupBy,
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", OrganizationID: "org-1",
		}, definition)
		if err != nil {
			t.Fatal(err)
		}
		query = strings.Join(strings.Fields(query), " ")
		if !strings.Contains(query, rule+" AS y") || !strings.Contains(query, source) {
			t.Errorf("%s by %q does not apply the completed-run rule to the newest counts:\n%s", shape.chartType, shape.groupBy, query)
		}
		for _, banned := range []string{"avg(", "failure_rate IS NOT NULL", "sum(failure_rate"} {
			if strings.Contains(query, banned) {
				t.Errorf("%s by %q reads %q:\n%s", shape.chartType, shape.groupBy, banned, query)
			}
		}
	}
}
