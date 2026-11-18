package report

import (
	"strings"
	"testing"
)

// testopsPipelineStoredKeys are the report keys charted over the stored rows of testops_pipeline_metrics_daily (the
// pooled duration and queue keys read the raw runs).
var testopsPipelineStoredKeys = []string{"success_count", "failure_count", "cancelled_count", "success_rate", "failure_rate", "cancel_rate", "rerun_rate"}

// A team of a TestOps pipeline chart is the owner of the repository as of the report run, never the stored team_id (the
// main owner at the end of the day): a team group is one bucket per owner team, a team filter keeps the teams' owned
// repositories. A service group reads the stored service; success_rate has no service dimension.
func TestTestopsPipelineChartsReadRepositoryOwnershipForTeam(t *testing.T) {
	t.Parallel()
	for _, metric := range testopsPipelineStoredKeys {
		grouped, groupedParameters := cicdChartStatement(t, ChartSpec{ChartType: "bar", Metric: metric, GroupBy: "team"})
		if !strings.Contains(grouped, "own.owner_team_id AS x") || !strings.Contains(grouped, "FROM team_repo_ownership AS o FINAL") ||
			strings.Contains(grouped, "'unscoped'") {
			t.Errorf("%s: a team group is not one bucket per owner team:\n%s", metric, grouped)
		}
		if groupedParameters["team_scope_as_of"] != "2026-03-01 00:00:00.000" {
			t.Errorf("%s: team group owners read at %v, want the report run 2026-03-01 00:00:00.000", metric, groupedParameters["team_scope_as_of"])
		}
		filtered, filteredParameters := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric, FilterTeams: []string{"team-1"}})
		if !strings.Contains(filtered, "team_repo_ownership") || strings.Contains(filtered, "team_id IN {filter_teams") {
			t.Errorf("%s: a team filter does not keep the team's owned repositories:\n%s", metric, filtered)
		}
		if filteredParameters["team_scope_as_of"] != "2026-03-01 00:00:00.000" {
			t.Errorf("%s: team filter owners read at %v, want the report run", metric, filteredParameters["team_scope_as_of"])
		}
		both, _ := cicdChartStatement(t, ChartSpec{ChartType: "bar", Metric: metric, GroupBy: "team", FilterTeams: []string{"team-2"}})
		if !strings.Contains(both, "own.owner_team_id AS x") || !strings.Contains(both, "o.team_id IN {team_scope_ids:Array(String)}") {
			t.Errorf("%s: a team group with a team filter does not keep the filtered owners only:\n%s", metric, both)
		}
		service, _ := cicdChartStatement(t, ChartSpec{ChartType: "bar", Metric: metric, GroupBy: "service"})
		wantService := "service_id AS x"
		if metric == "success_rate" {
			wantService = "'unscoped' AS x"
		}
		if !strings.Contains(service, wantService) {
			t.Errorf("%s: a service group is not %q:\n%s", metric, wantService, service)
		}
		organization, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric})
		if strings.Contains(organization, "team_repo_ownership") || strings.Contains(organization, "team_id") {
			t.Errorf("%s: a chart with no team filter or team group reads a team:\n%s", metric, organization)
		}
	}
}

// A mean over no row is no value: a scorecard of a mean key with no row in its buckets has no point, never NaN.
func TestTestopsPipelineMeanOfNoRowIsNoPoint(t *testing.T) {
	t.Parallel()
	for _, metric := range []string{"cancel_rate", "rerun_rate"} {
		statement, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric})
		if !strings.Contains(statement, "avgOrNull("+metric+") AS y") {
			t.Errorf("%s: the mean of no row is not NULL:\n%s", metric, statement)
		}
	}
}
