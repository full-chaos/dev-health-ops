package report

import (
	"strings"
	"testing"
)

// testopsTestStoredKeys are the report keys charted over the stored rows of testops_test_metrics_daily (the two suite
// duration keys are pooled over the raw suite runs).
var testopsTestStoredKeys = []string{"passed_count", "failed_count", "skipped_count", "quarantined_count", "total_cases", "total_suites",
	"pass_rate", "flake_rate", "retry_dependency_rate", "failure_recurrence_score"}

// A team of a TestOps test chart is the owner of the repository as of the report run, never the stored team_id (the
// main owner at the end of the day): a team group is one bucket per owner team, a team filter keeps the teams' owned
// repositories. A service group reads the stored service.
func TestTestopsTestChartsReadRepositoryOwnershipForTeam(t *testing.T) {
	t.Parallel()
	for _, metric := range testopsTestStoredKeys {
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
		if !strings.Contains(service, "service_id AS x") {
			t.Errorf("%s: a service group is not the stored service:\n%s", metric, service)
		}
		organization, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric})
		if strings.Contains(organization, "team_repo_ownership") || strings.Contains(organization, "team_id") {
			t.Errorf("%s: a chart with no team filter or team group reads a team:\n%s", metric, organization)
		}
	}
}
