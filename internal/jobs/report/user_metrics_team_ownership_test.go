package report

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// userMetricsKeys are the chartable report keys of user_metrics_daily, sorted.
func userMetricsKeys() []string {
	var keys []string
	for name, registered := range supportedMetrics {
		definition := withChartRule(registered)
		if definition.SourceTable == "user_metrics_daily" && definition.ValueKind == valueKindNumeric {
			keys = append(keys, name)
		}
	}
	sort.Strings(keys)
	return keys
}

func userMetricsIsSum(metric string) bool {
	return strings.HasSuffix(metric, "_count") || supportedMetrics[metric].Unit == "count"
}

// A team of a per-person chart is the owner of the row's repository as of the report run, never the stored team_id (the
// person's membership label): a team group is one bucket per owner team, a team filter keeps the teams' owned
// repositories. A chart with no team filter or team group reads no team. The work-item keys are not applicable to a team
// (TestUserMetricsWorkItemKeysAreNotApplicableToATeam).
func TestUserMetricsChartsReadRepositoryOwnershipForTeam(t *testing.T) {
	t.Parallel()
	keys := userMetricsKeys()
	if len(keys) != 32 {
		t.Fatalf("user_metrics_daily has %d chartable keys, want 32: %v", len(keys), keys)
	}
	sums := 0
	for _, metric := range keys {
		organization, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric})
		if strings.Contains(organization, "team_repo_ownership") || strings.Contains(organization, "team_id") {
			t.Errorf("%s: a chart with no team filter or team group reads a team:\n%s", metric, organization)
		}
		wantY := "avgOrNull(" + metric + ") AS y"
		if userMetricsIsSum(metric) {
			wantY, sums = "toFloat64(sum("+metric+")) AS y", sums+1
		}
		if !strings.Contains(organization, wantY) {
			t.Errorf("%s: the organization chart is not %q (a mean of no row is no point):\n%s", metric, wantY, organization)
		}
		if teamNotApplicableTo(metric) {
			continue
		}
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
		repo, _ := cicdChartStatement(t, ChartSpec{ChartType: "bar", Metric: metric, GroupBy: "repo"})
		if !strings.Contains(repo, "repo_id AS x") || strings.Contains(repo, "team_repo_ownership") {
			t.Errorf("%s: a repo group is not the stored repository:\n%s", metric, repo)
		}
	}
	if sums != 4 {
		t.Errorf("user_metrics_daily has %d SUM keys, want 4 (commits_count, context_spread_count, large_commits_count, delivery_units)", sums)
	}
}

func teamNotApplicableTo(metric string) bool {
	return metric == "work_items_completed" || metric == "work_items_active"
}

// The work items of a person are on the person's row of no repository: under a team filter or a team group the two
// work-item keys have no team value by repository ownership. The chart sends no query and is "not applicable" with its
// reason, never 0 and never the stored membership team. The organization chart is read as before.
func TestUserMetricsWorkItemKeysAreNotApplicableToATeam(t *testing.T) {
	t.Parallel()
	for _, metric := range []string{"work_items_completed", "work_items_active"} {
		for _, spec := range []ChartSpec{
			{ChartType: "scorecard", Metric: metric, FilterTeams: []string{"team-1"}},
			{ChartType: "bar", Metric: metric, GroupBy: "team"},
			{ChartType: "bar", Metric: metric, GroupBy: "team", FilterTeams: []string{"team-1"}},
			{ChartType: "line", Metric: metric, GroupBy: "day", FilterTeams: []string{"team-1"}},
		} {
			conn := &statementConn{}
			adapter := &ClickHouseQueryAdapter{conn: conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
			spec.OrganizationID = "org-1"
			result, err := adapter.executeChart(context.Background(), spec)
			if err != nil || conn.statement != "" {
				t.Errorf("%s %+v: err %v, statement %q: want no query", metric, spec, err, conn.statement)
				continue
			}
			rendered := renderChart(result)
			if len(result.DataPoints) != 0 || !strings.Contains(rendered, "_Not applicable: ") ||
				!strings.Contains(rendered, "work items belong to no repository") || !strings.Contains(rendered, "#### "+supportedMetrics[metric].DisplayName) {
				t.Errorf("%s %+v: not served as not applicable:\n%s", metric, spec, rendered)
			}
		}
		organization, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric})
		if !strings.Contains(organization, "FROM (SELECT * FROM") {
			t.Errorf("%s: the organization chart is not read:\n%s", metric, organization)
		}
	}
}

// delivery_units counts merged pull requests on a git row and completed work items on the row of no repository: a team
// chart counts the merged pull requests of the team's repositories and names the left-out work-item part.
func TestUserMetricsDeliveryUnitsTeamChartNamesTheWorkItemPart(t *testing.T) {
	t.Parallel()
	adapter := &ClickHouseQueryAdapter{conn: &asOfConn{}, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	for _, spec := range []ChartSpec{
		{ChartType: "scorecard", Metric: "delivery_units", FilterTeams: []string{"team-1"}, OrganizationID: "org-1"},
		{ChartType: "bar", Metric: "delivery_units", GroupBy: "team", OrganizationID: "org-1"},
	} {
		result, err := adapter.executeChart(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if rendered := renderChart(result); !strings.Contains(rendered, "_Note: ") || !strings.Contains(rendered, "merged pull requests") {
			t.Errorf("%+v: no work-item note:\n%s", spec, rendered)
		}
	}
	result, err := adapter.executeChart(context.Background(), ChartSpec{ChartType: "scorecard", Metric: "delivery_units", OrganizationID: "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	if rendered := renderChart(result); strings.Contains(rendered, "_Note: ") {
		t.Errorf("the organization chart has a team note:\n%s", rendered)
	}
}
