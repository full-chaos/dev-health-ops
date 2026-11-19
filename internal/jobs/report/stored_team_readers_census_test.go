package report

import (
	"sort"
	"strings"
	"testing"
	"time"
)

// storedTeamPending are the charted tables whose report charts still read the stored team_id of a row that is one
// repository-day (the main owner at the end of the day), each with the ticket that moves it to repository ownership.
// Remove a table here when it joins repositoryOwnedTables.
var storedTeamPending = map[string]string{
	"testops_coverage_metrics_daily": "CHAOS-14107",
	"user_metrics_daily":             "CHAOS-14109",
	"work_item_cycle_times":          "CHAOS-13917",
}

// storedTeamProblems lists every chartable report key with the team dimension whose table is neither team-keyed (team_id
// in its read key: one row per team), nor repository-owned, nor pending with a ticket; and every pending entry that is
// stale. A new table with a team key fails here until it is placed.
func storedTeamProblems(metrics map[string]metricDefinition, reads map[string]tableRead, owned map[string]bool, pending map[string]string) []string {
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	var problems []string
	pendingSeen := map[string]bool{}
	for name, registered := range metrics {
		definition := withChartRule(registered)
		if definition.ValueKind != valueKindNumeric || !definition.hasDimension("team") {
			continue
		}
		read, declared := reads[definition.SourceTable]
		if !declared {
			continue // validateChartMetrics refuses it: never charted
		}
		if _, pooled := pooledCharts[definition.CanonicalName]; pooled && withPooledChart(definition, at).pooled != nil {
			continue // a pooled chart reads the owners at the run (repositoryOwnerJoin)
		}
		teamKeyed := false
		for _, column := range read.Key {
			teamKeyed = teamKeyed || column == "team_id"
		}
		switch {
		case teamKeyed || owned[definition.SourceTable]:
		case pending[definition.SourceTable] != "":
			pendingSeen[definition.SourceTable] = true
		default:
			problems = append(problems, "report key "+name+" of "+definition.SourceTable+" has the team dimension and reads the stored team_id of a table that is not team-keyed: add the table to repositoryOwnedTables, or to storedTeamPending with its ticket")
		}
	}
	for table, ticket := range pending {
		switch {
		case owned[table]:
			problems = append(problems, "pending table "+table+" ("+ticket+") is in repositoryOwnedTables: remove it from storedTeamPending")
		case !pendingSeen[table]:
			problems = append(problems, "pending table "+table+" ("+ticket+") has no report key that reads its stored team: remove it from storedTeamPending")
		}
	}
	sort.Strings(problems)
	return problems
}

func TestNoReportChartReadsAStoredMainOwnerTeamCensus(t *testing.T) {
	t.Parallel()
	for _, problem := range storedTeamProblems(supportedMetrics, tableReads, repositoryOwnedTables, storedTeamPending) {
		t.Error(problem)
	}
	// Every key of a repository-owned table reads the owners at the run, never the stored team.
	for name, registered := range supportedMetrics {
		definition := withChartRule(registered)
		if !repositoryOwnedTables[definition.SourceTable] || definition.ValueKind != valueKindNumeric || !definition.hasDimension("team") {
			continue
		}
		filtered, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: name, FilterTeams: []string{"team-1"}})
		grouped, _ := cicdChartStatement(t, ChartSpec{ChartType: "bar", Metric: name, GroupBy: "team"})
		if strings.Contains(filtered, "team_id IN {filter_teams") || !strings.Contains(filtered, "team_repo_ownership") ||
			!strings.Contains(grouped, "own.owner_team_id AS x") {
			t.Errorf("%s of repository-owned %s does not read the owners at the run:\n%s\n%s", name, definition.SourceTable, filtered, grouped)
		}
	}
}

// The census is loud: a planted table with a team key and no place fails it, and so does a stale pending entry.
func TestTheStoredTeamCensusFailsOnAPlantedTable(t *testing.T) {
	t.Parallel()
	reads := map[string]tableRead{}
	for table, read := range tableReads {
		reads[table] = read
	}
	reads["planted_daily"] = tableRead{Key: []string{"org_id", "repo_id", "day"}, Version: "computed_at"}
	metrics := map[string]metricDefinition{}
	for name, definition := range supportedMetrics {
		metrics[name] = definition
	}
	metrics["planted_rate"] = metricDefinition{CanonicalName: "planted_rate", SourceTable: "planted_daily", Dimensions: []string{"repo", "team", "day"}, ValueKind: valueKindNumeric}
	pending := map[string]string{"never_charted_daily": "CHAOS-0"}
	for table, ticket := range storedTeamPending {
		pending[table] = ticket
	}
	problems := strings.Join(storedTeamProblems(metrics, reads, repositoryOwnedTables, pending), "\n")
	for _, want := range []string{"report key planted_rate of planted_daily", "pending table never_charted_daily"} {
		if !strings.Contains(problems, want) {
			t.Errorf("the census does not name %q:\n%s", want, problems)
		}
	}
}
