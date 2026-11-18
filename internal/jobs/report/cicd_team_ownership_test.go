package report

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// statementConn records the chart statement executeChart sends and refuses it, so a cell reads the statement the real
// adapter builds (with its owners instant) and needs no server.
type statementConn struct {
	driver.Conn
	statement  string
	parameters []any
}

func (conn *statementConn) Query(_ context.Context, statement string, parameters ...any) (driver.Rows, error) {
	conn.statement, conn.parameters = statement, parameters
	return nil, errors.New("statement recorded")
}

func cicdChartStatement(t *testing.T, spec ChartSpec) (string, map[string]any) {
	t.Helper()
	conn := &statementConn{}
	adapter := &ClickHouseQueryAdapter{conn: conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	spec.OrganizationID = "org-1"
	if _, err := adapter.executeChart(context.Background(), spec); err == nil || conn.statement == "" {
		t.Fatalf("%s: no statement recorded (err %v)", spec.Metric, err)
	}
	named := map[string]any{}
	for _, parameter := range conn.parameters {
		if value, ok := parameter.(driver.NamedValue); ok {
			named[value.Name] = value.Value
		}
	}
	return conn.statement, named
}

// A team of a CI/CD chart is the owner of the repository as of the report run: a team group is one bucket per owner
// team (an ownership join), a team filter keeps the teams' owned repositories. cicd_metrics_daily has no team column, so
// the stored team is never read. A service group stays one 'unscoped' bucket: the table has no service.
func TestCicdChartsReadRepositoryOwnershipForTeam(t *testing.T) {
	t.Parallel()
	for _, metric := range []string{"pipelines_count", "avg_duration_minutes", "p90_duration_minutes", "avg_queue_minutes"} {
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
		if !strings.Contains(service, "'unscoped' AS x") {
			t.Errorf("%s: a service group is not one unscoped bucket:\n%s", metric, service)
		}
	}
}

// emptyRows is a chart answer with no row.
type emptyRows struct{ driver.Rows }

func (emptyRows) Next() bool   { return false }
func (emptyRows) Err() error   { return nil }
func (emptyRows) Close() error { return nil }

// asOfConn answers every chart with no row and keeps the owners instant each statement binds.
type asOfConn struct {
	driver.Conn
	instants []any
}

func (conn *asOfConn) Query(_ context.Context, _ string, parameters ...any) (driver.Rows, error) {
	for _, parameter := range parameters {
		if value, ok := parameter.(driver.NamedValue); ok && value.Name == "team_scope_as_of" {
			conn.instants = append(conn.instants, value.Value)
		}
	}
	return emptyRows{}, nil
}

// fixedReport loads one report definition.
type fixedReport ReportDefinition

func (report fixedReport) Load(context.Context, QueryInput) (ReportDefinition, error) {
	return ReportDefinition(report), nil
}

// Every chart of one report reads the owners at one instant, even when the clock moves between two charts: two charts
// of one report never put one repository under two different owners.
func TestOneReportReadsTheOwnersAtOneInstant(t *testing.T) {
	t.Parallel()
	clock := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	tick := func() time.Time {
		now := clock
		clock = clock.Add(time.Second)
		return now
	}
	charts := []ChartSpec{
		{ChartID: "c-1", PlanID: "plan-1", ChartType: "bar", Metric: "pipelines_count", GroupBy: "team", OrganizationID: "org-1"},
		{ChartID: "c-2", PlanID: "plan-1", ChartType: "bar", Metric: "avg_duration_minutes", GroupBy: "team", OrganizationID: "org-1"},
		{ChartID: "c-3", PlanID: "plan-1", ChartType: "scorecard", Metric: "pipelines_count", FilterTeams: []string{"team-1"}, OrganizationID: "org-1"},
	}
	conn := &asOfConn{}
	loader := fixedReport{Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"}, Charts: charts}
	adapter := &ClickHouseQueryAdapter{loader: loader, conn: conn, now: tick}
	if _, err := adapter.Query(context.Background(), QueryInput{ReportID: "report-1", RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	if len(conn.instants) != len(charts) {
		t.Fatalf("instants %v, want one per chart", conn.instants)
	}
	for _, instant := range conn.instants {
		if instant != conn.instants[0] {
			t.Fatalf("one report read the owners at %v", conn.instants)
		}
	}
}

// A team filter of blank ids only has no team to keep: the chart reads no repository, and its statement stays valid.
func TestBlankTeamFilterKeepsNoRepository(t *testing.T) {
	t.Parallel()
	for _, metric := range []string{"pipelines_count", "avg_duration_minutes", "p90_duration_minutes", "avg_queue_minutes", "p95_duration_seconds"} {
		for _, teams := range [][]string{{""}, {"", ""}} {
			statement, _ := cicdChartStatement(t, ChartSpec{ChartType: "scorecard", Metric: metric, FilterTeams: teams})
			if !strings.Contains(statement, " AND\n        0") {
				t.Errorf("%s, teams %q: no false team predicate:\n%s", metric, teams, statement)
			}
		}
	}
}
