//go:build integration

package report

import (
	"reflect"
	"testing"
	"time"
)

// The cells of the team form of a report chart filtered to ONE team (CHAOS-14353). Team P is the primary team of the
// items of scope S; team C co-owns some of them, so C has 'team' rows only. Day 2026-02-01 holds a legacy row only;
// day 2026-02-02 holds the rows of the org writer (the legacy row retracted with a row of zeros, an org row, a scope
// row) and, in the team data set, the team rows of the same keys. A filter of several teams is a team set: it keeps
// the org form (each item once), so its answer does not change.

type teamChartRow struct {
	day, team string
	completed uint32
	at, view  string
}

func (e *pooledEnv) seedTeamChartRows(org string, rows []teamChartRow) {
	e.t.Helper()
	for _, row := range rows {
		e.exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?)`, row.day, row.team, row.team, row.completed, row.at, org, row.view)
	}
}

// teamChart is the items_completed chart by day of org, filtered to teams.
func (e *pooledEnv) teamChart(org string, teams ...string) map[string]float64 {
	e.t.Helper()
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: "line", Metric: "items_completed", GroupBy: "day",
		OrganizationID: org, TimeRangeStart: "2026-02-01", TimeRangeEnd: "2026-02-28", FilterTeams: teams})
	if err != nil {
		e.t.Fatalf("items_completed chart %v: %v", teams, err)
	}
	points := map[string]float64{}
	for _, point := range result.DataPoints {
		points[point.X] = point.Y
	}
	return points
}

func wantTeamChart(t *testing.T, what string, got, want map[string]float64) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %v, want %v", what, got, want)
	}
}

func orgWriterDay() []teamChartRow {
	return []teamChartRow{
		{"2026-02-01", "P", 3, "2026-02-02 09:00:00", ""},
		{"2026-02-02", "P", 10, "2026-02-02 09:00:00", ""}, {"2026-02-02", "P", 0, "2026-02-02 11:00:00", ""},
		{"2026-02-02", "P", 4, "2026-02-02 10:00:00", "org"}, {"2026-02-02", "P", 9, "2026-02-02 10:00:00", "scope"},
	}
}

// (1) Today's data holds no team row: a one-team chart reads what the org form reads, as on main.
func TestReportTeamChartIsANoOpOnTodaysData(t *testing.T) {
	e := startPooledEnv(t)
	const org = "org-14353-chart-noop"
	e.seedTeamChartRows(org, orgWriterDay())
	orgDays := map[string]float64{"2026-02-01": 3, "2026-02-02": 4}
	wantTeamChart(t, "team P", e.teamChart(org, "P"), orgDays)
	wantTeamChart(t, "teams P and C", e.teamChart(org, "P", "C"), orgDays)
	wantTeamChart(t, "no team filter", e.teamChart(org), orgDays)
	wantTeamChart(t, "team C (no row)", e.teamChart(org, "C"), map[string]float64{})
}

// (2) A team row wins where one exists, for a chart of ONE team. A chart of several teams stays on the org form.
func TestReportTeamChartTakesTheTeamRow(t *testing.T) {
	e := startPooledEnv(t)
	const org = "org-14353-chart-team"
	e.seedTeamChartRows(org, append(orgWriterDay(),
		teamChartRow{"2026-02-02", "P", 7, "2026-02-02 10:00:00", "team"},
		teamChartRow{"2026-02-02", "C", 2, "2026-02-02 10:00:00", "team"},
	))
	wantTeamChart(t, "team P", e.teamChart(org, "P"), map[string]float64{"2026-02-01": 3, "2026-02-02": 7})
	wantTeamChart(t, "team C", e.teamChart(org, "C"), map[string]float64{"2026-02-02": 2})
	wantTeamChart(t, "teams P and C (a team set: org form)", e.teamChart(org, "P", "C"), map[string]float64{"2026-02-01": 3, "2026-02-02": 4})
	wantTeamChart(t, "no team filter (org form)", e.teamChart(org), map[string]float64{"2026-02-01": 3, "2026-02-02": 4})
}

// (3) Live first: a team key that a later run retracted reads the live org row; a key with no live view reads no row.
func TestReportTeamChartLiveFirst(t *testing.T) {
	e := startPooledEnv(t)
	const org = "org-14353-chart-live"
	e.seedTeamChartRows(org, []teamChartRow{
		{"2026-02-01", "P", 3, "2026-02-02 09:00:00", ""},
		{"2026-02-02", "P", 8, "2026-02-02 09:00:00", "team"},
		{"2026-02-02", "P", 6, "2026-02-02 10:00:00", "org"},
		{"2026-02-02", "P", 0, "2026-02-02 11:00:00", "team"},
		// A key whose only view is retracted: no point, not a point of 0.
		{"2026-02-03", "P", 0, "2026-02-03 10:00:00", "org"},
	})
	wantTeamChart(t, "team P", e.teamChart(org, "P"), map[string]float64{"2026-02-01": 3, "2026-02-02": 6})
}

// Two organizations never mix through the team source of a one-team chart.
func TestReportTeamChartNeverMixesTwoOrgs(t *testing.T) {
	e := startPooledEnv(t)
	const orgA, orgB = "org-14353-chart-two-a", "org-14353-chart-two-b"
	e.seedTeamChartRows(orgA, []teamChartRow{
		{"2026-02-01", "P", 3, "2026-02-02 09:00:00", ""},
		{"2026-02-02", "P", 4, "2026-02-02 10:00:00", "org"}, {"2026-02-02", "P", 7, "2026-02-02 10:00:00", "team"},
	})
	e.seedTeamChartRows(orgB, []teamChartRow{
		{"2026-02-02", "P", 70, "2026-02-02 11:00:00", "team"}, {"2026-02-03", "P", 90, "2026-02-03 11:00:00", "team"},
		{"2026-02-02", "C", 20, "2026-02-02 11:00:00", "team"},
	})
	wantTeamChart(t, "org A team P", e.teamChart(orgA, "P"), map[string]float64{"2026-02-01": 3, "2026-02-02": 7})
	wantTeamChart(t, "org A team C (rows in org B only)", e.teamChart(orgA, "C"), map[string]float64{})
}
