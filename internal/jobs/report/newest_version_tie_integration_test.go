//go:build integration

package report

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// The cells of the report chart's newest-version pick on a computed_at tie
// (CHAOS-14407): two versions of one key with the same computed_at go in as
// separate INSERTs with merges stopped, in both insert orders, into ONE
// organization (a hash of the whole row holds org_id). The env owns its
// ClickHouse, so the cells truncate the tables between orders.

const reportTieOrg = "org-14407-report"

func (e *pooledEnv) tieReset() {
	e.t.Helper()
	for _, table := range []string{"work_item_metrics_daily", "repo_metrics_daily"} {
		e.exec("TRUNCATE TABLE " + table)
	}
}

// repoChart is the bus_factor chart by day of org (repo_metrics_daily, the
// whole-row hash form).
func (e *pooledEnv) repoChart(org string) map[string]float64 {
	e.t.Helper()
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: "line", Metric: "bus_factor", GroupBy: "day",
		OrganizationID: org, TimeRangeStart: "2026-02-01", TimeRangeEnd: "2026-02-28"})
	if err != nil {
		e.t.Fatalf("bus_factor chart: %v", err)
	}
	points := map[string]float64{}
	for _, point := range result.DataPoints {
		points[point.X] = point.Y
	}
	return points
}

func (e *pooledEnv) repoRow(busFactor int, at string) {
	e.t.Helper()
	e.exec(`INSERT INTO repo_metrics_daily (repo_id, day, commits_count, bus_factor, computed_at, org_id)
		VALUES ('14407000-0000-0000-0000-000000000009', '2026-02-02', 1, ?, ?, ?)`, busFactor, at, reportTieOrg)
}

type reportTieForm struct {
	name string
	read func(e *pooledEnv) string
	seed func(e *pooledEnv, first bool, at string)
}

func reportTieForms() []reportTieForm {
	workItem := func(e *pooledEnv, first bool, at string) {
		completed := uint32(5)
		if first {
			completed = 1
		}
		e.seedTeamChartRows(reportTieOrg, []teamChartRow{{"2026-02-02", "P", completed, at, ""}})
	}
	return []reportTieForm{
		{"D1_team_form", func(e *pooledEnv) string { return fmt.Sprint(e.teamChart(reportTieOrg, "P")) }, workItem},
		{"D2_org_form", func(e *pooledEnv) string { return fmt.Sprint(e.teamChart(reportTieOrg)) }, workItem},
		{"D2_org_form_whole_row_hash", func(e *pooledEnv) string { return fmt.Sprint(e.repoChart(reportTieOrg)) },
			func(e *pooledEnv, first bool, at string) {
				if first {
					e.repoRow(2, at)
				} else {
					e.repoRow(7, at)
				}
			}},
	}
}

func TestReportChartPicksOneVersionOnATie(t *testing.T) {
	e := startPooledEnv(t)
	e.exec(`SYSTEM STOP MERGES work_item_metrics_daily`)
	e.exec(`SYSTEM STOP MERGES repo_metrics_daily`)
	for _, form := range reportTieForms() {
		t.Run(form.name, func(t *testing.T) {
			values := func(at1, at2 string) []string {
				set := map[string]bool{}
				for order := 0; order < 2; order++ {
					for rep := 0; rep < 3; rep++ {
						e.tieReset()
						if order == 0 {
							form.seed(e, true, at1)
							form.seed(e, false, at2)
						} else {
							form.seed(e, false, at2)
							form.seed(e, true, at1)
						}
						value := form.read(e)
						t.Logf("%s order=%d rep=%d: %s", form.name, order, rep, value)
						set[value] = true
					}
				}
				out := make([]string, 0, len(set))
				for value := range set {
					out = append(out, value)
				}
				sort.Strings(out)
				return out
			}
			control := values("2026-02-02 10:00:00", "2026-02-02 10:00:01")
			e.tieReset()
			form.seed(e, false, "2026-02-02 10:00:01")
			if newest := form.read(e); len(control) != 1 || control[0] != newest {
				t.Errorf("control: the newer version must win in both insert orders: %v, newer alone %s", control, newest)
			}
			if tie := values("2026-02-02 10:00:00", "2026-02-02 10:00:00"); len(tie) != 1 {
				t.Errorf("a computed_at tie gives a chart that depends on the insert order: %v", tie)
			}
		})
	}
}
