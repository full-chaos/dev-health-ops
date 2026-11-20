package home

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	ch "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
)

// A query double of Home for the driver of a one-team request (CHAOS-14379; acp ruling on PLAN-10664-ops2a2-delta-2):
// one team and one metric at a time. For the daily data it applies the aggregate and the blocked predicate of the
// SELECT that Home sends to the seeded rows; for a value of the items linked to named repositories (what.repos) it
// returns item and attribution rows to the real Go reducer. A driver read is recorded, so a cell can say whether one
// was sent.

const driverDoubleTeam = "jira:platform"
const driverDoubleRepo = "2bb3aa60-06fb-4a00-af55-26ce6891dfaa"

type driverDoubleRow struct {
	day    time.Time
	status string
	value  float64
}

func driverDoubleReduce(rows []driverDoubleRow, begin string, operation string, blocked bool) (float64, int64) {
	var total float64
	var count int64
	for _, row := range rows {
		if formatDay(row.day) != begin || (blocked && row.status != "blocked") {
			continue
		}
		total += row.value
		count++
	}
	if operation == "avg" && count > 0 {
		total /= float64(count)
	}
	return total, count
}

type driverDoubleRead struct {
	current, prior       float64
	currentSQL, priorSQL string
	dayFilter            bool
	linked               bool
}

func (r driverDoubleRead) String() string {
	if r.currentSQL == "" {
		return "skipped"
	}
	return fmt.Sprintf("%s/%s blocked-only=%t repository-filter=%t prior=%g current=%g",
		r.priorSQL, r.currentSQL, r.dayFilter, r.linked, r.prior, r.current)
}

// This query double replays one team and one metric at a time. For the daily
// data it takes the aggregate and blocked predicate from the SELECT emitted by
// Home and applies those to the seeded, already unique live rows. For the
// repo-linked data it returns item/attribution rows to the actual Go reducer.
func driverDoubleHome(t *testing.T, metric string, linked, orgRows bool, ids []string) (*Response, driverDoubleRead, driverDoubleRead) {
	t.Helper()
	cur, prev := day(2026, 10, 21), day(2026, 10, 20)
	now := cur.AddDate(0, 0, 1).Add(12 * time.Hour)
	column := metricColumn(metric)
	var rows []driverDoubleRow
	switch metric {
	case "cycle_time":
		rows = []driverDoubleRow{{prev, "", 8}, {cur, "", 4}}
	case "throughput":
		rows = []driverDoubleRow{{prev, "", 3}, {cur, "", 2}, {cur, "", 2}}
	case "wip_saturation":
		rows = []driverDoubleRow{{prev, "", 0.25}, {cur, "", 0.5}}
	case "blocked_work":
		rows = []driverDoubleRow{{prev, "blocked", 4}, {prev, "in_progress", 20}, {cur, "blocked", 8}, {cur, "in_progress", 2}}
	}
	var items, attributions, blocked [][]any
	addItem := func(id, status string, start time.Time, done any) {
		items = append(items, []any{driverDoubleRepo, now, id, "jira", status, "PROJ", "123", "", "Platform Project",
			start, done, "story", []string{}, start, done, nil})
		attributions = append(attributions, []any{driverDoubleRepo, "jira", id, driverDoubleTeam, "Platform"})
	}
	if linked {
		priorStart, curStart := prev.Add(time.Hour), cur.Add(time.Hour)
		priorDuration, curDuration := 2*time.Hour, 2*time.Hour
		if metric == "cycle_time" {
			curDuration = 8 * time.Hour
		}
		if metric == "throughput" {
			addItem("P-0", "done", curStart, curStart.Add(curDuration))
		}
		addItem("P-1", "done", priorStart, priorStart.Add(priorDuration))
		addItem("P-2", "done", curStart, curStart.Add(curDuration))
		if metric == "wip_saturation" || metric == "blocked_work" {
			addItem("P-3", "in_progress", prev.Add(-time.Hour), nil)
		}
		if metric == "wip_saturation" {
			rows = []driverDoubleRow{{prev, "", 0.5}, {cur, "", 0.25}}
			addItem("P-4", "in_progress", cur.Add(time.Hour), nil)
		}
		if metric == "blocked_work" {
			blocked = [][]any{{prev, driverDoubleTeam, 2.0, int64(1)}, {cur, driverDoubleTeam, 8.0, int64(1)}}
		}
	}
	var summary, caption driverDoubleRead
	client := fakeQueryClient{t: t, handler: func(t *testing.T, q string, bindings []ch.Binding) (ch.RowScanner, error) {
		scan := func(rows [][]any) (ch.RowScanner, error) {
			return &fixtureRowScanner{rows: rows}, nil
		}
		bound := func(name string) string {
			v, _ := bindingValue(bindings, name)
			s, _ := v.(string)
			return s
		}
		batch := strings.Contains(q, "BY metric")
		arms := regexp.MustCompile(`SELECT team_id AS id, (sum|avg)\(([a-z_0-9]+)\) AS value`).FindAllStringSubmatchIndex(q, -1)
		if len(arms) > 0 && q[arms[0][4]:arms[0][5]] == column {
			if len(arms) != 2 {
				return nil, fmt.Errorf("expected two driver arms for %s, got %d", metric, len(arms))
			}
			first, second := q[arms[0][0]:arms[1][0]], q[arms[1][0]:]
			rule := driverDoubleRead{
				currentSQL: q[arms[0][2]:arms[0][3]], priorSQL: q[arms[1][2]:arms[1][3]],
				dayFilter: strings.Contains(first, "status = 'blocked'") && strings.Contains(second, "status = 'blocked'"),
				linked:    strings.Contains(q, "work_graph_issue_pr"),
			}
			chosen := rows
			if !strings.Contains(q, "LIMIT 1 BY") && !orgRows {
				// The daily rows are team rows unless orgRows: the org form reads none of them.
				chosen = nil
			}
			current, hasCurrent := driverDoubleReduce(chosen, bound("start_day"), rule.currentSQL, strings.Contains(first, "status = 'blocked'"))
			prior, hasPrior := driverDoubleReduce(chosen, bound("compare_start"), rule.priorSQL, strings.Contains(second, "status = 'blocked'"))
			rule.current, rule.prior = current, prior
			if batch {
				caption = rule
			} else {
				summary = rule
			}
			delta := deltarule.Of(current, prior, hasCurrent > 0, hasPrior > 0).Pct
			if delta == nil {
				return scan(nil)
			}
			if batch {
				return scan([][]any{{metric, driverDoubleTeam}})
			}
			return scan([][]any{{driverDoubleTeam, current, *delta}})
		}
		switch {
		case linked && strings.Contains(q, "link_repo_ids") && strings.Contains(q, "SELECT toString(repo_id), last_synced"):
			return scan(items)
		case linked && strings.Contains(q, "link_repo_ids") && strings.Contains(q, "SELECT toString(repo_id), provider, work_item_id"):
			return scan(attributions)
		case linked && strings.Contains(q, "FROM work_item_blocked_durations_daily"):
			return scan(blocked)
		case linked && strings.Contains(q, "FROM repos FINAL") && (strings.Contains(q, "{held_repo_ids:") && strings.HasPrefix(strings.TrimSpace(q), "SELECT DISTINCT toString(id) AS repo_id") || strings.Contains(q, "WHERE repo = {repo_name:String}") && strings.HasPrefix(strings.TrimSpace(q), "SELECT toString(id) AS id")):
			return scan([][]any{{driverDoubleRepo}})
		case strings.HasPrefix(q, "SELECT id FROM teams FINAL"):
			return scan([][]any{{driverDoubleTeam}})
		case strings.Contains(q, "name AS display_name"):
			return scan([][]any{{driverDoubleTeam, "Platform"}})
		case !linked && (strings.Contains(q, "LIMIT 1 BY") || orgRows) && strings.Contains(q, "argMax("+column+",") && (strings.Contains(q, "AS value") || strings.Contains(q, "AS blocked_hours")):
			start, _ := time.Parse(time.DateOnly, bound("start_day"))
			operation := "sum"
			if strings.Contains(q, "avg("+column+") AS") || strings.Contains(q, "avg("+column+")) AS") {
				operation = "avg"
			}
			value, n := driverDoubleReduce(rows, bound("start_day"), operation, strings.Contains(q, "status = 'blocked'"))
			if n == 0 {
				return scan(nil)
			}
			if strings.Contains(q, "AS row_count") {
				return scan([][]any{{n, value}})
			}
			return scan([][]any{{start, value}})
		default:
			return scan(nil)
		}
	}}
	f := Filters{Time: TimeFilter{RangeDays: 1, CompareDays: 1, EndDate: &now}, Scope: ScopeFilter{Level: "team", IDs: ids}}
	if linked {
		f.What.Repos = []string{"acme/platform"}
	}
	resp, err := BuildResponse(context.Background(), client, nil, "org-14379-driver-double", f, now)
	if err != nil {
		t.Fatal(err)
	}
	return resp, summary, caption
}

func driverDoubleDelta(resp *Response, metric string) MetricDelta {
	for _, delta := range resp.Deltas {
		if delta.Metric == metric {
			return delta
		}
	}
	panic("missing metric")
}

// driverDoubleSummary is the summary sentence and the driver captions of the metric signals.
func driverDoubleSummary(resp *Response) (string, []string) {
	text := ""
	if len(resp.Summary) > 0 {
		text = resp.Summary[0].Text
	}
	var captions []string
	for _, signal := range resp.Signals {
		if signal.Driver != nil {
			captions = append(captions, *signal.Driver)
		}
	}
	return text, captions
}

// One team: the driver is the team itself when the value states a move, with no driver read of its own, so it has the
// value's source, predicates and reducer (sum for throughput, blocked hours only for blocked_work). The same for team
// rows and for org rows.
func TestHomeOneTeamDriverIsTheValuesMove(t *testing.T) {
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		for _, orgRows := range []bool{false, true} {
			resp, summaryRead, captionRead := driverDoubleHome(t, metric, false, orgRows, []string{driverDoubleTeam})
			d := driverDoubleDelta(resp, metric)
			if d.PriorValue == nil || !d.HasData {
				t.Fatalf("%s org rows %v: the value has no move: %+v", metric, orgRows, d)
			}
			text, captions := driverDoubleSummary(resp)
			if !strings.HasPrefix(text, d.Label+" ") || !strings.HasSuffix(text, " driven by Platform.") {
				t.Errorf("%s org rows %v: summary %q, want the move driven by Platform", metric, orgRows, text)
			}
			if summaryRead.currentSQL != "" || captionRead.currentSQL != "" {
				t.Errorf("%s org rows %v: a driver read was sent for one team (summary %s, caption %s)", metric, orgRows, summaryRead, captionRead)
			}
			if len(captions) == 0 {
				t.Errorf("%s org rows %v: no caption names the driver", metric, orgRows)
			}
			for _, caption := range captions {
				if !strings.Contains(caption, "Platform") {
					t.Errorf("%s org rows %v: caption %q does not name Platform", metric, orgRows, caption)
				}
			}
		}
	}
}

// One team and a value of the items linked to the named repositories (what.repos): no team-wide driver, for team rows
// and for org rows. Two teams keep main's answer (pinned from main ef0758cf3a42): main names the team-wide driver of
// the org rows (ticket 14425), and none for team rows.
func TestHomeOneTeamLinkedValueHasNoDriver(t *testing.T) {
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		for _, orgRows := range []bool{false, true} {
			resp, summaryRead, _ := driverDoubleHome(t, metric, true, orgRows, []string{driverDoubleTeam})
			text, _ := driverDoubleSummary(resp)
			if strings.Contains(text, "driven by") || summaryRead.currentSQL != "" {
				t.Errorf("%s org rows %v: one team linked summary %q (driver read %s), want no driver", metric, orgRows, text, summaryRead)
			}
			two, _, _ := driverDoubleHome(t, metric, true, orgRows, []string{driverDoubleTeam, "jira:other"})
			twoText, _ := driverDoubleSummary(two)
			if want := driverDoubleMainTwoTeamsLinked(metric, orgRows); twoText != want {
				t.Errorf("%s org rows %v: two teams linked summary %q, want main's %q", metric, orgRows, twoText, want)
			}
		}
	}
}

// driverDoubleMainTwoTeamsLinked is main's summary for two teams and a linked value (log ops2a2-r1-repro-1.log
// 9a8fd350ae6f, main ef0758cf3a42, the control run R1_ORG_ROWS=1 for org rows).
func driverDoubleMainTwoTeamsLinked(metric string, orgRows bool) string {
	sentence := map[string]string{
		"cycle_time":     "Cycle Time rose 300%",
		"throughput":     "Throughput rose 100%",
		"wip_saturation": "WIP Saturation rose 100%",
		"blocked_work":   "Blocked Work rose 300%",
	}[metric]
	if orgRows {
		return sentence + " driven by Platform."
	}
	return sentence + "."
}
