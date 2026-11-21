package home

import (
	"context"
	"fmt"
	"regexp"
	"sort"
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

// driverDoubleNewTeam has rows in the current window only (scenario "from-zero-absent").
const driverDoubleNewTeam = "jira:new"
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
	// zero is the rank of a rise from a measured 0 (delta_abs); absentIsZero is its rule for a group with no prior row.
	zero, absentIsZero bool
	ids                []string
}

func (r driverDoubleRead) String() string {
	if r.currentSQL == "" {
		return "skipped"
	}
	return fmt.Sprintf("%s/%s blocked-only=%t repository-filter=%t zero=%t absent-is-zero=%t prior=%g current=%g ids=%v",
		r.priorSQL, r.currentSQL, r.dayFilter, r.linked, r.zero, r.absentIsZero, r.prior, r.current, r.ids)
}

// This query double replays one team and one metric at a time. For the daily
// data it takes the aggregate and blocked predicate from the SELECT emitted by
// Home and applies those to the seeded, already unique live rows. For the
// repo-linked data it returns item/attribution rows to the actual Go reducer.
func driverDoubleHome(t *testing.T, metric string, linked, orgRows bool, ids []string, scenarios ...string) (*Response, driverDoubleRead, driverDoubleRead) {
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

	absent := len(scenarios) == 1 && scenarios[0] == "from-zero-absent"
	if absent {
		scenarios = []string{"from-zero"}
	}
	if len(scenarios) == 1 {
		for i := range rows {
			if rows[i].status != "" && rows[i].status != "blocked" {
				continue
			}
			if rows[i].day.Equal(prev) {
				rows[i].value = 0
			}
			if scenarios[0] == "steady" {
				rows[i].value = 0
			}
		}
		if scenarios[0] == "missing" {
			kept := rows[:0]
			for _, row := range rows {
				if row.day.Equal(cur) {
					kept = append(kept, row)
				}
			}
			rows = kept
		}
	}
	byTeam := map[string][]driverDoubleRow{driverDoubleTeam: rows}
	teams := []string{driverDoubleTeam}
	allRows := rows
	if absent {
		// A team with no prior row beside the measured 0: a summed value counts it as 0, an average does not.
		byTeam[driverDoubleNewTeam] = map[string][]driverDoubleRow{
			"cycle_time":     {{cur, "", 6}},
			"throughput":     {{cur, "", 10}},
			"wip_saturation": {{cur, "", 0.75}},
			"blocked_work":   {{cur, "blocked", 20}},
		}[metric]
		teams = append(teams, driverDoubleNewTeam)
		allRows = append(append([]driverDoubleRow{}, rows...), byTeam[driverDoubleNewTeam]...)
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
		arms := regexp.MustCompile(`SELECT team_id AS id, (?:toFloat64\()?(sum|avg)\(([a-z_0-9]+)\)\)? AS value`).FindAllStringSubmatchIndex(q, -1)
		if len(arms) > 0 && q[arms[0][4]:arms[0][5]] == column {
			if len(arms) != 2 {
				return nil, fmt.Errorf("expected two driver arms for %s, got %d", metric, len(arms))
			}
			first, second := q[arms[0][0]:arms[1][0]], q[arms[1][0]:]
			rule := driverDoubleRead{
				currentSQL: q[arms[0][2]:arms[0][3]], priorSQL: q[arms[1][2]:arms[1][3]],
				dayFilter:    strings.Contains(first, "status = 'blocked'") && strings.Contains(second, "status = 'blocked'"),
				linked:       strings.Contains(q, "work_graph_issue_pr"),
				zero:         strings.Contains(q, "AS delta_abs"),
				absentIsZero: strings.Contains(q, "(previous.present = 0 OR previous.value = 0)"),
			}
			if rule.zero != strings.Contains(q, "current.value > 0") || rule.zero == strings.Contains(q, "AS delta_pct") {
				return nil, fmt.Errorf("a driver read that is neither the percent form nor the absolute form:\n%s", q)
			}
			// The daily rows are team rows unless orgRows: the org form reads none of them.
			teamRows := !strings.Contains(q, "LIMIT 1 BY") && !orgRows
			type ranked struct {
				id            string
				current, rank float64
			}
			var out []ranked
			for _, team := range teams {
				chosen := byTeam[team]
				if teamRows {
					chosen = nil
				}
				current, hasCurrent := driverDoubleReduce(chosen, bound("start_day"), rule.currentSQL, strings.Contains(first, "status = 'blocked'"))
				prior, hasPrior := driverDoubleReduce(chosen, bound("compare_start"), rule.priorSQL, strings.Contains(second, "status = 'blocked'"))
				if team == driverDoubleTeam {
					rule.current, rule.prior = current, prior
				}
				if hasCurrent == 0 {
					continue
				}
				if rule.zero {
					measuredZero := hasPrior > 0 && prior == 0
					if current > 0 && (measuredZero || hasPrior == 0 && rule.absentIsZero) {
						out = append(out, ranked{team, current, current - prior})
					}
					continue
				}
				if delta := deltarule.Of(current, prior, true, hasPrior > 0).Pct; delta != nil {
					out = append(out, ranked{team, current, *delta})
				}
			}
			sort.SliceStable(out, func(i, j int) bool {
				if out[i].rank != out[j].rank {
					return out[i].rank > out[j].rank
				}
				return out[i].id < out[j].id
			})
			var result [][]any
			for _, r := range out {
				rule.ids = append(rule.ids, r.id)
				if batch {
					result = append(result, []any{metric, r.id})
				} else {
					result = append(result, []any{r.id, r.current, r.rank})
				}
			}
			if batch {
				caption = rule
			} else {
				summary = rule
			}
			return scan(result)
		}
		if strings.Contains(q, "AS previous ON") {
			return nil, fmt.Errorf("a driver read the double does not know:\n%s", q)
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
			return scan([][]any{{driverDoubleTeam}, {driverDoubleNewTeam}})
		case strings.Contains(q, "name AS display_name"):
			return scan([][]any{{driverDoubleTeam, "Platform"}, {driverDoubleNewTeam, "New"}})
		// A value read picks one version of every measure (CHAOS-14430): it is the read of this metric when it aggregates
		// this column.
		case !linked && (strings.Contains(q, "LIMIT 1 BY") || orgRows) && strings.Contains(q, "AS picked") && (strings.Contains(q, "avg("+column+")") || strings.Contains(q, "sum("+column)) && (strings.Contains(q, "AS value") || strings.Contains(q, "AS blocked_hours")):
			start, _ := time.Parse(time.DateOnly, bound("start_day"))
			operation := "sum"
			if strings.Contains(q, "avg("+column+") AS") || strings.Contains(q, "avg("+column+")) AS") {
				operation = "avg"
			}
			value, n := driverDoubleReduce(allRows, bound("start_day"), operation, strings.Contains(q, "status = 'blocked'"))
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

// A value of the items linked to the named repositories (what.repos) names no team-wide driver, for team rows and org
// rows: one team (CHAOS-14379) and two teams (CHAOS-14425; main ef0758cf3a42 and b737db18f3b2 named "driven by
// Platform" for org rows, log ops2a2-r1-repro-1.log 9a8fd350ae6f). No driver read is sent.
func TestHomeOneTeamLinkedValueHasNoDriver(t *testing.T) {
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		for _, orgRows := range []bool{false, true} {
			for _, ids := range [][]string{{driverDoubleTeam}, {driverDoubleTeam, "jira:other"}, nil} {
				resp, summaryRead, captionRead := driverDoubleHome(t, metric, true, orgRows, ids)
				text, captions := driverDoubleSummary(resp)
				if strings.Contains(text, "driven by") || len(captions) > 0 || summaryRead.currentSQL != "" || captionRead.currentSQL != "" {
					t.Errorf("%s org rows %v teams %d: linked summary %q captions %q (driver reads %s / %s), want no driver",
						metric, orgRows, len(ids), text, captions, summaryRead, captionRead)
				}
			}
		}
	}
}

// A move from a measured 0 (the prior window measured 0, the current not) is stated in absolute values and has no
// percent. One team: the summary and the captions name the team, with no driver read, for team rows ([t] and [t,t])
// and org rows.
func TestHomeOneTeamDriverOnAMoveFromAMeasuredZero(t *testing.T) {
	// The query double only exposes this value's rows. A measured 0 can be
	// live (e.g. WIP present with no completed items, a completion with no
	// remaining WIP, a completed item's start/end equal, or items_touched > 0).
	metrics := []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"}
	for _, metric := range metrics {
		t.Run(metric, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				ids     []string
				orgRows bool
			}{
				{"team-view", []string{driverDoubleTeam}, false},
				{"team-view-duplicate", []string{driverDoubleTeam, driverDoubleTeam}, false},
				{"org-fallback", []string{driverDoubleTeam}, true},
			} {
				resp, summaryRead, captionRead := driverDoubleHome(t, metric, false, test.orgRows, test.ids, "from-zero")
				d := driverDoubleDelta(resp, metric)
				if !d.FromZero() || d.PriorValue == nil || *d.PriorValue != 0 || d.Value <= 0 {
					t.Fatalf("%s: expected a measured 0 -> positive move, got %+v", test.name, d)
				}
				summary, captions := driverDoubleSummary(resp)
				var signalDelta string
				for _, signal := range resp.Signals {
					if signal.Metric == metric && signal.Delta != nil {
						signalDelta = *signal.Delta
					}
				}
				if signalDelta == "" || !strings.Contains(summary, "rose from") {
					t.Fatalf("%s: the response did not state the move: summary=%q signal delta=%q", test.name, summary, signalDelta)
				}
				if summaryRead.currentSQL != "" || captionRead.currentSQL != "" {
					t.Errorf("%s: unexpected driver SQL, summary %s, caption %s", test.name, summaryRead, captionRead)
				}
				named := strings.Contains(summary, "driven by Platform") &&
					len(captions) == 1 && strings.Contains(captions[0], "Platform")
				t.Logf("%s prior=%g current=%g summary=%q signal delta=%q captions=%q driver SQL=%t",
					test.name, *d.PriorValue, d.Value, summary, signalDelta, captions,
					summaryRead.currentSQL != "" || captionRead.currentSQL != "")
				if !named {
					t.Errorf("%s: the one-team move must name Platform in both outputs", test.name)
				}
			}
		})
	}
}

// The controls of the driver: one team ([t], [t,t]) names the team for a percent move with no driver read; no team and
// a team set read the drivers of the org rows; a linked value names none in any form (CHAOS-14425); a steady or a
// missing value names none.
func TestHomeOneTeamDriverControls(t *testing.T) {
	scopes := []struct {
		name string
		ids  []string
	}{
		{"none", nil},
		{"one", []string{driverDoubleTeam}},
		{"duplicate", []string{driverDoubleTeam, driverDoubleTeam}},
		{"set", []string{driverDoubleTeam, "jira:other"}},
		{"set-duplicate", []string{driverDoubleTeam, "jira:other", driverDoubleTeam}},
	}
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		t.Run(metric, func(t *testing.T) {
			for _, scope := range scopes {
				isOne := scope.name == "one" || scope.name == "duplicate"
				for _, orgRows := range []bool{false, true} {
					for _, linked := range []bool{false, true} {
						resp, summaryRead, captionRead := driverDoubleHome(t, metric, linked, orgRows, scope.ids)
						summary, captions := driverDoubleSummary(resp)
						summaryNamed := strings.Contains(summary, "driven by Platform")
						captionNamed := len(captions) == 1 && strings.Contains(captions[0], "Platform")
						wantSummary := !linked && (isOne || orgRows)
						wantCaption := wantSummary
						wantSummaryRead := !isOne && orgRows && !linked
						wantCaptionRead := wantSummaryRead
						gotSummaryRead, gotCaptionRead := summaryRead.currentSQL != "", captionRead.currentSQL != ""
						t.Logf("%s org=%t linked=%t summary-name=%t caption-name=%t summary-SQL=%t caption-SQL=%t",
							scope.name, orgRows, linked, summaryNamed, captionNamed, gotSummaryRead, gotCaptionRead)
						if summaryNamed != wantSummary || captionNamed != wantCaption ||
							gotSummaryRead != wantSummaryRead || gotCaptionRead != wantCaptionRead {
							t.Errorf("wrong driver behavior: summary %q caption %v; want name in summary=%t caption=%t, driver SQL in summary=%t caption=%t",
								summary, captions, wantSummary, wantCaption, wantSummaryRead, wantCaptionRead)
						}
					}
				}
			}
			for _, scenario := range []string{"steady", "missing"} {
				resp, summaryRead, captionRead := driverDoubleHome(t, metric, false, false, []string{driverDoubleTeam}, scenario)
				summary, captions := driverDoubleSummary(resp)
				if strings.Contains(summary, "driven by") || len(captions) > 0 ||
					summaryRead.currentSQL != "" || captionRead.currentSQL != "" {
					t.Errorf("%s: unexpected driver: summary=%q captions=%q", scenario, summary, captions)
				}
				t.Logf("%s summary=%q captions=%q", scenario, summary, captions)
			}
		})
	}
}
