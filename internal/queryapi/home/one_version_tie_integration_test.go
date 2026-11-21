//go:build integration

package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// Two versions of one key that share computed_at serve ONE version in Home and explain, whatever the insert order and
// whether the parts merged: the version with the larger (computed_at, cityHash64 of its measures), the order every
// served reader of the table picks by. The organization form, a team set and one team serve that same version, and the
// live-row rule is read from it.

// tieVersion is one stored version of team P's key on the current day of nullableNewestDays: a work_item_metrics_daily
// row and a 'blocked' work_item_state_durations_daily row. A version with every measure 0 and cycle nil is a retraction
// row of both tables.
type tieVersion struct {
	name      string
	completed uint32
	cycle     any
	wip       float64
	wipCount  uint32
	blocked   float64
	touched   uint32
}

func seedTieVersion(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, day, at time.Time, v tieVersion) {
	t.Helper()
	if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
		(org_id, day, provider, work_scope_id, team_id, team_name, row_view, items_completed, wip_count_end_of_day, wip_congestion_ratio, cycle_time_p50_hours, computed_at)
		VALUES (?, ?, 'jira', 'S', 'P', 'P', 'org', ?, ?, ?, ?, ?)`, org, day, v.completed, v.wipCount, v.wip, v.cycle, at); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
		(org_id, day, provider, work_scope_id, team_id, team_name, row_view, status, duration_hours, items_touched, computed_at)
		VALUES (?, ?, 'jira', 'S', 'P', 'P', 'org', 'blocked', ?, ?, ?)`, org, day, v.blocked, v.touched, at); err != nil {
		t.Fatal(err)
	}
}

// tieWinners names the version each table serves for org's key on day: the stored row with the largest (computed_at,
// cityHash64 of the table's measures), read from ClickHouse with the measure lists written out here.
func tieWinners(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, day time.Time) (workItems, durations string) {
	t.Helper()
	pick := func(query string) string {
		var tag string
		if err := admin.QueryRow(ctx, query, org, day).Scan(&tag); err != nil {
			t.Fatal(err)
		}
		return tag
	}
	workItems = pick(`SELECT concat(toString(items_completed), '/', ifNull(toString(cycle_time_p50_hours), 'NULL'))
		FROM work_item_metrics_daily WHERE org_id = ? AND day = ?
		ORDER BY computed_at DESC, cityHash64(tuple(items_started, items_completed, items_started_unassigned,
			items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio,
			story_points_completed, new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio,
			predictability_score, cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
			wip_age_p50_hours, wip_age_p90_hours)) DESC LIMIT 1`)
	durations = pick(`SELECT toString(duration_hours)
		FROM work_item_state_durations_daily WHERE org_id = ? AND day = ? AND status = 'blocked'
		ORDER BY computed_at DESC, cityHash64(tuple(duration_hours, items_touched, avg_wip)) DESC LIMIT 1`)
	return workItems, durations
}

// tieServed is the served answer of the four team metrics in Home and explain, for no team, the team set P, Q and the
// one team P: value, presence, and explain's value with its contributors and drivers.
func tieServed(ctx context.Context, t *testing.T, client QueryClient, reader *explain.Reader, org string, teams ...string) string {
	t.Helper()
	prev, cur, _, _, _ := nullableNewestDays()
	metrics := []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"}
	lines, _, _ := nullableServed(ctx, t, client, org, metrics, teams...)
	var parts []string
	for _, m := range metrics {
		parts = append(parts, strings.SplitN(lines[m], " spark=", 2)[0])
		params := explain.Params{Metric: m, ScopeLevel: "org", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur}
		if teams != nil {
			params.ScopeLevel, params.ScopeIDs = "team", teams
		}
		resp, err := explain.BuildExplainResponse(ctx, reader, org, params)
		if err != nil {
			t.Fatal(err)
		}
		contributors := make([]string, 0, len(resp.Contributors))
		for _, c := range resp.Contributors {
			contributors = append(contributors, fmt.Sprintf("%s:%g", c.ID, c.Value))
		}
		drivers := make([]string, 0, len(resp.Drivers))
		for _, d := range resp.Drivers {
			drivers = append(drivers, fmt.Sprintf("%s:%g", d.ID, d.Value))
		}
		parts = append(parts, fmt.Sprintf("explain %s=%g contributors=%v drivers=%v", m, resp.Value, contributors, drivers))
	}
	return strings.Join(parts, "; ")
}

// tieCase runs the two versions in both insert orders (one org each), merges stopped, then merged, and wants every form
// to serve want(winners) in every state.
func tieCase(t *testing.T, name string, a, b tieVersion, want func(workItems, durations string) string) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	prev, cur, _, older, _ := nullableNewestDays()
	stopMerges(ctx, t, admin, "work_item_metrics_daily")
	stopMerges(ctx, t, admin, "work_item_state_durations_daily")
	var orgs []string
	for _, order := range [][2]tieVersion{{a, b}, {b, a}} {
		org := fmt.Sprintf("org-14430-%s-%s-then-%s", name, order[0].name, order[1].name)
		orgs = append(orgs, org)
		// The prior day holds one version, so drivers have a comparison.
		seedTieVersion(ctx, t, admin, org, prev, older, tieVersion{completed: 1, cycle: 24.0, wip: 0.4, wipCount: 1, blocked: 4, touched: 1})
		seedTieVersion(ctx, t, admin, org, cur, older, order[0])
		seedTieVersion(ctx, t, admin, org, cur, older, order[1])
	}
	// The merge keeps one stored version of the key (ReplacingMergeTree keeps the last inserted of a tie), so the
	// winner is read from the stored rows again after it.
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			mergeParts(ctx, t, admin, "work_item_metrics_daily")
			mergeParts(ctx, t, admin, "work_item_state_durations_daily")
		}
		for _, org := range orgs {
			workItems, durations := tieWinners(ctx, t, admin, org, cur)
			t.Logf("%s %s: winners work_item_metrics_daily %s, blocked %s", phase, org, workItems, durations)
			wanted := want(workItems, durations)
			for _, teams := range [][]string{nil, {"P", "Q"}, {"P"}} {
				wantHome(t, fmt.Sprintf("%s %s %v", phase, org, teams), tieServed(ctx, t, client, reader, org, teams...), wanted)
			}
		}
	}
}

// The prior day (one version): cycle 24 h = 1 d, throughput 1, wip 40 %, blocked 4.
var tiePrior = map[string]string{"cycle_time": "1", "throughput": "1", "wip_saturation": "40", "blocked_work": "4"}

// tieLine is the served line of one metric whose current day has the value v (home form, delta pct, explain form).
func tieLine(metric, home, pct, explainValue string) string {
	return fmt.Sprintf("%s=%s has=true prior=%s pct=%s; explain %s=%s contributors=[P:%s] drivers=[P:%s]",
		metric, home, tiePrior[metric], pct, metric, explainValue, explainValue, explainValue)
}

// tieNone is the served line of one metric whose current day holds no value: no data, and the team is a driver of
// the prior value only.
func tieNone(metric string) string {
	return fmt.Sprintf("%s=0 has=false prior=%s pct=nil; explain %s=0 contributors=[] drivers=[P:0]", metric, tiePrior[metric], metric)
}

// Two live versions: A (2 items, 10 h, 20 %, 5 blocked hours) and B (4 items, no cycle, 60 %, 9 blocked hours). Each
// table picks its own winner.
func TestHomeAndExplainServeOneVersionOnAComputedAtTie(t *testing.T) {
	a := tieVersion{name: "A", completed: 2, cycle: 10.0, wip: 0.2, wipCount: 1, blocked: 5, touched: 1}
	b := tieVersion{name: "B", completed: 4, cycle: nil, wip: 0.6, wipCount: 1, blocked: 9, touched: 2}
	tieCase(t, "two-live", a, b, func(workItems, durations string) string {
		var parts []string
		switch workItems {
		case "2/10":
			parts = append(parts,
				tieLine("cycle_time", "0.4166666666666667", "-58.3333", "0.4166666666666667"),
				tieLine("throughput", "2", "100", "2"),
				tieLine("wip_saturation", "20", "-50", "20"))
		case "4/NULL":
			parts = append(parts, tieNone("cycle_time"),
				tieLine("throughput", "4", "300", "4"),
				tieLine("wip_saturation", "60", "50", "60"))
		default:
			t.Fatalf("work_item_metrics_daily winner %q is neither version", workItems)
		}
		switch durations {
		case "5":
			parts = append(parts, tieLine("blocked_work", "5", "25", "5"))
		case "9":
			parts = append(parts, tieLine("blocked_work", "9", "125", "9"))
		default:
			t.Fatalf("work_item_state_durations_daily winner %q is neither version", durations)
		}
		return strings.Join(parts, "; ")
	})
}

// A live version L (3 items, 12 h, 50 %, 6 blocked hours) and a retraction row R (every measure 0, no cycle) of one
// computed_at: in each table the key is present iff the picked version is live.
func TestHomeAndExplainTieOfALiveAndARetractedVersion(t *testing.T) {
	l := tieVersion{name: "L", completed: 3, cycle: 12.0, wip: 0.5, wipCount: 1, blocked: 6, touched: 1}
	r := tieVersion{name: "R"}
	tieCase(t, "live-retracted", l, r, func(workItems, durations string) string {
		var parts []string
		switch workItems {
		case "3/12":
			parts = append(parts,
				tieLine("cycle_time", "0.5", "-50", "0.5"),
				tieLine("throughput", "3", "200", "3"),
				tieLine("wip_saturation", "50", "25", "50"))
		case "0/NULL":
			parts = append(parts, tieNone("cycle_time"), tieNone("throughput"), tieNone("wip_saturation"))
		default:
			t.Fatalf("work_item_metrics_daily winner %q is neither version", workItems)
		}
		switch durations {
		case "6":
			parts = append(parts, tieLine("blocked_work", "6", "50", "6"))
		case "0":
			parts = append(parts, tieNone("blocked_work"))
		default:
			t.Fatalf("work_item_state_durations_daily winner %q is neither version", durations)
		}
		return strings.Join(parts, "; ")
	})
}

// No tie: the newer version serves (the answer of main), in every form and merge state.
func TestHomeAndExplainNoTieControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	prev, cur, _, older, newer := nullableNewestDays()
	const org = "org-14430-no-tie"
	stopMerges(ctx, t, admin, "work_item_metrics_daily")
	stopMerges(ctx, t, admin, "work_item_state_durations_daily")
	seedTieVersion(ctx, t, admin, org, prev, older, tieVersion{completed: 1, cycle: 24.0, wip: 0.4, wipCount: 1, blocked: 4, touched: 1})
	seedTieVersion(ctx, t, admin, org, cur, newer, tieVersion{completed: 2, cycle: 10.0, wip: 0.2, wipCount: 1, blocked: 5, touched: 1})
	seedTieVersion(ctx, t, admin, org, cur, older, tieVersion{completed: 4, cycle: 30.0, wip: 0.6, wipCount: 1, blocked: 9, touched: 2})
	want := strings.Join([]string{
		tieLine("cycle_time", "0.4166666666666667", "-58.3333", "0.4166666666666667"),
		tieLine("throughput", "2", "100", "2"),
		tieLine("wip_saturation", "20", "-50", "20"),
		tieLine("blocked_work", "5", "25", "5"),
	}, "; ")
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			mergeParts(ctx, t, admin, "work_item_metrics_daily")
			mergeParts(ctx, t, admin, "work_item_state_durations_daily")
		}
		for _, teams := range [][]string{nil, {"P", "Q"}, {"P"}} {
			wantHome(t, fmt.Sprintf("%s %v", phase, teams), tieServed(ctx, t, client, reader, org, teams...), want)
		}
	}
}
