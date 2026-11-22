//go:build integration

package home

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// The cells of the team view of the one-team Home metric reads (CHAOS-14379): the value, the comparison value and the
// spark of the team metrics of work_item_metrics_daily (cycle_time, throughput, wip_saturation) and the blocked hours of
// work_item_state_durations_daily (blocked_work), read through BuildResponse. Team P is the primary team of the items
// of scope S; team C co-owns some of them, so C has 'team' rows only. D1 holds a legacy row only; D2 holds the rows of
// the org writer (the legacy row retracted with a row of zeros, an org row and a scope row) and, in the team data set,
// the team rows of the same keys. The comparison window holds no row.

func homeTeamViewNow() time.Time { return time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC) }

func homeTeamViewDays() (d1, d2 time.Time) {
	return time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
}

func homeTeamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

// homeTeamViewRow is one stored row of a key of both tables: the work item row (items_completed,
// cycle_time_p50_hours, wip_congestion_ratio) and the blocked state row (duration_hours). cycle nil stores NULL. A row
// of zeros with cycle nil is a retraction row of both tables.
type homeTeamViewRow struct {
	day       time.Time
	team      string
	completed uint32
	cycle     *float64
	wip       float64
	blocked   float64
	at        time.Time
	view      string
}

func homeHours(v float64) *float64 { return &v }

func seedHomeTeamView(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []homeTeamViewRow) {
	t.Helper()
	for _, row := range rows {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, cycle_time_p50_hours, wip_congestion_ratio, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?)`,
			row.day, row.team, row.team, row.completed, row.cycle, row.wip, row.at, org, row.view); err != nil {
			t.Fatalf("insert work item row %+v: %v", row, err)
		}
		items := uint32(0)
		if row.blocked != 0 {
			items = 1
		}
		if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
			(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, 'blocked', ?, ?, ?, ?, ?)`,
			row.day, row.team, row.team, row.blocked, items, row.at, org, row.view); err != nil {
			t.Fatalf("insert state row %+v: %v", row, err)
		}
	}
}

func homeOrgWriterDays() []homeTeamViewRow {
	d1, d2 := homeTeamViewDays()
	t1, t2, t3 := homeTeamViewVersions()
	return []homeTeamViewRow{
		{d1, "P", 2, homeHours(10), 0.2, 3, t1, ""},
		{d2, "P", 7, homeHours(30), 0.7, 10, t1, ""}, {d2, "P", 0, nil, 0, 0, t3, ""},
		{d2, "P", 4, homeHours(20), 0.4, 4, t2, "org"}, {d2, "P", 6, homeHours(25), 0.6, 6, t2, "scope"},
	}
}

func homeTeamViewResponse(ctx context.Context, t *testing.T, client QueryClient, org string, teams ...string) *Response {
	t.Helper()
	f := DefaultFilters()
	if teams != nil {
		f.Scope = ScopeFilter{Level: "team", IDs: teams}
	}
	resp, err := BuildResponse(ctx, client, nil, org, f, homeTeamViewNow())
	if err != nil {
		t.Fatalf("%v BuildResponse: %v", teams, err)
	}
	return resp
}

// homeTeamMetrics is every served value of the four team metrics: value, presence flags, delta and the spark values.
func homeTeamMetrics(ctx context.Context, t *testing.T, client QueryClient, org string, teams ...string) string {
	t.Helper()
	resp := homeTeamViewResponse(ctx, t, client, org, teams...)
	var parts []string
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		found := false
		for _, delta := range resp.Deltas {
			if delta.Metric != metric {
				continue
			}
			found = true
			pct := "nil"
			if delta.DeltaPct != nil {
				pct = fmt.Sprintf("%.6g", *delta.DeltaPct)
			}
			var spark []string
			for _, point := range delta.Spark {
				spark = append(spark, fmt.Sprintf("%.6g", point.Value))
			}
			parts = append(parts, fmt.Sprintf("%s=%.6g has=%v prior=%v pct=%s spark=[%s]", metric, delta.Value, delta.HasData, delta.HasPriorData, pct, strings.Join(spark, " ")))
		}
		if !found {
			parts = append(parts, metric+"=missing")
		}
	}
	return strings.Join(parts, "; ")
}

// homeServedDigest is the sha256 prefix of the WHOLE served Home response JSON (every field; BuildResponse takes a
// fixed now, so nothing in it is random or wall-clock). Compared only against an answer pinned from main.
func homeServedDigest(ctx context.Context, t *testing.T, client QueryClient, org string, teams ...string) string {
	t.Helper()
	served, err := json.Marshal(homeTeamViewResponse(ctx, t, client, org, teams...))
	if err != nil {
		t.Fatal(err)
	}
	again, err := json.Marshal(homeTeamViewResponse(ctx, t, client, org, teams...))
	if err != nil {
		t.Fatal(err)
	}
	if string(served) != string(again) {
		t.Fatalf("%v: the served response is not deterministic", teams)
	}
	digest := sha256.Sum256(served)
	return fmt.Sprintf("%x", digest[:6])
}

func wantHome(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s:\n got  %s\n want %s", what, got, want)
	}
}

// The org form of the org writer days: D1 legacy (2 items, 10 h, 0.2, 3 blocked h) and D2 org (4, 20 h, 0.4, 4 h).
// cycle_time = avg(10, 20) h = 15 h = 0.625 d; throughput = 2 + 4 = 6; wip_saturation = avg(0.2, 0.4) = 30 %;
// blocked_work = 3 + 4 = 7. The comparison window is empty: no prior, no delta.
const homeOrgFormAnswer = "cycle_time=0.625 has=true prior=false pct=nil spark=[0.416667 0.833333]; " +
	"throughput=6 has=true prior=false pct=nil spark=[2 4]; " +
	"wip_saturation=30 has=true prior=false pct=nil spark=[20 40]; " +
	"blocked_work=7 has=true prior=false pct=nil spark=[3 4]"

const homeNoRowAnswer = "cycle_time=0 has=false prior=false pct=nil spark=[]; " +
	"throughput=0 has=false prior=false pct=nil spark=[]; " +
	"wip_saturation=0 has=false prior=false pct=nil spark=[]; " +
	"blocked_work=0 has=false prior=false pct=nil spark=[]"

// (1) Today's data holds no team row: every served value equals main's.
func TestHomeTeamViewIsANoOpOnTodaysData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-14379-home-noop"
	seedHomeTeamView(ctx, t, admin, org, homeOrgWriterDays())
	wantHome(t, "team P", homeTeamMetrics(ctx, t, client, org, "P"), homeOrgFormAnswer)
	wantHome(t, "no team", homeTeamMetrics(ctx, t, client, org), homeOrgFormAnswer)
	wantHome(t, "teams P and C", homeTeamMetrics(ctx, t, client, org, "P", "C"), homeOrgFormAnswer)
	wantHome(t, "team C (no row)", homeTeamMetrics(ctx, t, client, org, "C"), homeNoRowAnswer)
	// The whole served response, pinned from main (see the consts).
	wantHome(t, "served, team P", homeServedDigest(ctx, t, client, org, "P"), homeMainServedTeamP)
	wantHome(t, "served, no team", homeServedDigest(ctx, t, client, org), homeMainServedNoTeam)
	wantHome(t, "served, teams P and C", homeServedDigest(ctx, t, client, org, "P", "C"), homeMainServedTeamsPC)
	wantHome(t, "served, team C", homeServedDigest(ctx, t, client, org, "C"), homeMainServedTeamC)
}

// Pinned from main ab06c5087 (the whole served response JSON, sha256 prefix; log ops2a2-home-main-pin-1.log f974d66e87f2).
const (
	homeMainServedTeamP   = "bf499aa3654b"
	homeMainServedNoTeam  = "a0b36419cf89"
	homeMainServedTeamsPC = "20be12e781b8"
	homeMainServedTeamC   = "a1b16857ff08"
)

// (2) A team row wins where one exists, for one team; a team set keeps the org view; the ids are a set.
func TestHomeTeamViewTakesTheTeamRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	_, d2 := homeTeamViewDays()
	_, t2, _ := homeTeamViewVersions()
	const org = "org-14379-home-team"
	seedHomeTeamView(ctx, t, admin, org, append(homeOrgWriterDays(),
		homeTeamViewRow{d2, "P", 9, homeHours(40), 0.9, 12, t2, "team"},
		homeTeamViewRow{d2, "C", 3, homeHours(8), 0.5, 5, t2, "team"},
	))
	// Team P: D1 legacy (2, 10 h, 0.2, 3 h) and D2 team (9, 40 h, 0.9, 12 h). cycle_time = avg(10, 40) = 25 h =
	// 1.04167 d; throughput = 2 + 9 = 11; wip_saturation = avg(0.2, 0.9) = 55 %; blocked_work = 3 + 12 = 15.
	teamP := "cycle_time=1.04167 has=true prior=false pct=nil spark=[0.416667 1.66667]; " +
		"throughput=11 has=true prior=false pct=nil spark=[2 9]; " +
		"wip_saturation=55 has=true prior=false pct=nil spark=[20 90]; " +
		"blocked_work=15 has=true prior=false pct=nil spark=[3 12]"
	// Team C: D2 team only (3, 8 h, 0.5, 5 h). cycle_time = 8 h = 0.333333 d; wip_saturation = 50 %.
	teamC := "cycle_time=0.333333 has=true prior=false pct=nil spark=[0.333333]; " +
		"throughput=3 has=true prior=false pct=nil spark=[3]; " +
		"wip_saturation=50 has=true prior=false pct=nil spark=[50]; " +
		"blocked_work=5 has=true prior=false pct=nil spark=[5]"
	wantHome(t, "team P", homeTeamMetrics(ctx, t, client, org, "P"), teamP)
	wantHome(t, "team C", homeTeamMetrics(ctx, t, client, org, "C"), teamC)
	// A team set reads the org view: P's legacy and org rows (C has none), the org form answer.
	wantHome(t, "teams P and C (a team set: org form)", homeTeamMetrics(ctx, t, client, org, "P", "C"), homeOrgFormAnswer)
	wantHome(t, "[P,P] = [P]", homeTeamMetrics(ctx, t, client, org, "P", "P"), teamP)
	wantHome(t, "[P,C,P] = [P,C]", homeTeamMetrics(ctx, t, client, org, "P", "C", "P"), homeOrgFormAnswer)
	wantHome(t, "served [P,P] = served [P]", homeServedDigest(ctx, t, client, org, "P", "P"), homeServedDigest(ctx, t, client, org, "P"))
	wantHome(t, "served [P,C,P] = served [P,C]", homeServedDigest(ctx, t, client, org, "P", "C", "P"), homeServedDigest(ctx, t, client, org, "P", "C"))
}

// (3) Live first: a retracted team row never hides the live org row of the same key.
func TestHomeTeamViewLiveFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	d1, d2 := homeTeamViewDays()
	t1, t2, t3 := homeTeamViewVersions()
	const org = "org-14379-home-live"
	seedHomeTeamView(ctx, t, admin, org, []homeTeamViewRow{
		{d1, "P", 2, homeHours(10), 0.2, 3, t1, ""},
		{d2, "P", 9, homeHours(40), 0.9, 12, t1, "team"}, {d2, "P", 4, homeHours(20), 0.4, 4, t2, "org"}, {d2, "P", 0, nil, 0, 0, t3, "team"},
	})
	// The newest team row of D2 is the retraction (t3), so the live org row (4, 20 h, 0.4, 4 h) is read: the org form
	// answer.
	wantHome(t, "team P", homeTeamMetrics(ctx, t, client, org, "P"), homeOrgFormAnswer)
}

// (4) Two organizations never mix through the team view.
func TestHomeTeamViewNeverMixesTwoOrgs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	d1, d2 := homeTeamViewDays()
	_, t2, t3 := homeTeamViewVersions()
	const orgA, orgB = "org-14379-home-two-a", "org-14379-home-two-b"
	seedHomeTeamView(ctx, t, admin, orgA, append(homeOrgWriterDays(), homeTeamViewRow{d2, "P", 9, homeHours(40), 0.9, 12, t2, "team"}))
	seedHomeTeamView(ctx, t, admin, orgB, []homeTeamViewRow{
		{d2, "P", 90, homeHours(400), 0.95, 120, t3, "team"}, {d2, "P", 40, homeHours(200), 0.85, 40, t3, "org"},
		{d1, "P", 70, homeHours(300), 0.75, 70, t3, "team"}, {d2, "C", 30, homeHours(80), 0.65, 50, t3, "team"},
	})
	// Org A team P: D1 legacy and D2 team, as in the team cell.
	wantHome(t, "org A team P", homeTeamMetrics(ctx, t, client, orgA, "P"),
		"cycle_time=1.04167 has=true prior=false pct=nil spark=[0.416667 1.66667]; "+
			"throughput=11 has=true prior=false pct=nil spark=[2 9]; "+
			"wip_saturation=55 has=true prior=false pct=nil spark=[20 90]; "+
			"blocked_work=15 has=true prior=false pct=nil spark=[3 12]")
	wantHome(t, "org A team C (rows in org B only)", homeTeamMetrics(ctx, t, client, orgA, "C"), homeNoRowAnswer)
	wantHome(t, "org A no team", homeTeamMetrics(ctx, t, client, orgA), homeOrgFormAnswer)
}

// The documented divergence (R2 of the plan, not a WANT): a live legacy row beside a live org row of one key, which no
// writer on main stores. The org form reads both; the team form reads the org row. Both answers are recorded.
func TestHomeTeamViewRecordsTheLiveLegacyBesideLiveOrgDivergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	d1, d2 := homeTeamViewDays()
	t1, t2, _ := homeTeamViewVersions()
	const org = "org-14379-home-divergence"
	seedHomeTeamView(ctx, t, admin, org, []homeTeamViewRow{
		{d1, "P", 2, homeHours(10), 0.2, 3, t1, ""},
		{d2, "P", 7, homeHours(30), 0.7, 10, t1, ""}, {d2, "P", 4, homeHours(20), 0.4, 4, t2, "org"},
	})
	t.Logf("DIVERGENCE (recorded, not a want) one team P: %s", homeTeamMetrics(ctx, t, client, org, "P"))
	t.Logf("DIVERGENCE (recorded, not a want) no team (org form): %s", homeTeamMetrics(ctx, t, client, org))
}

// seedHomeTeamViewTeams stores P (Platform) and C (Core) as active teams: the driver names are read from them.
func seedHomeTeamViewTeams(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string) {
	t.Helper()
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, team := range [][2]string{{"P", "Platform"}, {"C", "Core"}} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, is_active)
			VALUES (?, generateUUIDv4(), ?, ['org/*'], ?, ?, 1)`, team[0], team[1], at, org); err != nil {
			t.Fatalf("insert team %v: %v", team, err)
		}
	}
}

// homeTeamViewCompareDay is a day of the comparison window [2026-09-23, 2026-10-07) of homeTeamViewNow.
func homeTeamViewCompareDay() time.Time { return time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC) }

// homeDriverText is the summary sentence and the driver captions of the metric signals of a response.
func homeDriverText(resp *Response) (summary string, captions []string) {
	if len(resp.Summary) > 0 {
		summary = resp.Summary[0].Text
	}
	for _, signal := range resp.Signals {
		if signal.Driver != nil {
			captions = append(captions, *signal.Driver)
		}
	}
	return summary, captions
}

// (2, driver rows) One team is one answer: the summary sentence and the driver captions name the drivers of the
// team view the values read. Team C has team rows only, so the org form has no driver row of C.
func TestHomeTeamViewDriverRowsReadTheTeamView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	_, d2 := homeTeamViewDays()
	_, t2, _ := homeTeamViewVersions()
	const org = "org-14379-home-drivers"
	seedHomeTeamViewTeams(ctx, t, admin, org)
	seedHomeTeamView(ctx, t, admin, org, []homeTeamViewRow{
		{homeTeamViewCompareDay(), "C", 2, homeHours(10), 0.2, 2, t2, "team"},
		{d2, "C", 3, homeHours(8), 0.5, 4, t2, "team"},
	})
	// Team C, team rows only: cycle 8 h against 10 h = -20 %; throughput 3 against 2 = +50 %; wip 50 % against 20 % =
	// +150 %; blocked 4 against 2 = +100 %. The largest move is wip_saturation.
	wantHome(t, "team C values", homeTeamMetrics(ctx, t, client, org, "C"),
		"cycle_time=0.333333 has=true prior=true pct=-20 spark=[0.333333]; "+
			"throughput=3 has=true prior=true pct=50 spark=[3]; "+
			"wip_saturation=50 has=true prior=true pct=150 spark=[50]; "+
			"blocked_work=4 has=true prior=true pct=100 spark=[4]")
	summary, captions := homeDriverText(homeTeamViewResponse(ctx, t, client, org, "C"))
	if !strings.HasPrefix(summary, "WIP Saturation ") || !strings.HasSuffix(summary, " driven by Core.") {
		t.Errorf("team C summary = %q, want the WIP Saturation move driven by Core", summary)
	}
	if len(captions) == 0 {
		t.Error("team C: no metric signal names a driver; want Core (the driver rows of the team view)")
	}
	for _, caption := range captions {
		if !strings.Contains(caption, "Core") {
			t.Errorf("team C driver caption %q does not name Core", caption)
		}
	}
}

// (R2, driver rows) A live legacy row beside a live org row of one key (no writer on main stores it). One team P reads
// the org row in the value and in the driver rows; the summary names the move of that value and its driver.
func TestHomeTeamViewR2DriverRowsMatchTheValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	_, d2 := homeTeamViewDays()
	t1, t2, _ := homeTeamViewVersions()
	const org = "org-14379-home-drivers-r2"
	seedHomeTeamViewTeams(ctx, t, admin, org)
	seedHomeTeamView(ctx, t, admin, org, []homeTeamViewRow{
		{homeTeamViewCompareDay(), "P", 4, homeHours(20), 0.4, 2, t1, ""},
		{d2, "P", 7, homeHours(30), 0.7, 10, t1, ""}, {d2, "P", 5, homeHours(10), 0.5, 2.5, t2, "org"},
	})
	// One team P: the current window reads the org row (org > legacy, both live), the comparison window the legacy
	// row. cycle 10 h against 20 h = -50 %; throughput 5 against 4 = +25 %; wip 50 % against 40 % = +25 %; blocked
	// 2.5 against 2 = +25 %. The largest move is cycle_time.
	wantHome(t, "R2 team P values", homeTeamMetrics(ctx, t, client, org, "P"),
		"cycle_time=0.416667 has=true prior=true pct=-50 spark=[0.416667]; "+
			"throughput=5 has=true prior=true pct=25 spark=[5]; "+
			"wip_saturation=50 has=true prior=true pct=25 spark=[50]; "+
			"blocked_work=2.5 has=true prior=true pct=25 spark=[2.5]")
	summary, captions := homeDriverText(homeTeamViewResponse(ctx, t, client, org, "P"))
	if !strings.HasPrefix(summary, "Cycle Time ") || !strings.HasSuffix(summary, " driven by Platform.") {
		t.Errorf("R2 team P summary = %q, want the Cycle Time move driven by Platform", summary)
	}
	for _, caption := range captions {
		if !strings.Contains(caption, "Platform") {
			t.Errorf("R2 team P driver caption %q does not name Platform", caption)
		}
	}
	t.Logf("DIVERGENCE (recorded, not a want) R2 no team (org form): %s", homeTeamMetrics(ctx, t, client, org))
}

// One team reads the newest row of a key, a NULL cycle included (ruling on PLAN-10664-ops2a2-delta-2, option B): its
// answer is the answer of the merged parts and the answer of explain, in both merge states. The org form of Home
// (no team, a team set) skips a newer NULL while an older row is still in another part (ticket 14424); that answer is
// recorded only.
func TestHomeTeamViewOneTeamIsTheMergedAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES work_item_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	const org, team = "org-14379-home-merged", "P"
	prev, cur := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	older, newer := cur.Add(time.Hour), cur.Add(2*time.Hour)
	now := cur.AddDate(0, 0, 1).Add(12 * time.Hour)
	for _, row := range []struct {
		at, day time.Time
		scope   string
		cycle   any
	}{
		{older, prev, "A", 48.0},
		{older, cur, "A", 48.0},
		{newer, cur, "A", nil},
		{older, cur, "B", 24.0},
	} {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(org_id, day, provider, work_scope_id, team_id, team_name, row_view, wip_count_end_of_day, cycle_time_p50_hours, computed_at)
			VALUES (?, ?, 'jira', ?, ?, ?, 'org', 1, ?, ?)`, org, row.day, row.scope, team, team, row.cycle, row.at); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	cycle := func(teams ...string) string {
		f := Filters{Time: TimeFilter{RangeDays: 1, CompareDays: 1, EndDate: &now}}
		if teams != nil {
			f.Scope = ScopeFilter{Level: "team", IDs: teams}
		}
		resp, err := BuildResponse(ctx, client, nil, org, f, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range resp.Deltas {
			if d.Metric == "cycle_time" {
				prior := "nil"
				if d.PriorValue != nil {
					prior = fmt.Sprintf("%g", *d.PriorValue)
				}
				var spark []string
				for _, p := range d.Spark {
					spark = append(spark, fmt.Sprintf("%g", p.Value))
				}
				return fmt.Sprintf("value=%g prior=%s spark=[%s]", d.Value, prior, strings.Join(spark, " "))
			}
		}
		return "missing"
	}
	explainCycle := func(teams ...string) string {
		params := explain.Params{Metric: "cycle_time", ScopeLevel: "org", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur}
		if teams != nil {
			params.ScopeLevel, params.ScopeIDs = "team", teams
		}
		resp, err := explain.BuildExplainResponse(ctx, reader, org, params)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("value=%g", resp.Value)
	}
	// The merged answer, by hand: day cur, scope A's newest row is live with a NULL cycle (no completed item), so A
	// has no cycle; scope B 24 h. avg = 24 h = 1 d. Day prev: scope A 48 h = 2 d. Explain serves days too: 1.
	const merged, explainMerged = "value=1 prior=2 spark=[1]", "value=1"
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			if err := admin.Exec(ctx, "SYSTEM START MERGES work_item_metrics_daily"); err != nil {
				t.Fatal(err)
			}
			if err := admin.Exec(ctx, "OPTIMIZE TABLE work_item_metrics_daily FINAL"); err != nil {
				t.Fatal(err)
			}
			wantHome(t, phase+": no team", cycle(), merged)
			wantHome(t, phase+": two teams", cycle(team, "Q"), merged)
		}
		wantHome(t, phase+": one team", cycle(team), merged)
		wantHome(t, phase+": explain one team", explainCycle(team), explainMerged)
		wantHome(t, phase+": explain no team", explainCycle(), explainMerged)
		t.Logf("RECORDED (ticket 14424, not a want) %s: no team %s; two teams %s", phase, cycle(), cycle(team, "Q"))
	}
}
