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
