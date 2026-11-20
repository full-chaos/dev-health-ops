//go:build integration

package explain

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the team view of the one-team explain headline (CHAOS-14379). Only the headline value read (current and
// comparison window) reads the team view, for exactly one team; the contributor and driver rows keep the org form.
// Team P is the primary team of the items of scope S; team C co-owns some of them, so C has 'team' rows only.
// D1 (the comparison window) holds a legacy row only. D2 (the current window) holds the rows of the org writer: the
// legacy row retracted with a row of zeros, an org row and a scope row; the team cells add team rows. D3 is in the
// current window too and only org B of the two-org cell has rows on it.
//
// Each seeded row has one number m, and every team metric column of the row is derived from it, so each answer tells
// which row was read:
//   - work_item_metrics_daily: items_completed = m, cycle_time_p50_hours = 24*m, wip_congestion_ratio = m/8;
//   - work_item_state_durations_daily (status 'blocked'): duration_hours = 2*m.
//
// A retraction row is a row of zeros: every measure 0 and every nullable measure NULL (liverow / teamkeytables
// LiveRow of both tables).

// The team metrics of explain on the two team-keyed tables (metricconfig.go): throughput (sum of items_completed),
// cycle_time (avg of cycle_time_p50_hours / 24), wip_saturation (avg of wip_congestion_ratio * 100) on
// work_item_metrics_daily, and blocked_work (sum of duration_hours, status 'blocked') on
// work_item_state_durations_daily.
func explainTeamViewMetrics() []string {
	return []string{"throughput", "cycle_time", "wip_saturation", "blocked_work"}
}

// explainTeamViewServed is the served value of a metric when one row of number m is read in the window:
// throughput m; cycle_time 24*m hours / 24 = m days; wip_saturation m/8 * 100 = 12.5*m; blocked_work 2*m hours.
func explainTeamViewServed(metric string, m float64) float64 {
	switch metric {
	case "throughput", "cycle_time":
		return m
	case "wip_saturation":
		return 12.5 * m
	case "blocked_work":
		return 2 * m
	}
	panic("no derivation for " + metric)
}

func explainTeamViewDay(n int) time.Time { return time.Date(2026, 10, 17+n, 0, 0, 0, 0, time.UTC) }

func explainTeamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

type explainTeamViewRow struct {
	day  int // 1, 2 or 3
	team string
	m    uint32 // 0 = a retraction row (a row of zeros)
	at   time.Time
	view string
}

func startExplainTeamView(t *testing.T) (context.Context, stdclickhouse.Conn, *Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return ctx, admin, reader
}

func seedExplainTeamView(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []explainTeamViewRow) {
	t.Helper()
	for _, row := range rows {
		day := explainTeamViewDay(row.day)
		var metricsErr, durationsErr error
		if row.m == 0 {
			metricsErr = admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
				(day, provider, work_scope_id, team_id, team_name, computed_at, org_id, row_view)
				VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?)`, day, row.team, row.team, row.at, org, row.view)
			durationsErr = admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
				(day, provider, work_scope_id, team_id, team_name, status, computed_at, org_id, row_view)
				VALUES (?, 'jira', 'S', ?, ?, 'blocked', ?, ?, ?)`, day, row.team, row.team, row.at, org, row.view)
		} else {
			m := float64(row.m)
			metricsErr = admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
				(day, provider, work_scope_id, team_id, team_name, items_completed, cycle_time_p50_hours, wip_congestion_ratio, computed_at, org_id, row_view)
				VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?)`, day, row.team, row.team, row.m, 24*m, m/8, row.at, org, row.view)
			durationsErr = admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
				(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id, row_view)
				VALUES (?, 'jira', 'S', ?, ?, 'blocked', ?, 1, ?, ?, ?)`, day, row.team, row.team, 2*m, row.at, org, row.view)
		}
		if metricsErr != nil {
			t.Fatalf("insert work_item_metrics_daily %+v: %v", row, metricsErr)
		}
		if durationsErr != nil {
			t.Fatalf("insert work_item_state_durations_daily %+v: %v", row, durationsErr)
		}
	}
}

// explainTeamViewOrgWriterDays: D1 a legacy row only; D2 the org writer's rows (legacy live at t1, legacy retraction
// at t3, org at t2, scope at t2).
func explainTeamViewOrgWriterDays(t1, t2, t3 time.Time) []explainTeamViewRow {
	return []explainTeamViewRow{
		{1, "P", 5, t1, ""},
		{2, "P", 7, t1, ""}, {2, "P", 0, t3, ""},
		{2, "P", 4, t2, "org"}, {2, "P", 6, t2, "scope"},
	}
}

// explainTeamViewRequest is one served request: the current window D2..D3, the comparison window D1. teams nil is a
// request with no team (scope level org).
func explainTeamViewRequest(ctx context.Context, t *testing.T, reader *Reader, org, metric string, teams []string) *Response {
	t.Helper()
	params := Params{
		Metric: metric, ScopeLevel: "org",
		StartDay: explainTeamViewDay(2), EndDay: explainTeamViewDay(4),
		CompareStart: explainTeamViewDay(1), CompareEnd: explainTeamViewDay(2),
	}
	if teams != nil {
		params.ScopeLevel, params.ScopeIDs = "team", teams
	}
	response, err := BuildExplainResponse(ctx, reader, org, params)
	if err != nil {
		t.Fatalf("%s %s %v: %v", org, metric, teams, err)
	}
	return response
}

func explainTeamViewNumber(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }

func explainTeamViewDelta(p *float64) string {
	if p == nil {
		return "null"
	}
	return explainTeamViewNumber(*p)
}

func explainTeamViewRows(rows []Contributor) string {
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf("%s=%s/%s/%v/%v", row.ID, explainTeamViewNumber(row.Value), explainTeamViewDelta(row.DeltaPct), row.HasData, row.HasPriorData))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// explainTeamViewSummary is the key numbers of a served response as plain fields: the headline value, its delta and
// window flags, and each contributor and driver row as id=value/delta/has_data/has_prior_data.
func explainTeamViewSummary(response *Response) string {
	return fmt.Sprintf("value=%s delta=%s data=%v/%v contributors=%s drivers=%s",
		explainTeamViewNumber(response.Value), explainTeamViewDelta(response.DeltaPct), response.HasData, response.HasPriorData,
		explainTeamViewRows(response.Contributors), explainTeamViewRows(response.Drivers))
}

// explainTeamViewWant is a hand-computed answer, in the numbers m of the seeded rows (0 = no row).
type explainTeamViewWant struct {
	current, prior float64    // the headline row of the current and of the comparison window
	contributor    float64    // the team's contributor row (current window)
	driver         [2]float64 // the team's driver row, current and comparison window
	team           string     // the id of the contributor and driver row; "" = P
}

// render is the summary the want gives: each value is explainTeamViewServed of its m, and a delta is (c - p) / p * 100
// (deltarule; the served raw values are a fixed multiple of m, so the percent is the same in m).
func (want explainTeamViewWant) render(metric string) string {
	served := func(m float64) string { return explainTeamViewNumber(explainTeamViewServed(metric, m)) }
	team := want.team
	if team == "" {
		team = "P"
	}
	pct := func(c, p float64) string {
		if c == 0 || p == 0 {
			return "null"
		}
		return explainTeamViewNumber((c - p) / p * 100)
	}
	contributors := "[]"
	if want.contributor != 0 {
		contributors = fmt.Sprintf("[%s=%s/null/true/false]", team, served(want.contributor))
	}
	drivers := "[]"
	if want.driver[0] != 0 || want.driver[1] != 0 {
		drivers = fmt.Sprintf("[%s=%s/%s/%v/%v]", team, served(want.driver[0]), pct(want.driver[0], want.driver[1]), want.driver[0] != 0, want.driver[1] != 0)
	}
	return fmt.Sprintf("value=%s delta=%s data=%v/%v contributors=%s drivers=%s",
		served(want.current), pct(want.current, want.prior), want.current != 0, want.prior != 0, contributors, drivers)
}

func wantExplainTeamView(ctx context.Context, t *testing.T, reader *Reader, org, what string, teams []string, want explainTeamViewWant) {
	t.Helper()
	for _, metric := range explainTeamViewMetrics() {
		got := explainTeamViewSummary(explainTeamViewRequest(ctx, t, reader, org, metric, teams))
		if wanted := want.render(metric); got != wanted {
			t.Errorf("%s %s:\n got  %s\n want %s", what, metric, got, wanted)
		}
	}
}

// The org form of team P in org A in every cell: the D2 org row m=4 (the legacy key's newest row is its retraction
// row; the scope row is not in the org view) and the D1 legacy row m=5. Contributor P = 4; driver P = 4 against 5,
// (4 - 5) / 5 * 100 = -20.
func explainTeamViewOrgFormOfP() explainTeamViewWant {
	return explainTeamViewWant{current: 4, prior: 5, contributor: 4, driver: [2]float64{4, 5}}
}

// explainTeamViewNoOpRequests are the requests of the no-op cell: one team P, no team, the team set [P, C] and one
// team C (no row).
func explainTeamViewNoOpRequests() []struct {
	name  string
	teams []string
} {
	return []struct {
		name  string
		teams []string
	}{{"P", []string{"P"}}, {"none", nil}, {"PC", []string{"P", "C"}}, {"C", []string{"C"}}}
}

// explainTeamViewMainServed is the sha256 prefix (12 hex) of the JSON of the whole served response, per metric and
// request, pinned from main ab06c5087 (log ops2a2-explain-pin-1.log e57d1c9f56a3). No field is blanked: the response
// holds no wall-clock and no random field (BuildExplainResponse's time.Now only picks the team membership of a
// repository metric, and no field serves it). [P] and [P, C] serve the same JSON (evidence links name the first id).
func explainTeamViewMainServed() map[string]string {
	return map[string]string{
		"throughput/P": "c37457607ab9", "throughput/none": "b3c652bab4bd", "throughput/PC": "c37457607ab9", "throughput/C": "bd355c2d19b5",
		"cycle_time/P": "fcbd30ed501c", "cycle_time/none": "2a422ca81dad", "cycle_time/PC": "fcbd30ed501c", "cycle_time/C": "07a38fbfc4c0",
		"wip_saturation/P": "abc7b0523b9f", "wip_saturation/none": "4688de87990b", "wip_saturation/PC": "abc7b0523b9f", "wip_saturation/C": "fdba2e85a93e",
		"blocked_work/P": "60926ce2adc0", "blocked_work/none": "537d070e5715", "blocked_work/PC": "60926ce2adc0", "blocked_work/C": "27bfbb047197",
	}
}

// (1) Today's data holds no team row: every served field equals main's.
func TestExplainTeamViewNoOp(t *testing.T) {
	ctx, admin, reader := startExplainTeamView(t)
	t1, t2, t3 := explainTeamViewVersions()
	const org = "org-14379-explain-noop"
	seedExplainTeamView(ctx, t, admin, org, explainTeamViewOrgWriterDays(t1, t2, t3))
	// The key numbers, hand-computed: P, no team and [P, C] read the org form of P (only P has rows); C reads nothing.
	wantExplainTeamView(ctx, t, reader, org, "team P", []string{"P"}, explainTeamViewOrgFormOfP())
	wantExplainTeamView(ctx, t, reader, org, "no team", nil, explainTeamViewOrgFormOfP())
	wantExplainTeamView(ctx, t, reader, org, "teams P and C", []string{"P", "C"}, explainTeamViewOrgFormOfP())
	wantExplainTeamView(ctx, t, reader, org, "team C (no row)", []string{"C"}, explainTeamViewWant{})
	// Every served field: the whole JSON, against main's.
	pinned := explainTeamViewMainServed()
	for _, metric := range explainTeamViewMetrics() {
		for _, request := range explainTeamViewNoOpRequests() {
			served, err := json.Marshal(explainTeamViewRequest(ctx, t, reader, org, metric, request.teams))
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(served)
			key := metric + "/" + request.name
			t.Logf("SERVED %s %x %s", key, digest[:6], served)
			if got := fmt.Sprintf("%x", digest[:6]); got != pinned[key] {
				t.Errorf("served %s: digest %s, main's %q", key, got, pinned[key])
			}
		}
	}
}

// (2) A team row wins where one exists, for one team; a team set keeps the org form. One team is one answer: the
// headline, the contributor row and the driver row of that team read the same team view (headline = contributor =
// driver current). A team set and no team read the org form in all three.
func TestExplainTeamViewTeamRowWins(t *testing.T) {
	ctx, admin, reader := startExplainTeamView(t)
	t1, t2, t3 := explainTeamViewVersions()
	const org = "org-14379-explain-team"
	seedExplainTeamView(ctx, t, admin, org, append(explainTeamViewOrgWriterDays(t1, t2, t3),
		explainTeamViewRow{2, "P", 9, t2, "team"},
		explainTeamViewRow{1, "C", 2, t2, "team"}, explainTeamViewRow{2, "C", 3, t2, "team"},
	))
	// One team P: the headline reads P's D2 team row m=9 (live, and team > org), the D1 legacy row m=5:
	// delta (9 - 5) / 5 * 100 = 80. The contributor row reads the same 9 and the driver row 9 against 5 (delta 80).
	teamP := explainTeamViewWant{current: 9, prior: 5, contributor: 9, driver: [2]float64{9, 5}}
	wantExplainTeamView(ctx, t, reader, org, "team P", []string{"P"}, teamP)
	// One team C: the headline reads C's team rows, D2 m=3 and D1 m=2: delta (3 - 2) / 2 * 100 = 50. The contributor
	// row reads the same 3 and the driver row 3 against 2 (delta 50).
	wantExplainTeamView(ctx, t, reader, org, "team C", []string{"C"}, explainTeamViewWant{current: 3, prior: 2, contributor: 3, driver: [2]float64{3, 2}, team: "C"})
	// A team set [P, C] reads the org form: P's org row 4 and legacy 5; C has no org-form row.
	wantExplainTeamView(ctx, t, reader, org, "teams P and C (a team set: org form)", []string{"P", "C"}, explainTeamViewOrgFormOfP())
	// The team id list is a set: [P, P] is the one team P, [P, C, P] the team set [P, C].
	wantExplainTeamView(ctx, t, reader, org, "[P,P]", []string{"P", "P"}, teamP)
	wantExplainTeamView(ctx, t, reader, org, "[P,C,P]", []string{"P", "C", "P"}, explainTeamViewOrgFormOfP())
	// No team reads the org form too.
	wantExplainTeamView(ctx, t, reader, org, "no team", nil, explainTeamViewOrgFormOfP())
}

// (3) Live first: a retracted team row never hides the live org row.
func TestExplainTeamViewLiveFirst(t *testing.T) {
	ctx, admin, reader := startExplainTeamView(t)
	t1, t2, t3 := explainTeamViewVersions()
	const org = "org-14379-explain-live"
	seedExplainTeamView(ctx, t, admin, org, []explainTeamViewRow{
		{1, "P", 5, t1, ""},
		{2, "P", 4, t2, "org"}, {2, "P", 8, t1, "team"}, {2, "P", 0, t3, "team"},
	})
	// D2: the newest team row (t3) is a retraction row, so the live org row m=4 wins; D1 legacy m=5:
	// delta (4 - 5) / 5 * 100 = -20. The contributor and driver rows read the same rows.
	wantExplainTeamView(ctx, t, reader, org, "team P", []string{"P"}, explainTeamViewOrgFormOfP())
}

// (4) Two organizations never mix through the team view.
func TestExplainTeamViewTwoOrgs(t *testing.T) {
	ctx, admin, reader := startExplainTeamView(t)
	t1, t2, t3 := explainTeamViewVersions()
	const orgA, orgB = "org-14379-explain-two-a", "org-14379-explain-two-b"
	seedExplainTeamView(ctx, t, admin, orgA, append(explainTeamViewOrgWriterDays(t1, t2, t3), explainTeamViewRow{2, "P", 9, t2, "team"}))
	seedExplainTeamView(ctx, t, admin, orgB, []explainTeamViewRow{
		{1, "P", 50, t3, ""}, {2, "P", 40, t3, "org"}, {2, "P", 90, t3, "team"},
		{3, "P", 60, t3, "org"}, {3, "P", 70, t3, "team"},
		{1, "C", 20, t3, "team"}, {2, "C", 30, t3, "team"}, {3, "C", 10, t3, "team"},
	})
	// Org A team P: A's team row 9 against A's legacy 5 (delta 80); the contributor and driver rows read the same rows.
	wantExplainTeamView(ctx, t, reader, orgA, "org A team P", []string{"P"}, explainTeamViewWant{current: 9, prior: 5, contributor: 9, driver: [2]float64{9, 5}})
	// Org A team C: C has rows in org B only.
	wantExplainTeamView(ctx, t, reader, orgA, "org A team C (rows in org B only)", []string{"C"}, explainTeamViewWant{})
	// Org A no team: A's org form of P.
	wantExplainTeamView(ctx, t, reader, orgA, "org A no team", nil, explainTeamViewOrgFormOfP())
}

// The documented divergence (R2 of the plan): a live legacy row beside a live org row of one key, which no writer on main
// stores. The org form reads both (recorded, not a want). One team reads the org row (org > legacy, both live), and the
// one-team answer is one answer: headline = contributor = driver current.
func TestExplainTeamViewDivergence(t *testing.T) {
	ctx, admin, reader := startExplainTeamView(t)
	t1, t2, _ := explainTeamViewVersions()
	const org = "org-14379-explain-divergence"
	seedExplainTeamView(ctx, t, admin, org, []explainTeamViewRow{{1, "P", 5, t1, ""}, {2, "P", 7, t1, ""}, {2, "P", 4, t2, "org"}})
	// One team P: D2 org row m=4, D1 legacy m=5: headline 4 (delta (4 - 5) / 5 * 100 = -20), contributor 4, driver 4
	// against 5.
	wantExplainTeamView(ctx, t, reader, org, "R2 one team P", []string{"P"}, explainTeamViewOrgFormOfP())
	for _, metric := range explainTeamViewMetrics() {
		t.Logf("DIVERGENCE (recorded, not a want) %s one team P: %s", metric, explainTeamViewSummary(explainTeamViewRequest(ctx, t, reader, org, metric, []string{"P"})))
		t.Logf("DIVERGENCE (recorded, not a want) %s no team (org form): %s", metric, explainTeamViewSummary(explainTeamViewRequest(ctx, t, reader, org, metric, nil)))
	}
}
