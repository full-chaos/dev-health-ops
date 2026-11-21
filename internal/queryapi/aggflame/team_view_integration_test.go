//go:build integration

package aggflame

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The cells of the team view of the cycle breakdown flame (CHAOS-14379): BuildResponse in mode cycle_breakdown reads
// work_item_state_durations_daily (fetchCycleBreakdown). Params.TeamID is ONE team id (a single string), so there is
// no team set here: cell 5 (a team set stays org) and the [P,P] / [P,C,P] parts of cell 2 do not apply.
// Team P is the primary team; team C co-owns, so C has 'team' rows only. Days: D0 (only org B has rows there), D1 (a
// legacy row only), D2 (the org writer: legacy row live at t1, legacy retraction row of zeros at t3, org row at t2,
// scope row at t2). Every row has its own value, so each answer tells which row was read.

func flameTeamViewDay(daysAgo int) time.Time {
	return time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysAgo)
}

func flameTeamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

// D2 = 0 days ago, D1 = 1, D0 = 2. The window is [D0, D2+1).
const (
	flameD2 = 0
	flameD1 = 1
	flameD0 = 2
)

type flameTeamViewRow struct {
	daysAgo int
	team    string
	status  string
	hours   float64
	touched uint32
	at      time.Time
	view    string
}

// startFlameTeamView starts ClickHouse through crossorg.Start, so this tagged file imports no clickhouse-go package with
// an init (the side-effect census of the integration shards).
func startFlameTeamView(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	admin, client := crossorg.Start(ctx, t)
	return ctx, admin, client
}

// A retraction row is a row of zeros: the live-row rule of the table is duration_hours, items_touched or avg_wip != 0.
func seedFlameTeamView(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []flameTeamViewRow) {
	t.Helper()
	for _, row := range rows {
		if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
			(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?)`,
			flameTeamViewDay(row.daysAgo), row.team, row.team, row.status, row.hours, row.touched, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// flameOrgWriterDays: D1 holds a legacy row only; D2 holds the org writer's rows for two statuses of team P.
func flameOrgWriterDays(t1, t2, t3 time.Time) []flameTeamViewRow {
	return []flameTeamViewRow{
		{flameD1, "P", "in_progress", 3, 1, t1, ""},
		{flameD2, "P", "in_progress", 7, 2, t1, ""}, {flameD2, "P", "in_progress", 0, 0, t3, ""},
		{flameD2, "P", "in_progress", 40, 4, t2, "org"}, {flameD2, "P", "in_progress", 60, 6, t2, "scope"},
		{flameD2, "P", "blocked", 9, 1, t1, ""}, {flameD2, "P", "blocked", 0, 0, t3, ""},
		{flameD2, "P", "blocked", 20, 2, t2, "org"}, {flameD2, "P", "blocked", 30, 3, t2, "scope"},
	}
}

// flameServe is the served answer of one request: the whole Response.
func flameServe(ctx context.Context, t *testing.T, client QueryClient, org, team string) *Response {
	t.Helper()
	response, err := BuildResponse(ctx, client, org, Params{
		Mode: "cycle_breakdown", StartDay: flameTeamViewDay(flameD0), EndDay: flameTeamViewDay(flameD2).AddDate(0, 0, 1),
		TeamID: team, Limit: 100,
	})
	if err != nil {
		t.Fatalf("team %q: %v", team, err)
	}
	return response
}

// flameJSON is the JSON of the whole served Response. No field is blanked: the Response holds no wall-clock time and
// no random value (window_start/window_end come from the request, filters from the request, notes and approximation
// are fixed for this mode).
func flameJSON(t *testing.T, response *Response) string {
	t.Helper()
	served, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return string(served)
}

// flameNumbers is the key numbers of the flame as plain fields: each node as name=value, children in brackets.
func flameNumbers(node Node) string {
	text := fmt.Sprintf("%s=%g", node.Name, node.Value)
	if len(node.Children) == 0 {
		return text
	}
	parts := make([]string, len(node.Children))
	for i, child := range node.Children {
		parts[i] = flameNumbers(child)
	}
	return text + "[" + strings.Join(parts, " ") + "]"
}

func wantFlame(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %s, want %s", what, got, want)
	}
}

// (1) Today's data holds no team row: the whole served answer equals main's.
func TestAggflameTeamViewNoOp(t *testing.T) {
	ctx, admin, client := startFlameTeamView(t)
	t1, t2, t3 := flameTeamViewVersions()
	const org = "org-14379-flame-noop"
	seedFlameTeamView(ctx, t, admin, org, flameOrgWriterDays(t1, t2, t3))
	for _, tc := range []struct{ what, team, json, numbers string }{
		{"team P", "P", flameMainServedTeamP, "Cycle Time=63[Active Work=43[in_progress=43] Blocked=20[blocked=20]]"},
		{"no team", "", flameMainServedNoTeam, "Cycle Time=63[Active Work=43[in_progress=43] Blocked=20[blocked=20]]"},
		{"team C (no row)", "C", flameMainServedTeamC, "Cycle Time=0"},
	} {
		response := flameServe(ctx, t, client, org, tc.team)
		served := flameJSON(t, response)
		t.Logf("served %s: %s", tc.what, served)
		wantFlame(t, tc.what+" served JSON", served, tc.json)
		// Hand check of the numbers: in_progress = D1 legacy 3 + D2 org 40 = 43 (the D2 legacy row is retracted at
		// t3, the scope row is not read); blocked = D2 org 20; root = 43 + 20 = 63.
		wantFlame(t, tc.what+" numbers", flameNumbers(response.Root), tc.numbers)
	}
}

// pinned from main ab06c5087 (log ops2a2-aggflame-main-pin-1.log 7b1f29ffda86)
const (
	flameMainServedTeamP  = `{"mode":"cycle_breakdown","unit":"hours","root":{"name":"Cycle Time","value":63,"children":[{"name":"Active Work","value":43,"children":[{"name":"in_progress","value":43,"children":[]}]},{"name":"Blocked","value":20,"children":[{"name":"blocked","value":20,"children":[]}]}]},"meta":{"window_start":"2026-10-18","window_end":"2026-10-21","filters":{"team_id":"P"},"notes":[],"approximation":{"used":false,"method":null}}}`
	flameMainServedNoTeam = `{"mode":"cycle_breakdown","unit":"hours","root":{"name":"Cycle Time","value":63,"children":[{"name":"Active Work","value":43,"children":[{"name":"in_progress","value":43,"children":[]}]},{"name":"Blocked","value":20,"children":[{"name":"blocked","value":20,"children":[]}]}]},"meta":{"window_start":"2026-10-18","window_end":"2026-10-21","filters":{},"notes":[],"approximation":{"used":false,"method":null}}}`
	flameMainServedTeamC  = `{"mode":"cycle_breakdown","unit":"hours","root":{"name":"Cycle Time","value":0,"children":[]},"meta":{"window_start":"2026-10-18","window_end":"2026-10-21","filters":{"team_id":"C"},"notes":[],"approximation":{"used":false,"method":null}}}`
)

// (2) A team row wins where one exists, for one team; no team keeps the org form.
func TestAggflameTeamViewTeamRowWins(t *testing.T) {
	ctx, admin, client := startFlameTeamView(t)
	t1, t2, t3 := flameTeamViewVersions()
	const org = "org-14379-flame-team"
	seedFlameTeamView(ctx, t, admin, org, append(flameOrgWriterDays(t1, t2, t3),
		flameTeamViewRow{flameD2, "P", "in_progress", 50, 5, t2, "team"},
		flameTeamViewRow{flameD2, "C", "in_progress", 11, 1, t2, "team"},
		flameTeamViewRow{flameD2, "C", "review", 13, 1, t2, "team"},
	))
	// Team P: in_progress = D1 legacy 3 + D2 team 50 = 53; blocked = D2 org 20 (no team row); root = 53 + 20 = 73.
	wantFlame(t, "team P", flameNumbers(flameServe(ctx, t, client, org, "P").Root),
		"Cycle Time=73[Active Work=53[in_progress=53] Blocked=20[blocked=20]]")
	// Team C: in_progress = 11, review = 13 (team rows only); root = 24; categories sorted by value: Review first.
	wantFlame(t, "team C", flameNumbers(flameServe(ctx, t, client, org, "C").Root),
		"Cycle Time=24[Review=13[review=13] Active Work=11[in_progress=11]]")
	// No team: the org form never reads team rows: in_progress = 3 + 40 = 43, blocked = 20, root = 63.
	wantFlame(t, "no team (org form)", flameNumbers(flameServe(ctx, t, client, org, "").Root),
		"Cycle Time=63[Active Work=43[in_progress=43] Blocked=20[blocked=20]]")
	// Params.TeamID is one string: there is no team set, so [P,C], [P,P] and [P,C,P] do not apply to this package.
}

// (3) Live first: a retracted team row never hides the live org row.
func TestAggflameTeamViewLiveFirst(t *testing.T) {
	ctx, admin, client := startFlameTeamView(t)
	t1, t2, t3 := flameTeamViewVersions()
	const org = "org-14379-flame-live"
	seedFlameTeamView(ctx, t, admin, org, []flameTeamViewRow{
		{flameD2, "P", "in_progress", 40, 4, t2, "org"},
		{flameD2, "P", "in_progress", 50, 5, t1, "team"}, {flameD2, "P", "in_progress", 0, 0, t3, "team"},
	})
	// The newest team row (t3) is a row of zeros, so the live org row wins: in_progress = 40, root = 40.
	wantFlame(t, "team P", flameNumbers(flameServe(ctx, t, client, org, "P").Root),
		"Cycle Time=40[Active Work=40[in_progress=40]]")
}

// (4) Two organizations never mix through the team view.
func TestAggflameTeamViewTwoOrgs(t *testing.T) {
	ctx, admin, client := startFlameTeamView(t)
	t1, t2, t3 := flameTeamViewVersions()
	const orgA, orgB = "org-14379-flame-two-a", "org-14379-flame-two-b"
	seedFlameTeamView(ctx, t, admin, orgA, append(flameOrgWriterDays(t1, t2, t3),
		flameTeamViewRow{flameD2, "P", "in_progress", 50, 5, t2, "team"}))
	seedFlameTeamView(ctx, t, admin, orgB, []flameTeamViewRow{
		{flameD2, "P", "in_progress", 900, 9, t3, "team"}, {flameD2, "P", "in_progress", 400, 4, t3, "org"},
		{flameD2, "P", "blocked", 200, 2, t3, "org"},
		{flameD2, "C", "in_progress", 300, 3, t3, "team"}, {flameD2, "C", "review", 500, 5, t3, "team"},
		{flameD0, "P", "in_progress", 700, 7, t3, "team"}, {flameD0, "P", "waiting", 800, 8, t3, "org"},
	})
	// Org A team P: in_progress = 3 + team 50 = 53, blocked = org 20, root = 73 (no org B value, no D0). Red at main:
	// main's org form does not read org A's team row (43, as cell 2).
	wantFlame(t, "org A team P", flameNumbers(flameServe(ctx, t, client, orgA, "P").Root),
		"Cycle Time=73[Active Work=53[in_progress=53] Blocked=20[blocked=20]]")
	// Org A team C: org A holds no C row (only org B does): the empty tree.
	wantFlame(t, "org A team C (rows in org B only)", flameNumbers(flameServe(ctx, t, client, orgA, "C").Root),
		"Cycle Time=0")
	// Org A no team: in_progress = 3 + 40 = 43, blocked = 20, root = 63.
	wantFlame(t, "org A no team", flameNumbers(flameServe(ctx, t, client, orgA, "").Root),
		"Cycle Time=63[Active Work=43[in_progress=43] Blocked=20[blocked=20]]")
}

// (6) The documented divergence (R2 of the plan, not a WANT): a live legacy row beside a live org row of one key.
// The org form sums both views; the team form reads the org row. Both answers are recorded.
func TestAggflameTeamViewR2Divergence(t *testing.T) {
	ctx, admin, client := startFlameTeamView(t)
	t1, t2, _ := flameTeamViewVersions()
	const org = "org-14379-flame-divergence"
	seedFlameTeamView(ctx, t, admin, org, []flameTeamViewRow{
		{flameD2, "P", "in_progress", 7, 2, t1, ""}, {flameD2, "P", "in_progress", 40, 4, t2, "org"},
	})
	t.Logf("DIVERGENCE (recorded, not a want) one team P: %s", flameNumbers(flameServe(ctx, t, client, org, "P").Root))
	t.Logf("DIVERGENCE (recorded, not a want) no team (org form): %s", flameNumbers(flameServe(ctx, t, client, org, "").Root))
}
