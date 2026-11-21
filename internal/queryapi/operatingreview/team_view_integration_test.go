//go:build integration

package operatingreview

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// The cells of the team view of the one-team operating review reads (CHAOS-14379): fetchWorkItems
// (work_item_metrics_daily) and fetchStateDurations (work_item_state_durations_daily), read through ResolveInput.
// Team P is the primary team of scope S; team C co-owns some items, so C has 'team' rows only. D1 holds a legacy row
// only; D2 holds the rows of the org writer (the legacy row at t1, retracted with a row of zeros at t3, an org row and
// a scope row at t2) and, in the team data set, the team rows of the same keys. Every other table of the review is
// empty. Each seeded row goes into both tables (status 'doing'), with distinct values, so each answer tells which row
// was read. All computed_at values of one key differ (no tie cells).

// orTVWeek is the reviewed week start (Monday); the prior week is empty.
func orTVWeek() time.Time { return time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC) }

func orTVVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 21, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

// orTVRow is one row of both tables. day = days after the week start (D1 = 1, D2 = 2, D3 = 3). cycle and wipAge
// < 0 mean NULL. A retraction row has every measure 0 and the nullable hours NULL (liverow: not live).
type orTVRow struct {
	day            int
	team, view     string
	at             time.Time
	completed, wip uint32
	cycle, wipAge  float64
	touched        uint32
	duration       float64
}

func orTVNullable(v float64) any {
	if v < 0 {
		return nil
	}
	return v
}

func seedOrTV(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []orTVRow) {
	t.Helper()
	for _, row := range rows {
		day := orTVWeek().AddDate(0, 0, row.day)
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day,
			 cycle_time_p50_hours, wip_age_p90_hours, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			day, row.team, row.team, row.completed, row.wip, orTVNullable(row.cycle), orTVNullable(row.wipAge),
			row.at, org, row.view); err != nil {
			t.Fatalf("insert work item row %+v: %v", row, err)
		}
		if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
			(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, 'doing', ?, ?, ?, ?, ?)`,
			day, row.team, row.team, row.duration, row.touched, row.at, org, row.view); err != nil {
			t.Fatalf("insert state row %+v: %v", row, err)
		}
	}
}

// orTVServed is the served review of a team selection: the key fields as plain text (team, week, and value/hasData
// of the five metrics these two reads feed), the sha256 prefix (12 hex) of the JSON of the WHOLE served
// *model.OperatingReview, and that JSON. No field is blanked: the review holds no wall-clock time and no random id
// (the request instant only picks incident mappings, and no incident is seeded).
func orTVServed(ctx context.Context, t *testing.T, client QueryClient, org string, teamID *string, teamIDs ...string) (fields, digest, raw string) {
	t.Helper()
	review, err := ResolveInput(ctx, client, org, teamID, teamIDs, graphqldate.New(orTVWeek()))
	if err != nil {
		t.Fatalf("resolve teamID=%v teamIDs=%v: %v", teamID, teamIDs, err)
	}
	team := "nil"
	if review.TeamID != nil {
		team = *review.TeamID
	}
	parts := []string{"team=" + team, "week=" + review.WeekStart.String() + "/" + review.PriorWeekStart.String()}
	for _, key := range []string{"throughput", "wip_count", "cycle_time_p50_hours", "wip_age_p90_hours", "state_duration_hours"} {
		found := ""
		for _, s := range review.Sections {
			for _, m := range s.Metrics {
				if m.Key == key {
					found = fmt.Sprintf("%s=%g/%v", key, m.Value, m.HasData)
				}
			}
		}
		if found == "" {
			found = key + "=missing"
		}
		parts = append(parts, found)
	}
	body, err := json.Marshal(review)
	if err != nil {
		t.Fatalf("marshal review: %v", err)
	}
	sum := sha256.Sum256(body)
	return strings.Join(parts, " "), fmt.Sprintf("%x", sum[:6]), string(body)
}

func orTVTeam(id string) *string { return &id }

// wantOrTV compares the key fields of one selection.
func wantOrTV(ctx context.Context, t *testing.T, client QueryClient, org, what, want string, teamID *string, teamIDs ...string) {
	t.Helper()
	got, _, _ := orTVServed(ctx, t, client, org, teamID, teamIDs...)
	if got != want {
		t.Errorf("%s:\n got %s\nwant %s", what, got, want)
	}
}

// orTVOrgWriterDays is today's data: D1 legacy only, D2 the org writer pattern. No team row.
func orTVOrgWriterDays(t1, t2, t3 time.Time) []orTVRow {
	return []orTVRow{
		{1, "P", "", t1, 2, 1, 10, 100, 3, 20},
		{2, "P", "", t1, 7, 5, 70, 700, 8, 80},
		{2, "P", "", t3, 0, 0, -1, -1, 0, 0},
		{2, "P", "org", t2, 4, 3, 40, 400, 5, 50},
		{2, "P", "scope", t2, 6, 4, 60, 600, 6, 65},
	}
}

// The team rows of the team data set: P on D2 (differs from org), C on D1 and D2.
func orTVTeamRows(t1, t2 time.Time) []orTVRow {
	return []orTVRow{
		{2, "P", "team", t2, 9, 6, 90, 900, 9, 90},
		{1, "C", "team", t1, 1, 1, 15, 150, 2, 15},
		{2, "C", "team", t2, 3, 2, 30, 300, 4, 30},
	}
}

// The org form of today's data: D1 legacy (2, 1, 10, 100; touched 3, duration 20) and D2 org (4, 3, 40, 400; 5, 50);
// the D2 legacy key's newest row is the retraction, the scope row is not in the org view.
// throughput 2+4 = 6, wip max(1,3) = 3, cycle avg(10,40) = 25, wip age avg(100,400) = 250,
// state duration avg(20,50) = 35 (one status, so the weighted mean is that status's mean).
const orTVOrgForm = "week=2026-10-19/2026-10-12 throughput=6/true wip_count=3/true cycle_time_p50_hours=25/true wip_age_p90_hours=250/true state_duration_hours=35/true"

// No row of the selection: every metric 0 with hasData false.
const orTVNoRows = "week=2026-10-19/2026-10-12 throughput=0/false wip_count=0/false cycle_time_p50_hours=0/false wip_age_p90_hours=0/false state_duration_hours=0/false"

// One team P with its team row: D1 legacy (2, 1, 10, 100; 3, 20) and D2 team (9, 6, 90, 900; 9, 90).
// throughput 2+9 = 11, wip max(1,6) = 6, cycle avg(10,90) = 50, wip age avg(100,900) = 500, state avg(20,90) = 55.
const orTVTeamP = "week=2026-10-19/2026-10-12 throughput=11/true wip_count=6/true cycle_time_p50_hours=50/true wip_age_p90_hours=500/true state_duration_hours=55/true"

// One team C: D1 team (1, 1, 15, 150; 2, 15) and D2 team (3, 2, 30, 300; 4, 30).
// throughput 1+3 = 4, wip max(1,2) = 2, cycle avg(15,30) = 22.5, wip age avg(150,300) = 225, state avg(15,30) = 22.5.
const orTVTeamC = "week=2026-10-19/2026-10-12 throughput=4/true wip_count=2/true cycle_time_p50_hours=22.5/true wip_age_p90_hours=225/true state_duration_hours=22.5/true"

// The whole served review on today's data, pinned from main ab06c5087 (log ops2a2-operatingreview-mainpin-1.log
// 033474b2a9fe): sha256 prefix of the JSON of *model.OperatingReview (see orTVServed; no blanked field).
// weekStart and priorWeekStart marshal as {} in that JSON, so the key fields carry them as text.
const (
	orTVMainServedTeamIDP  = "76f04dd738c6"
	orTVMainServedTeamIDsP = "76f04dd738c6"
	orTVMainServedNoTeam   = "67a7e217be9e"
	orTVMainServedTeamsPC  = "67a7e217be9e"
	orTVMainServedTeamC    = "3e4366152e9a"
)

// (1) Today's data holds no team row: the whole served review equals main's.
func TestOperatingReviewTeamViewNoOp(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	t1, t2, t3 := orTVVersions()
	const org = "org-14379-or-noop"
	seedOrTV(ctx, t, admin, org, orTVOrgWriterDays(t1, t2, t3))
	cells := []struct {
		what, fields, digest string
		teamID               *string
		teamIDs              []string
	}{
		{"team P (teamId)", "team=P " + orTVOrgForm, orTVMainServedTeamIDP, orTVTeam("P"), nil},
		{"team P (teamIds)", "team=P " + orTVOrgForm, orTVMainServedTeamIDsP, nil, []string{"P"}},
		{"no team", "team=nil " + orTVOrgForm, orTVMainServedNoTeam, nil, nil},
		{"teams P and C", "team=nil " + orTVOrgForm, orTVMainServedTeamsPC, nil, []string{"P", "C"}},
		{"team C (no row)", "team=C " + orTVNoRows, orTVMainServedTeamC, nil, []string{"C"}},
	}
	for _, c := range cells {
		fields, digest, raw := orTVServed(ctx, t, client, org, c.teamID, c.teamIDs...)
		t.Logf("SERVED %s digest=%s fields=%s json=%s", c.what, digest, fields, raw)
		if fields != c.fields {
			t.Errorf("%s fields:\n got %s\nwant %s", c.what, fields, c.fields)
		}
		if digest != c.digest {
			t.Errorf("%s whole served review: got digest %s, want %s (main)", c.what, digest, c.digest)
		}
	}
}

// (2) A team row wins for one team; a team set keeps the org form. RED at main (team rows are not in its org form).
func TestOperatingReviewTeamViewTeamRowWins(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	t1, t2, t3 := orTVVersions()
	const org = "org-14379-or-team"
	seedOrTV(ctx, t, admin, org, append(orTVOrgWriterDays(t1, t2, t3), orTVTeamRows(t1, t2)...))
	wantOrTV(ctx, t, client, org, "team P (teamId)", "team=P "+orTVTeamP, orTVTeam("P"))
	wantOrTV(ctx, t, client, org, "team P (teamIds)", "team=P "+orTVTeamP, nil, "P")
	wantOrTV(ctx, t, client, org, "team C", "team=C "+orTVTeamC, nil, "C")
	// A team set and no team: the org form; C has no org or legacy row, so [P,C] = P's org form.
	wantOrTV(ctx, t, client, org, "teams P and C (org form)", "team=nil "+orTVOrgForm, nil, "P", "C")
	wantOrTV(ctx, t, client, org, "no team (org form)", "team=nil "+orTVOrgForm, nil)
	// The team id list is a set: [P,P] = [P], [P,C,P] = [P,C].
	wantOrTV(ctx, t, client, org, "[P,P]", "team=P "+orTVTeamP, nil, "P", "P")
	wantOrTV(ctx, t, client, org, "[P,C,P]", "team=nil "+orTVOrgForm, nil, "P", "C", "P")
}

// (3) Live first: a retracted team row never hides the live org row.
func TestOperatingReviewTeamViewLiveFirst(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	t1, t2, t3 := orTVVersions()
	const org = "org-14379-or-live"
	seedOrTV(ctx, t, admin, org, []orTVRow{
		{2, "P", "org", t2, 4, 3, 40, 400, 5, 50},
		{2, "P", "team", t1, 8, 7, 80, 800, 8, 80},
		{2, "P", "team", t3, 0, 0, -1, -1, 0, 0},
	})
	// The newest team row (t3) is retracted, so the live org row (t2) is read: throughput 4, wip 3, cycle 40,
	// wip age 400, state duration 50.
	wantOrTV(ctx, t, client, org, "team P", "team=P week=2026-10-19/2026-10-12 throughput=4/true wip_count=3/true cycle_time_p50_hours=40/true wip_age_p90_hours=400/true state_duration_hours=50/true", nil, "P")
}

// (4) Two organizations never mix through the team view.
func TestOperatingReviewTeamViewTwoOrgs(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	t1, t2, t3 := orTVVersions()
	const orgA, orgB = "org-14379-or-two-a", "org-14379-or-two-b"
	seedOrTV(ctx, t, admin, orgA, append(orTVOrgWriterDays(t1, t2, t3), orTVTeamRows(t1, t2)...))
	// Org B: the same keys with other values (team rows of P and C included) and a day of its own (D3).
	seedOrTV(ctx, t, admin, orgB, []orTVRow{
		{1, "P", "", t1, 20, 20, 200, 2000, 20, 200},
		{2, "P", "org", t3, 40, 30, 400, 4000, 40, 400},
		{2, "P", "team", t3, 90, 60, 900, 9000, 90, 900},
		{1, "C", "team", t3, 10, 10, 150, 1500, 10, 150},
		{2, "C", "team", t3, 30, 20, 300, 3000, 30, 300},
		{3, "P", "team", t3, 70, 50, 700, 7000, 70, 700},
		{3, "P", "org", t3, 75, 55, 750, 7500, 75, 750},
	})
	// Org A's rows only: the same answers as cell 2.
	wantOrTV(ctx, t, client, orgA, "org A team P", "team=P "+orTVTeamP, nil, "P")
	wantOrTV(ctx, t, client, orgA, "org A team C", "team=C "+orTVTeamC, nil, "C")
	wantOrTV(ctx, t, client, orgA, "org A no team", "team=nil "+orTVOrgForm, nil)
}

// (6) The documented divergence (R2 of the plan, not a WANT): a live legacy row beside a live org row of one key.
func TestOperatingReviewTeamViewDivergence(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	t1, t2, _ := orTVVersions()
	const org = "org-14379-or-divergence"
	seedOrTV(ctx, t, admin, org, []orTVRow{
		{2, "P", "", t1, 7, 5, 70, 700, 8, 80},
		{2, "P", "org", t2, 4, 3, 40, 400, 5, 50},
	})
	one, _, _ := orTVServed(ctx, t, client, org, nil, "P")
	all, _, _ := orTVServed(ctx, t, client, org, nil)
	t.Logf("DIVERGENCE (recorded, not a want) one team P: %s", one)
	t.Logf("DIVERGENCE (recorded, not a want) no team (org form): %s", all)
}

// A computed_at tie (CHAOS-14405 order): two versions of one org key share computed_at. The one-team form picks the
// version by the org form's own order (workItemsVersionOrder, stateDurationsVersionOrder), so it serves the version
// the org form serves, in either insert order (merges stopped).
func TestOperatingReviewTeamViewTieFollowsTheOrgFormOrder(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	// Merges stopped: a merge keeps the last inserted row of a tie and so would decide the version for both forms.
	for _, table := range []string{"work_item_metrics_daily", "work_item_state_durations_daily"} {
		if err := admin.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	t1, t2, _ := orTVVersions()
	versionA := orTVRow{2, "P", "org", t2, 4, 3, 40, 400, 5, 50}
	versionB := orTVRow{2, "P", "org", t2, 9, 6, 90, 900, 9, 90}
	legacy := orTVRow{1, "P", "", t1, 2, 1, 10, 100, 3, 20}
	values := func(fields string) string { return fields[strings.Index(fields, " ")+1:] }
	var served []string
	for i, rows := range [][]orTVRow{{legacy, versionA, versionB}, {legacy, versionB, versionA}} {
		org := fmt.Sprintf("org-14379-or-tie-%d", i)
		seedOrTV(ctx, t, admin, org, rows)
		oneTeam, _, _ := orTVServed(ctx, t, client, org, nil, "P")
		orgForm, _, _ := orTVServed(ctx, t, client, org, nil, "P", "Q")
		if values(oneTeam) != values(orgForm) {
			t.Errorf("insert order %d: one team %s, org form %s: the tie serves another version", i, oneTeam, orgForm)
		}
		served = append(served, values(oneTeam))
	}
	if served[0] != served[1] {
		t.Errorf("the tie depends on the insert order: %s vs %s", served[0], served[1])
	}
	t.Logf("tie served: %s", served[0])
}
