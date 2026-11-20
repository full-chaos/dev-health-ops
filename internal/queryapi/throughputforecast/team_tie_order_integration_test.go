//go:build integration

package throughputforecast

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// The one-team form picks a key's version by the order of the org form (rowview.WorkItemMetricsNewestOrder): on a
// computed_at tie of two versions of one key, both forms serve the same version whatever the insert order
// (CHAOS-14405). Versions go in as separate INSERT statements into a table with merges stopped.

type tieRow struct {
	scope, team, view       string
	completed, wip, started uint32
	age                     *float64
	at                      time.Time
}

func tieInsert(org string, row tieRow) func(ctx context.Context, admin stdclickhouse.Conn) error {
	return func(ctx context.Context, admin stdclickhouse.Conn) error {
		var p90 *float64
		if row.age != nil {
			p90 = ageHours(*row.age + 1)
		}
		return admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, items_started,
			 wip_age_p50_hours, wip_age_p90_hours, computed_at, org_id, row_view)
			VALUES (?, 'jira', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			teamViewToday().AddDate(0, 0, -1), row.scope, row.team, row.team, row.completed, row.wip, row.started,
			row.age, p90, row.at, org, row.view)
	}
}

// servedFields is every value the five work-item reads serve, without the forecast's team id (the org form names none).
func servedFields(r teamViewReads) string {
	fields, _, _ := strings.Cut(r.String(), " team=")
	return fields
}

type tieHarness struct {
	ctx    context.Context
	admin  stdclickhouse.Conn
	client QueryClient
}

func startTieHarness(t *testing.T) tieHarness {
	t.Helper()
	ctx, admin, client := startThroughputForecastSchema(t)
	if err := admin.Exec(ctx, `SYSTEM STOP MERGES work_item_metrics_daily`); err != nil {
		t.Fatal(err)
	}
	return tieHarness{ctx: ctx, admin: admin, client: client}
}

// seed empties the table and inserts rows in the given order, one INSERT each.
func (h tieHarness) seed(t *testing.T, org string, rows []tieRow) {
	t.Helper()
	if err := h.admin.Exec(h.ctx, `TRUNCATE TABLE work_item_metrics_daily`); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := tieInsert(org, row)(h.ctx, h.admin); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

func reversed(rows []tieRow) []tieRow {
	out := make([]tieRow, len(rows))
	for i, row := range rows {
		out[len(rows)-1-i] = row
	}
	return out
}

// perOrder reads the team form (team-a) and the org form for the rows in insert order and in reverse order, 3 times
// each.
func (h tieHarness) perOrder(t *testing.T, org string, rows []tieRow, read func(teams ...string) string) (team, orgForm map[string]bool) {
	t.Helper()
	team, orgForm = map[string]bool{}, map[string]bool{}
	for o, order := range [][]tieRow{rows, reversed(rows)} {
		for rep := range 3 {
			h.seed(t, org, order)
			teamRead, orgRead := read("team-a"), read()
			t.Logf("order=%d rep=%d team: %s | org: %s", o, rep, teamRead, orgRead)
			if teamRead != orgRead {
				t.Errorf("order %d rep %d: the team form serves another version than the org form:\n team %s\n org  %s", o, rep, teamRead, orgRead)
			}
			team[teamRead], orgForm[orgRead] = true, true
		}
	}
	return team, orgForm
}

func keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// One ClickHouse for the cells of the one-team form's version order (CHAOS-14405); each cell has its own org and
// empties the table first.
func TestTeamFormPicksByTheOrgFormOrder(t *testing.T) {
	h := startTieHarness(t)
	for _, cell := range []struct {
		name string
		run  func(*testing.T, tieHarness)
	}{
		{"tie_team_equals_org", teamFormServesTheOrgFormVersionOnATie},
		{"tie_one_version_per_row", teamFormTieServesEveryColumnFromOneVersion},
		{"non_tie_answer_of_main", teamFormNonTieServesTheNewestVersion},
		{"newest_retraction_absent", teamFormNewestRetractionKeyStaysAbsent},
		{"view_rank_before_computed_at", teamFormViewRankBeatsComputedAt},
	} {
		t.Run(cell.name, func(t *testing.T) { cell.run(t, h) })
	}
}

// Four keys, each with two versions at one computed_at (two legacy, two org view): the team form serves, in both
// insert orders, the version the org form serves, in all five reads (history, average and current WIP, stale WIP,
// backlog).
func teamFormServesTheOrgFormVersionOnATie(t *testing.T, h tieHarness) {
	const org = "org-14405-tie"
	at := teamViewToday().Add(-12 * time.Hour)
	var rows []tieRow
	for k, view := range []string{"", "org", "", "org"} {
		scope := fmt.Sprintf("S%d", k+1)
		base := uint32(10 * (k + 1))
		rows = append(rows,
			tieRow{scope, "team-a", view, base + 1, base + 2, base + 3, ageHours(float64(base + 4)), at},
			tieRow{scope, "team-a", view, base + 5, base + 6, base + 7, ageHours(float64(base + 8)), at})
	}
	read := func(teams ...string) string {
		return servedFields(readTeamView(h.ctx, t, h.client, org, teams...))
	}
	team, orgForm := h.perOrder(t, org, rows, read)
	if len(team) != 1 || len(orgForm) != 1 {
		t.Errorf("one version per form whatever the insert order: team %v, org %v", keys(team), keys(orgForm))
	}
}

// One key, two versions at one computed_at with a different value in every column: every served column of the team
// form comes from one version, never items_completed of one and wip of the other.
func teamFormTieServesEveryColumnFromOneVersion(t *testing.T, h tieHarness) {
	const org = "org-14405-mixed"
	at := teamViewToday().Add(-12 * time.Hour)
	v1 := tieRow{"S1", "team-a", "", 1, 2, 3, ageHours(10), at}
	v2 := tieRow{"S1", "team-a", "", 5, 6, 7, ageHours(30), at}
	read := func(teams ...string) string {
		r := readTeamView(h.ctx, t, h.client, org, teams...)
		return fmt.Sprintf("history=%v currentWIP=%g stale=%g backlog=%d", r.History, r.CurrentWIP, *r.StaleP50, r.BacklogSize)
	}
	team, _ := h.perOrder(t, org, []tieRow{v1, v2}, read)
	allowed := map[string]bool{
		"history=[1] currentWIP=2 stale=10 backlog=2": true,
		"history=[5] currentWIP=6 stale=30 backlog=6": true,
	}
	for v := range team {
		if !allowed[v] {
			t.Errorf("the team form mixes columns of two versions: %s", v)
		}
	}
}

// No tie: a newer computed_at wins in both insert orders, and the served output of both forms is the answer of main.
// S1 has an older and a newer version, S2 one org-view version, S3 an older live version and a NEWEST retraction row.
func teamFormNonTieServesTheNewestVersion(t *testing.T, h tieHarness) {
	const org = "org-14405-control"
	older, newer := teamViewToday().Add(-13*time.Hour), teamViewToday().Add(-12*time.Hour)
	rows := []tieRow{
		{"S1", "team-a", "", 1, 2, 3, ageHours(10), older},
		{"S1", "team-a", "", 5, 6, 7, ageHours(30), newer},
		{"S2", "team-a", "org", 10, 20, 30, ageHours(100), newer},
		{"S3", "team-a", "", 7, 8, 9, ageHours(70), older},
		{"S3", "team-a", "", 0, 0, 0, nil, newer},
	}
	// Pinned from a run at main (aa7e4bb0cb76): every served field and the digest of the whole served forecast.
	want := map[string]string{
		"team": "history=[15] currentWIP=26 averageWIP=26 stale=65/66 backlog=26 MEASURED asOf=2026-10-19 team=\"team-a\" forecast=7c9c8570af1a",
		"org":  "history=[15] currentWIP=26 averageWIP=26 stale=65/66 backlog=26 MEASURED asOf=2026-10-19 team=\"\" forecast=1057c7aa2a9b",
	}
	for o, order := range [][]tieRow{rows, reversed(rows)} {
		for rep := range 3 {
			h.seed(t, org, order)
			for form, teams := range map[string][]string{"team": {"team-a"}, "org": nil} {
				got := readTeamView(h.ctx, t, h.client, org, teams...)
				t.Logf("order=%d rep=%d %s: %s", o, rep, form, got.String())
				wantTeamViewReads(t, fmt.Sprintf("order %d rep %d %s form", o, rep, form), got, want[form])
			}
		}
	}
}

// A key whose newest row is a retraction row gives what a key that was never stored gives, in the team form.
func teamFormNewestRetractionKeyStaysAbsent(t *testing.T, h tieHarness) {
	const org = "org-14405-retracted"
	older, newer := teamViewToday().Add(-13*time.Hour), teamViewToday().Add(-12*time.Hour)
	live := []tieRow{{"S1", "team-a", "", 5, 6, 7, ageHours(30), newer}}
	retracted := []tieRow{{"S2", "team-a", "", 7, 8, 9, ageHours(70), older}, {"S2", "team-a", "", 0, 0, 0, nil, newer}}
	h.seed(t, org, live)
	without := servedFields(readTeamView(h.ctx, t, h.client, org, "team-a"))
	h.seed(t, org, append(append([]tieRow{}, live...), retracted...))
	with := servedFields(readTeamView(h.ctx, t, h.client, org, "team-a"))
	if with != without {
		t.Errorf("a newest retraction row changed the team form:\n with    %s\n without %s", with, without)
	}
}

// The view rank still decides before computed_at: an older team row beats a newer org row of the same key, and a tie
// of two team rows serves one of them whatever the insert order.
func teamFormViewRankBeatsComputedAt(t *testing.T, h tieHarness) {
	const org = "org-14405-rank"
	older, newer := teamViewToday().Add(-13*time.Hour), teamViewToday().Add(-12*time.Hour)
	rows := []tieRow{
		{"S1", "team-a", "team", 2, 3, 4, ageHours(20), older},
		{"S1", "team-a", "team", 4, 5, 6, ageHours(40), older},
		{"S1", "team-a", "org", 9, 9, 9, ageHours(90), newer},
	}
	served := map[string]bool{}
	for _, order := range [][]tieRow{rows, reversed(rows)} {
		for range 3 {
			h.seed(t, org, order)
			r := readTeamView(h.ctx, t, h.client, org, "team-a")
			served[fmt.Sprint(r.History)] = true
		}
	}
	if len(served) != 1 || served["[9]"] {
		t.Errorf("the team form serves one team row whatever the insert order, never the newer org row: %v", keys(served))
	}
}
