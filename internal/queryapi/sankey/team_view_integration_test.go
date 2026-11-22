//go:build integration

package sankey

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the team view of the one-team sankey reads (CHAOS-14379), through the served entry point
// BuildResponse: mode "expense" (work_item_metrics_daily, plus the unchanged work_item_cycle_times read) and mode
// "state" (work_item_state_durations_daily). Team P is the primary team of the items of scope S; team C co-owns some
// of them, so C has 'team' rows only. The older day (D1) holds a legacy row only; the newer day (D2) holds the rows of
// the org writer (a legacy row, that legacy row retracted with a row of zeros, an org row and a scope row) and, in the
// team data set, the team rows of the same keys.
//
// One seeded row with value v is one row of each table:
//   - work_item_metrics_daily: new_bugs_count = new_items_count = items_completed = v, bug_completed_ratio 0.5
//     (0 for v = 0, so a row of zeros is a retraction row: every measure 0, every nullable measure NULL);
//   - work_item_state_durations_daily: four status keys, items_touched todo 4v, in_progress 3v, in_review 2v, done v,
//     duration_hours = avg_wip = v (all 0 for v = 0: a retraction row).
//
// So for a sum S of the v values a read takes:
//   - expense: Planned->Unplanned = S, Unplanned->Rework = min(S, S/2) = S/2, Rework->Abandonment = min(S/2, canceled),
//     canceled = the canceled work_item_cycle_times items of the selection (P 2, C 1; a read without a team: 3);
//   - state: Todo->In Progress = min(4S, 3S) = 3S, In Progress->In Review = min(3S, 2S) = 2S, In Review->Done =
//     min(2S, S) = S. A link of value 0 is dropped.

func sankeyTVToday() time.Time { return time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC) }

func sankeyTVVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

type sankeyTVRow struct {
	daysAgo int
	team    string
	v       uint32
	at      time.Time
	view    string
}

func startSankeyTV(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	return ctx, admin, client
}

func seedSankeyTV(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []sankeyTVRow) {
	t.Helper()
	for _, row := range rows {
		day := sankeyTVToday().AddDate(0, 0, -row.daysAgo)
		ratio := 0.5
		if row.v == 0 {
			ratio = 0
		}
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, new_bugs_count, new_items_count, items_completed, bug_completed_ratio, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			day, row.team, row.team, row.v, row.v, row.v, ratio, row.at, org, row.view); err != nil {
			t.Fatalf("insert metrics %+v: %v", row, err)
		}
		for status, factor := range map[string]uint32{"todo": 4, "in_progress": 3, "in_review": 2, "done": 1} {
			if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
				(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at, org_id, row_view)
				VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				day, row.team, row.team, status, float64(row.v), factor*row.v, float64(row.v), row.at, org, row.view); err != nil {
				t.Fatalf("insert state %s %+v: %v", status, row, err)
			}
		}
	}
}

// seedSankeyTVCycles stores the canceled items the expense flow reads (unchanged read): canceledP items of team P and
// canceledC items of team C on D2, plus one done item of P that does not count.
func seedSankeyTVCycles(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, canceledP, canceledC int) {
	t.Helper()
	_, t2, _ := sankeyTVVersions()
	insert := func(id, team, status string) {
		if err := admin.Exec(ctx, `INSERT INTO work_item_cycle_times
			(work_item_id, provider, day, work_scope_id, team_id, team_name, type, status, created_at, computed_at, org_id)
			VALUES (?, 'jira', ?, 'S', ?, ?, 'task', ?, ?, ?, ?)`,
			id, sankeyTVToday().AddDate(0, 0, -1), team, team, status, t2, t2, org); err != nil {
			t.Fatalf("insert cycle time %s: %v", id, err)
		}
	}
	for i := 0; i < canceledP; i++ {
		insert(fmt.Sprintf("%s-P-%d", org, i), "P", "canceled")
	}
	for i := 0; i < canceledC; i++ {
		insert(fmt.Sprintf("%s-C-%d", org, i), "C", "canceled")
	}
	insert(org+"-P-done", "P", "done")
}

// sankeyTVOrgWriterDays is today's data: D1 (2 days ago) a legacy row only; D2 (1 day ago) the org writer's rows.
func sankeyTVOrgWriterDays(t1, t2, t3 time.Time) []sankeyTVRow {
	return []sankeyTVRow{
		{2, "P", 2, t1, ""},
		{1, "P", 7, t1, ""}, {1, "P", 0, t3, ""},
		{1, "P", 4, t2, "org"}, {1, "P", 6, t2, "scope"},
	}
}

// sankeyTVServe is the served answer of one mode for a team selection (no team = org scope).
func sankeyTVServe(ctx context.Context, t *testing.T, client QueryClient, org, mode string, teams ...string) *Response {
	t.Helper()
	params := Params{Mode: mode, StartDay: sankeyTVToday().AddDate(0, 0, -10), EndDay: sankeyTVToday().AddDate(0, 0, 1)}
	if len(teams) > 0 {
		params.ScopeLevel = "team"
		params.ScopeIDs = teams
	}
	resp, err := BuildResponse(ctx, client, org, params)
	if err != nil {
		t.Fatalf("%s %v: %v", mode, teams, err)
	}
	return resp
}

// sankeyTVSummary is the key numbers of an answer: every node (name/group) and every link (source>target=value), in
// served order.
func sankeyTVSummary(resp *Response) string {
	nodes := make([]string, 0, len(resp.Nodes))
	for _, node := range resp.Nodes {
		group := "nil"
		if node.Group != nil {
			group = *node.Group
		}
		nodes = append(nodes, node.Name+"/"+group)
	}
	links := make([]string, 0, len(resp.Links))
	for _, link := range resp.Links {
		links = append(links, fmt.Sprintf("%s>%s=%g", link.Source, link.Target, link.Value))
	}
	return fmt.Sprintf("nodes=[%s] links=[%s]", strings.Join(nodes, ", "), strings.Join(links, ", "))
}

// sankeyTVServed is the key numbers plus the sha256 digest prefix of the JSON of the WHOLE served Response. No field
// is blanked: the Response holds no random id and no wall-clock time.
func sankeyTVServed(t *testing.T, resp *Response) string {
	t.Helper()
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	t.Logf("served JSON: %s", body)
	return fmt.Sprintf("%s sha=%x", sankeyTVSummary(resp), digest[:6])
}

func wantSankeyTV(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s:\n got  %s\n want %s", what, got, want)
	}
}

// Hand-computed answers (see the file comment for the arithmetic).
const (
	// S = 6, canceled 2.
	sankeyTVExpenseS6P = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[Planned work>Unplanned work=6, Unplanned work>Rework=3, Rework>Abandonment / rewrite=2]"
	// S = 6, canceled 3 (P and C).
	sankeyTVExpenseS6All = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[Planned work>Unplanned work=6, Unplanned work>Rework=3, Rework>Abandonment / rewrite=3]"
	// S = 11, canceled 2: min(5.5, 2) = 2.
	sankeyTVExpenseS11P = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[Planned work>Unplanned work=11, Unplanned work>Rework=5.5, Rework>Abandonment / rewrite=2]"
	// S = 8, canceled 1: min(4, 1) = 1.
	sankeyTVExpenseS8C = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[Planned work>Unplanned work=8, Unplanned work>Rework=4, Rework>Abandonment / rewrite=1]"
	// S = 0: the four nodes, no link.
	sankeyTVExpenseNone = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[]"
	// S = 6: 18, 12, 6.
	sankeyTVStateS6 = "nodes=[Todo/state, In Progress/state, In Review/state, Done/state] links=[Todo>In Progress=18, In Progress>In Review=12, In Review>Done=6]"
	// S = 11: 33, 22, 11.
	sankeyTVStateS11 = "nodes=[Todo/state, In Progress/state, In Review/state, Done/state] links=[Todo>In Progress=33, In Progress>In Review=22, In Review>Done=11]"
	// S = 8: 24, 16, 8.
	sankeyTVStateS8 = "nodes=[Todo/state, In Progress/state, In Review/state, Done/state] links=[Todo>In Progress=24, In Progress>In Review=16, In Review>Done=8]"
	// No live row: no status, no link.
	sankeyTVStateNone = "nodes=[] links=[]"
)

// (1) Today's data holds no team row: the whole served Response of each mode equals main's.
// Org form S = D1 legacy 2 + D2 legacy retraction 0 + D2 org 4 = 6 (the scope row 6 and the retracted legacy 7 are
// never read). One team P on head: D1 legacy 2 + D2 live org row 4 (the newest legacy row is retracted) = 6.
func TestSankeyTeamViewNoOp(t *testing.T) {
	ctx, admin, client := startSankeyTV(t)
	t1, t2, t3 := sankeyTVVersions()
	const org = "org-14379-sankey-noop"
	seedSankeyTV(ctx, t, admin, org, sankeyTVOrgWriterDays(t1, t2, t3))
	seedSankeyTVCycles(ctx, t, admin, org, 2, 1)
	cells := []struct {
		what, mode string
		teams      []string
		want       string
	}{
		{"expense team P", "expense", []string{"P"}, sankeyTVMainExpenseTeamP},
		{"expense no team", "expense", nil, sankeyTVMainExpenseNoTeam},
		{"expense teams P and C", "expense", []string{"P", "C"}, sankeyTVMainExpenseTeamsPC},
		{"expense team C (no row)", "expense", []string{"C"}, sankeyTVMainExpenseTeamC},
		{"state team P", "state", []string{"P"}, sankeyTVMainStateTeamP},
		{"state no team", "state", nil, sankeyTVMainStateNoTeam},
		{"state teams P and C", "state", []string{"P", "C"}, sankeyTVMainStateTeamsPC},
		{"state team C (no row)", "state", []string{"C"}, sankeyTVMainStateTeamC},
	}
	for _, cell := range cells {
		got := sankeyTVServed(t, sankeyTVServe(ctx, t, client, org, cell.mode, cell.teams...))
		t.Logf("PIN %s: %s", cell.what, got)
		wantSankeyTV(t, cell.what, got, cell.want)
	}
	// The key numbers as plain fields too (hand-checked against the arithmetic above).
	wantSankeyTV(t, "expense team P numbers", sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "expense", "P")), sankeyTVExpenseS6P)
	wantSankeyTV(t, "expense no team numbers", sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "expense")), sankeyTVExpenseS6All)
	wantSankeyTV(t, "state team P numbers", sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "state", "P")), sankeyTVStateS6)
	wantSankeyTV(t, "state team C numbers", sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "state", "C")), sankeyTVStateNone)
}

// Pinned from main ab06c5087 (log ops2a2-sankey-mainpin-1.log ee2b92df4de7). Key numbers plus the sha256 prefix of the
// whole served Response JSON (json.Marshal of *Response; no field blanked).
const (
	sankeyTVMainExpenseTeamP   = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[Planned work>Unplanned work=6, Unplanned work>Rework=3, Rework>Abandonment / rewrite=2] sha=bf51654c1985"
	sankeyTVMainExpenseNoTeam  = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[Planned work>Unplanned work=6, Unplanned work>Rework=3, Rework>Abandonment / rewrite=3] sha=20745e7745d8"
	sankeyTVMainExpenseTeamsPC = sankeyTVMainExpenseNoTeam
	sankeyTVMainExpenseTeamC   = "nodes=[Planned work/planned, Unplanned work/unplanned, Rework/rework, Abandonment / rewrite/abandonment] links=[] sha=8abc377dc0ba"
	sankeyTVMainStateTeamP     = "nodes=[Todo/state, In Progress/state, In Review/state, Done/state] links=[Todo>In Progress=18, In Progress>In Review=12, In Review>Done=6] sha=3b438aa6f943"
	sankeyTVMainStateNoTeam    = sankeyTVMainStateTeamP
	sankeyTVMainStateTeamsPC   = sankeyTVMainStateTeamP
	sankeyTVMainStateTeamC     = "nodes=[] links=[] sha=f1164efc5640"
)

// (2) A team row wins where one exists, for one team; a team set keeps the org form. The id list is a set:
// [P,P] = [P], [P,C,P] = [P,C].
//   - one team P: D1 legacy 2 (no team row that day) + D2 team row 9 = 11;
//   - one team C: C's team rows D1 5 + D2 3 = 8, canceled 1;
//   - team set [P,C] (org form, legacy and org views only; C has none): S = 6, canceled 2 + 1 = 3.
func TestSankeyTeamViewTeamRowWins(t *testing.T) {
	ctx, admin, client := startSankeyTV(t)
	t1, t2, t3 := sankeyTVVersions()
	const org = "org-14379-sankey-team"
	seedSankeyTV(ctx, t, admin, org, append(sankeyTVOrgWriterDays(t1, t2, t3),
		sankeyTVRow{1, "P", 9, t2, "team"},
		sankeyTVRow{2, "C", 5, t2, "team"},
		sankeyTVRow{1, "C", 3, t2, "team"},
	))
	seedSankeyTVCycles(ctx, t, admin, org, 2, 1)
	for _, cell := range []struct {
		what                 string
		teams                []string
		wantExpense, wantSte string
	}{
		{"team P", []string{"P"}, sankeyTVExpenseS11P, sankeyTVStateS11},
		{"team C", []string{"C"}, sankeyTVExpenseS8C, sankeyTVStateS8},
		{"teams P and C (a team set: org form)", []string{"P", "C"}, sankeyTVExpenseS6All, sankeyTVStateS6},
		{"[P,P]", []string{"P", "P"}, sankeyTVExpenseS11P, sankeyTVStateS11},
		{"[P,C,P]", []string{"P", "C", "P"}, sankeyTVExpenseS6All, sankeyTVStateS6},
	} {
		wantSankeyTV(t, "expense "+cell.what, sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "expense", cell.teams...)), cell.wantExpense)
		wantSankeyTV(t, "state "+cell.what, sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "state", cell.teams...)), cell.wantSte)
	}
}

// (3) Live first: on D2 an org row 4 (t2, live), a team row 8 (t1, live) and a team retraction row (t3). The newest
// team row is retracted, so one team P reads the live org row: S = D1 legacy 2 + 4 = 6 (not 2 + 8 = 10, not 2).
func TestSankeyTeamViewLiveFirst(t *testing.T) {
	ctx, admin, client := startSankeyTV(t)
	t1, t2, t3 := sankeyTVVersions()
	const org = "org-14379-sankey-live"
	seedSankeyTV(ctx, t, admin, org, []sankeyTVRow{
		{2, "P", 2, t1, ""},
		{1, "P", 8, t1, "team"}, {1, "P", 4, t2, "org"}, {1, "P", 0, t3, "team"},
	})
	seedSankeyTVCycles(ctx, t, admin, org, 2, 1)
	wantSankeyTV(t, "expense team P", sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "expense", "P")), sankeyTVExpenseS6P)
	wantSankeyTV(t, "state team P", sankeyTVSummary(sankeyTVServe(ctx, t, client, org, "state", "P")), sankeyTVStateS6)
}

// (4) Two organizations never mix through the team view. Org A: today's data plus P's team row 9 on D2. Org B: the
// same keys with other values (team rows of P and C, an org row of P), a day of its own (today) and 5 + 4 canceled
// items. Org A answers: team P = 2 + 9 = 11 (canceled 2); team C = no row (C's rows are in org B only); no team = 6
// (canceled 3).
func TestSankeyTeamViewTwoOrgs(t *testing.T) {
	ctx, admin, client := startSankeyTV(t)
	t1, t2, t3 := sankeyTVVersions()
	const orgA, orgB = "org-14379-sankey-two-a", "org-14379-sankey-two-b"
	seedSankeyTV(ctx, t, admin, orgA, append(sankeyTVOrgWriterDays(t1, t2, t3), sankeyTVRow{1, "P", 9, t2, "team"}))
	seedSankeyTVCycles(ctx, t, admin, orgA, 2, 1)
	seedSankeyTV(ctx, t, admin, orgB, []sankeyTVRow{
		{1, "P", 90, t3, "team"}, {1, "P", 40, t3, "org"}, {0, "P", 70, t3, "team"}, {1, "C", 30, t3, "team"}, {2, "C", 50, t3, "team"},
	})
	seedSankeyTVCycles(ctx, t, admin, orgB, 5, 4)
	wantSankeyTV(t, "org A expense team P", sankeyTVSummary(sankeyTVServe(ctx, t, client, orgA, "expense", "P")), sankeyTVExpenseS11P)
	wantSankeyTV(t, "org A state team P", sankeyTVSummary(sankeyTVServe(ctx, t, client, orgA, "state", "P")), sankeyTVStateS11)
	wantSankeyTV(t, "org A expense team C", sankeyTVSummary(sankeyTVServe(ctx, t, client, orgA, "expense", "C")), sankeyTVExpenseNone)
	wantSankeyTV(t, "org A state team C", sankeyTVSummary(sankeyTVServe(ctx, t, client, orgA, "state", "C")), sankeyTVStateNone)
	wantSankeyTV(t, "org A expense no team", sankeyTVSummary(sankeyTVServe(ctx, t, client, orgA, "expense")), sankeyTVExpenseS6All)
	wantSankeyTV(t, "org A state no team", sankeyTVSummary(sankeyTVServe(ctx, t, client, orgA, "state")), sankeyTVStateS6)
}

// The documented divergence (R2 of the plan, not a WANT): a live legacy row (7, t1) beside a live org row (4, t2) of
// one key, which no writer on main stores. The org form sums both; the team form reads the org row. Both are recorded.
func TestSankeyTeamViewDivergence(t *testing.T) {
	ctx, admin, client := startSankeyTV(t)
	t1, t2, _ := sankeyTVVersions()
	const org = "org-14379-sankey-divergence"
	seedSankeyTV(ctx, t, admin, org, []sankeyTVRow{{1, "P", 7, t1, ""}, {1, "P", 4, t2, "org"}})
	seedSankeyTVCycles(ctx, t, admin, org, 2, 1)
	for _, mode := range []string{"expense", "state"} {
		t.Logf("DIVERGENCE (recorded, not a want) %s one team P: %s", mode, sankeyTVSummary(sankeyTVServe(ctx, t, client, org, mode, "P")))
		t.Logf("DIVERGENCE (recorded, not a want) %s no team (org form): %s", mode, sankeyTVSummary(sankeyTVServe(ctx, t, client, org, mode)))
	}
}
