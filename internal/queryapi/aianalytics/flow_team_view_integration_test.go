//go:build integration

package aianalytics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the team view of the one-team flow opportunities read (CHAOS-14379): FlowOpportunities reads the team
// rows of work_item_metrics_daily over the window (avg cycle_time_p50_hours, avg wip_congestion_ratio, sum
// items_completed, HAVING data_days >= 5). Team P is the primary team of scope S; team C co-owns, so C has 'team' rows
// only. Days 5..2 ago hold a legacy row only (the older days; five days are needed for HAVING data_days >= 5). Day 1
// ago (D2) holds the rows of the org writer: the legacy row live at t1, its retraction (zeros) at t3, an org row and a
// scope row at t2.
//
// The scope input holds ONE team id (a string), not a list: there is no team set, so the team set cells ([P,C],
// [P,P], [P,C,P]) do not apply to this read. The no-team read (every active team) is the org form.
//
// The served answer is the whole *model.ImproveOpportunitiesResult as JSON. No field is blanked: it holds no
// wall-clock or random field (opportunityId is the sha256 of kind:entity, the window is the request's).

const flowTeamViewWindow = 30

func flowTeamViewDay(daysAgo int) time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()-daysAgo, 0, 0, 0, 0, time.UTC)
}

func flowTeamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

func flowNum(v float64) *float64 { return &v }

type flowTeamViewRow struct {
	daysAgo int
	team    string
	cycle   *float64 // nil = NULL (a retraction row holds NULL here)
	wip     float64
	items   uint32
	at      time.Time
	view    string
}

// flowRetraction is a row of zeros: liverow calls it not live.
func flowRetraction(daysAgo int, team string, at time.Time, view string) flowTeamViewRow {
	return flowTeamViewRow{daysAgo: daysAgo, team: team, at: at, view: view}
}

func startFlowTeamView(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
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
	return ctx, admin, client
}

// seedFlowTeams stores P and C as active teams of org: the no-team read lists the active teams only.
func seedFlowTeams(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string) {
	t.Helper()
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, team := range [][2]string{{"P", "Platform"}, {"C", "Core"}} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, is_active)
			VALUES (?, generateUUIDv4(), ?, ['org/*'], ?, ?, 1)`, team[0], team[1], at, org); err != nil {
			t.Fatalf("insert team %v: %v", team, err)
		}
	}
}

func seedFlowTeamView(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []flowTeamViewRow) {
	t.Helper()
	for _, row := range rows {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, cycle_time_p50_hours, wip_congestion_ratio, items_completed, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?, ?)`,
			flowTeamViewDay(row.daysAgo), row.team, row.team, row.cycle, row.wip, row.items, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// flowServed calls the served entry point for one team ("" = no team) and returns the JSON of the whole served
// answer, its sha256 prefix, and the key numbers in served order ("KIND entity value; ...").
func flowServed(ctx context.Context, t *testing.T, client QueryClient, org, team string) (string, string, string) {
	t.Helper()
	in := &model.AIScopeInput{}
	if team != "" {
		in.TeamID = &team
	}
	got, err := FlowOpportunities(ctx, client, org, in, 50, flowTeamViewWindow)
	if err != nil {
		t.Fatalf("team %q: %v", team, err)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	var keys []string
	for _, o := range got.Opportunities {
		keys = append(keys, string(o.Kind)+" "+o.EntityID+" "+strconv.FormatFloat(o.Value, 'g', 10, 64))
	}
	served := string(raw)
	t.Logf("SERVED org=%s team=%q json=%s digest=%s keys=%s", org, team, served, hex.EncodeToString(digest[:])[:12], strings.Join(keys, "; "))
	return served, hex.EncodeToString(digest[:])[:12], strings.Join(keys, "; ")
}

func wantFlowKeys(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %q, want %q", what, got, want)
	}
}

// flowOlderDays are days 5..2 ago: a legacy row of P only.
func flowOlderDays(t1 time.Time) []flowTeamViewRow {
	var rows []flowTeamViewRow
	for d := 5; d >= 2; d-- {
		rows = append(rows, flowTeamViewRow{d, "P", flowNum(130), 0.5, 0, t1, ""})
	}
	return rows
}

// flowOrgWriterDays is today's data: the older days and D2 of the org writer.
func flowOrgWriterDays(t1, t2, t3 time.Time) []flowTeamViewRow {
	return append(flowOlderDays(t1),
		flowTeamViewRow{1, "P", flowNum(200), 0.9, 5, t1, ""},
		flowRetraction(1, "P", t3, ""),
		flowTeamViewRow{1, "P", flowNum(150), 0.7, 1, t2, "org"},
		flowTeamViewRow{1, "P", flowNum(300), 0.95, 9, t2, "scope"},
	)
}

// The org form of P on today's data: days 5..2 legacy (130, 0.5, 0) + D2 org row (150, 0.7, 1):
// cycle (4*130+150)/5 = 134; wip (4*0.5+0.7)/5 = 0.54; items 0+1 = 1.
// Scores: wip (0.54/0.4-1)/2+0.5 = 0.675; cycle (134/120-1)/2+0.5 = 0.558; throughput gap 2-1=1: (1/2-1)/2+0.5 = 0.25.
const flowOrgFormP = "HIGH_WIP P 0.54; SLOW_CYCLE_TIME P 134; LOW_THROUGHPUT P 1"

// (1) Today's data holds no team row: the served answer equals main's.
func TestFlowTeamViewNoOp(t *testing.T) {
	ctx, admin, client := startFlowTeamView(t)
	t1, t2, t3 := flowTeamViewVersions()
	const org = "org-14379-flow-noop"
	seedFlowTeams(ctx, t, admin, org)
	seedFlowTeamView(ctx, t, admin, org, flowOrgWriterDays(t1, t2, t3))
	for _, c := range []struct{ what, team, digest, keys string }{
		{"team P", "P", flowMainDigestTeamP, flowOrgFormP},
		{"no team", "", flowMainDigestNoTeam, flowOrgFormP},
		{"team C (no row)", "C", flowMainDigestTeamC, ""},
	} {
		served, digest, keys := flowServed(ctx, t, client, org, c.team)
		wantFlowKeys(t, c.what+" served json digest", digest, c.digest)
		wantFlowKeys(t, c.what+" key numbers", keys, c.keys)
		if c.team == "C" {
			wantFlowKeys(t, c.what+" served json", served, flowMainServedTeamC)
		}
	}
}

// The sha256 prefix of the whole served JSON, pinned from main ab06c5087 (log ops2a2-aianalytics-pin-1.log
// 3376143bbf37). Team P and no team serve the same JSON at main.
const (
	flowMainDigestTeamP  = "9813e6f25e47"
	flowMainDigestNoTeam = "9813e6f25e47"
	flowMainDigestTeamC  = "f823a0d5b381"
	flowMainServedTeamC  = `{"orgId":"org-14379-flow-noop","opportunities":[],"detectorReady":true,"totalCount":0}`
)

// (2) A team row wins where one exists, for one team. No team keeps the org form.
func TestFlowTeamViewTeamRowWins(t *testing.T) {
	ctx, admin, client := startFlowTeamView(t)
	t1, t2, t3 := flowTeamViewVersions()
	const org = "org-14379-flow-team"
	seedFlowTeams(ctx, t, admin, org)
	rows := append(flowOrgWriterDays(t1, t2, t3), flowTeamViewRow{1, "P", flowNum(250), 0.8, 0, t2, "team"})
	for d := 5; d >= 1; d-- {
		rows = append(rows, flowTeamViewRow{d, "C", flowNum(160), 0.6, 3, t2, "team"})
	}
	seedFlowTeamView(ctx, t, admin, org, rows)
	// P: days 5..2 legacy (130, 0.5, 0) + D2 team row (250, 0.8, 0):
	// cycle (520+250)/5 = 154; wip (2+0.8)/5 = 0.56; items 0.
	// Scores: wip (0.56/0.4-1)/2+0.5 = 0.7; cycle (154/120-1)/2+0.5 = 0.642; throughput gap 2: (2/2-1)/2+0.5 = 0.5.
	_, _, keys := flowServed(ctx, t, client, org, "P")
	wantFlowKeys(t, "team P", keys, "HIGH_WIP P 0.56; SLOW_CYCLE_TIME P 154; LOW_THROUGHPUT P 0")
	// C: 5 team rows (160, 0.6, 3): cycle 160; wip 0.6; items 15 (no low throughput).
	// Scores: wip (0.6/0.4-1)/2+0.5 = 0.75; cycle (160/120-1)/2+0.5 = 0.667.
	_, _, keys = flowServed(ctx, t, client, org, "C")
	wantFlowKeys(t, "team C", keys, "HIGH_WIP C 0.6; SLOW_CYCLE_TIME C 160")
	// No team: the org form; C has no org or legacy row.
	_, _, keys = flowServed(ctx, t, client, org, "")
	wantFlowKeys(t, "no team (org form)", keys, flowOrgFormP)
}

// (3) Live first: a retracted team row never hides the live org row.
func TestFlowTeamViewLiveFirst(t *testing.T) {
	ctx, admin, client := startFlowTeamView(t)
	t1, t2, t3 := flowTeamViewVersions()
	const org = "org-14379-flow-live"
	seedFlowTeams(ctx, t, admin, org)
	seedFlowTeamView(ctx, t, admin, org, append(flowOlderDays(t1),
		flowTeamViewRow{1, "P", flowNum(150), 0.7, 1, t2, "org"},
		flowTeamViewRow{1, "P", flowNum(400), 0.99, 0, t1, "team"},
		flowRetraction(1, "P", t3, "team"),
	))
	// The newest team row (t3) is zeros, so the live org row (150, 0.7, 1) is read: same arithmetic as flowOrgFormP.
	_, _, keys := flowServed(ctx, t, client, org, "P")
	wantFlowKeys(t, "team P", keys, flowOrgFormP)
	_, _, keys = flowServed(ctx, t, client, org, "")
	wantFlowKeys(t, "no team", keys, flowOrgFormP)
}

// (4) Two organizations never mix through the team view.
func TestFlowTeamViewTwoOrgs(t *testing.T) {
	ctx, admin, client := startFlowTeamView(t)
	t1, t2, t3 := flowTeamViewVersions()
	const orgA, orgB = "org-14379-flow-two-a", "org-14379-flow-two-b"
	seedFlowTeams(ctx, t, admin, orgA)
	seedFlowTeams(ctx, t, admin, orgB)
	seedFlowTeamView(ctx, t, admin, orgA, flowOrgWriterDays(t1, t2, t3))
	// Org B: the same keys (days 5..1) with other values, team rows of P and C, and a day of its own (day 0).
	var rowsB []flowTeamViewRow
	for d := 5; d >= 0; d-- {
		rowsB = append(rowsB,
			flowTeamViewRow{d, "P", flowNum(500), 0.2, 0, t3, "team"},
			flowTeamViewRow{d, "P", flowNum(600), 0.3, 7, t3, "org"},
			flowTeamViewRow{d, "C", flowNum(700), 0.45, 4, t3, "team"},
		)
	}
	seedFlowTeamView(ctx, t, admin, orgB, rowsB)
	// Org A holds no team row: team P reads org A's org form (flowOrgFormP); C has rows in org B only.
	_, _, keys := flowServed(ctx, t, client, orgA, "P")
	wantFlowKeys(t, "org A team P", keys, flowOrgFormP)
	_, _, keys = flowServed(ctx, t, client, orgA, "C")
	wantFlowKeys(t, "org A team C (rows in org B only)", keys, "")
	_, _, keys = flowServed(ctx, t, client, orgA, "")
	wantFlowKeys(t, "org A no team", keys, flowOrgFormP)
}

// (6) The documented divergence (R2 of the plan, not a WANT): a live legacy row beside a live org row of one key,
// which no writer on main stores. The org form reads both; the team form reads the org row. Both answers are recorded.
func TestFlowTeamViewDivergence(t *testing.T) {
	ctx, admin, client := startFlowTeamView(t)
	t1, t2, _ := flowTeamViewVersions()
	const org = "org-14379-flow-divergence"
	seedFlowTeams(ctx, t, admin, org)
	seedFlowTeamView(ctx, t, admin, org, append(flowOlderDays(t1),
		flowTeamViewRow{1, "P", flowNum(200), 0.9, 5, t1, ""},
		flowTeamViewRow{1, "P", flowNum(150), 0.7, 1, t2, "org"},
	))
	_, _, keys := flowServed(ctx, t, client, org, "P")
	t.Logf("DIVERGENCE (recorded, not a want) one team P: %s", keys)
	_, _, keys = flowServed(ctx, t, client, org, "")
	t.Logf("DIVERGENCE (recorded, not a want) no team (org form): %s", keys)
}
