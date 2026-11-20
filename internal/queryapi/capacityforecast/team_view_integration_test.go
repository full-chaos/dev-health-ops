//go:build integration

package capacityforecast

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the team view of the one-team capacity forecast reads (CHAOS-14376): loadThroughput (a list of days)
// and loadBacklog (each team's newest day). Team P is the primary team of the items of scope S; team C co-owns some of
// them, so C has 'team' rows only. The older day holds a legacy row only; the newer day holds the rows of the org
// writer (the legacy row retracted with a row of zeros, an org row and a scope row) and, in the team data set, the
// team rows of the same keys. The served forecast itself is a Monte Carlo run with a random seed, so the cells compare
// every value the two reads serve to it.

func capacityTeamViewToday() time.Time { return time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC) }

func capacityTeamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

type capacityTeamViewRow struct {
	daysAgo        int
	team           string
	completed, wip uint32
	at             time.Time
	view           string
}

func startCapacityTeamView(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
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

func seedCapacityTeamView(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []capacityTeamViewRow) {
	t.Helper()
	for _, row := range rows {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, ?, ?, ?, ?)`,
			capacityTeamViewToday().AddDate(0, 0, -row.daysAgo), row.team, row.team, row.completed, row.wip, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// capacityReads is every value the two reads serve for a team selection, read through selectedTeams as the resolver
// does (the team id list is a set there).
func capacityReads(ctx context.Context, t *testing.T, client QueryClient, org string, teams ...string) string {
	t.Helper()
	teamIDs, _ := selectedTeams(&model.CapacityForecastInput{TeamIds: teams})
	history, err := loadThroughput(ctx, client, org, teamIDs, nil, 10, capacityTeamViewToday())
	if err != nil {
		t.Fatalf("%v throughput: %v", teams, err)
	}
	backlog, err := loadBacklog(ctx, client, org, teamIDs, nil)
	if err != nil {
		t.Fatalf("%v backlog: %v", teams, err)
	}
	return fmt.Sprintf("throughput=%v backlog=%d", history, backlog)
}

func wantCapacityReads(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %s, want %s", what, got, want)
	}
}

func capacityOrgWriterDays(t1, t2, t3 time.Time) []capacityTeamViewRow {
	return []capacityTeamViewRow{
		{2, "P", 2, 1, t1, ""},
		{1, "P", 7, 5, t1, ""}, {1, "P", 0, 0, t3, ""},
		{1, "P", 4, 3, t2, "org"}, {1, "P", 6, 4, t2, "scope"},
	}
}

// (1) Today's data holds no team row: every served value equals main's (pinned from main, 2026-11-20).
func TestCapacityTeamReadsAreANoOpOnTodaysData(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)
	t1, t2, t3 := capacityTeamViewVersions()
	const org = "org-14376-cap-noop"
	seedCapacityTeamView(ctx, t, admin, org, capacityOrgWriterDays(t1, t2, t3))
	wantCapacityReads(t, "team P", capacityReads(ctx, t, client, org, "P"), "throughput=[2 4] backlog=3")
	wantCapacityReads(t, "no team", capacityReads(ctx, t, client, org), "throughput=[2 4] backlog=3")
	wantCapacityReads(t, "teams P and C", capacityReads(ctx, t, client, org, "P", "C"), "throughput=[2 4] backlog=3")
	wantCapacityReads(t, "team C (no row)", capacityReads(ctx, t, client, org, "C"), "throughput=[] backlog=0")
}

// (2) A team row wins where one exists, for one team; a team set keeps the org view (L2d).
func TestCapacityTeamReadsTakeTheTeamRow(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)
	t1, t2, t3 := capacityTeamViewVersions()
	const org = "org-14376-cap-team"
	seedCapacityTeamView(ctx, t, admin, org, append(capacityOrgWriterDays(t1, t2, t3),
		capacityTeamViewRow{1, "P", 9, 6, t2, "team"},
		capacityTeamViewRow{1, "C", 3, 2, t2, "team"},
	))
	wantCapacityReads(t, "team P", capacityReads(ctx, t, client, org, "P"), "throughput=[2 9] backlog=6")
	wantCapacityReads(t, "team C", capacityReads(ctx, t, client, org, "C"), "throughput=[3] backlog=2")
	wantCapacityReads(t, "teams P and C (a team set: org form)", capacityReads(ctx, t, client, org, "P", "C"), "throughput=[2 4] backlog=3")
	// The team id list is a set (selectedTeams): [P, P] is the one team P, [P, C, P] the team set [P, C].
	wantCapacityReads(t, "[P,P]", capacityReads(ctx, t, client, org, "P", "P"), "throughput=[2 9] backlog=6")
	wantCapacityReads(t, "[P,C,P]", capacityReads(ctx, t, client, org, "P", "C", "P"), "throughput=[2 4] backlog=3")
}

// (3) Live first: a retracted team row never hides the live org row. The backlog keeps a newest day whose every view
// is retracted as that team's newest day (a backlog of 0, as the org form gives), not an older day's backlog.
func TestCapacityTeamReadsLiveFirst(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)
	t1, t2, t3 := capacityTeamViewVersions()
	const org = "org-14376-cap-live"
	seedCapacityTeamView(ctx, t, admin, org, []capacityTeamViewRow{
		{2, "P", 2, 1, t1, ""},
		{1, "P", 8, 7, t1, "team"}, {1, "P", 4, 3, t2, "org"}, {1, "P", 0, 0, t3, "team"},
	})
	wantCapacityReads(t, "team P", capacityReads(ctx, t, client, org, "P"), "throughput=[2 4] backlog=3")
	const retired = "org-14376-cap-live-retired"
	seedCapacityTeamView(ctx, t, admin, retired, []capacityTeamViewRow{
		{2, "P", 2, 1, t1, "org"},
		{1, "P", 5, 4, t1, "org"}, {1, "P", 0, 0, t3, "org"},
		{1, "P", 6, 5, t1, "team"}, {1, "P", 0, 0, t3, "team"},
	})
	wantCapacityReads(t, "team P, newest day all retracted", capacityReads(ctx, t, client, retired, "P"), "throughput=[2] backlog=0")
}

// (4) Two organizations never mix through the team view.
func TestCapacityTeamReadsNeverMixTwoOrgs(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)
	t1, t2, t3 := capacityTeamViewVersions()
	const orgA, orgB = "org-14376-cap-two-a", "org-14376-cap-two-b"
	seedCapacityTeamView(ctx, t, admin, orgA, append(capacityOrgWriterDays(t1, t2, t3), capacityTeamViewRow{1, "P", 9, 6, t2, "team"}))
	seedCapacityTeamView(ctx, t, admin, orgB, []capacityTeamViewRow{
		{1, "P", 90, 60, t3, "team"}, {1, "P", 40, 30, t3, "org"}, {0, "P", 70, 50, t3, "team"}, {1, "C", 30, 20, t3, "team"},
	})
	wantCapacityReads(t, "org A team P", capacityReads(ctx, t, client, orgA, "P"), "throughput=[2 9] backlog=6")
	wantCapacityReads(t, "org A team C (rows in org B only)", capacityReads(ctx, t, client, orgA, "C"), "throughput=[] backlog=0")
	wantCapacityReads(t, "org A no team", capacityReads(ctx, t, client, orgA), "throughput=[2 4] backlog=3")
}

// The documented divergence (R2 of the plan, not a WANT): a live legacy row beside a live org row of one key, which no
// writer on main stores. The org form sums both; the team form reads the org row. Both answers are recorded.
func TestCapacityTeamReadsRecordTheLiveLegacyBesideLiveOrgDivergence(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)
	t1, t2, _ := capacityTeamViewVersions()
	const org = "org-14376-cap-divergence"
	seedCapacityTeamView(ctx, t, admin, org, []capacityTeamViewRow{{1, "P", 7, 5, t1, ""}, {1, "P", 4, 3, t2, "org"}})
	t.Logf("DIVERGENCE (recorded, not a want) one team P: %s", capacityReads(ctx, t, client, org, "P"))
	t.Logf("DIVERGENCE (recorded, not a want) no team (org form): %s", capacityReads(ctx, t, client, org))
}
