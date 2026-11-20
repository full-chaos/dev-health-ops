//go:build integration

package remaining

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the team form of the one-team job reads (CHAOS-14353): the capacity forecast reads of one team
// (loadThroughput, loadBacklog) and the recommendations loader of one team (loadWIPThroughput, the cycle times of
// loadSustainabilitySignals). Team P is the primary team of the items of scope S; team C co-owns some of them, so C
// has 'team' rows only. Day 1 holds a legacy row only (a day no writer computed again); day 2 holds the rows of the
// org writer (the legacy row retracted with a row of zeros, an org row and a scope row) and, in the team data set,
// the team rows of the same keys.

func teamViewDay(n int) time.Time { return time.Date(2026, 2, n, 0, 0, 0, 0, time.UTC) }

func teamViewVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 2, 2, 9, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

type teamViewRow struct {
	day         int
	scope, team string
	completed   uint32
	wip         uint32
	cycle       *float64
	at          time.Time
	view        string
}

func hours(v float64) *float64 { return &v }

func startTeamViewStore(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func seedTeamViewRows(ctx context.Context, t *testing.T, conn driver.Conn, org string, rows []teamViewRow) {
	t.Helper()
	for _, row := range rows {
		if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, cycle_time_p50_hours,
			 computed_at, org_id, row_view)
			VALUES (?, 'jira', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			teamViewDay(row.day), row.scope, row.team, row.team, row.completed, row.wip, row.cycle, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// teamViewReads is what the four reads give for one team.
type teamViewReads struct {
	Throughput []int
	Backlog    int
	WIP, TP    []float64
	Cycle      []float64
}

func readTeamView(ctx context.Context, t *testing.T, conn driver.Conn, org, team string) teamViewReads {
	t.Helper()
	capacity := &CapacityExecutor{conn: conn}
	target := capacityTarget{TeamID: &team}
	throughput, err := capacity.loadThroughput(ctx, org, target, 30, teamViewDay(3))
	if err != nil {
		t.Fatalf("team %s throughput: %v", team, err)
	}
	backlog, err := capacity.loadBacklog(ctx, org, target)
	if err != nil {
		t.Fatalf("team %s backlog: %v", team, err)
	}
	loader, err := NewRecommendationsLoader(conn, org)
	if err != nil {
		t.Fatal(err)
	}
	wip, tp, err := loader.loadWIPThroughput(ctx, team, teamViewDay(1), teamViewDay(3))
	if err != nil {
		t.Fatalf("team %s wip/throughput: %v", team, err)
	}
	_, _, cycle, _, err := loader.loadSustainabilitySignals(ctx, team, teamViewDay(1), teamViewDay(3))
	if err != nil {
		t.Fatalf("team %s cycle times: %v", team, err)
	}
	reads := teamViewReads{Throughput: throughput.DailyThroughputs, Backlog: backlog, WIP: wip, TP: tp, Cycle: cycle}
	if reads.Throughput == nil {
		reads.Throughput = []int{}
	}
	return reads
}

func wantTeamViewReads(t *testing.T, what string, got, want teamViewReads) {
	t.Helper()
	normalize := func(r teamViewReads) teamViewReads {
		for _, list := range []*[]float64{&r.WIP, &r.TP, &r.Cycle} {
			if *list == nil {
				*list = []float64{}
			}
		}
		if r.Throughput == nil {
			r.Throughput = []int{}
		}
		return r
	}
	if got, want := normalize(got), normalize(want); !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\n got  %+v\n want %+v", what, got, want)
	}
}

// (1) Today's data holds no team row: every one-team read gives the answer of the org form, as on main.
func TestJobsTeamReadsAreANoOpOnTodaysData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startTeamViewStore(ctx, t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14353-noop"
	seedTeamViewRows(ctx, t, conn, org, []teamViewRow{
		{1, "S", "P", 3, 5, hours(10), t1, ""},
		{2, "S", "P", 10, 9, hours(30), t1, ""}, {2, "S", "P", 0, 0, nil, t3, ""},
		{2, "S", "P", 4, 6, hours(12), t2, "org"}, {2, "S", "P", 9, 9, hours(15), t2, "scope"},
	})
	wantTeamViewReads(t, "team P", readTeamView(ctx, t, conn, org, "P"), teamViewReads{
		Throughput: []int{3, 4}, Backlog: 6, WIP: []float64{5, 6}, TP: []float64{3, 4}, Cycle: []float64{10, 12},
	})
	wantTeamViewReads(t, "team C (no row)", readTeamView(ctx, t, conn, org, "C"), teamViewReads{})
}

// (2) A team row wins where one exists: team P reads its team row of day 2 (7, not the org row's 4), and the co-owner
// team C, which has no org row, reads its team row. The legacy-only day reads as before.
func TestJobsTeamReadsTakeTheTeamRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startTeamViewStore(ctx, t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14353-team"
	seedTeamViewRows(ctx, t, conn, org, []teamViewRow{
		{1, "S", "P", 3, 5, hours(10), t1, ""},
		{2, "S", "P", 10, 9, hours(30), t1, ""}, {2, "S", "P", 0, 0, nil, t3, ""},
		{2, "S", "P", 4, 6, hours(12), t2, "org"}, {2, "S", "P", 9, 9, hours(15), t2, "scope"},
		{2, "S", "P", 7, 8, hours(14), t2, "team"},
		{2, "S", "C", 2, 3, hours(20), t2, "team"},
	})
	wantTeamViewReads(t, "team P", readTeamView(ctx, t, conn, org, "P"), teamViewReads{
		Throughput: []int{3, 7}, Backlog: 8, WIP: []float64{5, 8}, TP: []float64{3, 7}, Cycle: []float64{10, 14},
	})
	wantTeamViewReads(t, "team C", readTeamView(ctx, t, conn, org, "C"), teamViewReads{
		Throughput: []int{2}, Backlog: 3, WIP: []float64{3}, TP: []float64{2}, Cycle: []float64{20},
	})
}

// (3) Live first: a team key that a later run retracted (its newest team row is a row of zeros) reads the live org
// row, never zeros and never a dropped day.
func TestJobsTeamReadsLiveFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startTeamViewStore(ctx, t)
	t1, t2, t3 := teamViewVersions()
	const org = "org-14353-live"
	seedTeamViewRows(ctx, t, conn, org, []teamViewRow{
		{1, "S", "P", 3, 5, hours(10), t1, ""},
		{2, "S", "P", 8, 9, hours(30), t1, "team"},
		{2, "S", "P", 6, 4, hours(12), t2, "org"},
		{2, "S", "P", 0, 0, nil, t3, "team"},
	})
	wantTeamViewReads(t, "team P", readTeamView(ctx, t, conn, org, "P"), teamViewReads{
		Throughput: []int{3, 6}, Backlog: 4, WIP: []float64{5, 4}, TP: []float64{3, 6}, Cycle: []float64{10, 12},
	})
}

// Two organizations never mix through the team view: the job callers scope their where by org_id, and the pick keys
// by org_id too. Org B holds rows of the same team, scope and days with other values, and a day of its own; a read of
// org A returns org A's rows only.
func TestJobsTeamReadsNeverMixTwoOrgs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := startTeamViewStore(ctx, t)
	t1, t2, t3 := teamViewVersions()
	const orgA, orgB = "org-14353-two-a", "org-14353-two-b"
	seedTeamViewRows(ctx, t, conn, orgA, []teamViewRow{
		{1, "S", "P", 3, 5, hours(10), t1, ""},
		{2, "S", "P", 4, 6, hours(12), t2, "org"}, {2, "S", "P", 7, 8, hours(14), t2, "team"},
	})
	seedTeamViewRows(ctx, t, conn, orgB, []teamViewRow{
		{1, "S", "P", 30, 50, hours(100), t1, ""},
		{2, "S", "P", 40, 60, hours(120), t3, "org"}, {2, "S", "P", 70, 80, hours(140), t3, "team"},
		{2, "S", "C", 20, 30, hours(200), t3, "team"},
	})
	wantTeamViewReads(t, "org A team P", readTeamView(ctx, t, conn, orgA, "P"), teamViewReads{
		Throughput: []int{3, 7}, Backlog: 8, WIP: []float64{5, 8}, TP: []float64{3, 7}, Cycle: []float64{10, 14},
	})
	wantTeamViewReads(t, "org A team C (rows in org B only)", readTeamView(ctx, t, conn, orgA, "C"), teamViewReads{})
}
