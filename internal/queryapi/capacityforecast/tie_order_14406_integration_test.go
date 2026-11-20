//go:build integration

package capacityforecast

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/throughputforecast"
)

// Two versions of one key that share computed_at serve ONE version whatever the insert order, in the org form and in
// the one-team form; both forms serve the same version, and the throughput forecast page serves it too. Versions go in
// as separate INSERT statements (same-key rows of one block collapse under ReplacingMergeTree) into tables with merges
// stopped. Each run has its own organization.

type capacityTieRow struct {
	daysAgo        int
	scope, view    string
	completed, wip uint32
	at             time.Time
}

func seedCapacityTie(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []capacityTieRow) {
	t.Helper()
	for _, row := range rows {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, computed_at, org_id, row_view)
			VALUES (?, 'jira', ?, 'P', 'P', ?, ?, ?, ?, ?)`,
			capacityTeamViewToday().AddDate(0, 0, -row.daysAgo), row.scope, row.completed, row.wip, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

func reversedCapacityTie(rows []capacityTieRow) []capacityTieRow {
	out := make([]capacityTieRow, len(rows))
	for i, row := range rows {
		out[len(rows)-1-i] = row
	}
	return out
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

type capacityTieRun struct {
	order, rep int
	org        string
	team, all  string
}

// capacityTieRuns seeds rows in insert order and in reverse order, 3 times each, each run in its own organization, and
// reads the one-team form (team P) and the org form with read.
func capacityTieRuns(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, cell string, rows []capacityTieRow,
	read func(org string, teams ...string) string,
) []capacityTieRun {
	t.Helper()
	var runs []capacityTieRun
	for o, order := range [][]capacityTieRow{rows, reversedCapacityTie(rows)} {
		for rep := range 3 {
			org := fmt.Sprintf("org-14406-%s-o%d-r%d", cell, o, rep)
			seedCapacityTie(ctx, t, admin, org, order)
			run := capacityTieRun{order: o, rep: rep, org: org, team: read(org, "P"), all: read(org)}
			t.Logf("order=%d rep=%d team: %s | org: %s", o, rep, run.team, run.all)
			runs = append(runs, run)
		}
	}
	return runs
}

func capacityTieAt() time.Time { return time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC) }

// capacityTiedRows: day 1 holds two legacy versions of one key, day 2 two org-view versions of one key, each pair at
// one computed_at, every served value different.
func capacityTiedRows() []capacityTieRow {
	at := capacityTieAt()
	return []capacityTieRow{
		{1, "S", "", 3, 4, at}, {1, "S", "", 7, 9, at},
		{2, "S", "org", 11, 12, at}, {2, "S", "org", 15, 16, at},
	}
}

// capacityControlRows: no tie. Six keys, each an older and a newer version (two legacy and two org-view keys on day 1,
// two legacy keys on day 2), every value different, the two days' sums different.
func capacityControlRows() []capacityTieRow {
	older, newer := capacityTieAt(), capacityTieAt().Add(time.Hour)
	var rows []capacityTieRow
	for k, key := range []struct {
		daysAgo int
		view    string
	}{{1, ""}, {1, ""}, {1, "org"}, {1, "org"}, {2, ""}, {2, ""}} {
		base := uint32(10*(k+1) + 100*(key.daysAgo-1))
		scope := fmt.Sprintf("S%d", k+1)
		rows = append(rows,
			capacityTieRow{key.daysAgo, scope, key.view, base + 1, base + 2, older},
			capacityTieRow{key.daysAgo, scope, key.view, base + 5, base + 7, newer})
	}
	return rows
}

// Pinned from a run at main 003fe32fa812 (before CHAOS-14406): every value the two reads serve, and the served
// forecast (capacityServed: the seed-free fields and the digest of the whole forecast with seed 7).
const (
	capacityControlReadsTeam = "throughput=[320 120] backlog=452"
	capacityControlReadsOrg  = capacityControlReadsTeam
	capacityControlServeTeam = "team=P scope=nil backlog=452 target=5 targetDate=<nil> computedAt=2026-10-20T00:00:00+00:00 mean=220 stddev=100 historyDays=2 insufficient=true highVariance=false seeded=d2416051cab1"
	capacityControlServeOrg  = "team=nil scope=nil backlog=452 target=5 targetDate=<nil> computedAt=2026-10-20T00:00:00+00:00 mean=220 stddev=100 historyDays=2 insufficient=true highVariance=false seeded=0be7ca4d9b0f"
)

func TestCapacityTieOrder14406(t *testing.T) {
	ctx, admin, client := startCapacityTeamView(t)
	for _, table := range []string{"work_item_metrics_daily", "capacity_forecasts"} {
		if err := admin.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	reads := func(org string, teams ...string) string { return capacityReads(ctx, t, client, org, teams...) }

	// The org form serves one version of a tied key whatever the insert order.
	t.Run("org_form_one_version", func(t *testing.T) {
		served := map[string]bool{}
		for _, run := range capacityTieRuns(ctx, t, admin, "org", capacityTiedRows(), reads) {
			served[run.all] = true
		}
		if len(served) != 1 {
			t.Errorf("the org form serves %d versions by insert order: %v", len(served), sortedSet(served))
		}
	})

	// The one-team form serves one version of a tied key whatever the insert order.
	t.Run("team_form_one_version", func(t *testing.T) {
		served := map[string]bool{}
		for _, run := range capacityTieRuns(ctx, t, admin, "team", capacityTiedRows(), reads) {
			served[run.team] = true
		}
		if len(served) != 1 {
			t.Errorf("the team form serves %d versions by insert order: %v", len(served), sortedSet(served))
		}
	})

	// Team and org serve the same version on a tie, in every run.
	t.Run("cross_form_same_version", func(t *testing.T) {
		for _, run := range capacityTieRuns(ctx, t, admin, "cross", capacityTiedRows(), reads) {
			if run.team != run.all {
				t.Errorf("order %d rep %d: team %s, org %s", run.order, run.rep, run.team, run.all)
			}
		}
	})

	// The throughput read and the backlog read are two statements: both take day 1 from one version, never the
	// items_completed of one and the WIP of the other.
	t.Run("one_version_across_reads", func(t *testing.T) {
		for _, run := range capacityTieRuns(ctx, t, admin, "mixed", capacityTiedRows(), reads) {
			for form, served := range map[string]string{"team": run.team, "org": run.all} {
				if !strings.HasSuffix(served, " 3] backlog=4") && !strings.HasSuffix(served, " 7] backlog=9") {
					t.Errorf("order %d rep %d %s form mixes two versions: %s", run.order, run.rep, form, served)
				}
			}
		}
	})

	// The capacity forecast and the throughput forecast serve one version of each of 8 tied keys, in both forms. The
	// two versions of key k differ by 2^k in the WIP, so the summed backlog names the version of every key.
	t.Run("cross_page_same_version", func(t *testing.T) {
		at := capacityTieAt()
		var rows []capacityTieRow
		for k := range 8 {
			scope := fmt.Sprintf("S%d", k+1)
			rows = append(rows,
				capacityTieRow{1, scope, "", uint32(100 + k), 1000, at},
				capacityTieRow{1, scope, "", uint32(200 + k), 1000 + uint32(1)<<k, at})
		}
		read := func(org string, teams ...string) string {
			teamIDs, _ := selectedTeams(&model.CapacityForecastInput{TeamIds: teams})
			capacity, err := loadBacklog(ctx, client, org, teamIDs, nil)
			if err != nil {
				t.Fatal(err)
			}
			forecast, err := throughputforecast.Resolve(ctx, client, org,
				model.ThroughputForecastInput{TeamIds: teams, HistoryWeeks: 4}, capacityTeamViewToday())
			if err != nil {
				t.Fatal(err)
			}
			return fmt.Sprintf("capacity=%d throughputForecast=%d", capacity, forecast.BacklogSize)
		}
		for _, run := range capacityTieRuns(ctx, t, admin, "page", rows, read) {
			for form, served := range map[string]string{"team": run.team, "org": run.all} {
				var capacity, forecast int
				if _, err := fmt.Sscanf(served, "capacity=%d throughputForecast=%d", &capacity, &forecast); err != nil {
					t.Fatal(err)
				}
				if capacity != forecast {
					t.Errorf("order %d rep %d %s form: the two pages serve other versions: %s", run.order, run.rep, form, served)
				}
			}
		}
	})

	// No tie: a newer computed_at wins in both insert orders, and every served value is main's.
	t.Run("non_tie_control", func(t *testing.T) {
		served := func(org string, teams ...string) string {
			return reads(org, teams...) + " | " + capacityServed(ctx, t, client, org, teams...)
		}
		want := map[string]string{
			"team": capacityControlReadsTeam + " | " + capacityControlServeTeam,
			"org":  capacityControlReadsOrg + " | " + capacityControlServeOrg,
		}
		for _, run := range capacityTieRuns(ctx, t, admin, "control", capacityControlRows(), served) {
			for form, got := range map[string]string{"team": run.team, "org": run.all} {
				if got != want[form] {
					t.Errorf("order %d rep %d %s form:\n got  %s\n want %s", run.order, run.rep, form, got, want[form])
				}
			}
		}
	})

	// A key whose newest row is a retraction row (a row of zeros) gives what a never-stored key gives, in both forms:
	// the throughput read leaves its day out, and the backlog adds 0 for it, not its older backlog.
	t.Run("newest_retraction_absent", func(t *testing.T) {
		older, newer := capacityTieAt(), capacityTieAt().Add(time.Hour)
		live := []capacityTieRow{{1, "S1", "", 5, 6, newer}}
		cases := map[string][]capacityTieRow{
			"without": live,
			"with": append(append([]capacityTieRow{}, live...),
				capacityTieRow{1, "S2", "", 7, 8, older}, capacityTieRow{1, "S2", "", 0, 0, newer},
				capacityTieRow{3, "S3", "org", 9, 9, older}, capacityTieRow{3, "S3", "org", 0, 0, newer}),
		}
		got := map[string]map[string]string{}
		for name, rows := range cases {
			got[name] = map[string]string{}
			for o, order := range [][]capacityTieRow{rows, reversedCapacityTie(rows)} {
				org := fmt.Sprintf("org-14406-retracted-%s-o%d", name, o)
				seedCapacityTie(ctx, t, admin, org, order)
				for form, teams := range map[string][]string{"team": {"P"}, "org": nil} {
					served := reads(org, teams...)
					t.Logf("%s order=%d %s: %s", name, o, form, served)
					if prior, ok := got[name][form]; ok && prior != served {
						t.Errorf("%s %s form moves with the insert order: %s vs %s", name, form, prior, served)
					}
					got[name][form] = served
				}
			}
		}
		for _, form := range []string{"team", "org"} {
			if got["with"][form] != got["without"][form] {
				t.Errorf("%s form: a newest retraction row changed the reads:\n with    %s\n without %s", form, got["with"][form], got["without"][form])
			}
		}
	})

	// The forecast list serves one stored forecast of a slice whose two copies share computed_at.
	t.Run("forecast_list_one_version", func(t *testing.T) {
		at := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
		copies := []struct {
			id      string
			backlog uint32
		}{{"f-a", 5}, {"f-b", 9}}
		served := map[string]bool{}
		for o := range 2 {
			for rep := range 3 {
				org := fmt.Sprintf("org-14406-list-o%d-r%d", o, rep)
				order := []int{0, 1}
				if o == 1 {
					order = []int{1, 0}
				}
				for _, i := range order {
					if err := admin.Exec(ctx, `INSERT INTO capacity_forecasts
						(forecast_id, computed_at, team_id, backlog_size, history_days, simulation_count, throughput_mean, throughput_stddev, org_id)
						VALUES (?, ?, NULL, ?, 30, 1000, 1.5, 0.5, ?)`, copies[i].id, at, copies[i].backlog, org); err != nil {
						t.Fatal(err)
					}
				}
				connection, err := ResolveForecasts(ctx, client, org, nil)
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, edge := range connection.Edges {
					got = append(got, fmt.Sprintf("%s/%d", edge.Node.ForecastID, edge.Node.BacklogSize))
				}
				t.Logf("order=%d rep=%d list: %v", o, rep, got)
				served[strings.Join(got, ",")] = true
			}
		}
		if len(served) != 1 {
			t.Errorf("the forecast list serves %d versions by insert order: %v", len(served), sortedSet(served))
		}
	})

	// No tie on the list: of eight slices, each with an older and a newer copy, the newer copy is served in both insert
	// orders.
	t.Run("forecast_list_non_tie_control", func(t *testing.T) {
		older := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
		for o := range 2 {
			org := fmt.Sprintf("org-14406-list-control-o%d", o)
			var want []string
			for s := range 8 {
				ids := []string{fmt.Sprintf("s%d-old", s), fmt.Sprintf("s%d-new", s)}
				ats := []time.Time{older, older.Add(time.Minute)}
				order := []int{0, 1}
				if o == 1 {
					order = []int{1, 0}
				}
				for _, i := range order {
					if err := admin.Exec(ctx, `INSERT INTO capacity_forecasts
						(forecast_id, computed_at, team_id, backlog_size, history_days, simulation_count, throughput_mean, throughput_stddev, org_id)
						VALUES (?, ?, NULL, 5, ?, 1000, 1.5, 0.5, ?)`, ids[i], ats[i], 10+s, org); err != nil {
						t.Fatal(err)
					}
				}
				want = append(want, ids[1])
			}
			connection, err := ResolveForecasts(ctx, client, org, &model.CapacityForecastFilterInput{Limit: 20})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, edge := range connection.Edges {
				got = append(got, edge.Node.ForecastID)
			}
			sort.Strings(got)
			sort.Strings(want)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("order %d: list %v, want the newer copy of each slice %v", o, got, want)
			}
		}
	})
}
