//go:build integration

package rowview

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the team form (CHAOS-14340). Each seeds rows of one organization and day and reads them through
// TeamSource, against the migrated schema.

// The day of every seed and three versions of it, oldest first. Functions, not package variables: a package-level
// variable that calls a function makes the integration shard run the package unfiltered.
func teamSourceDay() time.Time { return time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC) }

func teamSourceVersions() (t1, t2, t3 time.Time) {
	t1 = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	return t1, t1.Add(time.Hour), t1.Add(2 * time.Hour)
}

func startTeamSourceStore(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(inst.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type metricsRow struct {
	scope, team string
	completed   uint32
	at          time.Time
	view        string
}

func seedMetricsRows(ctx context.Context, t *testing.T, conn driver.Conn, org string, rows []metricsRow) {
	t.Helper()
	for _, row := range rows {
		if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, computed_at, org_id, row_view)
			VALUES (?, 'jira', ?, ?, ?, ?, ?, ?, ?)`,
			teamSourceDay(), row.scope, row.team, row.team, row.completed, row.at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// readKeys reads "scope/team=completed" per key, sorted, from the source in from.
func readKeys(ctx context.Context, t *testing.T, conn driver.Conn, from, org string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT m.work_scope_id, m.team_id, m.items_completed FROM `+from+` AS m
		ORDER BY m.work_scope_id, m.team_id`, org, teamSourceDay())
	if err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var scope, team string
		var completed uint32
		if err := rows.Scan(&scope, &team, &completed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		keys = append(keys, fmt.Sprintf("%s/%s=%d", scope, team, completed))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return keys
}

const metricsFilter = "work_item_metrics_daily.org_id = ? AND work_item_metrics_daily.day = ?"

// orgForm is the org form of today's readers: FINAL, the org predicate, the live-row rule.
func orgForm(t *testing.T, table string) string {
	t.Helper()
	declared, ok := teamkeytables.ByTable(table)
	if !ok {
		t.Fatalf("table %s is not declared", table)
	}
	return `(SELECT * FROM ` + table + ` FINAL WHERE ` + table + `.org_id = ? AND ` + table + `.day = ? AND ` +
		OrgPredicate(table, "") + ` AND ` + declared.LiveRow(table+".") + `)`
}

// The fallback: the team row of a key, else its org row, else its legacy row. A scope row is never read.
func TestTeamSourcePrefersTeamThenOrgThenLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := startTeamSourceStore(ctx, t)
	teamSourceT1, teamSourceT2, teamSourceT3 := teamSourceVersions()
	const org = "org-14340-rank"
	seedMetricsRows(ctx, t, conn, org, []metricsRow{
		// A key the team writer computed: the legacy row retracted, an org, a scope and a team row.
		{"S1", "P", 10, teamSourceT1, ""}, {"S1", "P", 0, teamSourceT3, ""},
		{"S1", "P", 4, teamSourceT2, "org"}, {"S1", "P", 9, teamSourceT2, "scope"}, {"S1", "P", 7, teamSourceT2, "team"},
		// A co-owner team: a team row only.
		{"S1", "C", 2, teamSourceT2, "team"},
		// A day no writer computed again: the legacy row only.
		{"S2", "P", 3, teamSourceT1, ""},
		// A key of the org writer, before team rows: the legacy row retracted, an org and a scope row.
		{"S3", "P", 10, teamSourceT1, ""}, {"S3", "P", 0, teamSourceT3, ""},
		{"S3", "P", 5, teamSourceT2, "org"}, {"S3", "P", 8, teamSourceT2, "scope"},
		// A live legacy row beside a live org row: the org row is read, once.
		{"S5", "P", 10, teamSourceT1, ""}, {"S5", "P", 4, teamSourceT2, "org"},
	})
	got := readKeys(ctx, t, conn, TeamSource("work_item_metrics_daily", metricsFilter, testOrders["work_item_metrics_daily"]), org)
	want := []string{"S1/C=2", "S1/P=7", "S2/P=3", "S3/P=5", "S5/P=4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("team form = %v, want %v", got, want)
	}
}

// Live first: a team key that a later run retracted (its newest team row is a row of zeros) reads its live org row,
// and a key whose views are all rows of zeros leaves the result.
func TestTeamSourceLiveFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := startTeamSourceStore(ctx, t)
	teamSourceT1, teamSourceT2, teamSourceT3 := teamSourceVersions()
	const org = "org-14340-live"
	seedMetricsRows(ctx, t, conn, org, []metricsRow{
		{"S4", "P", 8, teamSourceT1, "team"}, {"S4", "P", 6, teamSourceT2, "org"}, {"S4", "P", 0, teamSourceT3, "team"},
		{"S6", "P", 5, teamSourceT1, "org"}, {"S6", "P", 0, teamSourceT3, "org"},
		{"S6", "P", 5, teamSourceT1, "team"}, {"S6", "P", 0, teamSourceT3, "team"},
	})
	got := readKeys(ctx, t, conn, TeamSource("work_item_metrics_daily", metricsFilter, testOrders["work_item_metrics_daily"]), org)
	want := []string{"S4/P=6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("team form = %v, want %v (a retracted team row must not hide a live org row)", got, want)
	}
}

// Today's data holds no team row: the team form reads what the org form reads, key for key, in both tables.
func TestTeamSourceIsANoOpOnTodaysData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := startTeamSourceStore(ctx, t)
	teamSourceT1, teamSourceT2, teamSourceT3 := teamSourceVersions()
	const org = "org-14340-noop"
	seedMetricsRows(ctx, t, conn, org, []metricsRow{
		{"S1", "P", 10, teamSourceT1, ""}, {"S1", "P", 0, teamSourceT3, ""},
		{"S1", "P", 4, teamSourceT2, "org"}, {"S1", "P", 9, teamSourceT2, "scope"},
		{"S1", "Q", 3, teamSourceT2, "org"}, {"S1", "Q", 3, teamSourceT2, "scope"},
		{"S2", "P", 3, teamSourceT1, ""},
		{"S3", "P", 6, teamSourceT1, "org"}, {"S3", "P", 0, teamSourceT3, "org"},
	})
	orgKeys := readKeys(ctx, t, conn, orgForm(t, "work_item_metrics_daily"), org)
	teamKeys := readKeys(ctx, t, conn, TeamSource("work_item_metrics_daily", metricsFilter, testOrders["work_item_metrics_daily"]), org)
	if want := []string{"S1/P=4", "S1/Q=3", "S2/P=3"}; !reflect.DeepEqual(orgKeys, want) {
		t.Fatalf("org form = %v, want %v: the seed is wrong", orgKeys, want)
	}
	if !reflect.DeepEqual(teamKeys, orgKeys) {
		t.Fatalf("work_item_metrics_daily: team form = %v, org form = %v", teamKeys, orgKeys)
	}

	for _, row := range []struct {
		team, status string
		hours        float64
		touched      uint32
		at           time.Time
		view         string
	}{
		{"P", "blocked", 10, 2, teamSourceT1, ""}, {"P", "blocked", 0, 0, teamSourceT3, ""},
		{"P", "blocked", 4, 1, teamSourceT2, "org"}, {"P", "blocked", 6, 2, teamSourceT2, "scope"},
		{"P", "in_progress", 5, 1, teamSourceT2, "org"}, {"P", "in_progress", 5, 1, teamSourceT2, "scope"},
		{"Q", "blocked", 2, 1, teamSourceT1, ""},
	} {
		if err := conn.Exec(ctx, `INSERT INTO work_item_state_durations_daily
			(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S1', ?, ?, ?, ?, ?, ?, ?, ?)`,
			teamSourceDay(), row.team, row.team, row.status, row.hours, row.touched, row.at, org, row.view); err != nil {
			t.Fatalf("insert state row %+v: %v", row, err)
		}
	}
	readStates := func(from string) []string {
		rows, err := conn.Query(ctx, `SELECT m.team_id, m.status, m.duration_hours FROM `+from+` AS m
			ORDER BY m.team_id, m.status`, org, teamSourceDay())
		if err != nil {
			t.Fatalf("read %s: %v", from, err)
		}
		defer func() { _ = rows.Close() }()
		var keys []string
		for rows.Next() {
			var team, status string
			var hours float64
			if err := rows.Scan(&team, &status, &hours); err != nil {
				t.Fatalf("scan: %v", err)
			}
			keys = append(keys, fmt.Sprintf("%s/%s=%g", team, status, hours))
		}
		return keys
	}
	orgStates := readStates(orgForm(t, "work_item_state_durations_daily"))
	teamStates := readStates(TeamSource("work_item_state_durations_daily",
		"work_item_state_durations_daily.org_id = ? AND work_item_state_durations_daily.day = ?",
		testOrders["work_item_state_durations_daily"]))
	if want := []string{"P/blocked=4", "P/in_progress=5", "Q/blocked=2"}; !reflect.DeepEqual(orgStates, want) {
		t.Fatalf("state org form = %v, want %v: the seed is wrong", orgStates, want)
	}
	if !reflect.DeepEqual(teamStates, orgStates) {
		t.Fatalf("work_item_state_durations_daily: team form = %v, org form = %v", teamStates, orgStates)
	}
}
