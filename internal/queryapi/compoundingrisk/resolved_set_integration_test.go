//go:build integration

package compoundingrisk

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-14480: a resolved set is bound whole. 50,000 synthetic repository ids;
// only the ids that sort first, in the middle and last have a repository and a
// risk row. Each reader binds all 50,000 in one statement and serves the three.
func TestRealClickHouse_ResolvedSetOf50000IdsIsServedWhole(t *testing.T) {
	ctx, conn, client := startStore(t)
	const n = 50000
	ids := make(resolvedIDs, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
	}
	sort.Strings(ids)
	first, middle, last := ids[0], ids[n/2], ids[n-1]
	now := sweepNow()
	day := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	world := sweepWorld{
		repos: []sweepRepo{{first, "acme/first"}, {middle, "acme/middle"}, {last, "acme/last"}},
		teams: []sweepTeam{{id: "t-none", name: "None"}},
		risk: []sweepRiskRow{
			bigRiskRow(day, first, .25, now),
			bigRiskRow(day, middle, .5, now),
			bigRiskRow(day, last, .75, now),
		},
	}
	uuidOf := map[string]string{first: first, middle: middle, last: last}
	sweepSeed(t, ctx, conn, world, uuidOf)
	t.Logf("joined ids: %d bytes", len(strings.Join(ids, ",")))

	rows, err := latestRowsLimit(ctx, client, sweepOrg, day, scopeRepo, ids, 0)
	if err != nil {
		t.Fatalf("latestRowsLimit with %d ids: the server refused or failed: %v", n, err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.scopeID)
	}
	sort.Strings(got)
	if want := []string{first, middle, last}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("latestRowsLimit rows %v; want %v (first, middle and last id of the set)", got, want)
	}

	trend, err := repoTrend(ctx, client, sweepOrg, day, 7, ids)
	if err != nil {
		t.Fatalf("repoTrend with %d ids: the server refused or failed: %v", n, err)
	}
	if len(trend) != 1 || trend[0].day.Format(dateLayout) != day.Format(dateLayout) || trend[0].avgScore == nil || *trend[0].avgScore != .5 {
		var points []string
		for _, p := range trend {
			points = append(points, p.day.Format(dateLayout)+"="+sweepNum(p.avgScore))
		}
		t.Errorf("repoTrend %v; want [%s=0.5] (the mean of the three rows)", points, day.Format(dateLayout))
	}

	picked, err := latestDay(ctx, client, sweepOrg, scopeRepo, ids, day.AddDate(0, 0, -6), day)
	if err != nil {
		t.Fatalf("latestDay with %d ids: the server refused or failed: %v", n, err)
	}
	if picked == nil || picked.Format(dateLayout) != day.Format(dateLayout) {
		t.Errorf("latestDay %v; want %s", picked, day.Format(dateLayout))
	}
}

// CHAOS-14521: stored team rows for 501 named teams on real ClickHouse. "rows":
// every team has a stored row on d; "day": only tm-500, the 501st named team,
// has a stored row on d (every other row is on the day before). Both pick the
// day and serve tm-500's stored row on d.
func TestRealClickHouse_FiveHundredOneNamedTeamsStoredRowsAnswerTheLastTeamsRowAndDay(t *testing.T) {
	_, conn, client := startStore(t)
	// Each answer reads ownership once per named team, more than once.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name     string
		lastOnly bool
		wantRows int
	}{
		{"rows", false, maxRows + 1},
		{"day", true, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			world, teamIDs := namedTeamsWorld(maxRows+1, d, c.lastOnly)
			uuidOf, _ := sweepUUIDs(world)
			sweepSeed(t, ctx, conn, world, uuidOf)
			got, err := Resolve(ctx, client, sweepOrg, namedTeamsFilter(teamIDs, nil), sweepNow())
			if err != nil {
				t.Fatal(err)
			}
			if last := servedTeamRow(got, "tm-500"); len(got.Rows) != c.wantRows || last != lastNamedTeamStoredRow {
				t.Errorf("rows=%d tm-500 row %s; want %d rows and tm-500 row %s (its stored row on the picked day)",
					len(got.Rows), last, c.wantRows, lastNamedTeamStoredRow)
			}
		})
	}
}

// CHAOS-14521: the stored team reads at 50,000 named team ids. The named list
// has no length cap of its own (CHAOS-14515); a request body bounds it. The
// client has the options of the query API's read client (chclient.Options:
// max_bytes_to_read unrestricted, max_result_rows 500,000) and its default
// max_execution_time of 10 s per statement. The work bound reads
// system.query_log: two statements, each with read_rows at most 1.5 x the
// 50,000 stored rows, each run with max_execution_time 10. No wall-clock bound.
// The server is private (SYSTEM FLUSH LOGS is a server command) and the log
// read keeps only this database and the log_comment the two reads carry.
func TestRealClickHouse_StoredTeamReadsBind50000NamedTeams(t *testing.T) {
	_, conn, _, uri := startStoreURI(t, containers.StartClickHouseIsolated)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	unrestricted, resultRows := uint64(0), uint(500_000)
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: uri, MaxBytesToRead: &unrestricted, MaxResultRows: &resultRows})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const n = 50000
	d := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	exec(t, ctx, conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity,
      w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
      SELECT '%s', toDate('%s'), 'team', concat('tm-', leftPad(toString(number), 5, '0')), 0.5, 'elevated',
      0.4, 0.3, 0.2, 0.1, 0.4, 0.7, toDateTime('2026-03-10 01:00:00', 'UTC') FROM numbers(%d)`, sweepOrg, d.Format(dateLayout), n)
	ids := make(namedTeamIDs, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, fmt.Sprintf("tm-%05d", i))
	}

	tag := fmt.Sprintf("stored-team-reads-%d", time.Now().UnixNano())
	reads := stdclickhouse.Context(ctx, stdclickhouse.WithSettings(stdclickhouse.Settings{"log_comment": tag}))
	picked, err := latestDay(reads, client, sweepOrg, scopeTeam, ids, d.AddDate(0, 0, -6), d)
	if err != nil {
		t.Fatalf("stored day pick with %d named teams: %v", n, err)
	}
	if picked == nil || picked.Format(dateLayout) != d.Format(dateLayout) {
		t.Fatalf("stored day pick %v; want %s", picked, d.Format(dateLayout))
	}
	rows, err := latestRowsLimit(reads, client, sweepOrg, d, scopeTeam, ids, n)
	if err != nil {
		t.Fatalf("stored row read with %d named teams: %v", n, err)
	}
	served := make(map[string]bool, len(rows))
	for _, r := range rows {
		served[r.scopeID] = true
	}
	var missing []string
	for _, id := range ids {
		if !served[id] {
			missing = append(missing, id)
		}
	}
	if len(rows) != n || len(missing) > 0 {
		t.Fatalf("stored row read: %d rows, %d named teams missing (first %v); want every one of the %d named teams", len(rows), len(missing), missing[:min(3, len(missing))], n)
	}

	exec(t, ctx, conn, "SYSTEM FLUSH LOGS")
	var statements, overBound, otherLimit uint64
	if err := conn.QueryRow(ctx, `SELECT count(), countIf(read_rows > ?), countIf(Settings['max_execution_time'] != '10')
      FROM system.query_log
      WHERE log_comment = ? AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND query_kind = 'Select'
        AND position(query, 'FROM compounding_risk_daily') > 0`,
		uint64(n*3/2), tag).Scan(&statements, &overBound, &otherLimit); err != nil {
		t.Fatalf("read system.query_log: %v", err)
	}
	if statements != 2 || overBound != 0 || otherLimit != 0 {
		t.Errorf("query_log: %d stored team statements, %d with read_rows over %d, %d without max_execution_time 10; want 2, 0, 0",
			statements, overBound, n*3/2, otherLimit)
	}
}
