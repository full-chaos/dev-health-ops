//go:build integration

package liverow

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTheRuleKeepsEachMeasurementAndDropsARetraction runs both forms of the
// rule on a real ClickHouse, on the schema of the migration chain, for EVERY
// registered table.
//
// Each table gets one key for each measure column, with 1 in that column and
// the default in every other: a row with one measure is a measurement,
// whichever measure it is. For compounding_risk_daily those are also the rows
// of a team that was measured with too little data: a stored weight, and NULL
// in the score. It also gets a key that was measured at
// first and holds a retraction row now (the key and computed_at only, as the
// daily writer stores it). Both forms must keep each of the first keys and
// drop the retracted one. A form that ran before the newest row was chosen
// would keep the retracted key, because its older row is a measurement.
//
// A third kind of key came back: it was measured, retracted, and measured
// again. Its newest row is a measurement, so both forms must keep it. A form
// that asked "did this key ever hold a retraction row" would drop it for ever.
func TestTheRuleKeepsEachMeasurementAndDropsARetraction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	engines := map[string]string{}
	for _, object := range baseline.Objects {
		engines[object.Name] = object.Engine
	}

	const org = "org-live-row-rule"
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)

	for _, table := range Tables() {
		// The team column and the measure columns come from the registry, or
		// from this package for the two tables with the rule in their own
		// writers.
		id, dayColumn, markers := "team_id", "day", ownWriterMarkers[table]
		if declared, ok := teamkeytables.ByTable(table); ok {
			id, dayColumn = declared.TeamColumn, declared.DayColumn
			markers = append(append([]string(nil), declared.Measures...), declared.NullableMeasures...)
		}
		if len(markers) == 0 {
			t.Fatalf("%s: no measure column", table)
		}
		var want []string
		for _, marker := range markers {
			key := "only " + marker
			want = append(want, key)
			if err := conn.Exec(ctx, fmt.Sprintf(
				"INSERT INTO %s (org_id, %s, %s, %s, computed_at) VALUES (?, ?, ?, 1, ?)", table, dayColumn, id, marker),
				org, day, key, newer); err != nil {
				t.Fatalf("%s: insert %q: %v", table, key, err)
			}
		}
		sort.Strings(want)
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (org_id, %s, %s, %s, computed_at) VALUES (?, ?, 'retracted', 5, ?)", table, dayColumn, id, markers[0]),
			org, day, older); err != nil {
			t.Fatalf("%s: insert the measured row of the retracted key: %v", table, err)
		}
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (org_id, %s, %s, computed_at) VALUES (?, ?, 'retracted', ?)", table, dayColumn, id),
			org, day, newer); err != nil {
			t.Fatalf("%s: insert the retraction row: %v", table, err)
		}

		for step, statement := range []string{
			fmt.Sprintf("INSERT INTO %s (org_id, %s, %s, %s, computed_at) VALUES (?, ?, 'came back', 5, ?)", table, dayColumn, id, markers[0]),
			fmt.Sprintf("INSERT INTO %s (org_id, %s, %s, computed_at) VALUES (?, ?, 'came back', ?)", table, dayColumn, id),
			fmt.Sprintf("INSERT INTO %s (org_id, %s, %s, %s, computed_at) VALUES (?, ?, 'came back', 7, ?)", table, dayColumn, id, markers[0]),
		} {
			if err := conn.Exec(ctx, statement, org, day, older.Add(time.Duration(step)*time.Hour)); err != nil {
				t.Fatalf("%s: insert step %d of the key that came back: %v", table, step, err)
			}
		}
		want = append(want, "came back")
		sort.Strings(want)

		forms := map[string]string{
			"newest row by argMax": fmt.Sprintf(
				"SELECT toString(ifNull(%[2]s, '')) AS id FROM %[1]s WHERE org_id = ? GROUP BY %[2]s HAVING %[3]s ORDER BY id",
				table, id, NewestPredicate(table, "")),
			"newest row by argMax, aliased": fmt.Sprintf(
				"SELECT toString(ifNull(x.%[2]s, '')) AS id FROM %[1]s AS x WHERE x.org_id = ? GROUP BY x.%[2]s HAVING %[3]s ORDER BY id",
				table, id, NewestPredicate(table, "x")),
		}
		// FINAL needs an engine that replaces; the plain MergeTree tables are
		// read by argMax only.
		if strings.HasPrefix(engines[table], "Replacing") {
			forms["FINAL"] = fmt.Sprintf(
				"SELECT toString(ifNull(%[2]s, '')) AS id FROM %[1]s FINAL WHERE org_id = ? AND %[3]s ORDER BY id",
				table, id, Predicate(table, ""))
			forms["FINAL, aliased"] = fmt.Sprintf(
				"SELECT toString(ifNull(x.%[2]s, '')) AS id FROM %[1]s AS x FINAL WHERE x.org_id = ? AND %[3]s ORDER BY id",
				table, id, Predicate(table, "x"))
		}
		for name, statement := range forms {
			rows, err := conn.Query(ctx, statement, org)
			if err != nil {
				t.Errorf("%s (%s): %v", table, name, err)
				continue
			}
			var got []string
			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err != nil {
					t.Fatalf("%s (%s): scan: %v", table, name, err)
				}
				got = append(got, key)
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("%s (%s): close: %v", table, name, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s (%s) keeps %v, want %v: one key for each measure column, the key that came back, and not the retracted key", table, name, got, want)
			}
		}
	}
}

// TestFinalFollowsInsertOrderWhenComputeTimesAreEqual holds what a FINAL read
// with the rule does when a measured row and a row of zeros of ONE key carry
// the same computed_at (the column has second precision in this table, so two
// writes inside one second tie). The engine keeps the row inserted last, so
// the key is a measurement when the measured row came last and a retraction
// when the row of zeros came last. A reader that takes the newest row with
// argMax has no such order and may take either row of a tie; that case is not
// asserted here, and a writer must not create it.
func TestFinalFollowsInsertOrderWhenComputeTimesAreEqual(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const org = "org-live-row-tie"
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	at := day.Add(10 * time.Hour)
	measured := `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, items_completed, computed_at)
VALUES (?, ?, 'github', 'scope', ?, 3, ?)`
	zeros := `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, computed_at)
VALUES (?, ?, 'github', 'scope', ?, ?)`
	for _, write := range []struct{ statement, team string }{
		{measured, "zeros last"}, {zeros, "zeros last"},
		{zeros, "measured last"}, {measured, "measured last"},
	} {
		if err := conn.Exec(ctx, write.statement, org, day, write.team, at); err != nil {
			t.Fatalf("insert for %q: %v", write.team, err)
		}
	}
	rows, err := conn.Query(ctx, "SELECT team_id FROM work_item_metrics_daily FINAL WHERE org_id = ? AND "+
		Predicate("work_item_metrics_daily", "")+" ORDER BY team_id", org)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for rows.Next() {
		var team string
		if err := rows.Scan(&team); err != nil {
			t.Fatal(err)
		}
		kept = append(kept, team)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"measured last"}; !reflect.DeepEqual(kept, want) {
		t.Fatalf("FINAL with the rule keeps %v, want %v: for equal compute times the row inserted last is the newest", kept, want)
	}
}
