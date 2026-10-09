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
				t.Errorf("%s (%s) keeps %v, want %v: one key for each measure column and not the retracted key", table, name, got, want)
			}
		}
	}
}
