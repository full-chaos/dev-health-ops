//go:build integration

package liverow

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTheCICDMarkerIsReadAfterTheNewestRow runs both forms of the rule on a real ClickHouse, on the schema of the
// migration chain, for cicd_metrics_daily (migration 159). Keys (repositories of one day):
//   - "live": one computed row -> kept.
//   - "value, then retraction": a computed row, then a NEWER row with retracted = 1 -> dropped. A form that filtered the
//     marker before it chose the newest row would serve the older row.
//   - "retraction, then value": a retraction, then a NEWER computed row -> kept.
func TestTheCICDMarkerIsReadAfterTheNewestRow(t *testing.T) {
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

	const (
		table     = "cicd_metrics_daily"
		org       = "org-cicd-marker"
		live      = "14016000-0000-4000-8000-0000000000e1"
		retracted = "14016000-0000-4000-8000-0000000000e2"
		cameBack  = "14016000-0000-4000-8000-0000000000e3"
	)
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)
	insert := func(repo string, marker uint8, at time.Time) {
		t.Helper()
		pipelines := uint32(2)
		if marker == 1 {
			pipelines = 0
		}
		if err := conn.Exec(ctx, `INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, computed_at, retracted)
VALUES (?, ?, ?, ?, 0.5, ?, ?)`, org, repo, day, pipelines, at, marker); err != nil {
			t.Fatal(err)
		}
	}
	insert(live, 0, newer)
	insert(retracted, 0, older)
	insert(retracted, 1, newer)
	insert(cameBack, 1, older)
	insert(cameBack, 0, newer)

	read := func(statement string) []string {
		t.Helper()
		rows, err := conn.Query(ctx, statement, org)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		defer rows.Close()
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return keys
	}
	want := []string{live, cameBack}
	forms := map[string]string{
		"newest row by argMax": fmt.Sprintf(
			"SELECT toString(repo_id) AS id FROM %[1]s WHERE org_id = ? GROUP BY repo_id HAVING %[2]s ORDER BY id",
			table, NewestPredicate(table, "")),
		"newest row by argMax, aliased": fmt.Sprintf(
			"SELECT toString(x.repo_id) AS id FROM %[1]s AS x WHERE x.org_id = ? GROUP BY x.repo_id HAVING %[2]s ORDER BY id",
			table, NewestPredicate(table, "x")),
		"newest row by LIMIT 1 BY": fmt.Sprintf(
			"SELECT toString(repo_id) AS id FROM (SELECT * FROM %[1]s WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS %[1]s WHERE %[2]s ORDER BY id",
			table, Predicate(table, "")),
		"FINAL": fmt.Sprintf(
			"SELECT toString(repo_id) AS id FROM %[1]s FINAL WHERE org_id = ? AND %[2]s ORDER BY id",
			table, Predicate(table, "")),
		"FINAL, aliased": fmt.Sprintf(
			"SELECT toString(x.repo_id) AS id FROM %[1]s AS x FINAL WHERE x.org_id = ? AND %[2]s ORDER BY id",
			table, Predicate(table, "x")),
	}
	for name, statement := range forms {
		if got := read(statement); !reflect.DeepEqual(got, want) {
			t.Errorf("%s keeps %v, want %v: the live key and the key that came back, not the retracted key", name, got, want)
		}
	}
	if got := Columns(table); !reflect.DeepEqual(got, []string{"retracted"}) {
		t.Errorf("Columns(%s) = %v, want the marker only", table, got)
	}
}
