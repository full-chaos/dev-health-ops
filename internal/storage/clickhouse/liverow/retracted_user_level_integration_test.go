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

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTheMarkerTellsARowOfZerosFromAMeasuredRowOfZeros runs both forms of the
// rule on a real ClickHouse, on the schema of the migration chain, for the two
// user-level tables (CHAOS-9135). A person can have a MEASURED row with every
// measure at 0 (assigned only to an item created that day): no measure tells
// it from the row of zeros, so the marker does, and it is read AFTER the newest
// row of a key is chosen.
//
// Keys:
//   - "measured zero": one row, every measure 0, marker 0 -> live.
//   - "value, then retraction": a measured row, then a NEWER row with marker 1
//     -> hidden. A form that filtered the marker before it chose the newest row
//     would serve the older row.
//   - "retraction, then value": a retraction, then a NEWER measured row -> live.
//   - "retracted, team filter": the same as the second key, read with a team
//     filter that runs before the newest row is chosen. The row of zeros copies
//     the team of the row it retracts, so the filter keeps both rows and the
//     newest decides; "no carry" is the control: a row of zeros that does not
//     carry the team is dropped by that filter and the older row is served.
func TestTheMarkerTellsARowOfZerosFromAMeasuredRowOfZeros(t *testing.T) {
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

	const org = "org-retracted-marker"
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)

	type tableCase struct {
		table string
		// insert writes one row of the key `person` with the marker, the team and the
		// version; nullTeam stores no team (a row of zeros that does not carry it).
		insert func(person string, marker uint8, team string, nullTeam bool, at time.Time) error
		// keyExpression is the key of the person in a read.
		keyExpression string
	}
	cases := []tableCase{
		{
			table: "work_item_user_metrics_daily", keyExpression: "user_identity",
			insert: func(person string, marker uint8, team string, nullTeam bool, at time.Time) error {
				teamValue := team
				if nullTeam {
					teamValue = ""
				}
				return conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
 (org_id, day, provider, work_scope_id, user_identity, team_id, team_name, computed_at, retracted) VALUES (?, ?, 'jira', 'PLAT', ?, ?, ?, ?, ?)`,
					org, day, person, teamValue, teamValue, at, marker)
			},
		},
		{
			table: "user_metrics_daily", keyExpression: "author_email",
			insert: func(person string, marker uint8, team string, nullTeam bool, at time.Time) error {
				var teamValue *string
				if !nullTeam {
					teamValue = &team
				}
				return conn.Exec(ctx, `INSERT INTO user_metrics_daily
 (org_id, day, repo_id, author_email, team_id, team_name, computed_at, retracted) VALUES (?, ?, '00000000-0000-4000-8000-0000000000a1', ?, ?, ?, ?, ?)`,
					org, day, person, teamValue, teamValue, at, marker)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.table, func(t *testing.T) {
			declared, ok := teamkeytables.ByTable(c.table)
			if !ok || declared.Retracted == "" {
				t.Fatalf("%s is not a table with a retraction marker", c.table)
			}
			must := func(err error, what string) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
			}
			must(c.insert("measured zero", 0, "team-a", false, newer), "measured zero")
			must(c.insert("value then retraction", 0, "team-a", false, older), "value")
			must(c.insert("value then retraction", 1, "team-a", false, newer), "retraction")
			must(c.insert("retraction then value", 1, "team-a", false, older), "retraction")
			must(c.insert("retraction then value", 0, "team-a", false, newer), "value again")
			// The control: the row of zeros does not carry the team.
			must(c.insert("no carry", 0, "team-a", false, older), "value")
			must(c.insert("no carry", 1, "", true, newer), "retraction without the team")

			read := func(statement string) []string {
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
			wantAll := []string{"measured zero", "retraction then value"}
			forms := map[string]string{
				"newest row by argMax": fmt.Sprintf(
					"SELECT toString(%[2]s) AS id FROM %[1]s WHERE org_id = ? GROUP BY %[2]s HAVING %[3]s ORDER BY id",
					c.table, c.keyExpression, NewestPredicate(c.table, "")),
				"newest row by argMax, aliased": fmt.Sprintf(
					"SELECT toString(x.%[2]s) AS id FROM %[1]s AS x WHERE x.org_id = ? GROUP BY x.%[2]s HAVING %[3]s ORDER BY id",
					c.table, c.keyExpression, NewestPredicate(c.table, "x")),
				"FINAL": fmt.Sprintf(
					"SELECT toString(%[2]s) AS id FROM %[1]s FINAL WHERE org_id = ? AND %[3]s ORDER BY id",
					c.table, c.keyExpression, Predicate(c.table, "")),
				"FINAL, aliased": fmt.Sprintf(
					"SELECT toString(x.%[2]s) AS id FROM %[1]s AS x FINAL WHERE x.org_id = ? AND %[3]s ORDER BY id",
					c.table, c.keyExpression, Predicate(c.table, "x")),
			}
			for name, statement := range forms {
				got := read(statement)
				// "no carry" is not hidden by the rule: its newest row holds the marker, so it is
				// hidden too; the control is the team-filtered form below.
				if !reflect.DeepEqual(got, wantAll) {
					t.Errorf("%s keeps %v, want %v: the measured row of zeros and the key that came back, not a retracted key", name, got, wantAll)
				}
			}
			// A reader that filters on the team BEFORE it takes the newest row: the
			// row of zeros that carries the team is kept next to its older row and
			// the newest decides; the row of zeros with no team is dropped by the
			// filter and the older row is served (the key "no carry" reads as live).
			filtered := read(fmt.Sprintf(
				"SELECT toString(%[2]s) AS id FROM %[1]s WHERE org_id = ? AND team_id = 'team-a' GROUP BY %[2]s HAVING %[3]s ORDER BY id",
				c.table, c.keyExpression, NewestPredicate(c.table, "")))
			wantFiltered := []string{"measured zero", "no carry", "retraction then value"}
			if !reflect.DeepEqual(filtered, wantFiltered) {
				t.Errorf("the team-filtered read keeps %v, want %v: a retraction that carries the team hides its key under a team filter, one that does not does not (control)", filtered, wantFiltered)
			}
			if len(Columns(c.table)) != 1 || Columns(c.table)[0] != "retracted" {
				t.Errorf("Columns(%s) = %v, want the marker only", c.table, Columns(c.table))
			}
		})
	}
}
