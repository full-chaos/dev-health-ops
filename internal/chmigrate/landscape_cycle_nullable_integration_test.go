//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const landscapeCycleVersion = "146_landscape_cycle_nullable.sql"

// landscapeCycleColumn is the type and the default of
// ic_landscape_rolling_30d.cycle_p50_30d_hours.
func landscapeCycleColumn(ctx context.Context, t *testing.T, conn driver.Conn) (columnType, defaultKind, defaultExpression string) {
	t.Helper()
	if err := conn.QueryRow(ctx, `SELECT type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'ic_landscape_rolling_30d' AND name = 'cycle_p50_30d_hours'`).
		Scan(&columnType, &defaultKind, &defaultExpression); err != nil {
		t.Fatal(err)
	}
	return columnType, defaultKind, defaultExpression
}

// landscapeCycles inserts one point with the cycle 0 and one point that leaves
// the column out (a row of zeros of the stale-key rule writes its keys only),
// and reads each point's cycle back.
func landscapeCycles(ctx context.Context, t *testing.T, conn driver.Conn, org string) (measured, keysOnly *float64) {
	t.Helper()
	exec := func(query string) {
		t.Helper()
		if err := conn.Exec(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	exec(`INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm, cycle_p50_30d_hours, org_id)
VALUES ('00000000-0000-0000-0000-000000000000', '2026-09-21', 'measured', 'team-1', 'churn_throughput', 1, 1, 0.5, 0.5, 0, '` + org + `')`)
	exec(`INSERT INTO ic_landscape_rolling_30d (repo_id, as_of_day, identity_id, team_id, map_name, org_id)
VALUES ('00000000-0000-0000-0000-000000000000', '2026-09-21', 'keys-only', 'team-1', 'churn_throughput', '` + org + `')`)
	read := func(identity string) *float64 {
		t.Helper()
		var value *float64
		if err := conn.QueryRow(ctx, `SELECT cycle_p50_30d_hours FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND identity_id = ?`, org, identity).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	return read("measured"), read("keys-only")
}

// INVARIANT (CHAOS-12562): the head stores a person's 30-day landscape cycle
// as Nullable(Float64) with the default NULL. A stored 0 is a measured cycle of
// 0 hours; a row that leaves the column out (the row of zeros the stale-key rule
// writes) holds NULL, so it is no measure and reads not live. The migration
// runs again with no change.
func TestMigration146MakesTheLandscapeCycleNullable(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	check := func(when string) {
		t.Helper()
		columnType, defaultKind, defaultExpression := landscapeCycleColumn(ctx, t, conn)
		if columnType != "Nullable(Float64)" || (defaultKind != "" && defaultExpression != "NULL") {
			t.Fatalf("%s: cycle_p50_30d_hours is %s %s %q, want Nullable(Float64) with no default but NULL",
				when, columnType, defaultKind, defaultExpression)
		}
	}
	check("at the head")
	measured, keysOnly := landscapeCycles(ctx, t, conn, "org-146")
	if measured == nil || *measured != 0 {
		t.Errorf("a stored cycle of 0 reads %v, want 0", measured)
	}
	if keysOnly != nil {
		t.Errorf("a row that leaves the cycle out reads %v, want NULL", *keysOnly)
	}

	body, err := os.ReadFile("sql/" + landscapeCycleVersion)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("run again: %s: %v", statement, err)
		}
	}
	check("after a second run")
}

// The hand-only rollback, rollback/146_landscape_cycle_nullable.down.sql,
// rehearsed up -> down -> up on this build: down gives the column back as
// Float64 DEFAULT 0 (a NULL becomes 0, the value the older build reads as no
// cycle) and removes the version row; the next upgrade applies 146 again.
func TestRollback146UpDownUp(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	if _, keysOnly := landscapeCycles(ctx, t, conn, "org-146-down"); keysOnly != nil {
		t.Fatalf("at the head a row that leaves the cycle out reads %v, want NULL", *keysOnly)
	}
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", landscapeCycleVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatalf("at the head schema_migrations holds no %s", landscapeCycleVersion)
	}

	body, err := os.ReadFile("rollback/146_landscape_cycle_nullable.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if columnType, defaultKind, defaultExpression := landscapeCycleColumn(ctx, t, conn); columnType != "Float64" ||
		defaultKind != "DEFAULT" || defaultExpression != "0" {
		t.Fatalf("after the rollback: %s %s %q, want Float64 DEFAULT 0", columnType, defaultKind, defaultExpression)
	}
	var cycle float64
	if err := conn.QueryRow(ctx, `SELECT cycle_p50_30d_hours FROM ic_landscape_rolling_30d FINAL
WHERE org_id = 'org-146-down' AND identity_id = 'keys-only'`).Scan(&cycle); err != nil || cycle != 0 {
		t.Fatalf("after the rollback the NULL cycle reads %v, %v; want 0", cycle, err)
	}
	if versionRows() != 0 {
		t.Fatalf("after the rollback schema_migrations still holds %s", landscapeCycleVersion)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != landscapeCycleVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want %s applied", result, err, landscapeCycleVersion)
	}
	if columnType, _, _ := landscapeCycleColumn(ctx, t, conn); columnType != "Nullable(Float64)" {
		t.Fatalf("after the upgrade again: %s, want Nullable(Float64)", columnType)
	}
}
