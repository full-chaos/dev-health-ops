//go:build integration

package chmigrate_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
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

// INVARIANT (CHAOS-12562): a row of zeros stored before 146 holds the old
// default 0 in the cycle. 146 sets it to NULL, so the row stays not live under
// the liveness test, which reads a Nullable measure as live when it is not
// NULL. A computed point with a cycle of 0 hours (x_norm above 0) keeps its 0.
//
// CHMIGRATE_146_SEED_ROWS (default 630) seeds that many more points before 146,
// one row of zeros in each 63 rows for 10 (the share of rows of zeros on the dev
// store: 10932 of 68865), the others computed points, some with a cycle of 0.
// The test logs the rows the UPDATE changed and the wall time of each statement
// of 146.
func TestMigration146LeavesAStoredRowOfZerosNotLive(t *testing.T) {
	ctx := context.Background()
	seedRows := 630
	if text := os.Getenv("CHMIGRATE_146_SEED_ROWS"); text != "" {
		parsed, err := strconv.Atoi(text)
		if err != nil || parsed < 0 {
			t.Fatalf("CHMIGRATE_146_SEED_ROWS = %q: want a count", text)
		}
		seedRows = parsed
	}
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
	var before []chmigrate.ChainFile
	var migration chmigrate.ChainFile
	for _, file := range chain {
		if file.Version == landscapeCycleVersion {
			migration = file
			continue
		}
		before = append(before, file)
	}
	if migration.Version == "" {
		t.Fatalf("the live chain does not hold %s", landscapeCycleVersion)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatalf("the chain before 146: %v", err)
	}
	if columnType, _, _ := landscapeCycleColumn(ctx, t, conn); columnType != "Float64" {
		t.Fatalf("before 146 the cycle is %s, want Float64", columnType)
	}
	const org = "org-146-zeros"
	exec := func(query string) {
		t.Helper()
		if err := conn.Exec(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	// The row of zeros the stale-key rule wrote before 146: keys only.
	exec(`INSERT INTO ic_landscape_rolling_30d (repo_id, as_of_day, identity_id, team_id, map_name, org_id)
VALUES ('00000000-0000-0000-0000-000000000000', '2026-09-21', 'retracted', 'team-retired', 'churn_throughput', '` + org + `')`)
	// A computed point of a person whose cycle is 0 hours.
	exec(`INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm, churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, org_id)
VALUES ('00000000-0000-0000-0000-000000000000', '2026-09-21', 'zero-hours', 'team-1', 'cycle_throughput', 0, 1, 0.5, 0.5, 0, 1, 0, 0, '` + org + `')`)
	// The seeded points: number % 63 < 10 is a row of zeros (keys only), the
	// others computed points (x_norm above 0), every fifth with a cycle of 0.
	if seedRows > 0 {
		exec(fmt.Sprintf(`INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
     churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, org_id)
SELECT toUUID('00000000-0000-0000-0000-000000000000'), toDate('2026-08-01') + intDiv(number, 2000),
    concat('seed-', toString(number)), 'team-seed', 'churn_throughput',
    if(zero, 0, 1), if(zero, 0, 2), if(zero, 0, 0.5), if(zero, 0, 0.5),
    if(zero, 0, 10), if(zero, 0, 1), if(zero, 0, toFloat64(number %% 5)), 0, 'org-146-seed'
FROM (SELECT number, number %% 63 < 10 AS zero FROM numbers(%d))`, seedRows))
	}
	nullCycles := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM ic_landscape_rolling_30d WHERE cycle_p50_30d_hours IS NULL").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	var total uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM ic_landscape_rolling_30d").Scan(&total); err != nil {
		t.Fatal(err)
	}

	// Each statement of 146 on its own, timed; then the upgrade records 146
	// (and runs its statements again, which changes nothing).
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		started := time.Now()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		t.Logf("146 statement in %s: %.80q", time.Since(started).Round(time.Millisecond), statement)
	}
	changed := nullCycles()
	t.Logf("146 set the cycle of %d of %d stored rows to NULL (seeded %d)", changed, total, seedRows)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("146 over the chain before it: %v", err)
	}
	if again := nullCycles(); again != changed {
		t.Errorf("the upgrade after the statements changed %d more rows, want none", again-changed)
	}
	if wantSeeded := uint64(seedRows/63*10 + min(seedRows%63, 10)); changed != 1+wantSeeded {
		t.Errorf("146 changed %d rows, want %d (the row of zeros and the %d seeded rows of zeros)", changed, 1+wantSeeded, wantSeeded)
	}
	read := func(identity string) (cycle *float64, live uint8) {
		t.Helper()
		if err := conn.QueryRow(ctx, `SELECT cycle_p50_30d_hours, toUInt8(`+teamkeytables.ICLandscapeRolling30d.LiveRow("")+`)
FROM ic_landscape_rolling_30d FINAL WHERE org_id = ? AND identity_id = ?`, org, identity).Scan(&cycle, &live); err != nil {
			t.Fatal(err)
		}
		return cycle, live
	}
	if cycle, live := read("retracted"); cycle != nil || live != 0 {
		t.Errorf("the stored row of zeros after 146: cycle %s, live %d; want NULL and not live", optionalCycle(cycle), live)
	}
	if cycle, live := read("zero-hours"); cycle == nil || *cycle != 0 || live != 1 {
		t.Errorf("the computed point of 0 hours after 146: cycle %s, live %d; want 0 and live", optionalCycle(cycle), live)
	}
}

func optionalCycle(value *float64) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprint(*value)
}
