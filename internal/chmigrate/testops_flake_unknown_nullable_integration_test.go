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
)

const testopsRatesVersion = "150_testops_test_flake_unknown_nullable.sql"

var testopsRateColumns = []string{"flake_rate", "retry_dependency_rate", "failure_recurrence_score"}

func testopsRateTypes(ctx context.Context, t *testing.T, conn driver.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 'testops_test_metrics_daily'
  AND name IN ('flake_rate', 'retry_dependency_rate', 'failure_recurrence_score')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types := map[string]string{}
	for rows.Next() {
		var name, columnType string
		if err := rows.Scan(&name, &columnType); err != nil {
			t.Fatal(err)
		}
		types[name] = columnType
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return types
}

func requireTestopsRateTypes(ctx context.Context, t *testing.T, conn driver.Conn, when, want string) {
	t.Helper()
	types := testopsRateTypes(ctx, t, conn)
	for _, column := range testopsRateColumns {
		if types[column] != want {
			t.Fatalf("%s: %s is %q, want %s", when, column, types[column], want)
		}
	}
}

// testopsChainBefore150 brings a scratch database to the chain before 150 and
// returns the connection, the migrator and 150's chain file.
func testopsChainBefore150(ctx context.Context, t *testing.T) (driver.Conn, chmigrate.DB, chmigrate.Baseline, []chmigrate.ChainFile, chmigrate.ChainFile) {
	t.Helper()
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
		if file.Version == testopsRatesVersion {
			migration = file
			continue
		}
		before = append(before, file)
	}
	if migration.Version == "" {
		t.Fatalf("the live chain does not hold %s", testopsRatesVersion)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatalf("the chain before 150: %v", err)
	}
	requireTestopsRateTypes(ctx, t, conn, "before 150", "Float64")
	return conn, db, baseline, chain, migration
}

// The rate sums are taken in millionths as integers, so two reads compare exactly whatever order the parts are summed in.
type testopsRateSums struct {
	rows, nulls              uint64
	flake, retry, recurrence int64
}

func readTestopsRateSums(ctx context.Context, t *testing.T, conn driver.Conn) testopsRateSums {
	t.Helper()
	var sums testopsRateSums
	if err := conn.QueryRow(ctx, `SELECT count(),
    countIf(flake_rate IS NULL OR retry_dependency_rate IS NULL OR failure_recurrence_score IS NULL),
    toInt64(sum(toInt64(round(flake_rate * 1000000)))), toInt64(sum(toInt64(round(retry_dependency_rate * 1000000)))),
    toInt64(sum(toInt64(round(failure_recurrence_score * 1000000))))
FROM testops_test_metrics_daily`).Scan(&sums.rows, &sums.nulls, &sums.flake, &sums.retry, &sums.recurrence); err != nil {
		t.Fatal(err)
	}
	return sums
}

type testopsMutations struct{ total, done uint64 }

func readTestopsMutations(ctx context.Context, t *testing.T, conn driver.Conn) testopsMutations {
	t.Helper()
	var m testopsMutations
	if err := conn.QueryRow(ctx, `SELECT count(), countIf(is_done) FROM system.mutations
WHERE database = currentDatabase() AND table = 'testops_test_metrics_daily'`).Scan(&m.total, &m.done); err != nil {
		t.Fatal(err)
	}
	return m
}

func testopsActiveParts(ctx context.Context, t *testing.T, conn driver.Conn) (parts, partitions uint64) {
	t.Helper()
	if err := conn.QueryRow(ctx, `SELECT count(), uniqExact(partition) FROM system.parts
WHERE database = currentDatabase() AND table = 'testops_test_metrics_daily' AND active`).Scan(&parts, &partitions); err != nil {
		t.Fatal(err)
	}
	return parts, partitions
}

func runTestopsRatesStatements(ctx context.Context, t *testing.T, conn driver.Conn, sql, label string) {
	t.Helper()
	for _, statement := range chmigrate.SplitStatements(sql) {
		started := time.Now()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %s: %v", label, statement, err)
		}
		t.Logf("%s statement in %s: %.90q", label, time.Since(started).Round(time.Millisecond), statement)
	}
}

// 150 makes the three TestOps test rates Nullable(Float64). A row stored before
// 150 keeps its values (a stored 0 stays 0), a row stored after it can hold
// NULL, and every statement can run again without a change or a new mutation.
func TestMigration150MakesTheTestOpsRatesNullable(t *testing.T) {
	ctx := context.Background()
	conn, db, baseline, chain, migration := testopsChainBefore150(ctx, t)
	const org = "org-150"
	if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, flake_rate, retry_dependency_rate, failure_recurrence_score, org_id, computed_at)
VALUES ('00000000-0000-4000-8000-000000000001', '2026-10-01', 10, 0.2, 0.1, 0.5, ?, now()),
       ('00000000-0000-4000-8000-000000000002', '2026-10-01', 10, 0, 0, 0, ?, now())`, org, org); err != nil {
		t.Fatal(err)
	}
	before := readTestopsRateSums(ctx, t, conn)

	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("150 over the chain before it: %v", err)
	}
	requireTestopsRateTypes(ctx, t, conn, "at the head", "Nullable(Float64)")
	if after := readTestopsRateSums(ctx, t, conn); after != before || after.nulls != 0 {
		t.Fatalf("stored rows after 150 = %+v, want %+v with no NULL", after, before)
	}
	read := func(repo string) [3]*float64 {
		t.Helper()
		var flake, retry, recurrence *float64
		if err := conn.QueryRow(ctx, `SELECT flake_rate, retry_dependency_rate, failure_recurrence_score
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = toUUID(?)`, org, repo).Scan(&flake, &retry, &recurrence); err != nil {
			t.Fatal(err)
		}
		return [3]*float64{flake, retry, recurrence}
	}
	if got := read("00000000-0000-4000-8000-000000000002"); got[0] == nil || *got[0] != 0 || got[1] == nil || got[2] == nil {
		t.Errorf("a stored 0 after 150 reads %v, want 0 (never NULL)", got)
	}
	if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, flake_rate, retry_dependency_rate, failure_recurrence_score, org_id, computed_at)
VALUES ('00000000-0000-4000-8000-000000000003', '2026-10-01', 10, NULL, NULL, NULL, ?, now())`, org); err != nil {
		t.Fatal(err)
	}
	if got := read("00000000-0000-4000-8000-000000000003"); got[0] != nil || got[1] != nil || got[2] != nil {
		t.Errorf("a NULL row after 150 reads %v, want NULL in the three", got)
	}

	mutations := readTestopsMutations(ctx, t, conn)
	stored := readTestopsRateSums(ctx, t, conn)
	runTestopsRatesStatements(ctx, t, conn, migration.SQL, "150 again")
	requireTestopsRateTypes(ctx, t, conn, "after a second run", "Nullable(Float64)")
	if again := readTestopsMutations(ctx, t, conn); again.total != mutations.total {
		t.Errorf("a second run of 150 queued %d mutation(s), want none", again.total-mutations.total)
	}
	if again := readTestopsRateSums(ctx, t, conn); again != stored {
		t.Errorf("a second run of 150 changed the rows: %+v, want %+v", again, stored)
	}
}

// The hand-only rollback, rollback/150_testops_test_flake_unknown_nullable.down.sql,
// rehearsed up -> down -> up: a NULL becomes 0 (the value the older build
// writes), the three columns are Float64 again, the version row goes, and the
// next upgrade applies 150 again.
func TestRollback150UpDownUp(t *testing.T) {
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
	if err := conn.Exec(ctx, `INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, flake_rate, retry_dependency_rate, failure_recurrence_score, org_id, computed_at)
VALUES ('00000000-0000-4000-8000-000000000001', '2026-10-01', 10, NULL, NULL, NULL, 'org-150-down', now()),
       ('00000000-0000-4000-8000-000000000002', '2026-10-01', 10, 0.2, 0.1, 0.5, 'org-150-down', now())`); err != nil {
		t.Fatal(err)
	}
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", testopsRatesVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatalf("at the head schema_migrations holds no %s", testopsRatesVersion)
	}
	body, err := os.ReadFile("rollback/150_testops_test_flake_unknown_nullable.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	runTestopsRatesStatements(ctx, t, conn, string(body), "150 down")
	requireTestopsRateTypes(ctx, t, conn, "after the rollback", "Float64")
	var defaults uint64
	if err := conn.QueryRow(ctx, `SELECT countIf(default_kind != '') FROM system.columns
WHERE database = currentDatabase() AND table = 'testops_test_metrics_daily'
  AND name IN ('flake_rate', 'retry_dependency_rate', 'failure_recurrence_score')`).Scan(&defaults); err != nil || defaults != 0 {
		t.Fatalf("after the rollback %d of the three columns keep a default (%v), want none, as before 150", defaults, err)
	}
	if sums := readTestopsRateSums(ctx, t, conn); sums.rows != 2 || sums.nulls != 0 || sums.flake != 200000 || sums.retry != 100000 || sums.recurrence != 500000 {
		t.Errorf("after the rollback the rows sum to %+v, want the NULL row as 0 and the other kept", sums)
	}
	if versionRows() != 0 {
		t.Fatalf("after the rollback schema_migrations still holds %s", testopsRatesVersion)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != testopsRatesVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want %s applied", result, err, testopsRatesVersion)
	}
	requireTestopsRateTypes(ctx, t, conn, "after the upgrade again", "Nullable(Float64)")
}

// 150 at production scale, timed: CHMIGRATE_150_SEED_ROWS rows (default 2400,
// 10x the production table) in CHMIGRATE_150_SEED_PARTS active parts (default
// 8) over CHMIGRATE_150_SEED_MONTHS monthly partitions (default 5). Every stored
// value is kept, the run waits for its mutations, and a second run queues none.
func TestMigration150AtProductionScale(t *testing.T) {
	ctx := context.Background()
	setting := func(name string, fallback, limit int) int {
		t.Helper()
		text := os.Getenv(name)
		if text == "" {
			return fallback
		}
		value, err := strconv.Atoi(text)
		if err != nil || value < 1 || value > limit {
			t.Fatalf("%s = %q: want 1..%d", name, text, limit)
		}
		return value
	}
	seedRows := setting("CHMIGRATE_150_SEED_ROWS", 2400, 5_000_000)
	seedParts := setting("CHMIGRATE_150_SEED_PARTS", 8, 500)
	seedMonths := setting("CHMIGRATE_150_SEED_MONTHS", 5, 60)
	if seedParts < seedMonths {
		t.Fatalf("%d parts cannot cover %d monthly partitions", seedParts, seedMonths)
	}
	conn, db, baseline, chain, migration := testopsChainBefore150(ctx, t)
	exec := func(query string) {
		t.Helper()
		if err := conn.Exec(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	// Each INSERT is one part in one monthly partition; merges are held so the
	// part count is the seeded one when 150 starts.
	exec("SYSTEM STOP MERGES testops_test_metrics_daily")
	perPart := seedRows / seedParts
	for part := 0; part < seedParts; part++ {
		rows := perPart
		if part == seedParts-1 {
			rows = seedRows - perPart*(seedParts-1)
		}
		exec(fmt.Sprintf(`INSERT INTO testops_test_metrics_daily
    (repo_id, day, total_cases, passed_count, flake_rate, retry_dependency_rate, failure_recurrence_score,
     org_id, computed_at, flake_cases, distinct_cases)
SELECT generateUUIDv4(), addMonths(toDate('2026-01-01'), %d) + (number %% 28), 10, 10,
    (number %% 7) / 10, (number %% 5) / 10, (number %% 3) / 10, 'org-150-scale', now(), number %% 7, 10
FROM numbers(%d)`, part%seedMonths, rows))
	}
	parts, partitions := testopsActiveParts(ctx, t, conn)
	before := readTestopsRateSums(ctx, t, conn)
	mutationsBefore := readTestopsMutations(ctx, t, conn)
	t.Logf("seeded %d rows in %d active parts over %d partitions (asked %d/%d/%d)", before.rows, parts, partitions, seedRows, seedParts, seedMonths)
	if before.rows != uint64(seedRows) || parts != uint64(seedParts) || partitions != uint64(seedMonths) {
		t.Fatalf("seed = %d rows, %d parts, %d partitions; want %d, %d, %d", before.rows, parts, partitions, seedRows, seedParts, seedMonths)
	}
	exec("SYSTEM START MERGES testops_test_metrics_daily")

	started := time.Now()
	runTestopsRatesStatements(ctx, t, conn, migration.SQL, "150")
	elapsed := time.Since(started)
	mutationsAfter := readTestopsMutations(ctx, t, conn)
	t.Logf("150: %s for %d rows; mutations %d -> %d, done %d", elapsed.Round(time.Millisecond), before.rows, mutationsBefore.total, mutationsAfter.total, mutationsAfter.done)
	if mutationsAfter.done != mutationsAfter.total {
		t.Errorf("150 returned with %d of %d mutation(s) not done, want every one done (mutations_sync = 2)", mutationsAfter.total-mutationsAfter.done, mutationsAfter.total)
	}
	requireTestopsRateTypes(ctx, t, conn, "after 150", "Nullable(Float64)")
	if after := readTestopsRateSums(ctx, t, conn); after != before || after.nulls != 0 {
		t.Errorf("rows after 150 = %+v, want %+v with no NULL", after, before)
	}

	started = time.Now()
	runTestopsRatesStatements(ctx, t, conn, migration.SQL, "150 again")
	t.Logf("150 again: %s", time.Since(started).Round(time.Millisecond))
	if again := readTestopsMutations(ctx, t, conn); again.total != mutationsAfter.total {
		t.Errorf("a second run of 150 queued %d mutation(s), want none", again.total-mutationsAfter.total)
	}
	if again := readTestopsRateSums(ctx, t, conn); again != before {
		t.Errorf("a second run of 150 changed the rows: %+v, want %+v", again, before)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("the upgrade records 150: %v", err)
	}
}
