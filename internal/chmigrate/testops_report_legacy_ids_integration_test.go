//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// The column lists of the build before 152 (providersync github_tests_effects_clickhouse.go at 151): an older worker inserts with them.
const (
	legacyIDsOldSuiteInsert = `INSERT INTO test_suite_results (org_id,repo_id,run_id,suite_id,suite_name,framework,environment,total_count,passed_count,failed_count,skipped_count,error_count,quarantined_count,retried_count,duration_seconds,started_at,finished_at,team_id,service_id,last_synced,report_slot,run_attempt)
VALUES ('org-152', '00000000-0000-4000-8000-000000000152', ?, 's1', 'pkg', NULL, NULL, 2, 1, 1, 0, 0, 0, 0, 1.5, now64(3), now64(3), NULL, NULL, now64(3), 'go-test-1', 1)`
	legacyIDsOldCoverageInsert = `INSERT INTO coverage_snapshots (org_id,repo_id,run_id,snapshot_id,report_format,lines_total,lines_covered,line_coverage_pct,branches_total,branches_covered,branch_coverage_pct,functions_total,functions_covered,commit_hash,branch,pr_number,team_id,service_id,last_synced,report_slot,run_attempt)
VALUES ('org-152', '00000000-0000-4000-8000-000000000152', ?, 'cov', 'lcov', 10, 7, 70, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, now64(3), 'go-test-1', 1)`
	legacyIDsOldSuiteRead = `SELECT run_id, suite_id, total_count, toString(report_slot), run_attempt FROM test_suite_results FINAL
WHERE org_id = 'org-152' ORDER BY run_id, suite_id`
)

var legacyIDsColumns = map[string]string{"test_suite_results": "legacy_suite_ids", "coverage_snapshots": "legacy_snapshot_ids"}

// legacyIDsChain splits the live chain at 152: the files before it, and 152.
func legacyIDsChain(t *testing.T) (chmigrate.Baseline, []chmigrate.ChainFile, []chmigrate.ChainFile, chmigrate.ChainFile) {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for index, file := range chain {
		if file.Version == testopsLegacyIDsVersion {
			return baseline, chain[:index+1], chain[:index], file
		}
	}
	t.Fatalf("the live chain does not hold %s", testopsLegacyIDsVersion)
	return baseline, nil, nil, chmigrate.ChainFile{}
}

func legacyIDsExec(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// legacyIDsColumnState gives table.name -> "type default_kind default_expression" of the new columns that exist.
func legacyIDsColumnState(ctx context.Context, t *testing.T, conn driver.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT table, name, type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table IN ('test_suite_results', 'coverage_snapshots') AND name IN ('legacy_suite_ids', 'legacy_snapshot_ids')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var table, name, columnType, kind, expression string
		if err := rows.Scan(&table, &name, &columnType, &kind, &expression); err != nil {
			t.Fatal(err)
		}
		out[table+"."+name] = columnType + " " + kind + " " + expression
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type legacyIDsStorage struct {
	Parts        []string
	Mutations    uint64
	StoredColumn uint64
}

func readLegacyIDsStorage(ctx context.Context, t *testing.T, conn driver.Conn) legacyIDsStorage {
	t.Helper()
	var storage legacyIDsStorage
	if err := conn.QueryRow(ctx, `SELECT arraySort(groupArray(concat(table, '/', name))) FROM system.parts
WHERE database = currentDatabase() AND table IN ('test_suite_results', 'coverage_snapshots') AND active`).Scan(&storage.Parts); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_suite_results', 'coverage_snapshots')`).Scan(&storage.Mutations); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('test_suite_results', 'coverage_snapshots') AND active
  AND column IN ('legacy_suite_ids', 'legacy_snapshot_ids')`).Scan(&storage.StoredColumn); err != nil {
		t.Fatal(err)
	}
	return storage
}

type legacyIDsSuite struct {
	RunID, SuiteID, Slot string
	Total, Attempt       uint32
}

func readLegacyIDsSuites(ctx context.Context, t *testing.T, conn driver.Conn) []legacyIDsSuite {
	t.Helper()
	rows, err := conn.Query(ctx, legacyIDsOldSuiteRead)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []legacyIDsSuite
	for rows.Next() {
		var row legacyIDsSuite
		if err := rows.Scan(&row.RunID, &row.SuiteID, &row.Total, &row.Slot, &row.Attempt); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// legacyIDsEmptyRows counts the rows of both tables whose new column reads [].
func legacyIDsEmptyRows(ctx context.Context, t *testing.T, conn driver.Conn) (empty, total uint64) {
	t.Helper()
	for table, column := range legacyIDsColumns {
		var e, n uint64
		if err := conn.QueryRow(ctx, "SELECT countIf(empty("+column+")), count() FROM "+table+" WHERE org_id = 'org-152'").Scan(&e, &n); err != nil {
			t.Fatal(err)
		}
		empty += e
		total += n
	}
	return empty, total
}

// INVARIANT (CHAOS-13925): 152 adds legacy_suite_ids and legacy_snapshot_ids as metadata only. The rows stored before it keep
// their parts (no mutation, no part holds a new column) and read []; the 151 build's INSERT and SELECT texts work unchanged after
// it; the migration runs again with no change.
func TestMigration152AddsTheColumnsWithoutRewritingAPartAndKeepsTheOldStatements(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, chain, before, migration := legacyIDsChain(t)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatalf("the chain before 152: %v", err)
	}
	if columns := legacyIDsColumnState(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("before 152 the tables already hold %v", columns)
	}
	for table := range legacyIDsColumns {
		legacyIDsExec(ctx, t, conn, "SYSTEM STOP MERGES "+table)
	}
	legacyIDsExec(ctx, t, conn, legacyIDsOldSuiteInsert, "43600000001")
	legacyIDsExec(ctx, t, conn, legacyIDsOldCoverageInsert, "43600000001")
	stored := readLegacyIDsStorage(ctx, t, conn)
	suitesBefore := readLegacyIDsSuites(ctx, t, conn)
	if len(stored.Parts) < 2 || stored.Mutations != 0 || stored.StoredColumn != 0 || len(suitesBefore) != 1 {
		t.Fatalf("before 152: %+v suites %v", stored, suitesBefore)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain[:len(before)+1])
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != testopsLegacyIDsVersion {
		t.Fatalf("the upgrade = %+v, %v; want %s applied", result, err, testopsLegacyIDsVersion)
	}
	want := map[string]string{
		"test_suite_results.legacy_suite_ids":    "Array(String) DEFAULT []",
		"coverage_snapshots.legacy_snapshot_ids": "Array(String) DEFAULT []",
	}
	if got := legacyIDsColumnState(ctx, t, conn); !reflect.DeepEqual(got, want) {
		t.Fatalf("after 152 the columns are %v, want %v", got, want)
	}
	if after := readLegacyIDsStorage(ctx, t, conn); !reflect.DeepEqual(after, stored) {
		t.Fatalf("152 changed the stored data: before %+v, after %+v", stored, after)
	}
	if empty, total := legacyIDsEmptyRows(ctx, t, conn); empty != 2 || total != 2 {
		t.Fatalf("after 152 %d of %d stored rows read [], want 2 of 2", empty, total)
	}
	if got := readLegacyIDsSuites(ctx, t, conn); !reflect.DeepEqual(got, suitesBefore) {
		t.Fatalf("the older read after 152 = %v, want %v", got, suitesBefore)
	}
	// The older writer's column lists after 152: the new columns take their default.
	legacyIDsExec(ctx, t, conn, legacyIDsOldSuiteInsert, "43600000002")
	legacyIDsExec(ctx, t, conn, legacyIDsOldCoverageInsert, "43600000002")
	if empty, total := legacyIDsEmptyRows(ctx, t, conn); empty != 4 || total != 4 {
		t.Fatalf("after an older insert %d of %d rows read [], want 4 of 4", empty, total)
	}

	again := readLegacyIDsStorage(ctx, t, conn)
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		legacyIDsExec(ctx, t, conn, statement)
	}
	if after := readLegacyIDsStorage(ctx, t, conn); !reflect.DeepEqual(after, again) {
		t.Fatalf("a second run of 152 changed the stored data: %+v, want %+v", after, again)
	}
}

// The hand-only rollback, rollback/152_testops_report_legacy_ids.down.sql, rehearsed up -> down -> up on this build: down drops
// the two columns and the version row and keeps every row; the 151 build's statements work on the rolled-back schema; the next
// upgrade applies 152 again.
func TestRollback152UpDownUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, chain, _, _ := legacyIDsChain(t)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	legacyIDsExec(ctx, t, conn, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, report_slot, run_attempt, legacy_suite_ids, last_synced)
VALUES ('org-152', '00000000-0000-4000-8000-000000000152', '43600000003', 's2', 'pkg', 3, 'go-test-1', 2, ['l1', 'l2'], now64(3))`)
	legacyIDsExec(ctx, t, conn, legacyIDsOldSuiteInsert, "43600000004")
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", testopsLegacyIDsVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatalf("at the head schema_migrations holds no %s", testopsLegacyIDsVersion)
	}

	body, err := os.ReadFile("rollback/152_testops_report_legacy_ids.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		legacyIDsExec(ctx, t, conn, statement)
	}
	if columns := legacyIDsColumnState(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("after the rollback the tables still hold %v", columns)
	}
	if versionRows() != 0 {
		t.Fatalf("after the rollback schema_migrations still holds %s", testopsLegacyIDsVersion)
	}
	if got := readLegacyIDsSuites(ctx, t, conn); len(got) != 2 {
		t.Fatalf("after the rollback the older read returns %v, want the 2 suite rows", got)
	}
	legacyIDsExec(ctx, t, conn, legacyIDsOldSuiteInsert, "43600000005")
	legacyIDsExec(ctx, t, conn, legacyIDsOldCoverageInsert, "43600000005")

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != testopsLegacyIDsVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want %s applied", result, err, testopsLegacyIDsVersion)
	}
	if columns := legacyIDsColumnState(ctx, t, conn); len(columns) != 2 {
		t.Fatalf("after the upgrade again the columns are %v, want 2", columns)
	}
}
