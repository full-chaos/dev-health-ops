//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// The column list of the build before 155 (daily testops_native_clickhouse.go writeTestopsPipelineMetrics): an older
// worker inserts with it.
const pipelineRunStatusCountsOldInsert = `INSERT INTO testops_pipeline_metrics_daily (
repo_id, day, pipelines_count, success_count, failure_count, cancelled_count,
success_rate, failure_rate, cancel_rate, rerun_rate,
median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds,
team_id, service_id, org_id, computed_at)
VALUES ('00000000-0000-4000-8000-000000000155', ?, 9, 4, 2, 1, 0.66, 0.33, 0.11, 0, NULL, NULL, NULL, NULL, NULL, NULL, 'org-155', now())`

const pipelineRunStatusCountsColumnList = "('skipped_count', 'unfinished_count', 'other_count', 'unfinished_stale_count')"

const pipelineRunStatusCountsOldRead = `SELECT toString(day), pipelines_count, success_count, failure_count, cancelled_count
FROM testops_pipeline_metrics_daily WHERE org_id = 'org-155' ORDER BY day`

// pipelineRunStatusCountsChain splits the live chain at 155: the files before it, and 155.
func pipelineRunStatusCountsChain(t *testing.T) (chmigrate.Baseline, []chmigrate.ChainFile, []chmigrate.ChainFile, chmigrate.ChainFile) {
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
		if file.Version == pipelineRunStatusCountsVersion {
			return baseline, chain, chain[:index], file
		}
	}
	t.Fatalf("the live chain does not hold %s", pipelineRunStatusCountsVersion)
	return baseline, nil, nil, chmigrate.ChainFile{}
}

func pipelineRunStatusCountsExec(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// pipelineRunStatusCountsColumnState gives name -> "type default_kind default_expression" of the new columns that exist.
func pipelineRunStatusCountsColumnState(ctx context.Context, t *testing.T, conn driver.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT name, type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'testops_pipeline_metrics_daily' AND name IN `+pipelineRunStatusCountsColumnList)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, columnType, kind, expression string
		if err := rows.Scan(&name, &columnType, &kind, &expression); err != nil {
			t.Fatal(err)
		}
		out[name] = strings.TrimSpace(columnType + " " + kind + " " + expression)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type pipelineRunStatusCountsStorage struct {
	Parts        []string
	Mutations    uint64
	StoredColumn uint64
}

func readPipelineRunStatusCountsStorage(ctx context.Context, t *testing.T, conn driver.Conn) pipelineRunStatusCountsStorage {
	t.Helper()
	var storage pipelineRunStatusCountsStorage
	if err := conn.QueryRow(ctx, `SELECT arraySort(groupArray(name)) FROM system.parts
WHERE database = currentDatabase() AND table = 'testops_pipeline_metrics_daily' AND active`).Scan(&storage.Parts); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 'testops_pipeline_metrics_daily'`).Scan(&storage.Mutations); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'testops_pipeline_metrics_daily' AND active
  AND column IN `+pipelineRunStatusCountsColumnList).Scan(&storage.StoredColumn); err != nil {
		t.Fatal(err)
	}
	return storage
}

type pipelineRunStatusCountsOldRow struct {
	Day                                  string
	Pipelines, Success, Failure, Cancels uint32
}

func readPipelineRunStatusCountsOldRows(ctx context.Context, t *testing.T, conn driver.Conn) []pipelineRunStatusCountsOldRow {
	t.Helper()
	rows, err := conn.Query(ctx, pipelineRunStatusCountsOldRead)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []pipelineRunStatusCountsOldRow
	for rows.Next() {
		var row pipelineRunStatusCountsOldRow
		if err := rows.Scan(&row.Day, &row.Pipelines, &row.Success, &row.Failure, &row.Cancels); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// pipelineRunStatusCountsNullRows counts the rows whose four new columns all read NULL.
func pipelineRunStatusCountsNullRows(ctx context.Context, t *testing.T, conn driver.Conn) (allNull, total uint64) {
	t.Helper()
	if err := conn.QueryRow(ctx, `SELECT countIf(skipped_count IS NULL AND unfinished_count IS NULL AND other_count IS NULL
  AND unfinished_stale_count IS NULL), count() FROM testops_pipeline_metrics_daily WHERE org_id = 'org-155'`).Scan(&allNull, &total); err != nil {
		t.Fatal(err)
	}
	return allNull, total
}

// INVARIANT: 155 adds the four bucket columns as metadata only. The rows stored before it keep their parts (no mutation,
// no part holds a new column) and read NULL, never 0; the 154 build's INSERT and SELECT texts work unchanged after it and
// its rows read NULL too; the migration runs again with no change.
func TestMigration155AddsNullableCountsWithoutRewritingAPartAndOldRowsReadNull(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, chain, before, migration := pipelineRunStatusCountsChain(t)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatalf("the chain before 155: %v", err)
	}
	if columns := pipelineRunStatusCountsColumnState(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("before 155 the table already holds %v", columns)
	}
	pipelineRunStatusCountsExec(ctx, t, conn, "SYSTEM STOP MERGES testops_pipeline_metrics_daily")
	pipelineRunStatusCountsExec(ctx, t, conn, pipelineRunStatusCountsOldInsert, "2026-11-15")
	pipelineRunStatusCountsExec(ctx, t, conn, pipelineRunStatusCountsOldInsert, "2026-11-16")
	stored := readPipelineRunStatusCountsStorage(ctx, t, conn)
	rowsBefore := readPipelineRunStatusCountsOldRows(ctx, t, conn)
	if len(stored.Parts) < 2 || stored.Mutations != 0 || stored.StoredColumn != 0 || len(rowsBefore) != 2 {
		t.Fatalf("before 155: %+v rows %v", stored, rowsBefore)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != pipelineRunStatusCountsVersion {
		t.Fatalf("the upgrade = %+v, %v; want %s applied", result, err, pipelineRunStatusCountsVersion)
	}
	want := map[string]string{}
	for _, column := range pipelineRunStatusCountsColumns {
		want[column] = "Nullable(UInt32)"
	}
	if got := pipelineRunStatusCountsColumnState(ctx, t, conn); !reflect.DeepEqual(got, want) {
		t.Fatalf("after 155 the columns are %v, want %v (no default)", got, want)
	}
	if after := readPipelineRunStatusCountsStorage(ctx, t, conn); !reflect.DeepEqual(after, stored) {
		t.Fatalf("155 changed the stored data: before %+v, after %+v", stored, after)
	}
	if allNull, total := pipelineRunStatusCountsNullRows(ctx, t, conn); allNull != 2 || total != 2 {
		t.Fatalf("after 155 %d of %d stored rows read NULL in the four columns, want 2 of 2", allNull, total)
	}
	if got := readPipelineRunStatusCountsOldRows(ctx, t, conn); !reflect.DeepEqual(got, rowsBefore) {
		t.Fatalf("the older read after 155 = %v, want %v", got, rowsBefore)
	}
	pipelineRunStatusCountsExec(ctx, t, conn, pipelineRunStatusCountsOldInsert, "2026-11-17")
	if allNull, total := pipelineRunStatusCountsNullRows(ctx, t, conn); allNull != 3 || total != 3 {
		t.Fatalf("after an older insert %d of %d rows read NULL in the four columns, want 3 of 3", allNull, total)
	}

	again := readPipelineRunStatusCountsStorage(ctx, t, conn)
	columnsAgain := pipelineRunStatusCountsColumnState(ctx, t, conn)
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		pipelineRunStatusCountsExec(ctx, t, conn, statement)
	}
	if after := readPipelineRunStatusCountsStorage(ctx, t, conn); !reflect.DeepEqual(after, again) {
		t.Fatalf("a second run of 155 changed the stored data: %+v, want %+v", after, again)
	}
	if got := pipelineRunStatusCountsColumnState(ctx, t, conn); !reflect.DeepEqual(got, columnsAgain) {
		t.Fatalf("a second run of 155 changed the columns: %v, want %v", got, columnsAgain)
	}
}

// The hand-only rollback, rollback/155_testops_pipeline_run_status_counts.down.sql, rehearsed up -> down -> up on this
// build: down drops the four columns and the version row and keeps every row; the 154 build's statements work on the
// rolled-back schema; the next upgrade applies 155 again.
func TestRollback155UpDownUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, chain, _, _ := pipelineRunStatusCountsChain(t)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	pipelineRunStatusCountsExec(ctx, t, conn, `INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count,
  skipped_count, unfinished_count, other_count, unfinished_stale_count, org_id, computed_at)
VALUES ('00000000-0000-4000-8000-000000000155', '2026-11-14', 10, 2, 1, 4, 3, 1, 'org-155', now())`)
	pipelineRunStatusCountsExec(ctx, t, conn, pipelineRunStatusCountsOldInsert, "2026-11-15")
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", pipelineRunStatusCountsVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatalf("at the head schema_migrations holds no %s", pipelineRunStatusCountsVersion)
	}

	body, err := os.ReadFile("rollback/155_testops_pipeline_run_status_counts.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		pipelineRunStatusCountsExec(ctx, t, conn, statement)
	}
	if columns := pipelineRunStatusCountsColumnState(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("after the rollback the table still holds %v", columns)
	}
	if versionRows() != 0 {
		t.Fatalf("after the rollback schema_migrations still holds %s", pipelineRunStatusCountsVersion)
	}
	if got := readPipelineRunStatusCountsOldRows(ctx, t, conn); len(got) != 2 {
		t.Fatalf("after the rollback the older read returns %v, want the 2 rows", got)
	}
	pipelineRunStatusCountsExec(ctx, t, conn, pipelineRunStatusCountsOldInsert, "2026-11-16")

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != pipelineRunStatusCountsVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want %s applied", result, err, pipelineRunStatusCountsVersion)
	}
	if columns := pipelineRunStatusCountsColumnState(ctx, t, conn); len(columns) != 4 {
		t.Fatalf("after the upgrade again the columns are %v, want 4", columns)
	}
}
