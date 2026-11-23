//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes the text of the prod read FORM-14531-3.sql byte for byte (the sha256 below is the
// sha256 of the .sql file) on the head schema with a synthetic fixture, and checks each WANT and that the cost gate fires.
const (
	formExecFcc14531SHA256 = "b8ccf15a7149a6ccfff756ab14ffb9c45978905da3c392f4e6901a9c1265353e"
	formExecFcc14531SQL    = `-- FORM-14531-3 (lane-14531, 2026-11-23, ops base 92df42e9f210). ClickHouse prod, SELECT only, CHAOS-14531 SIZE.
-- No SET statement, no SETTINGS clause. Org prefix c6a38355 only (startsWith), system.parts = the named exception.
-- Each statement gates itself: a throwIf term in the WHERE of every table read stops the statement with
-- STOP-COST-GATE when that table holds more than 20000000 active rows or 4294967296 bytes (system.parts, all orgs).
-- Run each statement once, in order. A SQL error or STOP-COST-GATE = STOP and report.
-- Tables: work_items = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, work_item_id), read FINAL.
-- work_item_transitions read raw (exists and max tests only, a re-synced copy changes none of them).

-- G0 INFO size line per table. WANT 2 rows (work_item_transitions, work_items), gate = 0 on both.
SELECT table, count() AS parts, sum(rows) AS rows_all, sum(bytes_on_disk) AS bytes_all,
       throwIf(sum(rows) > 20000000 OR sum(bytes_on_disk) > 4294967296, 'STOP-COST-GATE') AS gate
FROM system.parts
WHERE database = currentDatabase() AND table IN ('work_items', 'work_item_transitions') AND active
GROUP BY table ORDER BY table;

-- F0 ORG-1. WANT orgs = 1.
SELECT uniqExact(org_id) AS orgs
FROM work_items
WHERE startsWith(org_id, 'c6a38355')
  AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0;

-- F1 stored rows per provider (FINAL = one row per (org_id, repo_id, work_item_id)).
SELECT provider,
       count()                                                                        AS rows_all,
       countIf(status = 'done')                                                       AS done_all,
       countIf(status = 'done' AND completed_at IS NULL)                              AS done_null,
       countIf(status = 'done' AND completed_at IS NULL AND closed_at IS NULL)        AS done_null_no_close,
       countIf(status = 'done' AND completed_at IS NULL AND startsWith(project_id, 'ghprojv2:')) AS done_null_board,
       countIf(status = 'done' AND completed_at IS NULL
               AND repo_id = toUUID('00000000-0000-0000-0000-000000000000'))         AS done_null_no_repo,
       countIf(status = 'done' AND completed_at IS NULL AND started_at IS NOT NULL)   AS done_null_started,
       countIf(status = 'canceled' AND completed_at IS NULL AND closed_at IS NULL)    AS canceled_no_end,
       minIf(created_at, status = 'done' AND completed_at IS NULL)                    AS done_null_first_created,
       maxIf(created_at, status = 'done' AND completed_at IS NULL)                    AS done_null_last_created
FROM work_items FINAL
WHERE startsWith(org_id, 'c6a38355')
  AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0
GROUP BY provider ORDER BY provider;

-- F2 items whose newest copy (max last_synced) is done with no completed_at. copies = stored copies of the item.
SELECT provider,
       count()                                   AS items,
       countIf(cur_done_null = 1)                AS items_current_done_null,
       countIf(cur_done_null = 1 AND copies > 1) AS current_done_null_multi_copy
FROM (
    SELECT provider, work_item_id, count() AS copies,
           argMax(toUInt8(status = 'done' AND completed_at IS NULL), last_synced) AS cur_done_null
    FROM work_items FINAL
    WHERE startsWith(org_id, 'c6a38355')
      AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0
    GROUP BY provider, work_item_id
)
GROUP BY provider ORDER BY provider;

-- F3 transitions of the items with a done copy that has no completed_at (options B/C feasibility).
SELECT w.provider AS provider,
       count()                          AS items,
       countIf(t.done_events > 0)       AS with_done_transition,
       countIf(t.last_to = 'done')      AS last_transition_done,
       countIf(t.transitions = 0)       AS no_transition
FROM (
    SELECT provider, work_item_id
    FROM work_items FINAL
    WHERE startsWith(org_id, 'c6a38355') AND status = 'done' AND completed_at IS NULL
      AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0
    GROUP BY provider, work_item_id
) AS w
LEFT JOIN (
    SELECT work_item_id, count() AS transitions, countIf(to_status = 'done') AS done_events,
           argMax(to_status, occurred_at) AS last_to
    FROM work_item_transitions
    WHERE startsWith(org_id, 'c6a38355')
      AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_transitions')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_transitions')) > 4294967296, 'STOP-COST-GATE') = 0
    GROUP BY work_item_id
) AS t ON w.work_item_id = t.work_item_id
GROUP BY w.provider ORDER BY provider;

-- F4 done and canceled rows per provider and source kind. ghprojv2 = project_id starts 'ghprojv2:' (GitHub board copy),
-- no_repo = other row with the zero repo_id (Linear, Jira, a GitHub board item with no repository), repo = the rest.
SELECT provider,
       multiIf(startsWith(project_id, 'ghprojv2:'), 'ghprojv2',
               repo_id = toUUID('00000000-0000-0000-0000-000000000000'), 'no_repo', 'repo') AS source_kind,
       countIf(status = 'done')                                  AS done_all,
       countIf(status = 'done' AND completed_at IS NULL)         AS done_null,
       countIf(status = 'canceled')                              AS canceled_all,
       countIf(status = 'canceled' AND completed_at IS NULL)     AS canceled_null,
       if(done_all = 0, 0, round(done_null / done_all, 4))       AS done_null_share
FROM work_items FINAL
WHERE startsWith(org_id, 'c6a38355')
  AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0
GROUP BY provider, source_kind ORDER BY provider, source_kind;

-- F5 later non-done state of each done copy with no completed_at, per provider and source kind of that copy.
SELECT d.provider AS provider, d.source_kind AS source_kind,
       count()                                                 AS done_null_copies,
       countIf(c.copies > 1)                                   AS with_other_copy,
       countIf(c.not_done_copies > 0)                          AS with_other_copy_not_done,
       countIf(t.done_events > 0)                              AS with_done_transition,
       countIf(t.done_events > 0 AND t.last_not_done_at > t.last_done_at) AS not_done_after_last_done,
       countIf(t.transitions = 0)                              AS no_transition
FROM (
    SELECT provider, work_item_id,
           multiIf(startsWith(project_id, 'ghprojv2:'), 'ghprojv2',
                   repo_id = toUUID('00000000-0000-0000-0000-000000000000'), 'no_repo', 'repo') AS source_kind
    FROM work_items FINAL
    WHERE startsWith(org_id, 'c6a38355') AND status = 'done' AND completed_at IS NULL
      AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0
) AS d
LEFT JOIN (
    SELECT provider, work_item_id, count() AS copies, countIf(status != 'done') AS not_done_copies
    FROM work_items FINAL
    WHERE startsWith(org_id, 'c6a38355')
      AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296, 'STOP-COST-GATE') = 0
    GROUP BY provider, work_item_id
) AS c ON c.provider = d.provider AND c.work_item_id = d.work_item_id
LEFT JOIN (
    SELECT work_item_id, count() AS transitions, countIf(to_status = 'done') AS done_events,
           maxIf(occurred_at, to_status = 'done') AS last_done_at,
           maxIf(occurred_at, to_status != 'done') AS last_not_done_at
    FROM work_item_transitions
    WHERE startsWith(org_id, 'c6a38355')
      AND throwIf((SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_transitions')) > 20000000 OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_transitions')) > 4294967296, 'STOP-COST-GATE') = 0
    GROUP BY work_item_id
) AS t ON t.work_item_id = d.work_item_id
GROUP BY d.provider, d.source_kind ORDER BY provider, source_kind;
`
)

type formExecFcc14531Result struct {
	columns []string
	rows    [][]string
}

func formExecFcc14531Open(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

func formExecFcc14531Exec(ctx context.Context, t *testing.T, conn driver.Conn, statement string) {
	t.Helper()
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// formExecFcc14531Statements splits the FORM text as pm runs it: the pieces between ";\n", each named by its first
// line "-- G<n>" or "-- F<n>". A piece with no SQL line (only comments) is not a statement.
func formExecFcc14531Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	var ids []string
	statements := map[string]string{}
	for _, piece := range strings.Split(text, ";\n") {
		statement := strings.TrimSpace(piece)
		id, hasSQL := "", false
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if id == "" && len(line) > 4 && (strings.HasPrefix(line, "-- G") || strings.HasPrefix(line, "-- F")) &&
					line[4] >= '0' && line[4] <= '9' {
					id = strings.Fields(line)[1]
				}
				continue
			}
			if strings.TrimSpace(line) != "" {
				hasSQL = true
			}
		}
		if !hasSQL {
			continue
		}
		if id == "" {
			t.Fatalf("a statement has no \"-- G<n>\" or \"-- F<n>\" line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	return ids, statements
}

func formExecFcc14531Query(ctx context.Context, conn driver.Conn, statement string) (formExecFcc14531Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecFcc14531Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecFcc14531Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecFcc14531Result{}, fmt.Errorf("scan: %w", err)
		}
		row := make([]string, len(dest))
		for index := range dest {
			value := reflect.ValueOf(dest[index]).Elem()
			for value.Kind() == reflect.Pointer && !value.IsNil() {
				value = value.Elem()
			}
			if value.Kind() == reflect.Pointer {
				row[index] = "NULL"
				continue
			}
			row[index] = fmt.Sprint(value.Interface())
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

func formExecFcc14531Cell(t *testing.T, results map[string]formExecFcc14531Result, id string, row int, column string) string {
	t.Helper()
	result, ok := results[id]
	if !ok {
		t.Fatalf("%s: not run", id)
	}
	if row >= len(result.rows) {
		t.Fatalf("%s: %d row(s), want a row %d", id, len(result.rows), row)
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[row][index]
		}
	}
	t.Fatalf("%s: no column %s in %v", id, column, result.columns)
	return ""
}

// formExecFcc14531Row checks the named columns of one row; a value ending in "*" is a prefix.
func formExecFcc14531Row(t *testing.T, results map[string]formExecFcc14531Result, id string, row int, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got := formExecFcc14531Cell(t, results, id, row, column)
		if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
			if !strings.HasPrefix(got, prefix) {
				t.Errorf("%s row %d %s = %s, want prefix %s", id, row, column, got, prefix)
			}
			continue
		}
		if got != value {
			t.Errorf("%s row %d %s = %s, want %s", id, row, column, got, value)
		}
	}
}

func formExecFcc14531Rows(t *testing.T, results map[string]formExecFcc14531Result, id string, want int) {
	t.Helper()
	if got := len(results[id].rows); got != want {
		t.Fatalf("%s: %d row(s), want %d", id, got, want)
	}
}

// CHAOS-14531 fixture. Org c6a38355-... (the org the FORM reads), all items created in 2026-09:
//
//	github gh:fx/repo#1: a board copy (zero repo_id, project ghprojv2:PVT_FX), done, no completed_at, started, last
//	    synced 11-20, with an older repo copy that is open (todo, 11-19). Transitions: todo -> in_progress -> done.
//	github gh:fx/repo#2: a repo copy, label done, no completed_at, no transition. An older version of the same key
//	    (in_progress, its own part) that FINAL folds away.
//	github gh:fx/repo#3: done with completed_at. #4: canceled, no completed_at. #5: canceled with completed_at.
//	linear lin:FX-1: done, no completed_at, zero repo_id. Transitions: -> done, then done -> in_progress (the row
//	    disagrees with its history: the control of F5). lin:FX-2: done with completed_at.
//
// Org d6a38355-... holds a done board copy with no completed_at, a canceled item and a later todo transition of
// gh:fx/repo#1, and must not count.
func TestFormExecFcc14531Size(t *testing.T) {
	ctx, conn := formExecFcc14531Open(t)
	const (
		org, other = "c6a38355-1453-4000-8000-000000000001", "d6a38355-1453-4000-8000-000000000002"
		repo, zero = "00000000-0000-4000-8000-00000000b001", "00000000-0000-0000-0000-000000000000"
	)
	item := func(orgID, repoID, id, provider, project, status, created, started, completed, closed, lastSynced string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', 'fixture', 'issue', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, '%s')",
			orgID, repoID, id, provider, status, status, project, created, created, started, completed, closed, lastSynced)
	}
	items := `INSERT INTO work_items (org_id, repo_id, work_item_id, provider, title, type, status, status_raw, project_id, created_at, updated_at, started_at, completed_at, closed_at, last_synced) VALUES `
	formExecFcc14531Exec(ctx, t, conn, items+item(org, repo, "gh:fx/repo#2", "github", "fx/repo", "in_progress",
		"2026-09-03 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-01 00:00:00.000"))
	formExecFcc14531Exec(ctx, t, conn, items+strings.Join([]string{
		item(org, zero, "gh:fx/repo#1", "github", "ghprojv2:PVT_FX", "done", "2026-09-01 00:00:00.000", "'2026-09-02 00:00:00.000'", "NULL", "NULL", "2026-11-20 00:00:00.000"),
		item(org, repo, "gh:fx/repo#1", "github", "fx/repo", "todo", "2026-09-01 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-19 00:00:00.000"),
		item(org, repo, "gh:fx/repo#2", "github", "fx/repo", "done", "2026-09-03 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-20 00:00:00.000"),
		item(org, repo, "gh:fx/repo#3", "github", "fx/repo", "done", "2026-09-04 00:00:00.000", "NULL", "'2026-10-01 00:00:00.000'", "'2026-10-01 00:00:00.000'", "2026-11-20 00:00:00.000"),
		item(org, repo, "gh:fx/repo#4", "github", "fx/repo", "canceled", "2026-09-06 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-20 00:00:00.000"),
		item(org, repo, "gh:fx/repo#5", "github", "fx/repo", "canceled", "2026-09-07 00:00:00.000", "NULL", "'2026-10-02 00:00:00.000'", "'2026-10-02 00:00:00.000'", "2026-11-20 00:00:00.000"),
		item(org, zero, "lin:FX-1", "linear", "lin-project-1", "done", "2026-09-08 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-20 00:00:00.000"),
		item(org, zero, "lin:FX-2", "linear", "lin-project-1", "done", "2026-09-09 00:00:00.000", "NULL", "'2026-10-03 00:00:00.000'", "'2026-10-03 00:00:00.000'", "2026-11-20 00:00:00.000"),
		item(other, zero, "gh:fx/repo#1", "github", "ghprojv2:PVT_OT", "done", "2026-08-01 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-20 00:00:00.000"),
		item(other, repo, "gh:fx/repo#9", "github", "fx/repo", "canceled", "2026-08-02 00:00:00.000", "NULL", "NULL", "NULL", "2026-11-20 00:00:00.000"),
	}, ", "))
	transition := func(orgID, id, provider, from, to, occurred string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '2026-11-20 00:00:00.000', 0)", orgID, zero, id, provider, from, to, occurred)
	}
	formExecFcc14531Exec(ctx, t, conn, `INSERT INTO work_item_transitions (org_id, repo_id, work_item_id, provider, from_status, to_status, occurred_at, last_synced, tie_ordinal) VALUES `+strings.Join([]string{
		transition(org, "gh:fx/repo#1", "github", "todo", "in_progress", "2026-09-02 00:00:00.000"),
		transition(org, "gh:fx/repo#1", "github", "in_progress", "done", "2026-09-05 00:00:00.000"),
		transition(org, "gh:fx/repo#3", "github", "in_progress", "done", "2026-10-01 00:00:00.000"),
		transition(org, "lin:FX-1", "linear", "in_progress", "done", "2026-09-10 00:00:00.000"),
		transition(org, "lin:FX-1", "linear", "done", "in_progress", "2026-09-12 00:00:00.000"),
		transition(other, "gh:fx/repo#1", "github", "done", "todo", "2026-09-20 00:00:00.000"),
	}, ", "))

	sum := sha256.Sum256([]byte(formExecFcc14531SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecFcc14531SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14531-3.sql)", got, formExecFcc14531SHA256)
	}
	ids, statements := formExecFcc14531Statements(t, formExecFcc14531SQL)
	if got := strings.Join(ids, ","); got != "G0,F0,F1,F2,F3,F4,F5" {
		t.Fatalf("statements = %s, want G0,F0,F1,F2,F3,F4,F5", got)
	}
	gates := map[string]int{"F0": 1, "F1": 1, "F2": 1, "F3": 2, "F4": 1, "F5": 3}
	for id, want := range gates {
		if got := strings.Count(statements[id], "'STOP-COST-GATE') = 0"); got != want {
			t.Errorf("%s holds %d cost gate term(s), want %d (one per table read)", id, got, want)
		}
	}
	results := map[string]formExecFcc14531Result{}
	for _, id := range ids {
		result, err := formExecFcc14531Query(ctx, conn, statements[id])
		if err != nil {
			t.Fatalf("FORM-14531-3 %s: %v", id, err)
		}
		t.Logf("FORM-14531-3 %s columns %v", id, result.columns)
		for index, row := range result.rows {
			t.Logf("FORM-14531-3 %s row %d %v", id, index, row)
		}
		results[id] = result
	}

	formExecFcc14531Rows(t, results, "G0", 2)
	formExecFcc14531Row(t, results, "G0", 0, map[string]string{"table": "work_item_transitions", "gate": "0"})
	formExecFcc14531Row(t, results, "G0", 1, map[string]string{"table": "work_items", "gate": "0"})

	formExecFcc14531Rows(t, results, "F0", 1)
	formExecFcc14531Row(t, results, "F0", 0, map[string]string{"orgs": "1"})

	formExecFcc14531Rows(t, results, "F1", 2)
	formExecFcc14531Row(t, results, "F1", 0, map[string]string{
		"provider": "github", "rows_all": "6", "done_all": "3", "done_null": "2", "done_null_no_close": "2",
		"done_null_board": "1", "done_null_no_repo": "1", "done_null_started": "1", "canceled_no_end": "1",
		"done_null_first_created": "2026-09-01 00:00:00*", "done_null_last_created": "2026-09-03 00:00:00*",
	})
	formExecFcc14531Row(t, results, "F1", 1, map[string]string{
		"provider": "linear", "rows_all": "2", "done_all": "2", "done_null": "1", "done_null_no_close": "1",
		"done_null_board": "0", "done_null_no_repo": "1", "done_null_started": "0", "canceled_no_end": "0",
		"done_null_first_created": "2026-09-08 00:00:00*", "done_null_last_created": "2026-09-08 00:00:00*",
	})

	formExecFcc14531Rows(t, results, "F2", 2)
	formExecFcc14531Row(t, results, "F2", 0, map[string]string{
		"provider": "github", "items": "5", "items_current_done_null": "2", "current_done_null_multi_copy": "1",
	})
	formExecFcc14531Row(t, results, "F2", 1, map[string]string{
		"provider": "linear", "items": "2", "items_current_done_null": "1", "current_done_null_multi_copy": "0",
	})

	formExecFcc14531Rows(t, results, "F3", 2)
	formExecFcc14531Row(t, results, "F3", 0, map[string]string{
		"provider": "github", "items": "2", "with_done_transition": "1", "last_transition_done": "1", "no_transition": "1",
	})
	formExecFcc14531Row(t, results, "F3", 1, map[string]string{
		"provider": "linear", "items": "1", "with_done_transition": "1", "last_transition_done": "0", "no_transition": "0",
	})

	formExecFcc14531Rows(t, results, "F4", 3)
	formExecFcc14531Row(t, results, "F4", 0, map[string]string{
		"provider": "github", "source_kind": "ghprojv2", "done_all": "1", "done_null": "1",
		"canceled_all": "0", "canceled_null": "0", "done_null_share": "1",
	})
	formExecFcc14531Row(t, results, "F4", 1, map[string]string{
		"provider": "github", "source_kind": "repo", "done_all": "2", "done_null": "1",
		"canceled_all": "2", "canceled_null": "1", "done_null_share": "0.5",
	})
	formExecFcc14531Row(t, results, "F4", 2, map[string]string{
		"provider": "linear", "source_kind": "no_repo", "done_all": "2", "done_null": "1",
		"canceled_all": "0", "canceled_null": "0", "done_null_share": "0.5",
	})

	formExecFcc14531Rows(t, results, "F5", 3)
	formExecFcc14531Row(t, results, "F5", 0, map[string]string{
		"provider": "github", "source_kind": "ghprojv2", "done_null_copies": "1", "with_other_copy": "1",
		"with_other_copy_not_done": "1", "with_done_transition": "1", "not_done_after_last_done": "0", "no_transition": "0",
	})
	formExecFcc14531Row(t, results, "F5", 1, map[string]string{
		"provider": "github", "source_kind": "repo", "done_null_copies": "1", "with_other_copy": "0",
		"with_other_copy_not_done": "0", "with_done_transition": "0", "not_done_after_last_done": "0", "no_transition": "1",
	})
	formExecFcc14531Row(t, results, "F5", 2, map[string]string{
		"provider": "linear", "source_kind": "no_repo", "done_null_copies": "1", "with_other_copy": "0",
		"with_other_copy_not_done": "0", "with_done_transition": "1", "not_done_after_last_done": "1", "no_transition": "0",
	})

	// The gate fires: F1 with its row cap lowered to 0 in a test copy only (the embedded text and its sha256 stay as
	// checked above). The fixture's work_items parts hold more than 0 rows, so the statement must fail with the gate.
	lowered := strings.ReplaceAll(statements["F1"], "> 20000000", "> 0")
	if strings.Count(statements["F1"], "> 20000000") != 1 || lowered == statements["F1"] {
		t.Fatalf("F1 holds %d row cap(s), want 1", strings.Count(statements["F1"], "> 20000000"))
	}
	if _, err := formExecFcc14531Query(ctx, conn, lowered); err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") {
		t.Fatalf("F1 with the row cap 0: err = %v, want STOP-COST-GATE", err)
	}
}
