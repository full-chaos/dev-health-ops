//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes the text of the prod read FORM-14190-GAP-1.sql byte for byte (the sha256 below is
// the sha256 of the .sql file) on the head schema with a synthetic fixture, checks each WANT, checks each data
// statement's pass-path read_rows against its work math, and fires each cost gate cap once (pm-guard FORM-TEMPLATE-3).
const (
	formExecFcc14190SHA256 = "b7d173093380e0f060aeaa786484fff1944034a650a7357ec911511666efdfd5"
	formExecFcc14190SQL    = `-- FORM-14190-GAP-1 (lane-14190g, 2026-11-23, ops base a4906f2cd6eb). ClickHouse prod, SELECT only, CHAOS-14190 gap size.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a). WANT text and work math: FORM-14190-GAP-1.md.
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix c6a38355 by startsWith on every FROM with org_id.
-- Each data statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- and system.one only), and every WHERE of a query level that reads a table, and the outer WHERE, tests the gate.
-- No data CTE: each table is named once per statement. Rows cap on the sum of the active rows of the statement's
-- tables: 5000000 (reopen events only) or 6000000 (reopen events + transitions). Bytes cap 4294967296 on the same sum.
-- Run each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP and report.
-- Gap definition. A reopen is lost when its work_item_reopen_events row has no work_item_transitions row of the same
-- org_id, work_item_id and occurred_at with the same to_status_raw. Both rows come from one provider event in one batch:
-- GitHub 'reopened' (reopen row and transition row), Linear the history entry's ToState.Name (both rows).
-- Both tables are read FINAL (ReplacingMergeTree(last_synced)): the stored state after merges.
-- lost_tie = the instant holds other transitions but not this one (the same-instant collapse signature).
-- lost_bare = the instant holds no transition of the item at all (another cause, not the collapse).

-- S0 INFO cost inputs, metadata only (system.one + system.parts). WANT 2 rows, table_rows > 0 in each (a wrong table
-- name gives a row with 0).
SELECT l.t AS table_name, p.table_rows AS table_rows, p.disk_bytes AS disk_bytes, p.active_parts AS active_parts
FROM (SELECT arrayJoin(['work_item_reopen_events', 'work_item_transitions']) AS t FROM system.one) AS l
LEFT JOIN (
    SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
    FROM system.parts
    WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions')
    GROUP BY table
) AS p ON p.table = l.t
ORDER BY table_name;

-- F0 ORG-1 and control. WANT orgs = 1, org_rows > 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events')) > 5000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM work_item_reopen_events
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0;

-- F1 lost reopens per provider (github = ids 'gh:' and 'ghpr:', linear = ids 'linear:'), today. One row per provider.
-- Control: items_kept > 0 in each row (reopens that DO have their transition). reopens_lost = reopens_lost_tie +
-- reopens_lost_bare. Days of the first and last lost reopen (1970-01-01 = no lost reopen).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT r.provider                                                AS provider,
       count()                                                   AS reopens_all,
       uniqExactIf(r.work_item_id, has(t.raws, r.to_raw))        AS items_kept,
       countIf(has(t.raws, r.to_raw))                            AS reopens_kept,
       uniqExactIf(r.work_item_id, NOT has(t.raws, r.to_raw))    AS items_lost,
       countIf(NOT has(t.raws, r.to_raw))                        AS reopens_lost,
       countIf(NOT has(t.raws, r.to_raw) AND notEmpty(t.raws))   AS reopens_lost_tie,
       countIf(empty(t.raws))                                    AS reopens_lost_bare,
       toDate(minIf(r.occurred_at, NOT has(t.raws, r.to_raw)))   AS lost_first_day,
       toDate(maxIf(r.occurred_at, NOT has(t.raws, r.to_raw)))   AS lost_last_day
FROM (
    SELECT org_id, work_item_id, occurred_at, ifNull(to_status_raw, '') AS to_raw,
           if(startsWith(work_item_id, 'linear:'), 'linear', 'github') AS provider
    FROM work_item_reopen_events FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
      AND (startsWith(work_item_id, 'gh:') OR startsWith(work_item_id, 'ghpr:') OR startsWith(work_item_id, 'linear:'))
) AS r
LEFT JOIN (
    SELECT org_id, work_item_id, occurred_at, groupUniqArray(to_status_raw) AS raws
    FROM work_item_transitions FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
      AND (startsWith(work_item_id, 'gh:') OR startsWith(work_item_id, 'ghpr:') OR startsWith(work_item_id, 'linear:'))
    GROUP BY org_id, work_item_id, occurred_at
) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id AND t.occurred_at = r.occurred_at
WHERE cost_gate = 0
GROUP BY r.provider ORDER BY provider;
`
)

// formExecFcc14190Gates are the cost gates of the data statements: the tables each reads (the gate's IN list), its cap
// literals, how many times it holds cost_gate = 0 (the outer WHERE plus each subquery that reads a table), and
// fixtureReadRows, the query_log read_rows of the statement on this fixture by the scan model of FORM-14190-GAP-1.md
// (merges stopped, one part per table: E 9 reopen event rows, T 11 transition rows, P 1 + 1). A gate evaluation reads
// 2 x P + 1, each further cost_gate reference 1 more row, a plain or FINAL read of a one-part table its rows once.
var formExecFcc14190Gates = []struct {
	id, tables, rowsCap, bytesCap string
	uses                          int
	fixtureReadRows               uint64
}{
	{"F0", "'work_item_reopen_events'", "> 5000000", "> 4294967296", 1, 12},
	{"F1", "'work_item_reopen_events', 'work_item_transitions'", "> 6000000", "> 4294967296", 3, 27},
}

type formExecFcc14190Result struct {
	columns []string
	rows    [][]string
}

func formExecFcc14190Open(t *testing.T) (context.Context, driver.Conn) {
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

func formExecFcc14190Exec(ctx context.Context, t *testing.T, conn driver.Conn, statement string) {
	t.Helper()
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// formExecFcc14190Statements splits the FORM text as pm runs it: the pieces between ";\n", each named by its first
// line "-- S<n>" or "-- F<n>". A piece with no SQL line (only comments) is not a statement.
var formExecFcc14190StatementID = regexp.MustCompile(`^[SF][0-9]+$`)

func formExecFcc14190Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	var ids []string
	statements := map[string]string{}
	for _, piece := range strings.Split(text, ";\n") {
		statement := strings.TrimSpace(piece)
		id, hasSQL := "", false
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if fields := strings.Fields(line); id == "" && len(fields) > 1 && formExecFcc14190StatementID.MatchString(fields[1]) {
					id = fields[1]
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
			t.Fatalf("a statement has no \"-- S<n>\" or \"-- F<n>\" line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	return ids, statements
}

func formExecFcc14190Query(ctx context.Context, conn driver.Conn, statement string) (formExecFcc14190Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecFcc14190Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecFcc14190Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecFcc14190Result{}, fmt.Errorf("scan: %w", err)
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

func formExecFcc14190Cell(t *testing.T, results map[string]formExecFcc14190Result, id string, row int, column string) string {
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

// formExecFcc14190QueryLog reads the finished query_log rows of one log_comment: their types and the read_rows of
// the last one. It logs read_rows and the ProfileEvents that explain it.
func formExecFcc14190QueryLog(ctx context.Context, t *testing.T, conn driver.Conn, label, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows, ProfileEvents['SelectedRows'], ProfileEvents['SelectedParts'],
		ProfileEvents['SelectedMarks'], ProfileEvents['SelectedRanges'], ProfileEvents['ScalarSubqueriesCacheMiss'],
		ProfileEvents['ScalarSubqueriesLocalCacheHit'], ProfileEvents['ScalarSubqueriesGlobalCacheHit'],
		arrayStringConcat(tables, ',')
		FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	defer func() { _ = rows.Close() }()
	var types []string
	var read uint64
	for rows.Next() {
		var typ, tables string
		var readRows, selectedRows, selectedParts, selectedMarks, selectedRanges, scalarMiss, scalarLocal, scalarGlobal uint64
		if err := rows.Scan(&typ, &readRows, &selectedRows, &selectedParts, &selectedMarks, &selectedRanges,
			&scalarMiss, &scalarLocal, &scalarGlobal, &tables); err != nil {
			t.Fatalf("%s: query log scan: %v", label, err)
		}
		types = append(types, typ)
		read = readRows
		t.Logf("%s type=%s read_rows=%d SelectedRows=%d SelectedParts=%d SelectedMarks=%d SelectedRanges=%d "+
			"ScalarSubqueriesCacheMiss=%d LocalCacheHit=%d GlobalCacheHit=%d tables=%s", label, typ, readRows, selectedRows,
			selectedParts, selectedMarks, selectedRanges, scalarMiss, scalarLocal, scalarGlobal, tables)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	return types, read
}

// formExecFcc14190Row checks the named columns of one row; a value ending in "*" is a prefix.
func formExecFcc14190Row(t *testing.T, results map[string]formExecFcc14190Result, id string, row int, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got := formExecFcc14190Cell(t, results, id, row, column)
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

func formExecFcc14190Rows(t *testing.T, results map[string]formExecFcc14190Result, id string, want int) {
	t.Helper()
	if got := len(results[id].rows); got != want {
		t.Fatalf("%s: %d row(s), want %d", id, got, want)
	}
}

// CHAOS-14190 fixture. Org c6a38355-... (the org the FORM reads), one INSERT per table (one part each):
//
//	gh:fx/repo#1: reopened 10-01 and 10-05; the transitions at each instant hold only another row (closed at 10-01,
//	    a status label at 10-05): two lost reopens of one item, both with the same-instant signature (lost_tie).
//	gh:fx/repo#2: reopened 10-02; the instant holds closed (tie_ordinal 0) and reopened (tie_ordinal 1): healed.
//	ghpr:fx/repo#3: reopened 10-03; its reopened transition is alone at the instant: kept.
//	gh:fx/repo#4: reopened 10-04; the item's only transition is at 10-03: lost with no transition at the instant
//	    (lost_bare).
//	gh:fx/repo#5: closed 10-08, never reopened: no reopen row, not counted.
//	linear:FX-1: reopened Done -> Todo 10-06; the instant holds only the Done transition: lost (lost_tie).
//	linear:FX-2: reopened Canceled -> Todo 10-07 with its Todo transition: kept.
//	jira:FX-9: a reopen with its transition: another provider, not counted.
//
// Org d6a38355-... holds a reopen of gh:fx/repo#4 at 10-04 and its reopened transition, and must not count.
func TestFormExecFcc14190Gap(t *testing.T) {
	ctx, conn := formExecFcc14190Open(t)
	const (
		org, other = "c6a38355-1419-4000-8000-000000000001", "d6a38355-1419-4000-8000-000000000002"
		zero       = "00000000-0000-0000-0000-000000000000"
		synced     = "2026-11-23 03:00:00.000"
	)
	// Merges stay off, so the part and row counts of the work math do not move while the test runs.
	formExecFcc14190Exec(ctx, t, conn, "SYSTEM STOP MERGES work_item_reopen_events")
	formExecFcc14190Exec(ctx, t, conn, "SYSTEM STOP MERGES work_item_transitions")
	reopen := func(orgID, id, from, to, fromRaw, toRaw, occurred string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', NULL, '%s')",
			orgID, id, occurred, from, to, fromRaw, toRaw, synced)
	}
	formExecFcc14190Exec(ctx, t, conn, `INSERT INTO work_item_reopen_events (org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced) VALUES `+strings.Join([]string{
		reopen(org, "gh:fx/repo#1", "done", "todo", "closed", "reopened", "2026-10-01 10:00:00.000"),
		reopen(org, "gh:fx/repo#2", "done", "todo", "closed", "reopened", "2026-10-02 10:00:00.000"),
		reopen(org, "ghpr:fx/repo#3", "done", "todo", "closed", "reopened", "2026-10-03 10:00:00.000"),
		reopen(org, "gh:fx/repo#4", "done", "todo", "closed", "reopened", "2026-10-04 10:00:00.000"),
		reopen(org, "gh:fx/repo#1", "done", "todo", "closed", "reopened", "2026-10-05 10:00:00.000"),
		reopen(org, "linear:FX-1", "done", "todo", "Done", "Todo", "2026-10-06 10:00:00.000"),
		reopen(org, "linear:FX-2", "canceled", "todo", "Canceled", "Todo", "2026-10-07 10:00:00.000"),
		reopen(org, "jira:FX-9", "done", "todo", "Done", "Open", "2026-10-08 10:00:00.000"),
		reopen(other, "gh:fx/repo#4", "done", "todo", "closed", "reopened", "2026-10-04 10:00:00.000"),
	}, ", "))
	transition := func(orgID, id, provider, from, to, toRaw, occurred string, ordinal int) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '', '%s', '', '%s', '%s', %d)",
			orgID, zero, id, provider, from, to, toRaw, occurred, synced, ordinal)
	}
	formExecFcc14190Exec(ctx, t, conn, `INSERT INTO work_item_transitions (org_id, repo_id, work_item_id, provider, from_status, to_status, from_status_raw, to_status_raw, actor, occurred_at, last_synced, tie_ordinal) VALUES `+strings.Join([]string{
		transition(org, "gh:fx/repo#1", "github", "todo", "done", "closed", "2026-10-01 10:00:00.000", 0),
		transition(org, "gh:fx/repo#1", "github", "todo", "blocked", "blocked", "2026-10-05 10:00:00.000", 0),
		transition(org, "gh:fx/repo#2", "github", "todo", "done", "closed", "2026-10-02 10:00:00.000", 0),
		transition(org, "gh:fx/repo#2", "github", "done", "todo", "reopened", "2026-10-02 10:00:00.000", 1),
		transition(org, "ghpr:fx/repo#3", "github", "canceled", "in_progress", "reopened", "2026-10-03 10:00:00.000", 0),
		transition(org, "gh:fx/repo#4", "github", "todo", "done", "closed", "2026-10-03 10:00:00.000", 0),
		transition(org, "gh:fx/repo#5", "github", "todo", "done", "closed", "2026-10-08 10:00:00.000", 0),
		transition(org, "linear:FX-1", "linear", "in_progress", "done", "Done", "2026-10-06 10:00:00.000", 0),
		transition(org, "linear:FX-2", "linear", "canceled", "todo", "Todo", "2026-10-07 10:00:00.000", 0),
		transition(org, "jira:FX-9", "jira", "done", "todo", "Open", "2026-10-08 10:00:00.000", 0),
		transition(other, "gh:fx/repo#4", "github", "done", "todo", "reopened", "2026-10-04 10:00:00.000", 0),
	}, ", "))

	sum := sha256.Sum256([]byte(formExecFcc14190SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecFcc14190SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14190-GAP-1.sql)", got, formExecFcc14190SHA256)
	}
	ids, statements := formExecFcc14190Statements(t, formExecFcc14190SQL)
	if got := strings.Join(ids, ","); got != "S0,F0,F1" {
		t.Fatalf("statements = %s, want S0,F0,F1", got)
	}
	// Static gate checks: one gate per data statement, over exactly the tables it reads, with one rows cap and one bytes
	// cap, and cost_gate = 0 in the outer WHERE and in every subquery that reads a table. S0 reads metadata only.
	for _, gate := range formExecFcc14190Gates {
		text := statements[gate.id]
		in := "table IN (" + gate.tables + ")"
		checks := []struct {
			what      string
			got, want int
		}{
			{"WITH (SELECT throwIf(", strings.Count(text, "WITH (SELECT throwIf("), 1},
			{"'STOP-COST-GATE'", strings.Count(text, "'STOP-COST-GATE'"), 1},
			{gate.rowsCap, strings.Count(text, gate.rowsCap), 1},
			{gate.bytesCap, strings.Count(text, gate.bytesCap), 1},
			{in, strings.Count(text, in), 2},
			{"cost_gate = 0", strings.Count(text, "cost_gate = 0"), gate.uses},
			{"FROM work_item_", strings.Count(text, "FROM work_item_"), len(strings.Split(gate.tables, ", "))},
		}
		for _, check := range checks {
			if check.got != check.want {
				t.Errorf("%s holds %q %d time(s), want %d", gate.id, check.what, check.got, check.want)
			}
		}
	}
	var s0SQL strings.Builder
	for _, line := range strings.Split(statements["S0"], "\n") {
		if !strings.HasPrefix(line, "--") {
			s0SQL.WriteString(line + "\n")
		}
	}
	if strings.Contains(s0SQL.String(), "throwIf") || strings.Contains(s0SQL.String(), "FROM work_item_") {
		t.Errorf("S0 must read metadata only")
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse version %s", version)
	// The pass path: every statement at its real caps, each tagged with a log_comment so its query_log row can be read.
	results := map[string]formExecFcc14190Result{}
	passComments := map[string]string{}
	for _, id := range ids {
		comment := fmt.Sprintf("form14190-pass-%s-%d", id, time.Now().UnixNano())
		passComments[id] = comment
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		result, err := formExecFcc14190Query(qctx, conn, statements[id])
		if err != nil {
			t.Fatalf("FORM-14190-GAP-1 %s: %v", id, err)
		}
		t.Logf("FORM-14190-GAP-1 %s columns %v", id, result.columns)
		for index, row := range result.rows {
			t.Logf("FORM-14190-GAP-1 %s row %d %v", id, index, row)
		}
		results[id] = result
	}

	formExecFcc14190Rows(t, results, "S0", 2)
	formExecFcc14190Row(t, results, "S0", 0, map[string]string{"table_name": "work_item_reopen_events", "table_rows": "9", "active_parts": "1"})
	formExecFcc14190Row(t, results, "S0", 1, map[string]string{"table_name": "work_item_transitions", "table_rows": "11", "active_parts": "1"})

	formExecFcc14190Rows(t, results, "F0", 1)
	formExecFcc14190Row(t, results, "F0", 0, map[string]string{"orgs": "1", "org_rows": "8"})

	formExecFcc14190Rows(t, results, "F1", 2)
	formExecFcc14190Row(t, results, "F1", 0, map[string]string{
		"provider": "github", "reopens_all": "5", "items_kept": "2", "reopens_kept": "2", "items_lost": "2",
		"reopens_lost": "3", "reopens_lost_tie": "2", "reopens_lost_bare": "1",
		"lost_first_day": "2026-10-01*", "lost_last_day": "2026-10-05*",
	})
	formExecFcc14190Row(t, results, "F1", 1, map[string]string{
		"provider": "linear", "reopens_all": "2", "items_kept": "1", "reopens_kept": "1", "items_lost": "1",
		"reopens_lost": "1", "reopens_lost_tie": "1", "reopens_lost_bare": "0",
		"lost_first_day": "2026-10-06*", "lost_last_day": "2026-10-06*",
	})

	// Sizes of the work math: E, T = active rows, P = active parts (system.parts, as the gate reads them).
	parts, sizes := map[string]uint64{}, map[string]uint64{}
	partRows, err := conn.Query(ctx, `SELECT table, count(), sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
		AND table IN ('work_item_reopen_events', 'work_item_transitions') GROUP BY table`)
	if err != nil {
		t.Fatal(err)
	}
	for partRows.Next() {
		var table string
		var count, rows uint64
		if err := partRows.Scan(&table, &count, &rows); err != nil {
			t.Fatal(err)
		}
		parts[table], sizes[table] = count, rows
	}
	_ = partRows.Close()
	if sizes["work_item_reopen_events"] != 9 || sizes["work_item_transitions"] != 11 ||
		parts["work_item_reopen_events"] != 1 || parts["work_item_transitions"] != 1 {
		t.Fatalf("fixture sizes E=%d T=%d P(work_item_reopen_events)=%d P(work_item_transitions)=%d, want 9, 11, 1, 1",
			sizes["work_item_reopen_events"], sizes["work_item_transitions"], parts["work_item_reopen_events"], parts["work_item_transitions"])
	}
	gateParts := func(tables string) uint64 {
		var p uint64
		for _, table := range strings.Split(tables, ", ") {
			p += parts[strings.Trim(table, "'")]
		}
		return p
	}

	// PASS-path cells (FORM-TEMPLATE-3): each data statement completed at its real caps (above); its query_log
	// QueryFinish read_rows must equal the work math on this fixture. The ProfileEvents of each query are logged.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, gate := range formExecFcc14190Gates {
		types, read := formExecFcc14190QueryLog(ctx, t, conn, "PASS "+gate.id, passComments[gate.id])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", gate.id, types)
		}
		if read != gate.fixtureReadRows {
			t.Errorf("PASS %s: query_log read_rows %d, want %d (work math on this fixture)", gate.id, read, gate.fixtureReadRows)
		}
	}

	// Probes of the scan model (INFO, logged only): the gate alone, and a plain and a FINAL read of each table with the
	// same org filter as the FORM.
	gateText := statements["F0"][strings.Index(statements["F0"], "WITH (SELECT throwIf(") : strings.Index(statements["F0"], "AS cost_gate")+len("AS cost_gate")]
	probes := []struct{ name, sql string }{
		{"gate-only", gateText + "\nSELECT cost_gate"},
		{"reopen-events-plain", "SELECT count() FROM work_item_reopen_events WHERE startsWith(org_id, 'c6a38355')"},
		{"reopen-events-final", "SELECT count() FROM work_item_reopen_events FINAL WHERE startsWith(org_id, 'c6a38355')"},
		{"transitions-plain", "SELECT count() FROM work_item_transitions WHERE startsWith(org_id, 'c6a38355')"},
		{"transitions-final", "SELECT count() FROM work_item_transitions FINAL WHERE startsWith(org_id, 'c6a38355')"},
	}
	probeComments := map[string]string{}
	for _, probe := range probes {
		comment := fmt.Sprintf("form14190-probe-%s-%d", probe.name, time.Now().UnixNano())
		probeComments[probe.name] = comment
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		if _, err := formExecFcc14190Query(qctx, conn, probe.sql); err != nil {
			t.Logf("PROBE %s: %v", probe.name, err)
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, probe := range probes {
		formExecFcc14190QueryLog(ctx, t, conn, "PROBE "+probe.name, probeComments[probe.name])
	}

	// The gates fire (FORM-TEMPLATE-3): each cap literal of each data statement lowered to 0 in a test copy only (the
	// embedded text and its sha256 stay as checked above). WANT code 395 STOP-COST-GATE, system.query_log type
	// ExceptionBeforeStart, and query_log read_rows <= 2 x P + 1 (P = active parts of the statement's tables: the gate
	// reads system.parts twice, plus system.one), so no data-table row is read.
	type fired struct {
		id, cap, comment string
		p                uint64
	}
	var fires []fired
	for _, gate := range formExecFcc14190Gates {
		p := gateParts(gate.tables)
		for _, literal := range []string{gate.rowsCap, gate.bytesCap} {
			lowered := strings.Replace(statements[gate.id], literal, "> 0", 1)
			if lowered == statements[gate.id] {
				t.Fatalf("%s: no %q to lower", gate.id, literal)
			}
			comment := fmt.Sprintf("form14190-gate-%s-%s-%d", gate.id, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			_, err := formExecFcc14190Query(qctx, conn, lowered)
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", gate.id, literal, err)
				continue
			}
			t.Logf("GATE %s %s lowered to 0: fired: %s", gate.id, literal, strings.SplitN(err.Error(), "\n", 2)[0])
			fires = append(fires, fired{gate.id, literal, comment, p})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		var types []string
		var logRead uint64
		rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, f.comment)
		if err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.id, f.cap, err)
		}
		for rows.Next() {
			var typ string
			var read uint64
			if err := rows.Scan(&typ, &read); err != nil {
				t.Fatalf("GATE %s %s: query log scan: %v", f.id, f.cap, err)
			}
			types = append(types, typ)
			logRead += read
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.id, f.cap, err)
		}
		_ = rows.Close()
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.id, f.cap, types, logRead, 2*f.p+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.id, f.cap, types)
		}
		if logRead > 2*f.p+1 {
			t.Errorf("GATE %s %s: query_log read_rows %d over 2 x P + 1 = %d", f.id, f.cap, logRead, 2*f.p+1)
		}
	}
	if len(fires) != 2*len(formExecFcc14190Gates) {
		t.Errorf("gates fired %d times, want %d (2 statements x 2 caps)", len(fires), 2*len(formExecFcc14190Gates))
	}
}
