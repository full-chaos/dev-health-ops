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

// DO NOT MERGE. This file executes the text of the prod read FORM-14190-READBACK-1.sql byte for byte (the sha256 below
// is the sha256 of the .sql file) on the head schema with a synthetic fixture in phases: the loss state before the
// backfill (one phase with an affected item the card's window misses, one without), a re-read under the old key (one
// row per instant), the re-read under the tie_ordinal key, and a control mutant. It checks each WANT in each phase,
// the pass-path read_rows of each data statement against its work math, and fires each cost gate cap once
// (pm-guard FORM-TEMPLATE-3).
const (
	formExecRb14190SHA256 = "dd6cb67abd7ad9954662bff13d720bc625fdabea5420698a630f748712906ebd"
	formExecRb14190SQL    = `-- FORM-14190-READBACK-1 (lane-14190c, 2026-11-23, ops pin a7febdf50002). ClickHouse prod, SELECT only, CHAOS-14190.
-- Read-back of the Linear work-items backfill of CARD-14190-BACKFILL-1. Run the whole file TWICE: PRE (after the acp
-- read, before the card's command) and POST (after every unit and daily run of the backfill is terminal).
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a). WANT text (PRE and POST) and work math:
-- FORM-14190-READBACK-1.md.
-- The three statements from the line "-- F0 ORG-1" to the end of the third are the text of FORM-14190-GAP-2.sql
-- (68a7ae72297b, acp FORM OK 03def4642bd0) byte for byte, so the lost reopens and the missing closes are keyed and
-- counted exactly as the gap read found them. NEW here: the metadata statement holds a third table (work_items), a
-- second metadata statement reads the transitions sorting key, and the last statement names, per affected Linear
-- item, its stored work_items row (coverage of the card's window and a re-read mark).
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix c6a38355 by startsWith on every FROM with org_id.
-- Each data statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- and system.one only), and every WHERE of a query level that reads a table, and the outer WHERE, tests the gate.
-- No data CTE: each table is named once per statement. Rows cap on the sum of the active rows of the statement's
-- tables: 5000000 (reopen events only) or 6000000 (two or three tables). Bytes cap 4294967296 on the same sum.
-- Run each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP and report.
-- Gap definition (as in the gap read). A reopen is lost when its work_item_reopen_events row has no
-- work_item_transitions row of the same org_id, work_item_id and occurred_at with the same to_status_raw. A close is
-- missing when no transition of the item in [the previous reopen, the reopen] (both ends included) goes to done or
-- canceled or has to_status_raw = the reopen's from_status_raw, and the item has a transition before the reopen.
-- Both data tables and work_items are read FINAL (ReplacingMergeTree(last_synced)): the stored state after merges.

-- S0 INFO cost inputs, metadata only (system.one + system.parts). WANT 3 rows, table_rows > 0 in each (a wrong table
-- name gives a row with 0).
SELECT l.t AS table_name, p.table_rows AS table_rows, p.disk_bytes AS disk_bytes, p.active_parts AS active_parts
FROM (SELECT arrayJoin(['work_item_reopen_events', 'work_item_transitions', 'work_items']) AS t FROM system.one) AS l
LEFT JOIN (
    SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
    FROM system.parts
    WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')
    GROUP BY table
) AS p ON p.table = l.t
ORDER BY table_name;

-- S1 the transitions sorting key, metadata only (system.one + system.tables). WANT sorting_key
-- 'org_id, repo_id, work_item_id, occurred_at, tie_ordinal' (ClickHouse migration 170 is live: a re-read writes each
-- transition of a same-instant tie under its own key).
SELECT l.t AS table_name, k.sorting_key AS sorting_key
FROM (SELECT 'work_item_transitions' AS t FROM system.one) AS l
LEFT JOIN (
    SELECT name, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_item_transitions'
) AS k ON k.name = l.t;

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

-- F2 lost closes per provider (github = ids 'gh:' and 'ghpr:', jira = 'jira:', linear = 'linear:'), one row each.
-- closes_missing = the LOWER BOUND of lost closes. Control: closes_found > 0 in >= 1 row. Each reopen is one of
-- closes_found, closes_missing, reopens_no_history. reopens_kept = F1 reopens_kept for github and linear.
-- INFO for the read of 2026-11-19: reopens_pair_mismatch = kept by to_status_raw but no transition at X with the
-- reopen's (from_status, to_status); reopens_kept_provider_empty = kept only by rows with provider ''
-- (no Go writer name); reopens_kept_synced_after_1119 = kept by a row with last_synced >= 2026-11-19 09:40 UTC.
-- rs[0] (i = 1) is the default tuple, occurred_at 1970-01-01: the first reopen has no lower bound.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT provider,
       sum(length(rs))                                                                        AS reopens_all,
       sum(arraySum(found))                                                                   AS closes_found,
       sum(arraySum(arrayMap((f, p) -> toUInt8(f = 0 AND p = 1), found, prior)))             AS closes_missing,
       countIf(arrayExists((f, p) -> f = 0 AND p = 1, found, prior))                          AS items_close_missing,
       sum(arraySum(arrayMap((f, p, k) -> toUInt8(f = 0 AND p = 1 AND k = 1), found, prior, kept))) AS closes_missing_reopen_kept,
       sum(arraySum(arrayMap((f, p) -> toUInt8(f = 0 AND p = 0), found, prior)))             AS reopens_no_history,
       sum(arraySum(kept))                                                                    AS reopens_kept,
       sum(arraySum(arrayMap((k, q) -> toUInt8(k = 1 AND q = 0), kept, pair)))               AS reopens_pair_mismatch,
       sum(arraySum(arrayMap((k, n) -> toUInt8(k = 1 AND n = 0), kept, named)))              AS reopens_kept_provider_empty,
       sum(arraySum(late))                                                                    AS reopens_kept_synced_after_1119
FROM (
    SELECT r.provider AS provider, r.rs AS rs,
           arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) >= tupleElement(rs[i - 1], 1)
                    AND tupleElement(x, 1) <= tupleElement(rs[i], 1)
                    AND (has(['done', 'canceled'], tupleElement(x, 3)) OR tupleElement(x, 5) = tupleElement(rs[i], 2)), t.ts)),
                    arrayEnumerate(rs)) AS found,
           arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) < tupleElement(rs[i], 1), t.ts)),
                    arrayEnumerate(rs)) AS prior,
           arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) = tupleElement(rs[i], 1)
                    AND tupleElement(x, 5) = tupleElement(rs[i], 3), t.ts)), arrayEnumerate(rs)) AS kept,
           arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) = tupleElement(rs[i], 1)
                    AND tupleElement(x, 2) = tupleElement(rs[i], 4) AND tupleElement(x, 3) = tupleElement(rs[i], 5), t.ts)),
                    arrayEnumerate(rs)) AS pair,
           arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) = tupleElement(rs[i], 1)
                    AND tupleElement(x, 5) = tupleElement(rs[i], 3) AND tupleElement(x, 6) != '', t.ts)),
                    arrayEnumerate(rs)) AS named,
           arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) = tupleElement(rs[i], 1)
                    AND tupleElement(x, 5) = tupleElement(rs[i], 3)
                    AND tupleElement(x, 7) >= toDateTime64('2026-11-19 09:40:00', 3, 'UTC'), t.ts)),
                    arrayEnumerate(rs)) AS late
    FROM (
        SELECT org_id, work_item_id,
               multiIf(startsWith(work_item_id, 'linear:'), 'linear', startsWith(work_item_id, 'jira:'), 'jira', 'github') AS provider,
               arraySort(groupArray((occurred_at, ifNull(from_status_raw, ''), ifNull(to_status_raw, ''), from_status, to_status))) AS rs
        FROM work_item_reopen_events FINAL
        WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
          AND (startsWith(work_item_id, 'gh:') OR startsWith(work_item_id, 'ghpr:') OR startsWith(work_item_id, 'linear:')
               OR startsWith(work_item_id, 'jira:'))
        GROUP BY org_id, work_item_id
    ) AS r
    LEFT JOIN (
        SELECT org_id, work_item_id,
               groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced)) AS ts
        FROM work_item_transitions FINAL
        WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
          AND (startsWith(work_item_id, 'gh:') OR startsWith(work_item_id, 'ghpr:') OR startsWith(work_item_id, 'linear:')
               OR startsWith(work_item_id, 'jira:'))
        GROUP BY org_id, work_item_id
    ) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id
    WHERE cost_gate = 0
)
WHERE cost_gate = 0
GROUP BY provider ORDER BY provider;

-- F3 Linear items (ids 'linear:') with a lost reopen (the F1 rule) or a missing close (the F2 rule), one row in all,
-- each joined to its stored work_items row. reopens_lost and closes_missing = the linear values of F1 and F2.
-- items_updated_before_since = affected items whose stored updated_at is before 2026-06-01, the card's --since: the
-- Linear issues query of a window filters on the issue's updatedAt, so the card does not re-read them (a stored
-- updated_at is at most the issue's current one). items_synced_last_day = affected items whose work_items row was
-- written in the 24 h before the read (re-read, INFO). event days = the first and last affected reopen.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count()                                                                          AS items_affected,
       countIf(a.lost > 0)                                                              AS items_reopen_lost,
       countIf(a.missing > 0)                                                           AS items_close_missing,
       sum(a.lost)                                                                      AS reopens_lost,
       sum(a.missing)                                                                   AS closes_missing,
       countIf(w.n > 0)                                                                 AS items_with_row,
       countIf(w.n > 0 AND w.updated < toDateTime64('2026-06-01 00:00:00', 3, 'UTC'))   AS items_updated_before_since,
       toDate(minIf(w.updated, w.n > 0))                                                AS updated_first_day,
       countIf(w.n > 0 AND w.synced >= now64(3) - INTERVAL 1 DAY)                       AS items_synced_last_day,
       toDate(min(a.first_at))                                                          AS event_first_day,
       toDate(max(a.last_at))                                                           AS event_last_day
FROM (
    SELECT org_id, work_item_id,
           arraySum(arrayMap(k -> toUInt8(k = 0), kept))                                AS lost,
           arraySum(arrayMap((f, p) -> toUInt8(f = 0 AND p = 1), found, prior))         AS missing,
           arrayMin(arrayFilter((o, k, f, p) -> k = 0 OR (f = 0 AND p = 1),
                    arrayMap(x -> tupleElement(x, 1), rs), kept, found, prior))         AS first_at,
           arrayMax(arrayFilter((o, k, f, p) -> k = 0 OR (f = 0 AND p = 1),
                    arrayMap(x -> tupleElement(x, 1), rs), kept, found, prior))         AS last_at
    FROM (
        SELECT r.org_id AS org_id, r.work_item_id AS work_item_id, r.rs AS rs,
               arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) >= tupleElement(rs[i - 1], 1)
                        AND tupleElement(x, 1) <= tupleElement(rs[i], 1)
                        AND (has(['done', 'canceled'], tupleElement(x, 3)) OR tupleElement(x, 5) = tupleElement(rs[i], 2)), t.ts)),
                        arrayEnumerate(rs)) AS found,
               arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) < tupleElement(rs[i], 1), t.ts)),
                        arrayEnumerate(rs)) AS prior,
               arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) = tupleElement(rs[i], 1)
                        AND tupleElement(x, 5) = tupleElement(rs[i], 3), t.ts)), arrayEnumerate(rs)) AS kept
        FROM (
            SELECT org_id, work_item_id,
                   arraySort(groupArray((occurred_at, ifNull(from_status_raw, ''), ifNull(to_status_raw, ''), from_status, to_status))) AS rs
            FROM work_item_reopen_events FINAL
            WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS r
        LEFT JOIN (
            SELECT org_id, work_item_id,
                   groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced)) AS ts
            FROM work_item_transitions FINAL
            WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id
        WHERE cost_gate = 0
    )
    WHERE cost_gate = 0
) AS a
LEFT JOIN (
    SELECT org_id, work_item_id, count() AS n, max(updated_at) AS updated, max(last_synced) AS synced
    FROM work_items FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
    GROUP BY org_id, work_item_id
) AS w ON w.org_id = a.org_id AND w.work_item_id = a.work_item_id
WHERE cost_gate = 0 AND (a.lost > 0 OR a.missing > 0);
`
)

type formExecRb14190Result struct {
	columns []string
	rows    [][]string
}

func formExecRb14190Exec(ctx context.Context, t *testing.T, conn driver.Conn, statement string) {
	t.Helper()
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// formExecRb14190Statements splits the FORM text as pm runs it: the pieces between ";\n", each named by its first
// line "-- S<n>" or "-- F<n>". A piece with no SQL line (only comments) is not a statement.
func formExecRb14190Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	statementID := regexp.MustCompile(`^[SF][0-9]+$`)
	var ids []string
	statements := map[string]string{}
	for _, piece := range strings.Split(text, ";\n") {
		statement := strings.TrimSpace(piece)
		id, hasSQL := "", false
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if fields := strings.Fields(line); id == "" && len(fields) > 1 && statementID.MatchString(fields[1]) {
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

func formExecRb14190Query(ctx context.Context, conn driver.Conn, statement string) (formExecRb14190Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecRb14190Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecRb14190Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecRb14190Result{}, fmt.Errorf("scan: %w", err)
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

func formExecRb14190Cell(t *testing.T, result formExecRb14190Result, label string, row int, column string) string {
	t.Helper()
	if row >= len(result.rows) {
		t.Fatalf("%s: %d row(s), want a row %d", label, len(result.rows), row)
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[row][index]
		}
	}
	t.Fatalf("%s: no column %s in %v", label, column, result.columns)
	return ""
}

// formExecRb14190Row checks the named columns of one row; a value ending in "*" is a prefix.
func formExecRb14190Row(t *testing.T, result formExecRb14190Result, label string, row int, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got := formExecRb14190Cell(t, result, label, row, column)
		if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
			if !strings.HasPrefix(got, prefix) {
				t.Errorf("%s row %d %s = %s, want prefix %s", label, row, column, got, prefix)
			}
			continue
		}
		if got != value {
			t.Errorf("%s row %d %s = %s, want %s", label, row, column, got, value)
		}
	}
}

// formExecRb14190QueryLog reads the finished query_log rows of one log_comment: their types and the summed read_rows.
func formExecRb14190QueryLog(ctx context.Context, t *testing.T, conn driver.Conn, label, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows, ProfileEvents['SelectedRows'], ProfileEvents['SelectedParts'],
		ProfileEvents['ScalarSubqueriesCacheMiss'], arrayStringConcat(tables, ',')
		FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	defer func() { _ = rows.Close() }()
	var types []string
	var read uint64
	for rows.Next() {
		var typ, tables string
		var readRows, selectedRows, selectedParts, scalarMiss uint64
		if err := rows.Scan(&typ, &readRows, &selectedRows, &selectedParts, &scalarMiss, &tables); err != nil {
			t.Fatalf("%s: query log scan: %v", label, err)
		}
		types = append(types, typ)
		read += readRows
		t.Logf("%s type=%s read_rows=%d SelectedRows=%d SelectedParts=%d ScalarSubqueriesCacheMiss=%d tables=%s",
			label, typ, readRows, selectedRows, selectedParts, scalarMiss, tables)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	return types, read
}

// CHAOS-14190 read-back fixture. Org c6a38355-... (the org the FORM reads); org d6a38355-... holds a lost Linear reopen
// and a work_items row and must not count. Phase 1 writes each table in ONE INSERT (one part each, merges stopped):
//
//	linear:FX-1: In Progress 10-01, Done 10-06; reopened Done -> Todo 10-06, its Todo transition lost in the tie.
//	linear:FX-2: In Progress 09-10, reopened Done -> Todo 09-20 with its Todo transition; the close is missing.
//	linear:FX-3: Done 08-01, reopened 08-05 with its transition: kept, close found.
//	linear:FX-4: reopened Canceled -> Todo 10-07 with its transition only: kept, no history before it.
//	linear:FX-6: Todo 05-01, Done 05-10; reopened 05-10, its Todo transition lost; its work_items row was last
//	    updated 05-20, before the card's --since.
//	gh:fx/repo#1 and jira:FX-9: closed, then reopened with their transitions (the controls).
//
// work_items rows of the affected items were last written 11-01 (not in the read's last 24 h).
func TestFormExecFcc14190Readback(t *testing.T) {
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

	const (
		org, other = "c6a38355-1419-4000-8000-0000000000c2", "d6a38355-1419-4000-8000-0000000000c3"
		zero       = "00000000-0000-0000-0000-000000000000"
		synced     = "2026-11-20 03:00:00.000"
		oldKey     = "2026-11-24 00:00:00.000"
		newKey     = "2026-11-24 01:00:00.000"
		itemSynced = "2026-11-01 00:00:00.000"
	)
	for _, table := range []string{"work_item_reopen_events", "work_item_transitions", "work_items"} {
		formExecRb14190Exec(ctx, t, conn, "SYSTEM STOP MERGES "+table)
	}
	reopen := func(orgID, id, from, to, fromRaw, toRaw, occurred string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', NULL, '%s')", orgID, id, occurred, from, to, fromRaw, toRaw, synced)
	}
	reopens := `INSERT INTO work_item_reopen_events (org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced) VALUES `
	transition := func(orgID, id, provider, from, to, toRaw, occurred, lastSynced string, ordinal int) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '', '%s', '', '%s', '%s', %d)",
			orgID, zero, id, provider, from, to, toRaw, occurred, lastSynced, ordinal)
	}
	transitions := `INSERT INTO work_item_transitions (org_id, repo_id, work_item_id, provider, from_status, to_status, from_status_raw, to_status_raw, actor, occurred_at, last_synced, tie_ordinal) VALUES `
	item := func(orgID, id, updated, lastSynced string) string {
		return fmt.Sprintf("('%s', '%s', '%s', 'linear', 'todo', '%s', '%s', %s)", orgID, zero, id, updated, updated, lastSynced)
	}
	items := `INSERT INTO work_items (org_id, repo_id, work_item_id, provider, status, created_at, updated_at, last_synced) VALUES `
	quoted := func(text string) string { return "'" + text + "'" }

	formExecRb14190Exec(ctx, t, conn, reopens+strings.Join([]string{
		reopen(org, "linear:FX-1", "done", "todo", "Done", "Todo", "2026-10-06 10:00:00.000"),
		reopen(org, "linear:FX-2", "done", "todo", "Done", "Todo", "2026-09-20 10:00:00.000"),
		reopen(org, "linear:FX-3", "done", "todo", "Done", "Todo", "2026-08-05 10:00:00.000"),
		reopen(org, "linear:FX-4", "canceled", "todo", "Canceled", "Todo", "2026-10-07 10:00:00.000"),
		reopen(org, "linear:FX-6", "done", "todo", "Done", "Todo", "2026-05-10 10:00:00.000"),
		reopen(org, "gh:fx/repo#1", "done", "todo", "closed", "reopened", "2026-10-02 10:00:00.000"),
		reopen(org, "jira:FX-9", "done", "todo", "Done", "Open", "2026-10-08 10:00:00.000"),
		reopen(other, "linear:FX-1", "done", "todo", "Done", "Todo", "2026-10-06 10:00:00.000"),
	}, ", "))
	formExecRb14190Exec(ctx, t, conn, transitions+strings.Join([]string{
		transition(org, "linear:FX-1", "linear", "todo", "in_progress", "In Progress", "2026-10-01 10:00:00.000", synced, 0),
		transition(org, "linear:FX-1", "linear", "in_progress", "done", "Done", "2026-10-06 10:00:00.000", synced, 0),
		transition(org, "linear:FX-2", "linear", "todo", "in_progress", "In Progress", "2026-09-10 10:00:00.000", synced, 0),
		transition(org, "linear:FX-2", "linear", "done", "todo", "Todo", "2026-09-20 10:00:00.000", synced, 0),
		transition(org, "linear:FX-3", "linear", "in_progress", "done", "Done", "2026-08-01 10:00:00.000", synced, 0),
		transition(org, "linear:FX-3", "linear", "done", "todo", "Todo", "2026-08-05 10:00:00.000", synced, 0),
		transition(org, "linear:FX-4", "linear", "canceled", "todo", "Todo", "2026-10-07 10:00:00.000", synced, 0),
		transition(org, "linear:FX-6", "linear", "backlog", "todo", "Todo", "2026-05-01 10:00:00.000", synced, 0),
		transition(org, "linear:FX-6", "linear", "todo", "done", "Done", "2026-05-10 10:00:00.000", synced, 0),
		transition(org, "gh:fx/repo#1", "github", "todo", "done", "closed", "2026-10-01 10:00:00.000", synced, 0),
		transition(org, "gh:fx/repo#1", "github", "done", "todo", "reopened", "2026-10-02 10:00:00.000", synced, 0),
		transition(org, "jira:FX-9", "jira", "in_progress", "done", "Done", "2026-10-07 10:00:00.000", synced, 0),
		transition(org, "jira:FX-9", "jira", "done", "todo", "Open", "2026-10-08 10:00:00.000", synced, 0),
	}, ", "))
	formExecRb14190Exec(ctx, t, conn, items+strings.Join([]string{
		item(org, "linear:FX-1", "2026-10-06 10:00:00.000", quoted(itemSynced)),
		item(org, "linear:FX-2", "2026-09-20 10:00:00.000", quoted(itemSynced)),
		item(org, "linear:FX-3", "2026-08-05 10:00:00.000", quoted(itemSynced)),
		item(org, "linear:FX-4", "2026-10-07 10:00:00.000", quoted(itemSynced)),
		item(org, "linear:FX-6", "2026-05-20 10:00:00.000", quoted(itemSynced)),
		item(other, "linear:FX-1", "2026-01-01 10:00:00.000", quoted(itemSynced)),
	}, ", "))

	sum := sha256.Sum256([]byte(formExecRb14190SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecRb14190SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14190-READBACK-1.sql)", got, formExecRb14190SHA256)
	}
	ids, statements := formExecRb14190Statements(t, formExecRb14190SQL)
	if got := strings.Join(ids, ","); got != "S0,S1,F0,F1,F2,F3" {
		t.Fatalf("statements = %s, want S0,S1,F0,F1,F2,F3", got)
	}
	// The gates of the data statements: the tables each reads (its IN list), its cap literals, how many times it holds
	// cost_gate = 0, and fixtureReadRows, the query_log read_rows on the phase 1 fixture by the scan model of
	// FORM-14190-READBACK-1.md: E 8, T 13, W 6 rows, one part each; a gate evaluation reads 2 x P + 1, each further
	// cost_gate reference 1 more row, a plain or FINAL read of a one-part table its rows once.
	gates := []struct {
		id, tables, rowsCap, bytesCap string
		reads, uses                   int
		fixtureReadRows               uint64
	}{
		{"F0", "'work_item_reopen_events'", "> 5000000", "> 4294967296", 1, 1, 8 + 3},
		{"F1", "'work_item_reopen_events', 'work_item_transitions'", "> 6000000", "> 4294967296", 2, 3, 8 + 13 + 5 + 2},
		{"F2", "'work_item_reopen_events', 'work_item_transitions'", "> 6000000", "> 4294967296", 2, 4, 8 + 13 + 5 + 3},
		{"F3", "'work_item_reopen_events', 'work_item_transitions', 'work_items'", "> 6000000", "> 4294967296", 3, 6, 8 + 13 + 6 + 7 + 5},
	}
	// Static gate checks: one gate per data statement, over exactly the tables it reads, with one rows cap and one bytes
	// cap, and cost_gate = 0 in the outer WHERE and in every query level that reads a table. S0 and S1 read metadata.
	for _, gate := range gates {
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
			{"FROM work_item", strings.Count(text, "FROM work_item"), gate.reads},
		}
		for _, check := range checks {
			if check.got != check.want {
				t.Errorf("%s holds %q %d time(s), want %d", gate.id, check.what, check.got, check.want)
			}
		}
	}
	for _, id := range []string{"S0", "S1"} {
		var sqlText strings.Builder
		for _, line := range strings.Split(statements[id], "\n") {
			if !strings.HasPrefix(line, "--") {
				sqlText.WriteString(line + "\n")
			}
		}
		if strings.Contains(sqlText.String(), "throwIf") || strings.Contains(sqlText.String(), "FROM work_item") {
			t.Errorf("%s must read metadata only", id)
		}
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse version %s", version)

	// run runs every statement of the FORM once, in order (as pm runs it), each tagged with a log_comment.
	run := func(phase string) (map[string]formExecRb14190Result, map[string]string) {
		results, comments := map[string]formExecRb14190Result{}, map[string]string{}
		for _, id := range ids {
			comment := fmt.Sprintf("form14190rb-%s-%s-%d", phase, id, time.Now().UnixNano())
			comments[id] = comment
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			result, err := formExecRb14190Query(qctx, conn, statements[id])
			if err != nil {
				t.Fatalf("FORM-14190-READBACK-1 %s %s: %v", phase, id, err)
			}
			t.Logf("FORM-14190-READBACK-1 %s %s columns %v", phase, id, result.columns)
			for index, row := range result.rows {
				t.Logf("FORM-14190-READBACK-1 %s %s row %d %v", phase, id, index, row)
			}
			results[id] = result
		}
		return results, comments
	}
	rowCount := func(results map[string]formExecRb14190Result, label, id string, want int) {
		t.Helper()
		if got := len(results[id].rows); got != want {
			t.Fatalf("%s %s: %d row(s), want %d", label, id, got, want)
		}
	}
	// wantPost checks the POST WANTs of the md and reports whether every one held: F1 linear reopens_lost 0 and
	// reopens_kept = reopens_all > 0, F2 linear closes_missing 0, F3 items_affected 0, and the controls (F0 orgs 1,
	// F1 github reopens_lost 0, F2 github and jira closes_missing 0).
	wantPost := func(results map[string]formExecRb14190Result, label string) bool {
		t.Helper()
		cell := func(id string, row int, column string) string {
			return formExecRb14190Cell(t, results[id], label+" "+id, row, column)
		}
		held := cell("F0", 0, "orgs") == "1" &&
			cell("F1", 0, "provider") == "github" && cell("F1", 0, "reopens_lost") == "0" &&
			cell("F1", 1, "provider") == "linear" && cell("F1", 1, "reopens_lost") == "0" &&
			cell("F1", 1, "reopens_all") != "0" && cell("F1", 1, "reopens_kept") == cell("F1", 1, "reopens_all") &&
			cell("F2", 0, "provider") == "github" && cell("F2", 0, "closes_missing") == "0" &&
			cell("F2", 1, "provider") == "jira" && cell("F2", 1, "closes_missing") == "0" &&
			cell("F2", 2, "provider") == "linear" && cell("F2", 2, "closes_missing") == "0" &&
			cell("F3", 0, "items_affected") == "0"
		t.Logf("%s: POST WANTs held = %v", label, held)
		return held
	}

	// Phase 1: the loss state, with FX-6 last updated before the card's --since.
	results, passComments := run("phase1")
	rowCount(results, "phase1", "S0", 3)
	formExecRb14190Row(t, results["S0"], "phase1 S0", 0, map[string]string{"table_name": "work_item_reopen_events", "table_rows": "8", "active_parts": "1"})
	formExecRb14190Row(t, results["S0"], "phase1 S0", 1, map[string]string{"table_name": "work_item_transitions", "table_rows": "13", "active_parts": "1"})
	formExecRb14190Row(t, results["S0"], "phase1 S0", 2, map[string]string{"table_name": "work_items", "table_rows": "6", "active_parts": "1"})
	rowCount(results, "phase1", "S1", 1)
	formExecRb14190Row(t, results["S1"], "phase1 S1", 0, map[string]string{
		"table_name": "work_item_transitions", "sorting_key": "org_id, repo_id, work_item_id, occurred_at, tie_ordinal"})
	rowCount(results, "phase1", "F0", 1)
	formExecRb14190Row(t, results["F0"], "phase1 F0", 0, map[string]string{"orgs": "1", "org_rows": "7"})
	rowCount(results, "phase1", "F1", 2)
	formExecRb14190Row(t, results["F1"], "phase1 F1", 0, map[string]string{
		"provider": "github", "reopens_all": "1", "items_kept": "1", "reopens_kept": "1", "items_lost": "0",
		"reopens_lost": "0", "reopens_lost_tie": "0", "reopens_lost_bare": "0",
	})
	formExecRb14190Row(t, results["F1"], "phase1 F1", 1, map[string]string{
		"provider": "linear", "reopens_all": "5", "items_kept": "3", "reopens_kept": "3", "items_lost": "2",
		"reopens_lost": "2", "reopens_lost_tie": "2", "reopens_lost_bare": "0",
		"lost_first_day": "2026-05-10*", "lost_last_day": "2026-10-06*",
	})
	rowCount(results, "phase1", "F2", 3)
	formExecRb14190Row(t, results["F2"], "phase1 F2", 0, map[string]string{
		"provider": "github", "reopens_all": "1", "closes_found": "1", "closes_missing": "0", "items_close_missing": "0",
		"closes_missing_reopen_kept": "0", "reopens_no_history": "0", "reopens_kept": "1", "reopens_pair_mismatch": "0",
		"reopens_kept_provider_empty": "0", "reopens_kept_synced_after_1119": "1",
	})
	formExecRb14190Row(t, results["F2"], "phase1 F2", 1, map[string]string{
		"provider": "jira", "reopens_all": "1", "closes_found": "1", "closes_missing": "0", "items_close_missing": "0",
		"closes_missing_reopen_kept": "0", "reopens_no_history": "0", "reopens_kept": "1", "reopens_pair_mismatch": "0",
		"reopens_kept_provider_empty": "0", "reopens_kept_synced_after_1119": "1",
	})
	formExecRb14190Row(t, results["F2"], "phase1 F2", 2, map[string]string{
		"provider": "linear", "reopens_all": "5", "closes_found": "3", "closes_missing": "1", "items_close_missing": "1",
		"closes_missing_reopen_kept": "1", "reopens_no_history": "1", "reopens_kept": "3", "reopens_pair_mismatch": "0",
		"reopens_kept_provider_empty": "0", "reopens_kept_synced_after_1119": "3",
	})
	rowCount(results, "phase1", "F3", 1)
	formExecRb14190Row(t, results["F3"], "phase1 F3", 0, map[string]string{
		"items_affected": "3", "items_reopen_lost": "2", "items_close_missing": "1", "reopens_lost": "2", "closes_missing": "1",
		"items_with_row": "3", "items_updated_before_since": "1", "updated_first_day": "2026-05-20*",
		"items_synced_last_day": "0", "event_first_day": "2026-05-10*", "event_last_day": "2026-10-06*",
	})
	if wantPost(results, "phase1") {
		t.Errorf("phase1 (the loss state): the POST WANTs held, want them to fail")
	}

	// Mutation cells on the phase 1 fixture: each mutant (a test copy; the embedded text stays as checked) must change
	// its value.
	mutants := []struct {
		name, id, from, to string
		row                int
		column, killed     string
	}{
		// The coverage cut is read: with --since a month earlier, FX-6 (updated 05-20) is covered.
		{"since-literal", "F3", "toDateTime64('2026-06-01 00:00:00', 3, 'UTC')", "toDateTime64('2026-05-01 00:00:00', 3, 'UTC')", 0, "items_updated_before_since", "1"},
		// An item with only a missing close is affected: without it FX-2 drops out.
		{"affected-without-missing-close", "F3", "WHERE cost_gate = 0 AND (a.lost > 0 OR a.missing > 0)", "WHERE cost_gate = 0 AND (a.lost > 0)", 0, "items_affected", "3"},
		// A lost reopen is keyed by its to_status_raw at its instant: a tie-blind rule (any transition at the instant)
		// takes the surviving close of FX-1 and FX-6 for their reopen, so a re-read that wrote only the close would pass.
		{"reopen-tie-blind", "F3", "AND tupleElement(x, 5) = tupleElement(rs[i], 3), t.ts)), arrayEnumerate(rs)) AS kept", ", t.ts)), arrayEnumerate(rs)) AS kept", 0, "items_reopen_lost", "2"},
	}
	for _, mutant := range mutants {
		text := statements[mutant.id]
		if got := strings.Count(text, mutant.from); got != 1 {
			t.Fatalf("MUTANT %s: %q %d time(s) in %s, want 1", mutant.name, mutant.from, got, mutant.id)
		}
		result, err := formExecRb14190Query(ctx, conn, strings.Replace(text, mutant.from, mutant.to, 1))
		if err != nil {
			t.Logf("MUTANT %s: %v (an error kills it)", mutant.name, err)
			continue
		}
		got := formExecRb14190Cell(t, result, "MUTANT "+mutant.name, mutant.row, mutant.column)
		t.Logf("MUTANT %s: %s row %d %s = %s (unmutated %s)", mutant.name, mutant.id, mutant.row, mutant.column, got, mutant.killed)
		if got == mutant.killed {
			t.Errorf("MUTANT %s SURVIVED: %s row %d %s = %s", mutant.name, mutant.id, mutant.row, mutant.column, got)
		}
	}

	// Sizes of the work math: active rows and parts per table (system.parts, as the gate reads them).
	parts, sizes := map[string]uint64{}, map[string]uint64{}
	partRows, err := conn.Query(ctx, `SELECT table, count(), sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
		AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items') GROUP BY table`)
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
	if sizes["work_item_reopen_events"] != 8 || sizes["work_item_transitions"] != 13 || sizes["work_items"] != 6 ||
		parts["work_item_reopen_events"] != 1 || parts["work_item_transitions"] != 1 || parts["work_items"] != 1 {
		t.Fatalf("fixture sizes %v parts %v, want E 8, T 13, W 6 rows in one part each", sizes, parts)
	}
	gateParts := func(tables string) uint64 {
		var p uint64
		for _, table := range strings.Split(tables, ", ") {
			p += parts[strings.Trim(table, "'")]
		}
		return p
	}

	// PASS-path cells (FORM-TEMPLATE-3): each data statement completed at its real caps in phase 1; its query_log
	// QueryFinish read_rows must equal the work math on this fixture.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, gate := range gates {
		types, read := formExecRb14190QueryLog(ctx, t, conn, "PASS "+gate.id, passComments[gate.id])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", gate.id, types)
		}
		if read != gate.fixtureReadRows {
			t.Errorf("PASS %s: query_log read_rows %d, want %d (work math on this fixture)", gate.id, read, gate.fixtureReadRows)
		}
	}

	// The gates fire: each cap literal of each data statement lowered to 0 in a test copy only. WANT code 395
	// STOP-COST-GATE, system.query_log type ExceptionBeforeStart, and read_rows <= 2 x P + 1.
	type fired struct {
		id, cap, comment string
		p                uint64
	}
	var fires []fired
	for _, gate := range gates {
		for _, literal := range []string{gate.rowsCap, gate.bytesCap} {
			lowered := strings.Replace(statements[gate.id], literal, "> 0", 1)
			if lowered == statements[gate.id] {
				t.Fatalf("%s: no %q to lower", gate.id, literal)
			}
			comment := fmt.Sprintf("form14190rb-gate-%s-%s-%d", gate.id, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			_, err := formExecRb14190Query(qctx, conn, lowered)
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", gate.id, literal, err)
				continue
			}
			t.Logf("GATE %s %s lowered to 0: fired: %s", gate.id, literal, strings.SplitN(err.Error(), "\n", 2)[0])
			fires = append(fires, fired{gate.id, literal, comment, gateParts(gate.tables)})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		types, read := formExecRb14190QueryLog(ctx, t, conn, "GATE "+f.id+" "+f.cap, f.comment)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.id, f.cap, types)
		}
		if read > 2*f.p+1 {
			t.Errorf("GATE %s %s: query_log read_rows %d over 2 x P + 1 = %d", f.id, f.cap, read, 2*f.p+1)
		}
	}
	if len(fires) != 2*len(gates) {
		t.Errorf("gates fired %d times, want %d (4 statements x 2 caps)", len(fires), 2*len(gates))
	}

	// Phase 2: FX-6 was edited in Linear on 06-10 (a newer work_items copy, written now): every affected item is in
	// the card's window (the PRE coverage WANT holds); the loss is still there (the POST WANTs fail).
	formExecRb14190Exec(ctx, t, conn, items+item(org, "linear:FX-6", "2026-06-10 10:00:00.000", "now64(3)"))
	results, _ = run("phase2")
	formExecRb14190Row(t, results["F3"], "phase2 F3", 0, map[string]string{
		"items_affected": "3", "reopens_lost": "2", "closes_missing": "1", "items_with_row": "3",
		"items_updated_before_since": "0", "updated_first_day": "2026-06-10*", "items_synced_last_day": "1",
	})
	if wantPost(results, "phase2") {
		t.Errorf("phase2 (the loss state, covered): the POST WANTs held, want them to fail")
	}

	// Phase 3: a re-read under the OLD key (one row per item and instant, ordinal 0, the last event of the instant
	// wins): FX-1 and FX-6 write their close again, FX-2 its reopen. Nothing comes back: the POST WANTs fail.
	formExecRb14190Exec(ctx, t, conn, transitions+strings.Join([]string{
		transition(org, "linear:FX-1", "linear", "in_progress", "done", "Done", "2026-10-06 10:00:00.000", oldKey, 0),
		transition(org, "linear:FX-2", "linear", "done", "todo", "Todo", "2026-09-20 10:00:00.000", oldKey, 0),
		transition(org, "linear:FX-6", "linear", "todo", "done", "Done", "2026-05-10 10:00:00.000", oldKey, 0),
	}, ", "))
	results, _ = run("phase3")
	formExecRb14190Row(t, results["F1"], "phase3 F1", 1, map[string]string{"provider": "linear", "reopens_lost": "2"})
	formExecRb14190Row(t, results["F2"], "phase3 F2", 2, map[string]string{"provider": "linear", "closes_missing": "1"})
	formExecRb14190Row(t, results["F3"], "phase3 F3", 0, map[string]string{"items_affected": "3"})
	if wantPost(results, "phase3") {
		t.Errorf("phase3 (re-read under the old key): the POST WANTs held, want them to fail")
	}

	// Phase 4: the re-read under the tie_ordinal key: every transition of a same-instant tie under its own ordinal
	// (the close 0, the reopen 1), newer last_synced, and the items' work_items rows written now. Every POST WANT holds.
	formExecRb14190Exec(ctx, t, conn, transitions+strings.Join([]string{
		transition(org, "linear:FX-1", "linear", "todo", "in_progress", "In Progress", "2026-10-01 10:00:00.000", newKey, 0),
		transition(org, "linear:FX-1", "linear", "in_progress", "done", "Done", "2026-10-06 10:00:00.000", newKey, 0),
		transition(org, "linear:FX-1", "linear", "done", "todo", "Todo", "2026-10-06 10:00:00.000", newKey, 1),
		transition(org, "linear:FX-2", "linear", "todo", "in_progress", "In Progress", "2026-09-10 10:00:00.000", newKey, 0),
		transition(org, "linear:FX-2", "linear", "in_progress", "done", "Done", "2026-09-20 10:00:00.000", newKey, 0),
		transition(org, "linear:FX-2", "linear", "done", "todo", "Todo", "2026-09-20 10:00:00.000", newKey, 1),
		transition(org, "linear:FX-6", "linear", "backlog", "todo", "Todo", "2026-05-01 10:00:00.000", newKey, 0),
		transition(org, "linear:FX-6", "linear", "todo", "done", "Done", "2026-05-10 10:00:00.000", newKey, 0),
		transition(org, "linear:FX-6", "linear", "done", "todo", "Todo", "2026-05-10 10:00:00.000", newKey, 1),
	}, ", "))
	formExecRb14190Exec(ctx, t, conn, items+strings.Join([]string{
		item(org, "linear:FX-1", "2026-10-06 10:00:00.000", "now64(3)"),
		item(org, "linear:FX-2", "2026-09-20 10:00:00.000", "now64(3)"),
	}, ", "))
	results, _ = run("phase4")
	formExecRb14190Row(t, results["F1"], "phase4 F1", 1, map[string]string{
		"provider": "linear", "reopens_all": "5", "reopens_kept": "5", "reopens_lost": "0", "items_lost": "0"})
	formExecRb14190Row(t, results["F2"], "phase4 F2", 2, map[string]string{
		"provider": "linear", "reopens_all": "5", "closes_found": "4", "closes_missing": "0", "items_close_missing": "0",
		"reopens_no_history": "1", "reopens_kept": "5"})
	formExecRb14190Row(t, results["F3"], "phase4 F3", 0, map[string]string{
		"items_affected": "0", "reopens_lost": "0", "closes_missing": "0", "items_updated_before_since": "0"})
	if !wantPost(results, "phase4") {
		t.Errorf("phase4 (re-read under the tie_ordinal key): a POST WANT failed, want every one to hold")
	}

	// Phase 5, the control mutant: a GitHub reopen with no transition and no close before it, and a Jira reopen with
	// no close before it. The controls fail; the Linear values stay healed.
	formExecRb14190Exec(ctx, t, conn, reopens+strings.Join([]string{
		reopen(org, "gh:fx/repo#2", "done", "todo", "closed", "reopened", "2026-10-03 10:00:00.000"),
		reopen(org, "jira:FX-8", "done", "todo", "Done", "Open", "2026-10-09 10:00:00.000"),
	}, ", "))
	formExecRb14190Exec(ctx, t, conn, transitions+strings.Join([]string{
		transition(org, "gh:fx/repo#2", "github", "todo", "in_progress", "in_progress", "2026-10-01 10:00:00.000", synced, 0),
		transition(org, "jira:FX-8", "jira", "todo", "in_progress", "In Progress", "2026-10-01 10:00:00.000", synced, 0),
		transition(org, "jira:FX-8", "jira", "done", "todo", "Open", "2026-10-09 10:00:00.000", synced, 0),
	}, ", "))
	results, _ = run("phase5")
	formExecRb14190Row(t, results["F0"], "phase5 F0", 0, map[string]string{"orgs": "1", "org_rows": "9"})
	formExecRb14190Row(t, results["F1"], "phase5 F1", 0, map[string]string{"provider": "github", "reopens_lost": "1"})
	formExecRb14190Row(t, results["F2"], "phase5 F2", 0, map[string]string{"provider": "github", "closes_missing": "1"})
	formExecRb14190Row(t, results["F2"], "phase5 F2", 1, map[string]string{"provider": "jira", "closes_missing": "1"})
	formExecRb14190Row(t, results["F2"], "phase5 F2", 2, map[string]string{"provider": "linear", "closes_missing": "0"})
	formExecRb14190Row(t, results["F3"], "phase5 F3", 0, map[string]string{"items_affected": "0"})
	if wantPost(results, "phase5") {
		t.Errorf("phase5 (control mutant): the POST WANTs held, want the controls to fail them")
	}
}
