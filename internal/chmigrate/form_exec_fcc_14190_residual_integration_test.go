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

// DO NOT MERGE. This file executes the text of the prod read FORM-14190-RESIDUAL-3.sql on the head schema with a
// synthetic fixture. The text below is the .sql with its org prefix replaced by {{ORG_PREFIX}}, nothing else (its
// sha256 is checked); the cell puts a synthetic prefix in at run time and checks the slot count first. It checks each
// statement's rows in a loss phase and a healed phase, an org mutant and a rule mutant per statement, the pass-path
// read_rows of each statement against its work math, and fires each cost gate cap once (pm-guard FORM-TEMPLATE-3).
const (
	formExecRes14190OrgSlot = "{{ORG_PREFIX}}"
	formExecRes14190SHA256  = "3f7e425ecb2e7f4557a2ba87510d92c14eb77226e9c545e0093169ec2ad137fb"
	formExecRes14190SQL     = `-- FORM-14190-RESIDUAL-3 (lane-14190r, 2026-11-23, ops pin a7febdf50002). ClickHouse prod, SELECT only, CHAOS-14190.
-- Replaces FORM-14190-RESIDUAL-2 (5610a01de90d) and -1 (1181a6d4fc41); both frozen, not to be run. -3 = -2 + R2 reads
-- the item's stored work_items row: stored_close_any = close time present OR a close transition at any instant.
-- Residual REPORT after READBACK POST = MISS (FORM-14190-READBACK-1 dd6cb67abd7a, POST out 3091612d0fe1): per affected
-- Linear item (R1) and per missing close (R2), the F3 rule of FORM-14190-READBACK-1 grouped per item, plus the work_items
-- version count behind its S0 growth (S2). WANT text, work math and the output rule: FORM-14190-RESIDUAL-3.md.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a). Run each statement once, in order. A SQL error
-- (STOP-COST-GATE included) = STOP and report.
-- NAMED EXCEPTION (acp): R1 and R2 print the 8-char cut AND the full Linear key of each item, and workflow state
-- names. The out file is 0600 in pm-guard/readback-1 only: never in chat, Linear, a PR or a log.
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix {{ORG_PREFIX}} by startsWith on every FROM with org_id.
-- Each statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- and system.one only); every WHERE of a query level that reads a table, and the outer WHERE, tests the gate.
-- No data CTE: each table is named once per statement. Rows cap on the sum of the active rows of the statement's
-- tables: 5000000 (one table) or 6000000 (two or three tables). Bytes cap 4294967296 on the same sum.
-- The rule (FORM-14190-READBACK-1 F3, its text kept for found, prior and kept). A reopen is lost when its
-- work_item_reopen_events row has no work_item_transitions row of the same org_id, work_item_id and occurred_at with
-- the same to_status_raw. A close is missing when no transition of the item in [the previous reopen, the reopen] (both
-- ends included) goes to done or canceled or has to_status_raw = the reopen's from_status_raw, and the item has a
-- transition before the reopen. All three tables are read FINAL in R1 and R2 (ReplacingMergeTree(last_synced)).

-- S0 ORG-1 and control. WANT orgs = 1, org_rows > 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events')) > 5000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM work_item_reopen_events
WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0;

-- S1 metadata (system.one, system.parts, system.tables; no data table). WANT 3 rows, table_rows > 0 in each (a wrong
-- table name gives a row with 0), work_item_transitions sorting_key 'org_id, repo_id, work_item_id, occurred_at,
-- tie_ordinal' (CH 170 live: R2 reads tie_ordinal).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT l.t AS table_name, p.table_rows AS table_rows, p.active_parts AS active_parts, k.sorting_key AS sorting_key
FROM (SELECT arrayJoin(['work_item_reopen_events', 'work_item_transitions', 'work_items']) AS t FROM system.one) AS l
LEFT JOIN (
    SELECT table, sum(rows) AS table_rows, count() AS active_parts
    FROM system.parts
    WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')
    GROUP BY table
) AS p ON p.table = l.t
LEFT JOIN (
    SELECT name, sorting_key FROM system.tables
    WHERE database = currentDatabase() AND name IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')
) AS k ON k.name = l.t
WHERE cost_gate = 0
ORDER BY table_name;

-- S2 work_items versions per provider, plain read (no FINAL: every stored version counts), INFO for the S0 growth of
-- READBACK POST. One group per sorting key (org_id, repo_id, work_item_id). versions_over_keys = rows not yet merged
-- away. items < item_keys = an item stored under two keys. keys_repo_not_nil: 0 WANT for linear (the Linear writer
-- writes the nil repo_id), INFO for the others. Control: item_keys > 0 in the linear row.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_items')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT item_provider                                                          AS provider,
       count()                                                                AS item_keys,
       uniqExact(work_item_id)                                                AS items,
       sum(n)                                                                 AS version_rows,
       sum(n) - count()                                                       AS versions_over_keys,
       countIf(n > 1)                                                         AS keys_with_versions,
       countIf(synced >= now64(3) - INTERVAL 1 DAY)                           AS keys_written_last_day,
       countIf(n = 1 AND synced >= now64(3) - INTERVAL 1 DAY)                 AS keys_one_version_last_day,
       countIf(repo_id != toUUID('00000000-0000-0000-0000-000000000000'))     AS keys_repo_not_nil
FROM (
    SELECT org_id, repo_id, work_item_id, any(provider) AS item_provider, count() AS n, max(last_synced) AS synced
    FROM work_items
    WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0
    GROUP BY org_id, repo_id, work_item_id
)
WHERE cost_gate = 0
GROUP BY item_provider
ORDER BY provider;

-- R1 one row per affected Linear item (ids 'linear:'): a lost reopen or a missing close. id_cut8 = the first 8
-- characters of the Linear key (the id after 'linear:'), linear_key = the full key. reopens_lost and closes_missing
-- per item (their sums = READBACK F3 reopens_lost, closes_missing); lost_reopen_days and missing_close_days = the day
-- of each (space separated). From the item's stored work_items row (FINAL, the newest version): synced_last_day = row
-- written in the 24 h before the read; last_synced_day, last_synced_minute, updated_minute (UTC); status_raw = the
-- provider's state name at the last write; close_time_present = closed_at is set (the provider's completedAt, else
-- canceledAt). not_reread_in_window = row older than 24 h although its updated_at is in the card's window (>= 06-01):
-- archived, moved to another team, or updated mid-run (no archive mark is stored in ClickHouse). has_row 'no' = no
-- work_items row (its columns are then '' or 'no'). event days = the first and last affected reopen.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT substring(replaceOne(a.work_item_id, 'linear:', ''), 1, 8)            AS id_cut8,
       replaceOne(a.work_item_id, 'linear:', '')                              AS linear_key,
       a.reopens_all                                                          AS reopens_all,
       a.transitions_all                                                      AS transitions_all,
       a.lost                                                                 AS reopens_lost,
       a.missing                                                              AS closes_missing,
       a.lost_days                                                            AS lost_reopen_days,
       a.missing_days                                                         AS missing_close_days,
       if(w.n > 0, 'yes', 'no')                                               AS has_row,
       if(w.n > 0 AND w.synced >= now64(3) - INTERVAL 1 DAY, 'yes', 'no')     AS synced_last_day,
       if(w.n > 0, toString(toDate(w.synced, 'UTC')), '')                     AS last_synced_day,
       if(w.n > 0, formatDateTime(w.synced, '%Y-%m-%d %H:%i', 'UTC'), '')     AS last_synced_minute,
       if(w.n > 0, formatDateTime(w.updated, '%Y-%m-%d %H:%i', 'UTC'), '')    AS updated_minute,
       if(w.n > 0 AND w.synced < now64(3) - INTERVAL 1 DAY
          AND w.updated >= toDateTime64('2026-06-01 00:00:00', 3, 'UTC'), 'yes', 'no') AS not_reread_in_window,
       if(w.n > 0, w.state_raw, '')                                           AS status_raw,
       if(w.n > 0 AND w.close_time = 1, 'yes', 'no')                          AS close_time_present,
       toDate(a.first_at)                                                     AS event_first_day,
       toDate(a.last_at)                                                      AS event_last_day
FROM (
    SELECT org_id, work_item_id, length(rs) AS reopens_all, transitions_all,
           arrayStringConcat(arrayMap(o -> toString(toDate(o, 'UTC')), arrayFilter((o, k) -> k = 0,
                    arrayMap(x -> tupleElement(x, 1), rs), kept)), ' ')                 AS lost_days,
           arrayStringConcat(arrayMap(o -> toString(toDate(o, 'UTC')), arrayFilter((o, f, p) -> f = 0 AND p = 1,
                    arrayMap(x -> tupleElement(x, 1), rs), found, prior)), ' ')         AS missing_days,
           arraySum(arrayMap(k -> toUInt8(k = 0), kept))                                AS lost,
           arraySum(arrayMap((f, p) -> toUInt8(f = 0 AND p = 1), found, prior))         AS missing,
           arrayMin(arrayFilter((o, k, f, p) -> k = 0 OR (f = 0 AND p = 1),
                    arrayMap(x -> tupleElement(x, 1), rs), kept, found, prior))         AS first_at,
           arrayMax(arrayFilter((o, k, f, p) -> k = 0 OR (f = 0 AND p = 1),
                    arrayMap(x -> tupleElement(x, 1), rs), kept, found, prior))         AS last_at
    FROM (
        SELECT r.org_id AS org_id, r.work_item_id AS work_item_id, r.rs AS rs, length(t.ts) AS transitions_all,
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
            WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS r
        LEFT JOIN (
            SELECT org_id, work_item_id,
                   groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced)) AS ts
            FROM work_item_transitions FINAL
            WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id
        WHERE cost_gate = 0
    )
    WHERE cost_gate = 0
) AS a
LEFT JOIN (
    SELECT org_id, work_item_id, count() AS n, max(updated_at) AS updated, max(last_synced) AS synced,
           argMax(status_raw, last_synced) AS state_raw, argMax(toUInt8(isNotNull(closed_at)), last_synced) AS close_time
    FROM work_items FINAL
    WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
    GROUP BY org_id, work_item_id
) AS w ON w.org_id = a.org_id AND w.work_item_id = a.work_item_id
WHERE cost_gate = 0 AND (a.lost > 0 OR a.missing > 0)
ORDER BY linear_key;

-- R2 one row per missing close (the F2 rule) and per transition row stored at its reopen instant (LEFT ARRAY JOIN:
-- rows_at_instant 0 gives one row with tie_ordinal 0 and empty statuses). reopen_kept = the reopen's own transition
-- is stored (F1 rule). prev_reopen_at = the window's lower bound (1970-01-01 = the first reopen of the item).
-- window_transitions = transitions in [prev_reopen_at, reopen_at]. last_before = the last transition strictly before
-- the reopen (by occurred_at, then tie_ordinal). provider_close_any = the item holds a close at ANY instant (the F2
-- close test without the window: to done or canceled, or to_status_raw = the reopen's from_status_raw): 'no' = the
-- provider served no close; 'yes' = a close exists outside [prev_reopen_at, reopen_at] (the window cut it).
-- last_close_before_at = the newest such close strictly before the reopen (1970-01-01 = none).
-- item_close_time_present = the item's stored work_items row (FINAL, newest) has closed_at set. stored_close_any = yes
-- when provider_close_any or item_close_time_present: 'no' = the provider served no close anywhere for the item.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events', 'work_item_transitions', 'work_items')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT substring(replaceOne(m.work_item_id, 'linear:', ''), 1, 8)            AS id_cut8,
       replaceOne(m.work_item_id, 'linear:', '')                              AS linear_key,
       m.reopen_at                                                            AS reopen_at,
       m.prev_reopen_at                                                       AS prev_reopen_at,
       m.reopen_from_raw                                                      AS reopen_from_raw,
       m.reopen_to_raw                                                        AS reopen_to_raw,
       m.reopen_kept                                                          AS reopen_kept,
       if(length(m.close_rows) > 0, 'yes', 'no')                              AS provider_close_any,
       arrayReverseSort(arrayMap(x -> tupleElement(x, 1),
                arrayFilter(x -> tupleElement(x, 1) < m.reopen_at, m.close_rows)))[1] AS last_close_before_at,
       if(m.item_close_time = 1, 'yes', 'no')                                 AS item_close_time_present,
       if(length(m.close_rows) > 0 OR m.item_close_time = 1, 'yes', 'no')     AS stored_close_any,
       m.window_transitions                                                  AS window_transitions,
       tupleElement(m.last_before, 1)                                         AS last_before_at,
       tupleElement(m.last_before, 3)                                         AS last_before_to,
       tupleElement(m.last_before, 5)                                         AS last_before_to_raw,
       length(m.at)                                                           AS rows_at_instant,
       tupleElement(tr, 8)                                                    AS tie_ordinal,
       tupleElement(tr, 2)                                                    AS from_status,
       tupleElement(tr, 3)                                                    AS to_status,
       tupleElement(tr, 4)                                                    AS from_status_raw,
       tupleElement(tr, 5)                                                    AS to_status_raw
FROM (
    SELECT j.work_item_id                                                     AS work_item_id,
           tupleElement(j.rs[i], 1)                                           AS reopen_at,
           tupleElement(j.rs[i - 1], 1)                                       AS prev_reopen_at,
           tupleElement(j.rs[i], 2)                                           AS reopen_from_raw,
           tupleElement(j.rs[i], 3)                                           AS reopen_to_raw,
           j.kept[i]                                                          AS reopen_kept,
           j.item_close_time                                                  AS item_close_time,
           arrayCount(x -> tupleElement(x, 1) >= tupleElement(j.rs[i - 1], 1)
                      AND tupleElement(x, 1) <= tupleElement(j.rs[i], 1), j.ts)  AS window_transitions,
           arrayReverseSort(x -> (tupleElement(x, 1), tupleElement(x, 8)),
                            arrayFilter(x -> tupleElement(x, 1) < tupleElement(j.rs[i], 1), j.ts))[1] AS last_before,
           arraySort(x -> tupleElement(x, 8), arrayFilter(x -> tupleElement(x, 1) = tupleElement(j.rs[i], 1), j.ts)) AS at,
           arrayFilter(x -> has(['done', 'canceled'], tupleElement(x, 3))
                       OR tupleElement(x, 5) = tupleElement(j.rs[i], 2), j.ts)                 AS close_rows
    FROM (
        SELECT r.org_id AS org_id, r.work_item_id AS work_item_id, r.rs AS rs, t.ts AS ts, w.close_time AS item_close_time,
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
            WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS r
        LEFT JOIN (
            SELECT org_id, work_item_id,
                   groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced, tie_ordinal)) AS ts
            FROM work_item_transitions FINAL
            WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id
        LEFT JOIN (
            SELECT org_id, work_item_id, argMax(toUInt8(isNotNull(closed_at)), last_synced) AS close_time
            FROM work_items FINAL
            WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS w ON w.org_id = r.org_id AND w.work_item_id = r.work_item_id
        WHERE cost_gate = 0
    ) AS j
    ARRAY JOIN arrayEnumerate(j.rs) AS i
    WHERE cost_gate = 0 AND j.found[i] = 0 AND j.prior[i] = 1
) AS m
LEFT ARRAY JOIN m.at AS tr
WHERE cost_gate = 0
ORDER BY linear_key, reopen_at, tie_ordinal;
`
)

type formExecRes14190Result struct {
	columns []string
	rows    [][]string
}

func formExecRes14190Exec(ctx context.Context, t *testing.T, conn driver.Conn, statement string) {
	t.Helper()
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// formExecRes14190Statements splits the FORM text as pm runs it: the pieces between ";\n", each named by its first
// line "-- S<n>" or "-- R<n>". A piece with no SQL line (only comments) is not a statement.
func formExecRes14190Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	statementID := regexp.MustCompile(`^[SR][0-9]+$`)
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
			t.Fatalf("a statement has no \"-- S<n>\" or \"-- R<n>\" line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	return ids, statements
}

func formExecRes14190Query(ctx context.Context, conn driver.Conn, statement string) (formExecRes14190Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecRes14190Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecRes14190Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecRes14190Result{}, fmt.Errorf("scan: %w", err)
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
			if stamp, isTime := value.Interface().(time.Time); isTime {
				row[index] = stamp.UTC().Format("2006-01-02 15:04:05.000")
				continue
			}
			row[index] = fmt.Sprint(value.Interface())
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

func formExecRes14190Cell(t *testing.T, result formExecRes14190Result, label string, row int, column string) string {
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

func formExecRes14190Row(t *testing.T, result formExecRes14190Result, label string, row int, want map[string]string) {
	t.Helper()
	for column, value := range want {
		if got := formExecRes14190Cell(t, result, label, row, column); got != value {
			t.Errorf("%s row %d %s = %s, want %s", label, row, column, got, value)
		}
	}
}

// formExecRes14190QueryLog reads the finished query_log rows of one log_comment: their types and the summed read_rows.
func formExecRes14190QueryLog(ctx context.Context, t *testing.T, conn driver.Conn, label, comment string) ([]string, uint64) {
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

// CHAOS-14190 residual fixture. Org 5e1f0c2a-... is the org the FORM reads (the synthetic prefix); org 4e1f0c2a-...
// holds a missing close of the same id and must not count. Phase 1 writes each table in ONE INSERT (one part each,
// merges stopped):
//
//	linear:RESID-101: In Progress 10-01, Done 10-06; reopened Done -> Todo 10-06, its Todo transition lost in the tie.
//	linear:RESID-102: In Progress 09-10; at 09-20 two rows (In Review ordinal 0, the reopen Done -> Todo ordinal 1); no
//	    close before the reopen: a missing close with a tie at the instant.
//	linear:RESID-103: In Progress 09-01; reopened 09-05, only the reopen row stored at the instant (old key): close missing.
//	linear:RESID-104: like RESID-103 (07-01, 07-15) but its work_items row was written 11-01 (not re-read) and
//	    holds state Done with a close time: the stored row has a close the transitions do not.
//	linear:RESID-105: Done 08-01, reopened 08-05 (close found), In Progress 08-10, reopened 08-20: the second close missing.
//	gh:rx/repo#1: a reopen with no close before it (not Linear: not read).
//
// Phase 2 re-reads RESID-101 and RESID-103 under the tie_ordinal key (close 0, reopen 1, newer last_synced) and writes their
// work_items rows again: both heal and leave R1 and R2; the others stay.
func TestFormExecFcc14190Residual(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	const (
		prefix     = "5e1f0c2a"
		org, other = prefix + "-1419-4000-8000-0000000000a1", "4e1f0c2a-1419-4000-8000-0000000000a2"
		zero       = "00000000-0000-0000-0000-000000000000"
		synced     = "2026-11-20 03:00:00.000"
		newKey     = "2026-11-24 01:00:00.000"
		oldItem    = "2026-11-01 00:00:00.000"
		formSlots  = 9
		orgFilters = 8
	)
	sum := sha256.Sum256([]byte(formExecRes14190SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecRes14190SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14190-RESIDUAL-3.sql with %s)", got, formExecRes14190SHA256, formExecRes14190OrgSlot)
	}
	if got := strings.Count(formExecRes14190SQL, formExecRes14190OrgSlot); got != formSlots {
		t.Fatalf("the text holds %s %d time(s), want %d", formExecRes14190OrgSlot, got, formSlots)
	}
	if got := strings.Count(formExecRes14190SQL, "startsWith(org_id, '"+formExecRes14190OrgSlot+"')"); got != orgFilters {
		t.Fatalf("the text holds %d org filters, want %d", got, orgFilters)
	}
	text := strings.ReplaceAll(formExecRes14190SQL, formExecRes14190OrgSlot, prefix)
	if strings.Contains(text, "{{") || strings.Count(text, prefix) != formSlots {
		t.Fatalf("the slot swap left %d prefix(es) or a slot", strings.Count(text, prefix))
	}
	ids, statements := formExecRes14190Statements(t, text)
	if got := strings.Join(ids, ","); got != "S0,S1,S2,R1,R2" {
		t.Fatalf("statements = %s, want S0,S1,S2,R1,R2", got)
	}
	orgFilter := "startsWith(org_id, '" + prefix + "')"

	// The gate of each statement: the tables it reads (its IN list), its cap literals, how many times the IN list and
	// cost_gate = 0 appear, the data tables it reads (each with the org filter), and fixtureReadRows, the query_log
	// read_rows on the phase 1 fixture by the scan model of FORM-14190-RESIDUAL-3.md (E 8, T 17, W 6 rows, one part
	// each; a gate evaluation reads 2 x P + 1, each further cost_gate reference 1 more row, a plain or FINAL read of a
	// one-part table its rows once). S1 reads metadata only: its read_rows is bounded, not modelled.
	gates := []struct {
		id, tables, rowsCap, bytesCap string
		inLists, uses, reads          int
		fixtureReadRows               uint64
		metadata                      bool
	}{
		{"S0", "'work_item_reopen_events'", "> 5000000", "> 4294967296", 2, 1, 1, 8 + 3, false},
		{"S1", "'work_item_reopen_events', 'work_item_transitions', 'work_items'", "> 6000000", "> 4294967296", 3, 1, 0, 64, true},
		{"S2", "'work_items'", "> 6000000", "> 4294967296", 2, 2, 1, 6 + 3 + 1, false},
		{"R1", "'work_item_reopen_events', 'work_item_transitions', 'work_items'", "> 6000000", "> 4294967296", 2, 6, 3, 8 + 17 + 6 + 7 + 5, false},
		{"R2", "'work_item_reopen_events', 'work_item_transitions', 'work_items'", "> 6000000", "> 4294967296", 2, 6, 3, 8 + 17 + 6 + 7 + 5, false},
	}
	for _, gate := range gates {
		statement := statements[gate.id]
		checks := []struct {
			what      string
			got, want int
		}{
			{"WITH (SELECT throwIf(", strings.Count(statement, "WITH (SELECT throwIf("), 1},
			{"'STOP-COST-GATE'", strings.Count(statement, "'STOP-COST-GATE'"), 1},
			{gate.rowsCap, strings.Count(statement, gate.rowsCap), 1},
			{gate.bytesCap, strings.Count(statement, gate.bytesCap), 1},
			{"table IN (" + gate.tables + ")", strings.Count(statement, "table IN ("+gate.tables+")"), gate.inLists},
			{"cost_gate = 0", strings.Count(statement, "cost_gate = 0"), gate.uses},
			{"FROM work_item", strings.Count(statement, "FROM work_item"), gate.reads},
			{orgFilter, strings.Count(statement, orgFilter), gate.reads},
		}
		for _, check := range checks {
			if check.got != check.want {
				t.Errorf("%s holds %q %d time(s), want %d", gate.id, check.what, check.got, check.want)
			}
		}
	}

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
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse version %s", version)

	for _, table := range []string{"work_item_reopen_events", "work_item_transitions", "work_items"} {
		formExecRes14190Exec(ctx, t, conn, "SYSTEM STOP MERGES "+table)
	}
	reopen := func(orgID, id, fromRaw, toRaw, occurred string) string {
		return fmt.Sprintf("('%s', '%s', '%s', 'done', 'todo', '%s', '%s', NULL, '%s')", orgID, id, occurred, fromRaw, toRaw, synced)
	}
	reopens := `INSERT INTO work_item_reopen_events (org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced) VALUES `
	transition := func(orgID, id, provider, from, to, fromRaw, toRaw, occurred, lastSynced string, ordinal int) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '', '%s', '%s', %d)",
			orgID, zero, id, provider, from, to, fromRaw, toRaw, occurred, lastSynced, ordinal)
	}
	transitions := `INSERT INTO work_item_transitions (org_id, repo_id, work_item_id, provider, from_status, to_status, from_status_raw, to_status_raw, actor, occurred_at, last_synced, tie_ordinal) VALUES `
	item := func(orgID, id, statusRaw, closedAt, updated, lastSynced string) string {
		return fmt.Sprintf("('%s', '%s', '%s', 'linear', 'todo', '%s', %s, '%s', '%s', %s)", orgID, zero, id, statusRaw, closedAt, updated, updated, lastSynced)
	}
	items := `INSERT INTO work_items (org_id, repo_id, work_item_id, provider, status, status_raw, closed_at, created_at, updated_at, last_synced) VALUES `
	now, old := "now64(3)", "'"+oldItem+"'"

	formExecRes14190Exec(ctx, t, conn, reopens+strings.Join([]string{
		reopen(org, "linear:RESID-101", "Done", "Todo", "2026-10-06 10:00:00.000"),
		reopen(org, "linear:RESID-102", "Done", "Todo", "2026-09-20 10:00:00.000"),
		reopen(org, "linear:RESID-103", "Done", "Todo", "2026-09-05 10:00:00.000"),
		reopen(org, "linear:RESID-104", "Done", "Todo", "2026-07-15 10:00:00.000"),
		reopen(org, "linear:RESID-105", "Done", "Todo", "2026-08-05 10:00:00.000"),
		reopen(org, "linear:RESID-105", "Done", "Todo", "2026-08-20 10:00:00.000"),
		reopen(org, "gh:rx/repo#1", "closed", "reopened", "2026-10-02 10:00:00.000"),
		reopen(other, "linear:RESID-102", "Done", "Todo", "2026-09-20 10:00:00.000"),
	}, ", "))
	formExecRes14190Exec(ctx, t, conn, transitions+strings.Join([]string{
		transition(org, "linear:RESID-101", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-10-01 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-101", "linear", "in_progress", "done", "In Progress", "Done", "2026-10-06 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-102", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-09-10 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-102", "linear", "in_progress", "in_progress", "In Progress", "In Review", "2026-09-20 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-102", "linear", "done", "todo", "Done", "Todo", "2026-09-20 10:00:00.000", synced, 1),
		transition(org, "linear:RESID-103", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-09-01 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-103", "linear", "done", "todo", "Done", "Todo", "2026-09-05 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-104", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-07-01 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-104", "linear", "done", "todo", "Done", "Todo", "2026-07-15 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-105", "linear", "in_progress", "done", "In Progress", "Done", "2026-08-01 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-105", "linear", "done", "todo", "Done", "Todo", "2026-08-05 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-105", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-08-10 10:00:00.000", synced, 0),
		transition(org, "linear:RESID-105", "linear", "done", "todo", "Done", "Todo", "2026-08-20 10:00:00.000", synced, 0),
		transition(org, "gh:rx/repo#1", "github", "todo", "in_progress", "open", "in_progress", "2026-10-01 10:00:00.000", synced, 0),
		transition(org, "gh:rx/repo#1", "github", "done", "todo", "closed", "reopened", "2026-10-02 10:00:00.000", synced, 0),
		transition(other, "linear:RESID-102", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-09-10 10:00:00.000", synced, 0),
		transition(other, "linear:RESID-102", "linear", "done", "todo", "Done", "Todo", "2026-09-20 10:00:00.000", synced, 0),
	}, ", "))
	formExecRes14190Exec(ctx, t, conn, items+strings.Join([]string{
		item(org, "linear:RESID-101", "Todo", "NULL", "2026-10-06 10:00:00.000", now),
		item(org, "linear:RESID-102", "Todo", "NULL", "2026-09-20 10:00:00.000", now),
		item(org, "linear:RESID-103", "Todo", "NULL", "2026-09-05 10:00:00.000", now),
		item(org, "linear:RESID-104", "Done", "'2026-07-20 10:00:00.000'", "2026-07-15 10:00:00.000", old),
		item(org, "linear:RESID-105", "Todo", "NULL", "2026-08-20 10:00:00.000", now),
		item(other, "linear:RESID-102", "Todo", "NULL", "2026-09-20 10:00:00.000", now),
	}, ", "))

	// run runs every statement of the FORM once, in order (as pm runs it), each tagged with a log_comment.
	run := func(phase string) (map[string]formExecRes14190Result, map[string]string) {
		results, comments := map[string]formExecRes14190Result{}, map[string]string{}
		for _, id := range ids {
			comment := fmt.Sprintf("form14190res-%s-%s-%d", phase, id, time.Now().UnixNano())
			comments[id] = comment
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			result, err := formExecRes14190Query(qctx, conn, statements[id])
			if err != nil {
				t.Fatalf("FORM-14190-RESIDUAL-3 %s %s: %v", phase, id, err)
			}
			t.Logf("FORM-14190-RESIDUAL-3 %s %s columns %v", phase, id, result.columns)
			for index, row := range result.rows {
				t.Logf("FORM-14190-RESIDUAL-3 %s %s row %d %v", phase, id, index, row)
			}
			results[id] = result
		}
		return results, comments
	}
	rowCount := func(results map[string]formExecRes14190Result, label, id string, want int) {
		t.Helper()
		if got := len(results[id].rows); got != want {
			t.Fatalf("%s %s: %d row(s), want %d", label, id, got, want)
		}
	}
	const epoch = "1970-01-01 00:00:00.000"
	// r1 checks one R1 row. The item written 11-01 (synced "no") is the not re-read one: its stored state is Done with
	// a close time; every other item's stored state is Todo with none.
	r1 := func(results map[string]formExecRes14190Result, label string, row int, id, reopensAll, transitionsAll, lost, missing, lostDays, missingDays, syncedLastDay, updated, eventDay string) {
		t.Helper()
		key := strings.TrimPrefix(id, "linear:")
		want := map[string]string{
			"id_cut8": key[:8], "linear_key": key, "reopens_all": reopensAll, "transitions_all": transitionsAll,
			"reopens_lost": lost, "closes_missing": missing, "lost_reopen_days": lostDays, "missing_close_days": missingDays,
			"has_row": "yes", "synced_last_day": syncedLastDay, "updated_minute": updated,
			"not_reread_in_window": "no", "status_raw": "Todo", "close_time_present": "no",
			"event_first_day": eventDay, "event_last_day": eventDay,
		}
		if syncedLastDay == "no" {
			want["last_synced_minute"], want["last_synced_day"] = "2026-11-01 00:00", "2026-11-01"
			want["not_reread_in_window"], want["status_raw"], want["close_time_present"] = "yes", "Done", "yes"
		}
		formExecRes14190Row(t, results["R1"], label+" R1", row, want)
		if minute := formExecRes14190Cell(t, results["R1"], label+" R1", row, "last_synced_minute"); !regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$`).MatchString(minute) {
			t.Errorf("%s R1 row %d last_synced_minute = %q, want YYYY-MM-DD HH:MM", label, row, minute)
		}
		if day := formExecRes14190Cell(t, results["R1"], label+" R1", row, "last_synced_day"); !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(day) {
			t.Errorf("%s R1 row %d last_synced_day = %q, want YYYY-MM-DD", label, row, day)
		}
	}
	// r2 checks one R2 row. closeAny and lastCloseBefore: the close of the item at any instant (the F2 close test
	// without the window) and the newest one before the reopen.
	r2 := func(results map[string]formExecRes14190Result, label string, row int, id, reopenAt, prevAt, closeAny, lastCloseBefore, window, lastBeforeAt, lastBeforeTo, lastBeforeToRaw, atInstant, ordinal, from, to, fromRaw, toRaw string) {
		t.Helper()
		key := strings.TrimPrefix(id, "linear:")
		itemClose, storedClose := "no", closeAny
		if key == "RESID-104" {
			itemClose, storedClose = "yes", "yes"
		}
		formExecRes14190Row(t, results["R2"], label+" R2", row, map[string]string{
			"id_cut8": key[:8], "linear_key": key, "provider_close_any": closeAny, "last_close_before_at": lastCloseBefore,
			"item_close_time_present": itemClose, "stored_close_any": storedClose,
			"reopen_at": reopenAt, "prev_reopen_at": prevAt, "reopen_from_raw": "Done",
			"reopen_to_raw": "Todo", "reopen_kept": "1", "window_transitions": window, "last_before_at": lastBeforeAt,
			"last_before_to": lastBeforeTo, "last_before_to_raw": lastBeforeToRaw, "rows_at_instant": atInstant,
			"tie_ordinal": ordinal, "from_status": from, "to_status": to, "from_status_raw": fromRaw, "to_status_raw": toRaw,
		})
	}
	// The residual rows that stay in both phases: RESID-102 (2 rows at its instant), RESID-104, RESID-105.
	r2Stay := func(results map[string]formExecRes14190Result, label string, first int) {
		t.Helper()
		r2(results, label, first, "linear:RESID-102", "2026-09-20 10:00:00.000", epoch, "no", epoch, "3", "2026-09-10 10:00:00.000", "in_progress", "In Progress",
			"2", "0", "in_progress", "in_progress", "In Progress", "In Review")
		r2(results, label, first+1, "linear:RESID-102", "2026-09-20 10:00:00.000", epoch, "no", epoch, "3", "2026-09-10 10:00:00.000", "in_progress", "In Progress",
			"2", "1", "done", "todo", "Done", "Todo")
	}
	r2Tail := func(results map[string]formExecRes14190Result, label string, first int) {
		t.Helper()
		r2(results, label, first, "linear:RESID-104", "2026-07-15 10:00:00.000", epoch, "no", epoch, "2", "2026-07-01 10:00:00.000", "in_progress", "In Progress",
			"1", "0", "done", "todo", "Done", "Todo")
		r2(results, label, first+1, "linear:RESID-105", "2026-08-20 10:00:00.000", "2026-08-05 10:00:00.000", "yes", "2026-08-01 10:00:00.000", "3", "2026-08-10 10:00:00.000", "in_progress", "In Progress",
			"1", "0", "done", "todo", "Done", "Todo")
	}

	// Phase 1: the loss state.
	results, passComments := run("phase1")
	rowCount(results, "phase1", "S0", 1)
	formExecRes14190Row(t, results["S0"], "phase1 S0", 0, map[string]string{"orgs": "1", "org_rows": "7"})
	rowCount(results, "phase1", "S1", 3)
	formExecRes14190Row(t, results["S1"], "phase1 S1", 0, map[string]string{"table_name": "work_item_reopen_events", "table_rows": "8", "active_parts": "1"})
	formExecRes14190Row(t, results["S1"], "phase1 S1", 1, map[string]string{"table_name": "work_item_transitions", "table_rows": "17", "active_parts": "1",
		"sorting_key": "org_id, repo_id, work_item_id, occurred_at, tie_ordinal"})
	formExecRes14190Row(t, results["S1"], "phase1 S1", 2, map[string]string{"table_name": "work_items", "table_rows": "6", "active_parts": "1"})
	rowCount(results, "phase1", "S2", 1)
	formExecRes14190Row(t, results["S2"], "phase1 S2", 0, map[string]string{
		"provider": "linear", "item_keys": "5", "items": "5", "version_rows": "5", "versions_over_keys": "0", "keys_with_versions": "0",
		"keys_written_last_day": "4", "keys_one_version_last_day": "4", "keys_repo_not_nil": "0",
	})
	rowCount(results, "phase1", "R1", 5)
	r1(results, "phase1", 0, "linear:RESID-101", "1", "2", "1", "0", "2026-10-06", "", "yes", "2026-10-06 10:00", "2026-10-06 00:00:00.000")
	r1(results, "phase1", 1, "linear:RESID-102", "1", "3", "0", "1", "", "2026-09-20", "yes", "2026-09-20 10:00", "2026-09-20 00:00:00.000")
	r1(results, "phase1", 2, "linear:RESID-103", "1", "2", "0", "1", "", "2026-09-05", "yes", "2026-09-05 10:00", "2026-09-05 00:00:00.000")
	r1(results, "phase1", 3, "linear:RESID-104", "1", "2", "0", "1", "", "2026-07-15", "no", "2026-07-15 10:00", "2026-07-15 00:00:00.000")
	r1(results, "phase1", 4, "linear:RESID-105", "2", "4", "0", "1", "", "2026-08-20", "yes", "2026-08-20 10:00", "2026-08-20 00:00:00.000")
	rowCount(results, "phase1", "R2", 5)
	r2Stay(results, "phase1", 0)
	r2(results, "phase1", 2, "linear:RESID-103", "2026-09-05 10:00:00.000", epoch, "no", epoch, "2", "2026-09-01 10:00:00.000", "in_progress", "In Progress",
		"1", "0", "done", "todo", "Done", "Todo")
	r2Tail(results, "phase1", 3)

	// Mutation cells: each mutant (a test copy; the checked text stays) must change its value. An org mutant drops every
	// org filter of the statement (the other org's RESID-102 then counts); a rule mutant changes one rule of it.
	type mutant struct {
		name, id, from, to string
		count              int
		row                int
		column             string // "" = the row count
		unmutated          string
	}
	runMutants := func(label string, mutants []mutant) {
		t.Helper()
		for _, m := range mutants {
			statement := statements[m.id]
			if got := strings.Count(statement, m.from); got != m.count {
				t.Fatalf("MUTANT %s: %q %d time(s) in %s, want %d", m.name, m.from, got, m.id, m.count)
			}
			result, err := formExecRes14190Query(ctx, conn, strings.ReplaceAll(statement, m.from, m.to))
			if err != nil {
				t.Logf("MUTANT %s %s: %v (an error kills it)", label, m.name, err)
				continue
			}
			got := fmt.Sprint(len(result.rows))
			if m.column != "" {
				got = formExecRes14190Cell(t, result, "MUTANT "+m.name, m.row, m.column)
			}
			t.Logf("MUTANT %s %s: %s %s = %s (unmutated %s)", label, m.name, m.id, m.column, got, m.unmutated)
			if got == m.unmutated {
				t.Errorf("MUTANT %s %s SURVIVED: %s %s = %s", label, m.name, m.id, m.column, got)
			}
		}
	}
	dropOrg := orgFilter + " AND "
	runMutants("phase1", []mutant{
		{"S0-org", "S0", dropOrg, "", 1, 0, "orgs", "1"},
		{"S0-control", "S0", "count() AS org_rows", "countIf(work_item_id = '') AS org_rows", 1, 0, "org_rows", "7"},
		{"S1-table-name", "S1", "'work_item_transitions', 'work_items'])", "'work_item_transitions', 'work_itemz'])", 1, 2, "table_rows", "6"},
		{"S2-org", "S2", dropOrg, "", 1, 0, "item_keys", "5"},
		{"R1-org", "R1", dropOrg, "", 3, 0, "", "5"},
		{"R1-window-lower-bound", "R1", "tupleElement(x, 1) >= tupleElement(rs[i - 1], 1)", "tupleElement(x, 1) >= tupleElement(rs[0], 1)", 1, 0, "", "5"},
		{"R1-reopen-tie-blind", "R1", "AND tupleElement(x, 5) = tupleElement(rs[i], 3), t.ts)), arrayEnumerate(rs)) AS kept", ", t.ts)), arrayEnumerate(rs)) AS kept", 1, 0, "", "5"},
		{"R1-synced-cut", "R1", "w.synced >= now64(3) - INTERVAL 1 DAY", "w.synced >= now64(3) - INTERVAL 3650 DAY", 1, 3, "synced_last_day", "no"},
		{"R2-org", "R2", dropOrg, "", 3, 0, "", "5"},
		{"R2-at-instant", "R2", "arrayFilter(x -> tupleElement(x, 1) = tupleElement(j.rs[i], 1), j.ts)", "arrayFilter(x -> tupleElement(x, 1) <= tupleElement(j.rs[i], 1), j.ts)", 1, 0, "", "5"},
		{"R2-missing-only", "R2", "j.found[i] = 0 AND j.prior[i] = 1", "j.prior[i] = 1", 1, 0, "", "5"},
		// The card's --since is read: a cut two months later puts the not re-read item outside the window.
		{"R1-since-literal", "R1", "w.updated >= toDateTime64('2026-06-01 00:00:00', 3, 'UTC')", "w.updated >= toDateTime64('2026-08-01 00:00:00', 3, 'UTC')", 1, 3, "not_reread_in_window", "yes"},
		// provider_close_any reads every instant, not the F2 window: windowed, RESID-105's 08-01 close drops out.
		{"R2-close-any-windowed", "R2", "if(length(m.close_rows) > 0, 'yes', 'no')", "if(length(arrayFilter(x -> tupleElement(x, 1) >= m.prev_reopen_at, m.close_rows)) > 0, 'yes', 'no')", 1, 4, "provider_close_any", "yes"},
		// One mutant per added column where it can be wrong: each reads another column or flips its test.
		{"R1-cut8-length", "R1", "substring(replaceOne(a.work_item_id, 'linear:', ''), 1, 8)", "substring(replaceOne(a.work_item_id, 'linear:', ''), 1, 9)", 1, 0, "id_cut8", "RESID-10"},
		{"R1-last-synced-day-from-updated", "R1", "toString(toDate(w.synced, 'UTC'))", "toString(toDate(w.updated, 'UTC'))", 1, 3, "last_synced_day", "2026-11-01"},
		{"R1-status-normalized", "R1", "argMax(status_raw, last_synced) AS state_raw", "argMax(status, last_synced) AS state_raw", 1, 3, "status_raw", "Done"},
		{"R1-close-time-from-completed", "R1", "isNotNull(closed_at)", "isNotNull(completed_at)", 1, 3, "close_time_present", "yes"},
		{"R1-lost-days-flipped", "R1", "arrayFilter((o, k) -> k = 0,", "arrayFilter((o, k) -> k = 1,", 1, 0, "lost_reopen_days", "2026-10-06"},
		{"R1-missing-days-without-prior", "R1", "arrayFilter((o, f, p) -> f = 0 AND p = 1,", "arrayFilter((o, f, p) -> f = 1,", 1, 1, "missing_close_days", "2026-09-20"},
		{"R2-cut8-length", "R2", "substring(replaceOne(m.work_item_id, 'linear:', ''), 1, 8)", "substring(replaceOne(m.work_item_id, 'linear:', ''), 1, 9)", 1, 0, "id_cut8", "RESID-10"},
		{"R2-item-close-from-completed", "R2", "isNotNull(closed_at)", "isNotNull(completed_at)", 1, 3, "item_close_time_present", "yes"},
		{"R2-stored-close-without-item", "R2", "length(m.close_rows) > 0 OR m.item_close_time = 1", "length(m.close_rows) > 0", 1, 3, "stored_close_any", "yes"},
		{"R2-last-close-after", "R2", "arrayFilter(x -> tupleElement(x, 1) < m.reopen_at, m.close_rows)", "arrayFilter(x -> tupleElement(x, 1) > m.reopen_at, m.close_rows)", 1, 4, "last_close_before_at", "2026-08-01 10:00:00.000"},
	})

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
	if sizes["work_item_reopen_events"] != 8 || sizes["work_item_transitions"] != 17 || sizes["work_items"] != 6 ||
		parts["work_item_reopen_events"] != 1 || parts["work_item_transitions"] != 1 || parts["work_items"] != 1 {
		t.Fatalf("fixture sizes %v parts %v, want E 8, T 17, W 6 rows in one part each", sizes, parts)
	}
	gateParts := func(tables string) uint64 {
		var p uint64
		for _, table := range strings.Split(tables, ", ") {
			p += parts[strings.Trim(table, "'")]
		}
		return p
	}

	// PASS-path cells (FORM-TEMPLATE-3): each statement completed at its real caps in phase 1; its query_log
	// QueryFinish read_rows must equal the work math on this fixture (S1: at most its bound).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, gate := range gates {
		types, read := formExecRes14190QueryLog(ctx, t, conn, "PASS "+gate.id, passComments[gate.id])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", gate.id, types)
		}
		if gate.metadata && read > gate.fixtureReadRows {
			t.Errorf("PASS %s: query_log read_rows %d over its bound %d", gate.id, read, gate.fixtureReadRows)
		}
		if !gate.metadata && read != gate.fixtureReadRows {
			t.Errorf("PASS %s: query_log read_rows %d, want %d (work math on this fixture)", gate.id, read, gate.fixtureReadRows)
		}
	}

	// The gates fire: each cap literal of each statement lowered to 0 in a test copy only. WANT code 395
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
			comment := fmt.Sprintf("form14190res-gate-%s-%s-%d", gate.id, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			_, err := formExecRes14190Query(qctx, conn, lowered)
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
		types, read := formExecRes14190QueryLog(ctx, t, conn, "GATE "+f.id+" "+f.cap, f.comment)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.id, f.cap, types)
		}
		if read > 2*f.p+1 {
			t.Errorf("GATE %s %s: query_log read_rows %d over 2 x P + 1 = %d", f.id, f.cap, read, 2*f.p+1)
		}
	}
	if len(fires) != 2*len(gates) {
		t.Errorf("gates fired %d times, want %d (5 statements x 2 caps)", len(fires), 2*len(gates))
	}

	// Phase 2: RESID-101 and RESID-103 are re-read under the tie_ordinal key (every transition of the instant under its own
	// ordinal, the close 0 and the reopen 1, newer last_synced) and their work_items rows are written again. Both heal;
	// RESID-102, RESID-104 and RESID-105 stay. S2 now counts two versions not merged away.
	formExecRes14190Exec(ctx, t, conn, transitions+strings.Join([]string{
		transition(org, "linear:RESID-101", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-10-01 10:00:00.000", newKey, 0),
		transition(org, "linear:RESID-101", "linear", "in_progress", "done", "In Progress", "Done", "2026-10-06 10:00:00.000", newKey, 0),
		transition(org, "linear:RESID-101", "linear", "done", "todo", "Done", "Todo", "2026-10-06 10:00:00.000", newKey, 1),
		transition(org, "linear:RESID-103", "linear", "todo", "in_progress", "Todo", "In Progress", "2026-09-01 10:00:00.000", newKey, 0),
		transition(org, "linear:RESID-103", "linear", "in_progress", "done", "In Progress", "Done", "2026-09-05 10:00:00.000", newKey, 0),
		transition(org, "linear:RESID-103", "linear", "done", "todo", "Done", "Todo", "2026-09-05 10:00:00.000", newKey, 1),
	}, ", "))
	formExecRes14190Exec(ctx, t, conn, items+strings.Join([]string{
		item(org, "linear:RESID-101", "Todo", "NULL", "2026-10-06 10:00:00.000", now),
		item(org, "linear:RESID-103", "Todo", "NULL", "2026-09-05 10:00:00.000", now),
	}, ", "))
	results, _ = run("phase2")
	formExecRes14190Row(t, results["S0"], "phase2 S0", 0, map[string]string{"orgs": "1", "org_rows": "7"})
	rowCount(results, "phase2", "S2", 1)
	formExecRes14190Row(t, results["S2"], "phase2 S2", 0, map[string]string{
		"provider": "linear", "item_keys": "5", "items": "5", "version_rows": "7", "versions_over_keys": "2", "keys_with_versions": "2",
		"keys_written_last_day": "4", "keys_one_version_last_day": "2", "keys_repo_not_nil": "0",
	})
	rowCount(results, "phase2", "R1", 3)
	r1(results, "phase2", 0, "linear:RESID-102", "1", "3", "0", "1", "", "2026-09-20", "yes", "2026-09-20 10:00", "2026-09-20 00:00:00.000")
	r1(results, "phase2", 1, "linear:RESID-104", "1", "2", "0", "1", "", "2026-07-15", "no", "2026-07-15 10:00", "2026-07-15 00:00:00.000")
	r1(results, "phase2", 2, "linear:RESID-105", "2", "4", "0", "1", "", "2026-08-20", "yes", "2026-08-20 10:00", "2026-08-20 00:00:00.000")
	rowCount(results, "phase2", "R2", 4)
	r2Stay(results, "phase2", 0)
	r2Tail(results, "phase2", 2)
	runMutants("phase2", []mutant{
		// A key is the sorting key: grouping by a version column too makes every stored version its own key.
		{"S2-key-with-version", "S2", "GROUP BY org_id, repo_id, work_item_id\n", "GROUP BY org_id, repo_id, work_item_id, last_synced\n", 1, 0, "versions_over_keys", "2"},
	})
}
