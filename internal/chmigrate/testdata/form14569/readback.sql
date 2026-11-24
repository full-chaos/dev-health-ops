-- FORM-14569-READBACK-1 (lane-14190c, 2026-11-23, ops pin a7febdf50002). ClickHouse prod, SELECT only, CHAOS-14190.
-- Copy of FORM-14190-READBACK-1 (sha256 dd6cb67abd7ad9954662bff13d720bc625fdabea5420698a630f748712906ebd) with ONE change: the close window of the read-back rule ends 1 s after the reopen, [previous reopen, reopen + 1 s], both ends included (a Linear close can be stamped a few ms after the reopen it follows). Comment text of the window and one added control statement F4; nothing else.
-- Read-back of the Linear work-items backfill of CARD-14190-BACKFILL-1. Run the whole file TWICE: PRE (after the acp
-- read, before the card's command) and POST (after every unit and daily run of the backfill is terminal).
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a). WANT text (PRE and POST) and work math:
-- FORM-14190-READBACK-1.md.
-- The three statements from the line "-- F0 ORG-1" to the end of the third are the text of FORM-14190-GAP-2.sql
-- (68a7ae72297b, acp FORM OK 03def4642bd0) byte for byte, so the lost reopens and the missing closes are keyed and
-- counted exactly as the gap read found them. NEW here: the metadata statement holds a third table (work_items), a
-- second metadata statement reads the transitions sorting key, and the last statement names, per affected Linear
-- item, its stored work_items row (coverage of the card's window and a re-read mark).
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix f0f0f0f0 by startsWith on every FROM with org_id.
-- Each data statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- and system.one only), and every WHERE of a query level that reads a table, and the outer WHERE, tests the gate.
-- No data CTE: each table is named once per statement. Rows cap on the sum of the active rows of the statement's
-- tables: 5000000 (reopen events only) or 6000000 (two or three tables). Bytes cap 4294967296 on the same sum.
-- Run each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP and report.
-- Gap definition (as in the gap read). A reopen is lost when its work_item_reopen_events row has no
-- work_item_transitions row of the same org_id, work_item_id and occurred_at with the same to_status_raw. A close is
-- missing when no transition of the item in [the previous reopen, the reopen + 1 second] (both ends included) goes to done or
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
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

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
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
      AND (startsWith(work_item_id, 'gh:') OR startsWith(work_item_id, 'ghpr:') OR startsWith(work_item_id, 'linear:'))
) AS r
LEFT JOIN (
    SELECT org_id, work_item_id, occurred_at, groupUniqArray(to_status_raw) AS raws
    FROM work_item_transitions FINAL
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
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
                    AND tupleElement(x, 1) <= tupleElement(rs[i], 1) + toIntervalMillisecond(1000)
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
        WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
          AND (startsWith(work_item_id, 'gh:') OR startsWith(work_item_id, 'ghpr:') OR startsWith(work_item_id, 'linear:')
               OR startsWith(work_item_id, 'jira:'))
        GROUP BY org_id, work_item_id
    ) AS r
    LEFT JOIN (
        SELECT org_id, work_item_id,
               groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced)) AS ts
        FROM work_item_transitions FINAL
        WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
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
                        AND tupleElement(x, 1) <= tupleElement(rs[i], 1) + toIntervalMillisecond(1000)
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
            WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS r
        LEFT JOIN (
            SELECT org_id, work_item_id,
                   groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced)) AS ts
            FROM work_item_transitions FINAL
            WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id
        WHERE cost_gate = 0
    )
    WHERE cost_gate = 0
) AS a
LEFT JOIN (
    SELECT org_id, work_item_id, count() AS n, max(updated_at) AS updated, max(last_synced) AS synced
    FROM work_items FINAL
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
    GROUP BY org_id, work_item_id
) AS w ON w.org_id = a.org_id AND w.work_item_id = a.work_item_id
WHERE cost_gate = 0 AND (a.lost > 0 OR a.missing > 0);

-- F4 CONTROL of the 1 s close window, system.one only (no data table, no cost gate). One reopen at 2026-10-15
-- 10:00:00.000 (its raw to-state 'Other' never equals the close's raw state) and one close row to 'done' at the reopen
-- plus close_offset_ms, tested by the found expression of this file. WANT 3 rows: close_offset_ms 0 -> closes_found 1,
-- 1000 -> 1 (the window end is included), 1001 -> 0 (one millisecond past the window).
SELECT offset_ms AS close_offset_ms,
       arraySum(arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) >= tupleElement(rs[i - 1], 1)
                AND tupleElement(x, 1) <= tupleElement(rs[i], 1) + toIntervalMillisecond(1000)
                AND (has(['done', 'canceled'], tupleElement(x, 3)) OR tupleElement(x, 5) = tupleElement(rs[i], 2)), ts)),
                arrayEnumerate(rs))) AS closes_found
FROM (
    SELECT offset_ms,
           [(toDateTime64('2026-10-15 10:00:00.000', 3, 'UTC'), 'Other', 'Todo', 'done', 'todo')] AS rs,
           [(toDateTime64('2026-10-15 10:00:00.000', 3, 'UTC') + toIntervalMillisecond(offset_ms), 'in_progress', 'done', 'In Progress', 'Closed')] AS ts
    FROM (SELECT arrayJoin([0, 1000, 1001]) AS offset_ms FROM system.one)
)
ORDER BY close_offset_ms;
