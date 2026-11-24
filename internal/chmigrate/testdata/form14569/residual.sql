-- FORM-14569-RESIDUAL-1 (lane-14190r, 2026-11-23, ops pin a7febdf50002). ClickHouse prod, SELECT only, CHAOS-14190.
-- Copy of FORM-14190-RESIDUAL-4 (sha256 f45a7edac452bfa0cf0a4b9b901c8273f60fb46d13a0a3f9ec6c3ed2f975be68) with ONE change: the close window of the read-back rule ends 1 s after the reopen, [previous reopen, reopen + 1 s], both ends included (a Linear close can be stamped a few ms after the reopen it follows). Comment text of the window and one added control statement R3; nothing else.
-- Replaces FORM-14190-RESIDUAL-3 (71f0887a2d3c), -2 (5610a01de90d), -1 (1181a6d4fc41); all frozen, not to be run.
-- -4 = -3 + the window's lower bound no longer reads rs[0]: for the first reopen (i = 1) it is 1970-01-01 by an
-- explicit if, and the index is greatest(i - 1, 1), so no arrayElement call gets index 0 (same values as -3).
-- Residual REPORT after READBACK POST = MISS (FORM-14190-READBACK-1 dd6cb67abd7a, POST out 3091612d0fe1): per affected
-- Linear item (R1) and per missing close (R2), the F3 rule of FORM-14190-READBACK-1 grouped per item, plus the work_items
-- version count behind its S0 growth (S2). WANT text, work math and the output rule: FORM-14190-RESIDUAL-4.md.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a). Run each statement once, in order. A SQL error
-- (STOP-COST-GATE included) = STOP and report.
-- NAMED EXCEPTION (acp): R1 and R2 print the 8-char cut AND the full Linear key of each item, and workflow state
-- names. The out file is 0600 in pm-guard/readback-1 only: never in chat, Linear, a PR or a log.
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix f0f0f0f0 by startsWith on every FROM with org_id.
-- Each statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- and system.one only); every WHERE of a query level that reads a table, and the outer WHERE, tests the gate.
-- No data CTE: each table is named once per statement. Rows cap on the sum of the active rows of the statement's
-- tables: 5000000 (one table) or 6000000 (two or three tables). Bytes cap 4294967296 on the same sum.
-- The rule (FORM-14190-READBACK-1 F3, its text kept for found, prior and kept). A reopen is lost when its
-- work_item_reopen_events row has no work_item_transitions row of the same org_id, work_item_id and occurred_at with
-- the same to_status_raw. A close is missing when no transition of the item in [the previous reopen, the reopen + 1 second] (both
-- ends included) goes to done or canceled or has to_status_raw = the reopen's from_status_raw, and the item has a
-- transition before the reopen. All three tables are read FINAL in R1 and R2 (ReplacingMergeTree(last_synced)).

-- S0 ORG-1 and control. WANT orgs = 1, org_rows > 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events')) > 5000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('work_item_reopen_events')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM work_item_reopen_events
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

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
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
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
               arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) >= if(i = 1, toDateTime64(0, 3), tupleElement(rs[greatest(i - 1, 1)], 1))
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
    SELECT org_id, work_item_id, count() AS n, max(updated_at) AS updated, max(last_synced) AS synced,
           argMax(status_raw, last_synced) AS state_raw, argMax(toUInt8(isNotNull(closed_at)), last_synced) AS close_time
    FROM work_items FINAL
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
    GROUP BY org_id, work_item_id
) AS w ON w.org_id = a.org_id AND w.work_item_id = a.work_item_id
WHERE cost_gate = 0 AND (a.lost > 0 OR a.missing > 0)
ORDER BY linear_key;

-- R2 one row per missing close (the F2 rule) and per transition row stored at its reopen instant (LEFT ARRAY JOIN:
-- rows_at_instant 0 gives one row with tie_ordinal 0 and empty statuses). reopen_kept = the reopen's own transition
-- is stored (F1 rule). prev_reopen_at = the window's lower bound (1970-01-01 = the first reopen of the item).
-- window_transitions = transitions in [prev_reopen_at, reopen_at + 1 second]. last_before = the last transition strictly before
-- the reopen (by occurred_at, then tie_ordinal). provider_close_any = the item holds a close at ANY instant (the F2
-- close test without the window: to done or canceled, or to_status_raw = the reopen's from_status_raw): 'no' = the
-- provider served no close; 'yes' = a close exists outside [prev_reopen_at, reopen_at + 1 second] (the window cut it).
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
           if(i = 1, toDateTime64(0, 3), tupleElement(j.rs[greatest(i - 1, 1)], 1)) AS prev_reopen_at,
           tupleElement(j.rs[i], 2)                                           AS reopen_from_raw,
           tupleElement(j.rs[i], 3)                                           AS reopen_to_raw,
           j.kept[i]                                                          AS reopen_kept,
           j.item_close_time                                                  AS item_close_time,
           arrayCount(x -> tupleElement(x, 1) >= if(i = 1, toDateTime64(0, 3), tupleElement(j.rs[greatest(i - 1, 1)], 1))
                      AND tupleElement(x, 1) <= tupleElement(j.rs[i], 1) + toIntervalMillisecond(1000), j.ts)  AS window_transitions,
           arrayReverseSort(x -> (tupleElement(x, 1), tupleElement(x, 8)),
                            arrayFilter(x -> tupleElement(x, 1) < tupleElement(j.rs[i], 1), j.ts))[1] AS last_before,
           arraySort(x -> tupleElement(x, 8), arrayFilter(x -> tupleElement(x, 1) = tupleElement(j.rs[i], 1), j.ts)) AS at,
           arrayFilter(x -> has(['done', 'canceled'], tupleElement(x, 3))
                       OR tupleElement(x, 5) = tupleElement(j.rs[i], 2), j.ts)                 AS close_rows
    FROM (
        SELECT r.org_id AS org_id, r.work_item_id AS work_item_id, r.rs AS rs, t.ts AS ts, w.close_time AS item_close_time,
               arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) >= if(i = 1, toDateTime64(0, 3), tupleElement(rs[greatest(i - 1, 1)], 1))
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
                   groupArray((occurred_at, from_status, to_status, from_status_raw, to_status_raw, provider, last_synced, tie_ordinal)) AS ts
            FROM work_item_transitions FINAL
            WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
            GROUP BY org_id, work_item_id
        ) AS t ON t.org_id = r.org_id AND t.work_item_id = r.work_item_id
        LEFT JOIN (
            SELECT org_id, work_item_id, argMax(toUInt8(isNotNull(closed_at)), last_synced) AS close_time
            FROM work_items FINAL
            WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND startsWith(work_item_id, 'linear:')
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

-- R3 CONTROL of the 1 s close window, system.one only (no data table, no cost gate). One reopen at 2026-10-15
-- 10:00:00.000 (its raw to-state 'Other' never equals the close's raw state) and one close row to 'done' at the reopen
-- plus close_offset_ms, tested by the found expression of this file. WANT 3 rows: close_offset_ms 0 -> closes_found 1,
-- 1000 -> 1 (the window end is included), 1001 -> 0 (one millisecond past the window).
SELECT offset_ms AS close_offset_ms,
       arraySum(arrayMap(i -> toUInt8(arrayExists(x -> tupleElement(x, 1) >= if(i = 1, toDateTime64(0, 3), tupleElement(rs[greatest(i - 1, 1)], 1))
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
