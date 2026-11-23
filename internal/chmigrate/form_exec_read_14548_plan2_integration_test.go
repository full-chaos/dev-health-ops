//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of a prod read form (READ-14548-PLAN-2): not for merge. p2x14548Form is the form file byte for byte.
const p2x14548FormSHA256 = "dffad83828fdf6de9f441de9821d7ae6c3d7121942ae84631e06116ed53ad841"

const p2x14548Form = `-- NOT EXECUTED. READ-14548-PLAN-2 (system tables and part index only; numbers and static names only). acr-mcp, 2026-11-23. acp FORM read first; then pm runs it once.
-- Exec leg: a throwaway CI leg runs each statement (FORM-TEMPLATE-3); its run id and job id are in READ-14548-PLAN-2.md only.
--   Until that leg is green and acp reads it, pm does not run this file.
-- Why (CHAOS-14548): READ-14548-PLAN-1 (out 2c01da68f0e7) showed no server, revision, settings or table change between the projector runs
--   at 08:56Z and 09:02Z, but read_rows 163,241 -> 209,805 and memory 113.6 -> 141.1 MB. Its processors read STOPPED at a day-scoped gate.
--   Lead hypothesis: a new work_items part that overlaps the key range of the big part stops the FINAL fast path until a merge.
-- S0: size proof of the two log windows (08:50-09:10Z) from the part index (mergeTreeIndex = one row per granule, no data column).
-- S1: the read counters (ProfileEvents numbers) of the 08:56 run and the 09:02 run, per run.
-- S2: the ReadFromMergeTree processors of the same two runs: table, step, processor, input and output rows. The query_id join stays inside.
-- S3a: the parts of work_items and project_membership_transitions at the start of each run and now: count, rows, levels.
-- S3b: now: how many other parts intersect the key range of the newest part (org range only), and their granule rows.
-- Output: no query text, no query_id, no key value, no org id, no String setting value. Names are static (event names, table names,
--   processor names, the 3 run sides).
-- Org scope: S0-S3a read system tables only (no org rows). S3b reads the part index of 2 data tables; each granule range is cut to the
--   org range ['c6a38355', 'c6a38356') (the prefix filter in range form, because a mark holds the FIRST key of its granule) and only counts leave.
-- Gates: a log-table read is gated on the rows of the granules that can hold the window (part index, upper bound of a primary-key read);
--   the part index read is gated first on the marks of the table (system.parts). part_log (S3a) is gated on its day parts. S3b is gated on
--   the marks of each data table. Every gate throws code 395 STOP-COST-GATE before a data row is read. WORK MATH: READ-14548-PLAN-2.md.
-- Named limits: (a) the log tables are per node: S1 runs_before = 0 or runs_after = 0 = STOP, do not read S2/S3a. (b) S2 with fewer than
--   2 run_side values = the runs did not log processors. (c) S3a: the 'now' parts differ from active_now = part_log on this node misses
--   events: then S3a before/after are not exact. (d) code 47 or 36 in S0/S1/S2 = the log sorting key or the part index differs from
--   26.x: STOP. (e) S3b error = no part index on this engine: S3a still answers.
-- Match key: startsWith(query, 'SELECT source, uniqExact(subject_kind') + type = 'QueryFinish' (edge_counts.go:50), as in PLAN-1.
--   The query column is read to match, never output.
-- SELECT only; no SET / SETTINGS.

-- S0: one row per log table. WANT 2 rows, key_ok = 1, window_rows > 0 (0 = wrong day, TTL or table off). Read: window_rows is the upper
--   bound of what S1/S2/S3a read from that table (caps: query_log 1,000,000, processors_profile_log 4,000,000). Gate: marks of each table <= 400,000.
SELECT log_table, key_ok, parts_all, marks_all, parts_day, rows_day, bytes_day, window_parts, window_granules, window_rows
FROM (
  SELECT 'query_log' AS log_table,
         (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) AS key_ok,
         (SELECT tuple(count(), sum(marks), countIf(min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')),
                       sumIf(rows, min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')),
                       sumIf(bytes_on_disk, min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')))
          FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') AS sizes,
         tupleElement(sizes, 1) AS parts_all, tupleElement(sizes, 2) AS marks_all, tupleElement(sizes, 3) AS parts_day,
         tupleElement(sizes, 4) AS rows_day, tupleElement(sizes, 5) AS bytes_day,
         uniqExact(part_name) AS window_parts, count() AS window_granules, sum(rows_in_granule) AS window_rows
  FROM (
    WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
    SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
           leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
           mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
    FROM mergeTreeIndex('system', 'query_log')
    WHERE marks_gate = 0)
  WHERE rows_in_granule > 0
    AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 09:10:00', 'UTC'))
    AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:50:00', 'UTC')))
  UNION ALL
  SELECT 'processors_profile_log' AS log_table,
         (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'processors_profile_log' AND startsWith(sorting_key, 'event_date, event_time')) AS key_ok,
         (SELECT tuple(count(), sum(marks), countIf(min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')),
                       sumIf(rows, min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')),
                       sumIf(bytes_on_disk, min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')))
          FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') AS sizes,
         tupleElement(sizes, 1) AS parts_all, tupleElement(sizes, 2) AS marks_all, tupleElement(sizes, 3) AS parts_day,
         tupleElement(sizes, 4) AS rows_day, tupleElement(sizes, 5) AS bytes_day,
         uniqExact(part_name) AS window_parts, count() AS window_granules, sum(rows_in_granule) AS window_rows
  FROM (
    WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
    SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
           leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
           mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
    FROM mergeTreeIndex('system', 'processors_profile_log')
    WHERE marks_gate = 0)
  WHERE rows_in_granule > 0
    AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 09:10:00', 'UTC'))
    AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:50:00', 'UTC')))
)
ORDER BY log_table;

-- S1: one row. WANT runs_before >= 1 and runs_after >= 1 (else STOP, limit a). before_* = the last run before 09:01Z (the 08:56 run),
--   after_* = the first run at or after 09:01Z (the 09:02 run). read_counters entry = (event name, before value, after value) for every
--   ProfileEvents name of either run that names a read, mark, range, part, filter, index, join, sort or merge counter (numbers only).
--   Control that can fail: runs_before and runs_after (0 = wrong day, prefix or type).
SELECT runs_before, runs_after, before_minute, after_minute,
       before_read_rows, after_read_rows, before_read_bytes, after_read_bytes, before_result_rows, after_result_rows,
       before_memory, after_memory, before_duration_ms, after_duration_ms,
       arrayMap(k -> tuple(k, before_events[k], after_events[k]),
                arrayFilter(k -> match(k, '(?i)select|read|rows|mark|range|part|final|filter|prewhere|index|condition|granule|join|sort|merg'),
                            arraySort(arrayDistinct(arrayConcat(mapKeys(before_events), mapKeys(after_events)))))) AS read_counters
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows_in_granule) FROM (
       WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
       SELECT rows_in_granule, tuple(event_date, event_time) AS k,
              leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
              mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
       FROM mergeTreeIndex('system', 'query_log')
       WHERE marks_gate = 0)
     WHERE rows_in_granule > 0
       AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 09:10:00', 'UTC'))
       AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:50:00', 'UTC')))) > 1000000
    OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
    'STOP-COST-GATE')) AS cost_gate
  SELECT countIf(event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS runs_before,
         countIf(event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS runs_after,
         toStartOfMinute(maxIf(event_time, event_time < toDateTime('2026-11-23 09:01:00', 'UTC'))) AS before_minute,
         toStartOfMinute(minIf(event_time, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC'))) AS after_minute,
         argMaxIf(read_rows, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_read_rows,
         argMinIf(read_rows, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_read_rows,
         argMaxIf(read_bytes, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_read_bytes,
         argMinIf(read_bytes, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_read_bytes,
         argMaxIf(result_rows, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_result_rows,
         argMinIf(result_rows, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_result_rows,
         argMaxIf(memory_usage, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_memory,
         argMinIf(memory_usage, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_memory,
         argMaxIf(query_duration_ms, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_duration_ms,
         argMinIf(query_duration_ms, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_duration_ms,
         argMaxIf(ProfileEvents, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_events,
         argMinIf(ProfileEvents, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_events
  FROM system.query_log
  WHERE cost_gate = 0
    AND event_date = toDate('2026-11-23')
    AND event_time >= toDateTime('2026-11-23 08:50:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:10:00', 'UTC')
    AND type = 'QueryFinish'
    AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
);

-- S2: the read steps of the two runs of S1. One row per run_side, run_minute, table, step and processor name. WANT: report. Read: per
--   step_table, compare 'before' and 'after'; a step or processor with about 46.5k more input or output rows, or an extra step, is the arm
--   that grew. FINAL steps show their merge processors (for example ReplacingSorted) next to MergeTreeSelect. Control that can fail:
--   2 run_side values (fewer = limit a or b). Two window gates (processors_profile_log 4,000,000 rows, query_log 1,000,000 rows).
SELECT r.run_side AS run_side, r.run_minute AS run_minute, p.plan_step_description AS step_table, p.plan_step AS step_id, p.name AS processor,
       count() AS processors, sum(p.input_rows) AS input_rows, sum(p.output_rows) AS output_rows, sum(p.output_bytes) AS output_bytes
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows_in_granule) FROM (
       WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
       SELECT rows_in_granule, tuple(event_date, event_time) AS k,
              leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
              mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
       FROM mergeTreeIndex('system', 'processors_profile_log')
       WHERE marks_gate = 0)
     WHERE rows_in_granule > 0
       AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 09:10:00', 'UTC'))
       AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:50:00', 'UTC')))) > 4000000
    OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'processors_profile_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
    'STOP-COST-GATE')) AS cost_gate
  SELECT query_id, name, plan_step, plan_step_description, input_rows, output_rows, output_bytes
  FROM system.processors_profile_log
  WHERE cost_gate = 0
    AND event_date = toDate('2026-11-23')
    AND event_time >= toDateTime('2026-11-23 08:50:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:10:00', 'UTC')
    AND plan_step_name = 'ReadFromMergeTree'
) AS p
INNER JOIN (
  SELECT tupleElement(run, 1) AS query_id, tupleElement(run, 2) AS run_side, tupleElement(run, 3) AS run_minute
  FROM (
    WITH (SELECT throwIf(
      (SELECT sum(rows_in_granule) FROM (
         WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
         SELECT rows_in_granule, tuple(event_date, event_time) AS k,
                leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
         FROM mergeTreeIndex('system', 'query_log')
         WHERE marks_gate = 0)
       WHERE rows_in_granule > 0
         AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 09:10:00', 'UTC'))
         AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:50:00', 'UTC')))) > 1000000
      OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
      'STOP-COST-GATE')) AS cost_gate
    SELECT arrayJoin([
             tuple(argMaxIf(query_id, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')), 'before',
                   toStartOfMinute(maxIf(event_time, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')))),
             tuple(argMinIf(query_id, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')), 'after',
                   toStartOfMinute(minIf(event_time, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC'))))]) AS run
    FROM system.query_log
    WHERE cost_gate = 0
      AND event_date = toDate('2026-11-23')
      AND event_time >= toDateTime('2026-11-23 08:50:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:10:00', 'UTC')
      AND type = 'QueryFinish'
      AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
  )
) AS r ON p.query_id = r.query_id
GROUP BY run_side, run_minute, step_table, step_id, processor
ORDER BY run_side DESC, step_table, step_id, processor;

-- S3a: the parts of work_items and project_membership_transitions at the start of the 08:56 run, of the 09:02 run, and now. One row per
--   run_side and table. A part exists at T when it was created at or before T and not removed by T. Created = its part_log create event
--   of the day, else its system.parts modification_time, else before the day. Removed = the first merge that consumed it, its RemovePart
--   event, or its system.parts remove_time. Level and max block come from the part name. WANT 6 rows. Read: before vs after: parts,
--   levels, level0_parts, newest_level / newest_rows; the hypothesis needs a new small part at 'after' that 'before' does not have.
--   Control that can fail: on 'now', parts = active_now (else limit c). Gates: part_log day parts 6,000,000 rows / 3,000,000,000 bytes;
--   query_log window 1,000,000 rows.
SELECT r.run_side AS run_side, u.table AS data_table, count() AS parts, sum(u.part_rows) AS rows, countIf(NOT u.rows_known) AS parts_rows_unknown,
       arraySort(groupArray(u.level)) AS levels, arrayReverseSort(groupArray(u.part_rows)) AS part_rows, countIf(u.level = 0) AS level0_parts,
       argMax(u.level, u.max_block) AS newest_level, argMax(u.part_rows, u.max_block) AS newest_rows,
       countIf(u.created_at < toDateTime('2026-11-23 00:00:00', 'UTC')) AS parts_from_before_day,
       if(data_table = 'work_items',
          (SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = 'work_items'),
          (SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = 'project_membership_transitions')) AS active_now
FROM (
  SELECT table, name,
         coalesce(minIfOrNull(t, kind = 'create'), minIfOrNull(t, kind = 'part'), toDateTime(0)) AS created_at,
         minIfOrNull(t, kind = 'remove') AS removed_at,
         coalesce(maxIfOrNull(n, kind = 'part'), maxIfOrNull(n, kind = 'create'), 0) AS part_rows,
         countIf(kind IN ('part', 'create')) > 0 AS rows_known,
         toUInt32OrZero(splitByChar('_', name)[4]) AS level,
         toUInt64OrZero(splitByChar('_', name)[3]) AS max_block
  FROM (
    SELECT toString(table) AS table, tupleElement(e, 1) AS name, tupleElement(e, 2) AS kind, tupleElement(e, 3) AS t, tupleElement(e, 4) AS n
    FROM (
      WITH (SELECT throwIf(
        (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 6000000
        OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 3000000000,
        'STOP-COST-GATE')) AS cost_gate
      SELECT table, arrayConcat(
               if(event_type IN ('NewPart', 'MergeParts', 'MutatePart', 'DownloadPart'), [tuple(part_name, 'create', event_time, rows)], []),
               if(event_type IN ('MergeParts', 'MutatePart'), arrayMap(x -> tuple(x, 'remove', event_time, toUInt64(0)), merged_from), []),
               if(event_type = 'RemovePart', [tuple(part_name, 'remove', event_time, toUInt64(0))], [])) AS events
      FROM system.part_log
      WHERE cost_gate = 0
        AND event_date = toDate('2026-11-23')
        AND database = currentDatabase()
        AND table IN ('project_membership_transitions', 'work_items')
    )
    ARRAY JOIN events AS e
    UNION ALL
    SELECT toString(table) AS table, tupleElement(e, 1) AS name, tupleElement(e, 2) AS kind, tupleElement(e, 3) AS t, tupleElement(e, 4) AS n
    FROM system.parts
    ARRAY JOIN arrayConcat([tuple(name, 'part', modification_time, rows)],
                           if(NOT active AND remove_time > toDateTime(0), [tuple(name, 'remove', remove_time, toUInt64(0))], [])) AS e
    WHERE database = currentDatabase() AND table IN ('project_membership_transitions', 'work_items')
  )
  GROUP BY table, name
) AS u
CROSS JOIN (
  SELECT tupleElement(run, 1) AS run_side, tupleElement(run, 2) AS run_start
  FROM (
    WITH (SELECT throwIf(
      (SELECT sum(rows_in_granule) FROM (
         WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
         SELECT rows_in_granule, tuple(event_date, event_time) AS k,
                leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
         FROM mergeTreeIndex('system', 'query_log')
         WHERE marks_gate = 0)
       WHERE rows_in_granule > 0
         AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 09:10:00', 'UTC'))
         AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:50:00', 'UTC')))) > 1000000
      OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
      'STOP-COST-GATE')) AS cost_gate
    SELECT arrayJoin([
             tuple('before', argMaxIf(query_start_time, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC'))),
             tuple('after', argMinIf(query_start_time, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC'))),
             tuple('now', now())]) AS run
    FROM system.query_log
    WHERE cost_gate = 0
      AND event_date = toDate('2026-11-23')
      AND event_time >= toDateTime('2026-11-23 08:50:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:10:00', 'UTC')
      AND type = 'QueryFinish'
      AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
  )
) AS r
WHERE u.created_at <= r.run_start AND (u.removed_at IS NULL OR u.removed_at > r.run_start)
GROUP BY run_side, data_table
ORDER BY data_table, run_side;

-- S3b: now, per data table: the parts whose granules hold org-range keys, the newest of them (highest max block), and how many OTHER parts
--   have a granule whose key range intersects the newest part's org key range, with those granules' rows. Each granule is [its mark key,
--   the next mark key] cut to the org range; the last granule of a part runs to the org range end. WANT 2 rows, newest_parts = 1 when
--   org_parts > 0. Read: other_parts_intersecting > 0 = FINAL must merge-read other_rows_intersecting rows of other parts with the newest part.
--   Control that can fail: org_parts > 0 (0 = wrong org range or database). Gates: marks of each data table <= 100,000.
SELECT data_table, org_parts, newest_parts, newest_org_rows, other_parts_intersecting, other_rows_intersecting, other_org_rows
FROM (
  SELECT 'work_items' AS data_table,
         uniqExact(part_name) AS org_parts,
         uniqExactIf(part_name, max_block = newest_block) AS newest_parts,
         sumIf(granule_rows, max_block = newest_block) AS newest_org_rows,
         uniqExactIf(part_name, max_block != newest_block AND k_lo <= newest_hi AND k_hi >= newest_lo) AS other_parts_intersecting,
         sumIf(granule_rows, max_block != newest_block AND k_lo <= newest_hi AND k_hi >= newest_lo) AS other_rows_intersecting,
         sumIf(granule_rows, max_block != newest_block) AS other_org_rows
  FROM (
    SELECT part_name, max_block, granule_rows, k_lo, k_hi, newest_block,
           minIf(k_lo, max_block = newest_block) OVER () AS newest_lo,
           maxIf(k_hi, max_block = newest_block) OVER () AS newest_hi
    FROM (
      SELECT part_name, max_block, granule_rows, k_lo, k_hi, max(max_block) OVER () AS newest_block
      FROM (
        SELECT part_name, toUInt64OrZero(splitByChar('_', part_name)[3]) AS max_block, rows_in_granule AS granule_rows,
               if(k > CAST(tuple('c6a38355', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), k,
                  CAST(tuple('c6a38355', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)')) AS k_lo,
               if(is_last OR k_next > CAST(tuple('c6a38356', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'),
                  CAST(tuple('c6a38356', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), k_next) AS k_hi
        FROM (
          WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'work_items') > 100000, 'STOP-COST-GATE')) AS cost_gate
          SELECT part_name, rows_in_granule, tuple(org_id, repo_id, work_item_id) AS k,
                 leadInFrame(tuple(org_id, repo_id, work_item_id)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                 mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
          FROM mergeTreeIndex(currentDatabase(), 'work_items')
          WHERE cost_gate = 0)
        WHERE rows_in_granule > 0)
      WHERE k_lo <= k_hi))
  UNION ALL
  SELECT 'project_membership_transitions' AS data_table,
         uniqExact(part_name) AS org_parts,
         uniqExactIf(part_name, max_block = newest_block) AS newest_parts,
         sumIf(granule_rows, max_block = newest_block) AS newest_org_rows,
         uniqExactIf(part_name, max_block != newest_block AND k_lo <= newest_hi AND k_hi >= newest_lo) AS other_parts_intersecting,
         sumIf(granule_rows, max_block != newest_block AND k_lo <= newest_hi AND k_hi >= newest_lo) AS other_rows_intersecting,
         sumIf(granule_rows, max_block != newest_block) AS other_org_rows
  FROM (
    SELECT part_name, max_block, granule_rows, k_lo, k_hi, newest_block,
           minIf(k_lo, max_block = newest_block) OVER () AS newest_lo,
           maxIf(k_hi, max_block = newest_block) OVER () AS newest_hi
    FROM (
      SELECT part_name, max_block, granule_rows, k_lo, k_hi, max(max_block) OVER () AS newest_block
      FROM (
        SELECT part_name, toUInt64OrZero(splitByChar('_', part_name)[3]) AS max_block, rows_in_granule AS granule_rows,
               if(k > CAST(tuple('c6a38355', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), k,
                  CAST(tuple('c6a38355', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)')) AS k_lo,
               if(is_last OR k_next > CAST(tuple('c6a38356', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'),
                  CAST(tuple('c6a38356', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), k_next) AS k_hi
        FROM (
          WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'project_membership_transitions') > 100000, 'STOP-COST-GATE')) AS cost_gate
          SELECT part_name, rows_in_granule, tuple(org_id, toString(subject_kind), repo_id, subject_id, occurred_at, event_id) AS k,
                 leadInFrame(tuple(org_id, toString(subject_kind), repo_id, subject_id, occurred_at, event_id)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                 mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
          FROM mergeTreeIndex(currentDatabase(), 'project_membership_transitions')
          WHERE cost_gate = 0)
        WHERE rows_in_granule > 0)
      WHERE k_lo <= k_hi))
)
ORDER BY data_table;
`

const (
	p2x14548OrgA = "c6a38355-0000-4000-8000-000000014548"
	p2x14548OrgB = "14548000-0000-4000-8000-0000000000bb"
)

// Synthetic rows. work_items: one merged part (org B 2,000 rows, then org A 40,900 rows over 40 repos; only repos 0-2 have a project),
// then merges stop and a 21-row org A part lands after every org A key (repo ffff...). transitions: one 20-row org A part.
const p2x14548Fixture = `
INSERT INTO work_items (org_id, repo_id, work_item_id, provider, project_id, created_at, updated_at, last_synced)
SELECT if(number < 2000, 'ORG_B', 'ORG_A'), toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(number % 40), 12, '0'))),
       concat('wi-', leftPad(toString(number), 6, '0')), 'linear', if(number % 40 < 3, 'p1', ''),
       toDateTime64('2026-11-01 00:00:00', 3), toDateTime64('2026-11-01 00:00:00', 3), toDateTime64('2026-11-20 00:00:00', 3)
FROM numbers(42900);
OPTIMIZE TABLE work_items FINAL;
SYSTEM STOP MERGES work_items;
INSERT INTO work_items (org_id, repo_id, work_item_id, provider, project_id, created_at, updated_at, last_synced)
SELECT 'ORG_A', toUUID('ffffffff-ffff-4fff-bfff-ffffffffffff'), concat('wz-', leftPad(toString(number), 6, '0')), 'linear', 'p9',
       toDateTime64('2026-11-01 00:00:00', 3), toDateTime64('2026-11-01 00:00:00', 3), toDateTime64('2026-11-20 00:00:00', 3)
FROM numbers(21);
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id)
SELECT 'ORG_A', toUUID('00000000-0000-4000-8000-000000000001'), 'work_item', concat('w', toString(number)), 'linear', '', 'p1',
       toDateTime64('2026-11-20 08:00:00', 3) + number, toDateTime64('2026-11-20 08:00:00', 3), concat('e', toString(number))
FROM numbers(20);
`

// 107 org A rows that rewrite existing keys spread over all 40 repos: a small part inside the big part's org key range.
const p2x14548Overlap = `INSERT INTO work_items (org_id, repo_id, work_item_id, provider, project_id, created_at, updated_at, last_synced)
SELECT 'ORG_A', toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(n % 40), 12, '0'))), concat('wi-', leftPad(toString(n), 6, '0')),
       'linear', if(n % 40 < 3, 'p1', ''), toDateTime64('2026-11-01 00:00:00', 3), toDateTime64('2026-11-23 09:00:00', 3),
       toDateTime64('2026-11-23 09:00:00', 3)
FROM (SELECT 2000 + number * 381 AS n FROM numbers(107))`

// The same first token as the projector statement (edge_counts.go:50); a transitions arm and a work_items arm, both FINAL.
const p2x14548Like = `SELECT source, uniqExact(subject_kind, subject_id) AS edge_count
FROM (
  SELECT 'transition' AS source, subject_kind, subject_id FROM project_membership_transitions FINAL WHERE org_id = 'ORG_A'
  UNION ALL
  SELECT 'work_item_column' AS source, 'work_item' AS subject_kind, work_item_id AS subject_id FROM work_items FINAL
  WHERE org_id = 'ORG_A' AND notEmpty(project_id) AND provider != 'gitlab'
)
GROUP BY source`

const p2x14548LabelList = "S0,S1,S2,S3a,S3b"

type p2x14548Statement struct{ label, text string }

func p2x14548Orgs(text string) string {
	return strings.NewReplacer("'ORG_A'", "'"+p2x14548OrgA+"'", "'ORG_B'", "'"+p2x14548OrgB+"'").Replace(text)
}

func p2x14548Split(body string) []p2x14548Statement {
	labels := strings.Split(p2x14548LabelList, ",")
	var out []p2x14548Statement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		if !strings.HasSuffix(strings.TrimSpace(chmigrate.StripLineComments(line)), ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		if len(out) < len(labels) {
			label = labels[len(out)]
		}
		out = append(out, p2x14548Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func p2x14548Deep(value reflect.Value, dbType string) string {
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return "NULL"
	}
	if ts, ok := value.Interface().(time.Time); ok {
		if strings.HasPrefix(dbType, "DateTime") {
			return ts.UTC().Format("2006-01-02 15:04:05")
		}
		return ts.UTC().Format("2006-01-02")
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		parts := make([]string, value.Len())
		for i := range parts {
			parts[i] = p2x14548Deep(value.Index(i), "")
		}
		return "[" + strings.Join(parts, " ") + "]"
	case reflect.Map:
		keys := value.MapKeys()
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, p2x14548Deep(k, "")+":"+p2x14548Deep(value.MapIndex(k), ""))
		}
		sort.Strings(parts)
		return "{" + strings.Join(parts, " ") + "}"
	}
	return fmt.Sprint(value.Interface())
}

func p2x14548Run(ctx context.Context, conn driver.Conn, text, comment string, extra clickhouse.Settings) ([]map[string]string, error) {
	settings := clickhouse.Settings{"log_comment": comment}
	for k, v := range extra {
		settings[k] = v
	}
	rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(settings)), text)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := rows.Columns()
	types := rows.ColumnTypes()
	var got []map[string]string
	for rows.Next() {
		cells := make([]any, len(types))
		for i, ct := range types {
			cells[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for i, name := range columns {
			row[name] = p2x14548Deep(reflect.ValueOf(cells[i]), types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func p2x14548Log(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
		WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("query log: %v", err)
	}
	defer rows.Close()
	var types []string
	var total uint64
	for rows.Next() {
		var typ string
		var read uint64
		if err := rows.Scan(&typ, &read); err != nil {
			t.Fatalf("query log scan: %v", err)
		}
		types = append(types, typ)
		total += read
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return types, total
}

func p2x14548Count(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func p2x14548Uint(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// p2x14548Caps returns the byte offsets of every cap literal "> N" (N a cap of this form) in text.
func p2x14548Caps(text string) [][2]int {
	caps := map[string]bool{"400000": true, "1000000": true, "4000000": true, "6000000": true, "3000000000": true, "100000": true}
	var out [][2]int
	for _, m := range regexp.MustCompile(`> (\d+)\b`).FindAllStringSubmatchIndex(text, -1) {
		if caps[text[m[2]:m[3]]] {
			out = append(out, [2]int{m[0], m[1]})
		}
	}
	return out
}

type p2x14548Logged struct {
	comment, id                                       string
	readRows, readBytes, resultRows, memory, duration uint64
	start                                             time.Time
	events                                            map[string]uint64
}

func TestReadPlan2Form14548ExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(p2x14548Form))
	if got := hex.EncodeToString(sum[:]); got != p2x14548FormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, p2x14548FormSHA256)
	}
	statements := p2x14548Split(p2x14548Form)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != p2x14548LabelList {
		t.Fatalf("form statements = %v", labels)
	}
	code := chmigrate.StripLineComments(p2x14548Form)
	for _, forbidden := range []string{`(?i)\b(FORMAT|EXPLAIN|INSERT|FINAL|LIMIT|OPTIMIZE|ALTER)\b`, `(?i)\bSETTINGS\s+\w+\s*=`, `(?im)^\s*SET\b`} {
		if regexp.MustCompile(forbidden).MatchString(code) {
			t.Fatalf("form holds a forbidden keyword (%s)", forbidden)
		}
	}
	// Static gate shape: cap literals, throw sites and log-table reads per statement.
	wantCaps := map[string]map[string]int{
		"S0":  {"400000": 2},
		"S1":  {"400000": 1, "1000000": 1},
		"S2":  {"400000": 2, "4000000": 1, "1000000": 1},
		"S3a": {"6000000": 1, "3000000000": 1, "400000": 1, "1000000": 1},
		"S3b": {"100000": 2},
	}
	wantReads := map[string]map[string]int{
		"S0":  {"query_log": 0, "processors_profile_log": 0, "part_log": 0},
		"S1":  {"query_log": 1, "processors_profile_log": 0, "part_log": 0},
		"S2":  {"query_log": 1, "processors_profile_log": 1, "part_log": 0},
		"S3a": {"query_log": 1, "processors_profile_log": 0, "part_log": 1},
		"S3b": {"query_log": 0, "processors_profile_log": 0, "part_log": 0},
	}
	for _, s := range statements {
		got := map[string]int{}
		for _, c := range p2x14548Caps(s.text) {
			got[strings.TrimPrefix(s.text[c[0]:c[1]], "> ")]++
		}
		if !reflect.DeepEqual(got, wantCaps[s.label]) {
			t.Fatalf("%s: cap literals %v, want %v", s.label, got, wantCaps[s.label])
		}
		throws := 0
		for _, n := range got {
			throws += n
		}
		if s.label == "S3a" {
			throws-- // part_log: one throw holds the rows cap and the bytes cap
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != throws {
			t.Fatalf("%s: %d throw sites, want %d", s.label, n, throws)
		}
		for table, want := range wantReads[s.label] {
			if n := strings.Count(s.text, "FROM system."+table+"\n"); n != want {
				t.Fatalf("%s: %d reads of system.%s, want %d", s.label, n, table, want)
			}
		}
		if n := strings.Count(s.text, "FROM mergeTreeIndex("); n != map[string]int{"S0": 2, "S1": 1, "S2": 2, "S3a": 1, "S3b": 2}[s.label] {
			t.Fatalf("%s: %d part index reads", s.label, n)
		}
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
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse %s", result.Head, version)
	for _, table := range []string{"query_log", "processors_profile_log", "part_log"} {
		var sortingKey, partitionKey string
		if err := conn.QueryRow(ctx, "SELECT sorting_key, partition_key FROM system.tables WHERE database = 'system' AND name = ?", table).Scan(&sortingKey, &partitionKey); err != nil {
			t.Fatalf("system.%s is not present: %v", table, err)
		}
		t.Logf("system.%s sorting_key %q partition_key %q", table, sortingKey, partitionKey)
	}
	for _, table := range []string{"work_items", "project_membership_transitions"} {
		var engine string
		if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = ?", table).Scan(&engine); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s engine %s", table, engine)
	}

	for _, statement := range chmigrate.SplitStatements(p2x14548Orgs(p2x14548Fixture)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	like := p2x14548Orgs(p2x14548Like)
	probe := "SELECT * FROM (" + like + ")"
	stamp := time.Now().UnixNano()
	runLike := func(name string) string {
		comment := fmt.Sprintf("p2x14548-like-%s-%d", name, stamp)
		if _, err := p2x14548Run(ctx, conn, like, comment, clickhouse.Settings{"log_processors_profiles": 1}); err != nil {
			t.Fatalf("like run %s: %v", name, err)
		}
		return comment
	}
	runProbe := func(name string) string {
		comment := fmt.Sprintf("p2x14548-probe-%s-%d", name, stamp)
		if _, err := p2x14548Run(ctx, conn, probe, comment, clickhouse.Settings{"use_query_condition_cache": 0}); err != nil {
			t.Fatalf("probe %s: %v", name, err)
		}
		return comment
	}
	// Phase A: big merged part + a part after every org A key. Phase B: + a 107-row part inside the org range. Phase C (end): merged.
	var phaseA, phaseB, phaseC, probes []string
	for i := 1; i <= 3; i++ {
		phaseA = append(phaseA, runLike(fmt.Sprintf("a%d", i)))
	}
	probes = append(probes, runProbe("a"))
	time.Sleep(1100 * time.Millisecond)
	var cut, day, nextDay string
	if err := conn.QueryRow(ctx, "SELECT toString(now('UTC')), toString(toDate(now('UTC'))), toString(toDate(now('UTC')) + 1)").Scan(&cut, &day, &nextDay); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := conn.Exec(ctx, p2x14548Orgs(p2x14548Overlap)); err != nil {
		t.Fatalf("overlap part: %v", err)
	}
	for i := 1; i <= 3; i++ {
		phaseB = append(phaseB, runLike(fmt.Sprintf("b%d", i)))
	}
	probes = append(probes, runProbe("b"))
	dated := func(text string) string {
		text = strings.ReplaceAll(text, "2026-11-23 08:50:00", day+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 09:10:00", nextDay+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 09:01:00", cut)
		return strings.ReplaceAll(text, "2026-11-23", day)
	}
	t.Logf("dated copies: window %s 00:00:00 .. %s 00:00:00 UTC, cut %s (the CI clock, not the prod day)", day, nextDay, cut)

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the logs are not flushable by this user: %v", err)
	}
	loadLike := func(comment string) p2x14548Logged {
		r := p2x14548Logged{comment: comment, events: map[string]uint64{}}
		if err := conn.QueryRow(ctx, `SELECT query_id, read_rows, read_bytes, result_rows, toUInt64(memory_usage), query_duration_ms, query_start_time, ProfileEvents
			FROM system.query_log WHERE log_comment = ? AND type = 'QueryFinish'`, comment).Scan(&r.id, &r.readRows, &r.readBytes, &r.resultRows, &r.memory, &r.duration, &r.start, &r.events); err != nil {
			t.Fatalf("like run %s in query_log: %v", comment, err)
		}
		return r
	}
	repro := func(phase string, comments []string) []p2x14548Logged {
		var out []p2x14548Logged
		for _, c := range comments {
			r := loadLike(c)
			t.Logf("REPRO %s %s: read_rows=%d memory=%d SelectedRows=%d SelectedMarks=%d SelectedParts=%d SelectedRanges=%d QueryConditionCacheHits=%d QueryConditionCacheMisses=%d",
				phase, strings.Split(c, "-")[2], r.readRows, r.memory, r.events["SelectedRows"], r.events["SelectedMarks"], r.events["SelectedParts"], r.events["SelectedRanges"],
				r.events["QueryConditionCacheHits"], r.events["QueryConditionCacheMisses"])
			out = append(out, r)
		}
		return out
	}
	likesA, likesB := repro("A", phaseA), repro("B", phaseB)
	repro("probe-no-condition-cache", probes)
	before, after := likesA[2], likesB[0]

	logToday := func(table string) uint64 {
		return p2x14548Count(ctx, t, conn, "SELECT count() FROM system."+table+" WHERE event_date = toDate(?)", day)
	}
	qlToday, ppToday := logToday("query_log"), logToday("processors_profile_log")

	passComments := map[string]string{}
	passRows := map[string][]map[string]string{}
	for _, s := range statements {
		passComments[s.label] = fmt.Sprintf("p2x14548-pass-%s-%d", s.label, time.Now().UnixNano())
		got, err := p2x14548Run(ctx, conn, dated(s.text), passComments[s.label], nil)
		if err != nil {
			t.Errorf("PASS %s: SQL error: %v", s.label, err)
			continue
		}
		passRows[s.label] = got
		t.Logf("PASS %s ROWS %v", s.label, got)
	}

	// S0: both log tables, key ok, the window holds at least the rows of the day counted before S0.
	if r := passRows["S0"]; len(r) != 2 || r[0]["log_table"] != "processors_profile_log" || r[1]["log_table"] != "query_log" {
		t.Errorf("S0: %v, want 2 rows (processors_profile_log, query_log)", r)
	} else {
		for i, today := range []uint64{ppToday, qlToday} {
			if r[i]["key_ok"] != "1" || p2x14548Uint(r[i]["window_rows"]) < today || p2x14548Uint(r[i]["marks_all"]) == 0 || p2x14548Uint(r[i]["parts_day"]) == 0 {
				t.Errorf("S0 %s: %v, want key_ok 1, window_rows >= %d (rows of the day), marks_all > 0, parts_day > 0", r[i]["log_table"], r[i], today)
			}
		}
	}

	// S1: the runs' own log values; read_counters = independent recompute from the two ProfileEvents maps.
	counterName := regexp.MustCompile(`(?i)select|read|rows|mark|range|part|final|filter|prewhere|index|condition|granule|join|sort|merg`)
	if r := passRows["S1"]; len(r) != 1 {
		t.Errorf("S1: %d rows, want 1", len(r))
	} else {
		s1 := r[0]
		want := map[string]string{
			"runs_before": "3", "runs_after": "3",
			"before_read_rows": fmt.Sprint(before.readRows), "after_read_rows": fmt.Sprint(after.readRows),
			"before_read_bytes": fmt.Sprint(before.readBytes), "after_read_bytes": fmt.Sprint(after.readBytes),
			"before_result_rows": fmt.Sprint(before.resultRows), "after_result_rows": fmt.Sprint(after.resultRows),
			"before_memory": fmt.Sprint(before.memory), "after_memory": fmt.Sprint(after.memory),
			"before_duration_ms": fmt.Sprint(before.duration), "after_duration_ms": fmt.Sprint(after.duration),
		}
		for k, v := range want {
			if s1[k] != v {
				t.Errorf("S1 %s = %s, want %s", k, s1[k], v)
			}
		}
		names := map[string]bool{}
		for _, m := range []map[string]uint64{before.events, after.events} {
			for k := range m {
				if counterName.MatchString(k) {
					names[k] = true
				}
			}
		}
		sorted := make([]string, 0, len(names))
		for k := range names {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		entries := make([]string, 0, len(sorted))
		for _, k := range sorted {
			entries = append(entries, fmt.Sprintf("[%s %d %d]", k, before.events[k], after.events[k]))
		}
		if wantCounters := "[" + strings.Join(entries, " ") + "]"; s1["read_counters"] != wantCounters {
			t.Errorf("S1 read_counters = %s, want %s", s1["read_counters"], wantCounters)
		}
		if !names["SelectedRows"] || !names["SelectedMarks"] {
			t.Errorf("S1 fixture: read_counters %v miss SelectedRows / SelectedMarks", sorted)
		}
	}

	// S2: per side, the ReadFromMergeTree processors = an independent read of processors_profile_log by the run's query_id.
	stepKey := func(table, step, processor string) string { return table + "|" + step + "|" + processor }
	expectSteps := func(id string) map[string]string {
		rows, err := conn.Query(ctx, `SELECT plan_step_description, toString(plan_step), name, count(), sum(input_rows), sum(output_rows), sum(output_bytes)
			FROM system.processors_profile_log WHERE query_id = ? AND plan_step_name = 'ReadFromMergeTree'
			GROUP BY plan_step_description, plan_step, name`, id)
		if err != nil {
			t.Fatalf("processors of %s: %v", id, err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var table, step, processor string
			var n, in, outRows, outBytes uint64
			if err := rows.Scan(&table, &step, &processor, &n, &in, &outRows, &outBytes); err != nil {
				t.Fatal(err)
			}
			out[stepKey(table, step, processor)] = fmt.Sprintf("%d %d %d %d", n, in, outRows, outBytes)
		}
		return out
	}
	gotSteps := map[string]map[string]string{"before": {}, "after": {}}
	for _, r := range passRows["S2"] {
		side := gotSteps[r["run_side"]]
		if side == nil {
			t.Errorf("S2 row with run_side %q", r["run_side"])
			continue
		}
		side[stepKey(r["step_table"], r["step_id"], r["processor"])] = fmt.Sprintf("%s %s %s %s", r["processors"], r["input_rows"], r["output_rows"], r["output_bytes"])
	}
	for side, id := range map[string]string{"before": before.id, "after": after.id} {
		want := expectSteps(id)
		if !reflect.DeepEqual(gotSteps[side], want) {
			t.Errorf("S2 %s = %v, want %v", side, gotSteps[side], want)
		}
		tables := map[string]bool{}
		for k := range want {
			tables[strings.SplitN(k, "|", 2)[0]] = true
		}
		if !tables[scratch.Name+".work_items"] || !tables[scratch.Name+".project_membership_transitions"] {
			t.Errorf("S2 %s: step tables %v, want work_items and project_membership_transitions", side, tables)
		}
	}

	// S3a: the parts at each run start and now, from the fixture order; 'now' = the active parts.
	partsNow := map[string]uint64{}
	for _, table := range []string{"work_items", "project_membership_transitions"} {
		partsNow[table] = p2x14548Count(ctx, t, conn, "SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = ?", table)
	}
	wantS3a := map[string]string{
		"before|work_items":                     "2 42921 [0 1] [42900 21] 0 21",
		"after|work_items":                      "3 43028 [0 0 1] [42900 107 21] 0 107",
		"now|work_items":                        "3 43028 [0 0 1] [42900 107 21] 0 107",
		"before|project_membership_transitions": "1 20 [0] [20] 0 20",
		"after|project_membership_transitions":  "1 20 [0] [20] 0 20",
		"now|project_membership_transitions":    "1 20 [0] [20] 0 20",
	}
	gotS3a := map[string]string{}
	for _, r := range passRows["S3a"] {
		key := r["run_side"] + "|" + r["data_table"]
		gotS3a[key] = strings.Join([]string{r["parts"], r["rows"], r["levels"], r["part_rows"], r["newest_level"], r["newest_rows"]}, " ")
		if r["parts_rows_unknown"] != "0" || r["parts_from_before_day"] != "0" {
			t.Errorf("S3a %s: %v, want parts_rows_unknown 0 and parts_from_before_day 0", key, r)
		}
		if r["run_side"] == "now" && r["parts"] != r["active_now"] {
			t.Errorf("S3a now %s: parts %s, active_now %s, want equal", r["data_table"], r["parts"], r["active_now"])
		}
		if r["active_now"] != fmt.Sprint(partsNow[r["data_table"]]) {
			t.Errorf("S3a %s: active_now %s, system.parts %d", key, r["active_now"], partsNow[r["data_table"]])
		}
	}
	if !reflect.DeepEqual(gotS3a, wantS3a) {
		t.Errorf("S3a = %v, want %v", gotS3a, wantS3a)
	}

	// S3b: the 107-row part is the newest; it intersects the big part (all its granules), not the repo-ffff part.
	bigRows := p2x14548Count(ctx, t, conn, "SELECT max(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'work_items'")
	wantS3b := map[string]string{
		"project_membership_transitions": "1 1 20 0 0 0",
		"work_items":                     fmt.Sprintf("3 1 107 1 %d %d", bigRows, bigRows+21),
	}
	gotS3b := map[string]string{}
	for _, r := range passRows["S3b"] {
		gotS3b[r["data_table"]] = strings.Join([]string{r["org_parts"], r["newest_parts"], r["newest_org_rows"], r["other_parts_intersecting"], r["other_rows_intersecting"], r["other_org_rows"]}, " ")
	}
	if !reflect.DeepEqual(gotS3b, wantS3b) {
		t.Errorf("S3b = %v, want %v", gotS3b, wantS3b)
	}

	// Control mutations: each must change the result so its WANT fails.
	mutateWith := func(label, text string) []map[string]string {
		got, err := p2x14548Run(ctx, conn, text, fmt.Sprintf("p2x14548-mutate-%s-%d", label, time.Now().UnixNano()), nil)
		if err != nil {
			t.Fatalf("MUTATE %s: %v", label, err)
		}
		t.Logf("MUTATE %s ROWS %v", label, got)
		return got
	}
	replaced := func(label, from, to string) string {
		text := dated(byLabel[label])
		mutated := strings.ReplaceAll(text, from, to)
		if mutated == text {
			t.Fatalf("MUTATE %s: %q not found", label, from)
		}
		return mutated
	}
	pastDay := func(label string) string {
		return strings.ReplaceAll(strings.ReplaceAll(dated(byLabel[label]), nextDay, "2000-01-02"), day, "2000-01-01")
	}
	prefix := "'SELECT source, uniqExact(subject_kind'"
	wrongPrefix := "'SELECT zource, uniqExact(subject_kind'"
	if got := mutateWith("S0-past-day", pastDay("S0")); len(got) != 2 || got[0]["window_rows"] != "0" || got[1]["window_rows"] != "0" {
		t.Errorf("MUTATE S0 past day: %v, want window_rows 0 and 0", got)
	}
	for name, text := range map[string]string{
		"S1-past-day": pastDay("S1"),
		"S1-prefix":   replaced("S1", prefix, wrongPrefix),
		"S1-type":     replaced("S1", "type = 'QueryFinish'", "type = 'ExceptionWhileProcessing'"),
	} {
		if got := mutateWith(name, text); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "0" {
			t.Errorf("MUTATE %s: %v, want runs 0 and 0", name, got)
		}
	}
	if got := mutateWith("S1-cut-at-window-start", replaced("S1", "toDateTime('"+cut+"', 'UTC')", "toDateTime('"+day+" 00:00:00', 'UTC')")); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "6" {
		t.Errorf("MUTATE S1 cut at the window start: %v, want runs_before 0, runs_after 6", got)
	}
	for name, text := range map[string]string{
		"S2-past-day": pastDay("S2"),
		"S2-prefix":   replaced("S2", prefix, wrongPrefix),
		"S2-step":     replaced("S2", "plan_step_name = 'ReadFromMergeTree'", "plan_step_name = 'ReadFromMergeTreez'"),
	} {
		if got := mutateWith(name, text); len(got) != 0 {
			t.Errorf("MUTATE %s: %v, want 0 rows", name, got)
		}
	}
	noRemove := dated(byLabel["S3a"])
	for _, from := range []string{"tuple(x, 'remove',", "tuple(part_name, 'remove',", "tuple(name, 'remove',"} {
		if !strings.Contains(noRemove, from) {
			t.Fatalf("MUTATE S3a: %q not found", from)
		}
		noRemove = strings.ReplaceAll(noRemove, from, strings.Replace(from, "'remove'", "'removez'", 1))
	}
	nowParts := ""
	for _, r := range mutateWith("S3a-no-removal", noRemove) {
		if r["run_side"] == "now" && r["data_table"] == "work_items" {
			nowParts = r["parts"] + "/" + r["active_now"]
		}
	}
	if nowParts == "" || strings.Split(nowParts, "/")[0] == strings.Split(nowParts, "/")[1] {
		t.Errorf("MUTATE S3a no removal: now work_items parts/active_now = %q, want unequal", nowParts)
	}
	if got := mutateWith("S3a-prefix", replaced("S3a", prefix, wrongPrefix)); len(got) != 2 {
		t.Errorf("MUTATE S3a prefix: %d rows, want 2 ('now' only)", len(got))
	}
	orgMutant := strings.ReplaceAll(replaced("S3b", "'c6a38355'", "'d6a38355'"), "'c6a38356'", "'d6a38356'")
	if got := mutateWith("S3b-org", orgMutant); len(got) != 2 || got[0]["org_parts"] != "0" || got[1]["org_parts"] != "0" {
		t.Errorf("MUTATE S3b org range: %v, want org_parts 0 and 0", got)
	}

	// PASS-path bounds from the part index after the runs: read_rows <= window rows + part index rows + system table rows.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	sizes, err := p2x14548Run(ctx, conn, dated(byLabel["S0"]), fmt.Sprintf("p2x14548-sizes-%d", time.Now().UnixNano()), nil)
	if err != nil || len(sizes) != 2 {
		t.Fatalf("sizes: %v %v", sizes, err)
	}
	wPP, mPP := p2x14548Uint(sizes[0]["window_rows"]), p2x14548Uint(sizes[0]["marks_all"])+p2x14548Uint(sizes[0]["parts_all"])
	wQL, mQL := p2x14548Uint(sizes[1]["window_rows"]), p2x14548Uint(sizes[1]["marks_all"])+p2x14548Uint(sizes[1]["parts_all"])
	plToday := logToday("part_log")
	mData := p2x14548Count(ctx, t, conn, "SELECT sum(marks) + count() FROM system.parts WHERE database = currentDatabase() AND table IN ('work_items', 'project_membership_transitions')")
	pAll := p2x14548Count(ctx, t, conn, "SELECT count() FROM system.parts WHERE (database = 'system' AND table IN ('query_log', 'processors_profile_log', 'part_log')) OR (database = currentDatabase() AND table IN ('work_items', 'project_membership_transitions'))")
	slack := 12*pAll + 100
	bounds := map[string]uint64{
		"S0":  mPP + mQL + slack,
		"S1":  wQL + mQL + slack,
		"S2":  wPP + wQL + mPP + mQL + slack,
		"S3a": plToday + wQL + mQL + slack,
		"S3b": mData + slack,
	}
	t.Logf("sizes after the runs: pp window %d marks+parts %d; ql window %d marks+parts %d; part_log today %d; data marks+parts %d; system.parts rows %d",
		wPP, mPP, wQL, mQL, plToday, mData, pAll)
	for _, s := range statements {
		if _, ok := passRows[s.label]; !ok {
			continue
		}
		types, read := p2x14548Log(ctx, t, conn, passComments[s.label])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", s.label, types)
		}
		t.Logf("PASS %s bound: 1 <= read_rows %d <= %d", s.label, read, bounds[s.label])
		if read < 1 || read > bounds[s.label] {
			t.Errorf("PASS %s: read_rows %d outside [1, %d]", s.label, read, bounds[s.label])
		}
	}

	// Gate fires: each cap literal lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE, ExceptionBeforeStart, no log or data row read.
	fires := 0
	for _, s := range statements {
		for i, c := range p2x14548Caps(s.text) {
			lowered := s.text[:c[0]] + "> 0" + s.text[c[1]:]
			comment := fmt.Sprintf("p2x14548-gate-%s-%d-%d", s.label, i, time.Now().UnixNano())
			_, err := p2x14548Run(ctx, conn, dated(lowered), comment, nil)
			var ex *clickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", s.label, s.text[c[0]:c[1]], err)
				continue
			}
			if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
				t.Fatal(err)
			}
			types, read := p2x14548Log(ctx, t, conn, comment)
			bound := mPP + mQL + mData + slack
			t.Logf("GATE %s %s: code %d, query_log types=%v read_rows=%d bound=%d", s.label, s.text[c[0]:c[1]], ex.Code, types, read, bound)
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
				t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", s.label, s.text[c[0]:c[1]], types)
			}
			if read > bound {
				t.Errorf("GATE %s %s: read_rows %d over %d", s.label, s.text[c[0]:c[1]], read, bound)
			}
			fires++
		}
	}
	if fires != 14 {
		t.Errorf("gates fired %d times, want 14", fires)
	}

	// Output cell: no column named like query text or id; no value holds a query_id, a comment, the statement text, an org id or a key.
	secrets := []string{p2x14548OrgA, p2x14548OrgB, "uniqExact(subject_kind", "wi-0", "wz-0", "ffffffff-ffff", "00000000-0000-4000-8000-0000000000"}
	for _, r := range append(append([]p2x14548Logged{}, likesA...), likesB...) {
		secrets = append(secrets, r.id, r.comment)
	}
	leaks := func(label, text string) []string {
		got, err := p2x14548Run(ctx, conn, dated(text), fmt.Sprintf("p2x14548-output-%s-%d", label, time.Now().UnixNano()), nil)
		if err != nil {
			t.Fatalf("OUTPUT %s: %v", label, err)
		}
		var bad []string
		for _, row := range got {
			for name, value := range row {
				if name == "query" || name == "query_id" || name == "initial_query_id" || name == "formatted_query" || name == "normalized_query_hash" {
					bad = append(bad, "column "+name)
				}
				for _, secret := range secrets {
					if strings.Contains(value, secret) {
						bad = append(bad, name+" holds "+secret)
					}
				}
			}
		}
		return bad
	}
	for _, s := range statements {
		if _, ok := passRows[s.label]; !ok {
			continue
		}
		if bad := leaks(s.label, s.text); len(bad) != 0 {
			t.Errorf("OUTPUT %s: %v", s.label, bad)
		} else {
			t.Logf("OUTPUT %s: no query text, query_id, org id or key value", s.label)
		}
	}
	s2Mutant := strings.Replace(byLabel["S2"], "SELECT r.run_side AS run_side,", "SELECT r.run_side AS run_side, p.query_id AS leaked,", 1)
	s2Mutant = strings.Replace(s2Mutant, "GROUP BY run_side,", "GROUP BY leaked, run_side,", 1)
	s1Mutant := strings.Replace(byLabel["S1"], "SELECT runs_before, runs_after,", "SELECT runs_before, runs_after, q_text,", 1)
	s1Mutant = strings.Replace(s1Mutant, "  SELECT countIf(", "  SELECT any(query) AS q_text, countIf(", 1)
	s3bMutant := strings.Replace(byLabel["S3b"], "SELECT data_table, org_parts,", "SELECT data_table, leaked_key, org_parts,", 1)
	s3bMutant = strings.Replace(s3bMutant, "  SELECT 'work_items' AS data_table,", "  SELECT 'work_items' AS data_table, toString(any(k_lo)) AS leaked_key,", 1)
	s3bMutant = strings.Replace(s3bMutant, "  SELECT 'project_membership_transitions' AS data_table,", "  SELECT 'project_membership_transitions' AS data_table, toString(any(k_lo)) AS leaked_key,", 1)
	for name, text := range map[string]string{"S2-query-id-mutant": s2Mutant, "S1-query-text-mutant": s1Mutant, "S3b-key-mutant": s3bMutant} {
		if text == byLabel[strings.SplitN(name, "-", 2)[0]] {
			t.Fatalf("OUTPUT %s: mutant edit not applied", name)
		}
		if bad := leaks(name, text); len(bad) == 0 {
			t.Errorf("OUTPUT %s: nothing found, the cell cannot fail", name)
		} else {
			t.Logf("OUTPUT %s caught: %d findings", name, len(bad))
		}
	}

	// Phase C: merges on, the table merged to one part; the same statement again. Measurement, not a WANT.
	for _, statement := range []string{"SYSTEM START MERGES work_items", "OPTIMIZE TABLE work_items FINAL"} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("phase C: %s: %v", statement, err)
		}
	}
	for i := 1; i <= 3; i++ {
		phaseC = append(phaseC, runLike(fmt.Sprintf("c%d", i)))
	}
	probeC := runProbe("c")
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	likesC := repro("C", phaseC)
	repro("probe-no-condition-cache", []string{probeC})
	t.Logf("work_items active parts after phase C: %d", p2x14548Count(ctx, t, conn, "SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = 'work_items'"))
	a, b, c := likesA[2].readRows, likesB[0].readRows, likesC[len(likesC)-1].readRows
	verdict := "NOT REPRODUCED"
	if b > a+1000 && c+1000 < b {
		verdict = "REPRODUCED"
	}
	t.Logf("REPRO RESULT %s: read_rows last A (big + far part) %d, first B (+107-row overlapping part) %d, last B %d, last C (merged) %d",
		verdict, a, b, likesB[2].readRows, c)
}
