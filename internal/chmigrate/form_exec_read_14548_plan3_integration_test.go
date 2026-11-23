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

// Throwaway exec proof of a prod read form (READ-14548-PLAN-3): not for merge. p3x14548Form is the form file with the app org prefix swapped to f0f0f0f0 (range end f0f0f0f1); the sha pins the swapped copy.
const p3x14548FormSHA256 = "d99c1cc83d91d27a32e49b77f796b9e2399f74f302e6986dc54f1c469aed3637"

const p3x14548Form = `-- NOT EXECUTED. READ-14548-PLAN-3 (system tables and part index only; numbers and static names only). acr-mcp, 2026-11-23. acp FORM read first; then pm runs it once.
-- Replaces READ-14548-PLAN-2 (8b4525e8c63c, frozen). Adds the per-run reads that pm and acp asked for: S3 (every run from 08:00Z:
--   read_rows next to the part state at its start, and a PREDICTION column that can fail) and S4 (every run: the FINAL read path per table).
-- Exec leg: a throwaway CI leg runs each statement (FORM-TEMPLATE-3); its run id and job id are in READ-14548-PLAN-3.md only. The CI copy
--   swaps the org prefix (f0f0f0f0 -> f0f0f0f0, f0f0f0f1 -> f0f0f0f1) and pins the sha of the swapped copy; this file is the prod text.
--   Until that leg is green and acp reads it, pm does not run this file.
-- Why (CHAOS-14548): projector statement 7cbf3538fee3 read 163,241 rows at 08:56Z and 209,805 at 09:02Z (first run after the acr roll at
--   09:01Z) and stayed at 209-214k. Lead hypothesis (reproduced on CI by PLAN-2's leg): a new work_items part that intersects the key range
--   of the big part moves FINAL from the no-merge read to the merge read until the parts merge. acp: the after-runs stayed flat across
--   merges, so the theory needs the part state at EVERY run. pm: the prediction must be a WANT that can fail.
-- S0: size proof of each log window. S1, S2: PLAN-2 S1, S2 unchanged (the 08:56 and 09:02 runs: read counters; read steps).
-- S3: every run from 08:00Z: read_rows + part state + theory_ok. S4: every run from 08:00Z: FINAL path per table. S5: PLAN-2 S3b (now).
-- Output: no query text, no query_id, no key value, no org id, no String setting value, no part name. Names are static (event names,
--   table names, processor names, run sides / kinds, window names).
-- Org scope: S0-S2, S4 read system tables only (no org rows). S3 and S5 read the part index of 2 data tables; each granule range is cut
--   to the org range ['f0f0f0f0', 'f0f0f0f1') (the prefix filter in range form: a mark holds the FIRST key of its granule); counts leave.
-- Gates: a log-table read is gated on the rows of the granules that can hold its window or its run seconds (part index = upper bound of
--   the primary-key read); each part index read is gated first on the marks of the table (system.parts). part_log is gated on its day
--   parts; the data part index on the marks of each data table. Every gate throws code 395 STOP-COST-GATE before a data row is read.
--   WORK MATH: READ-14548-PLAN-3.md.
-- Named limits: (a) the log tables are per node: S1 runs_before = 0 or runs_after = 0, or S3 with no 'run' row = STOP. (b) S2 with fewer
--   than 2 run_side values, or S4 with a run that has no row = the runs did not log processors. (c) S3 'now' row wi_parts !=
--   wi_active_now = part_log on this node misses events. (d) code 47 or 36 = the log sorting key or the part index differs from 26.x:
--   STOP. (e) S3 / S5 error = no part index on this engine.
-- Match key: startsWith(query, 'SELECT source, uniqExact(subject_kind') + type = 'QueryFinish' (edge_counts.go:50), as in PLAN-1/2.
--   The query column is read to match, never output.
-- SELECT only; no SET / SETTINGS.

-- S0: size proof, one row per log table and window. WANT 4 rows, key_ok = 1, window_rows > 0 (0 = wrong day, TTL or table off).
--   window_name 0850_0910 = the two runs of S1/S2; from_0800 = every run of the day from 08:00Z (S3, S4); run_seconds = the
--   processors_profile_log granules that hold the finish second (+-1 s) of a run from 08:00Z (S4). window_rows is the upper bound of the
--   primary-key read of that window; caps: query_log 1,000,000 (each window), processors_profile_log 4,000,000 (0850_0910) and 2,000,000
--   (run_seconds). Gates: marks of each log table <= 400,000; the run_seconds branch reads query_log under the from_0800 gate.
SELECT log_table, window_name, key_ok, parts_all, marks_all, window_parts, window_granules, window_rows
FROM (
  SELECT 'query_log' AS log_table, '0850_0910' AS window_name,
         (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) AS key_ok,
         (SELECT count() FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') AS parts_all,
         (SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') AS marks_all,
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
  SELECT 'query_log' AS log_table, 'from_0800' AS window_name,
         (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) AS key_ok,
         (SELECT count() FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') AS parts_all,
         (SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') AS marks_all,
         uniqExact(part_name) AS window_parts, count() AS window_granules, sum(rows_in_granule) AS window_rows
  FROM (
    WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
    SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
           leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
           mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
    FROM mergeTreeIndex('system', 'query_log')
    WHERE marks_gate = 0)
  WHERE rows_in_granule > 0
          AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-24 00:00:00', 'UTC'))
          AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:00:00', 'UTC')))
  UNION ALL
  SELECT 'processors_profile_log' AS log_table, '0850_0910' AS window_name,
         (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'processors_profile_log' AND startsWith(sorting_key, 'event_date, event_time')) AS key_ok,
         (SELECT count() FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') AS parts_all,
         (SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') AS marks_all,
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
  UNION ALL
  SELECT 'processors_profile_log' AS log_table, 'run_seconds' AS window_name,
         (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'processors_profile_log' AND startsWith(sorting_key, 'event_date, event_time')) AS key_ok,
         (SELECT count() FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') AS parts_all,
         (SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') AS marks_all,
         uniqExact(part_name) AS window_parts, count() AS window_granules, sum(rows_in_granule) AS window_rows
  FROM (
    WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
    SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
           leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
           mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
    FROM mergeTreeIndex('system', 'processors_profile_log')
    WHERE marks_gate = 0)
  WHERE rows_in_granule > 0
          AND arrayExists(s -> k <= tuple(toDate(s), s) AND (is_last OR k_next >= tuple(toDate(s), s)),
                          (SELECT groupArray(s) FROM (
                            WITH (SELECT throwIf(
                              (SELECT sum(rows_in_granule) FROM (
                                 WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
                                 SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
                                        leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                                        mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
                                 FROM mergeTreeIndex('system', 'query_log')
                                 WHERE marks_gate = 0)
                               WHERE rows_in_granule > 0
                                 AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-24 00:00:00', 'UTC'))
                                 AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:00:00', 'UTC')))) > 1000000
                              OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
                              'STOP-COST-GATE')) AS cost_gate
                            SELECT arrayJoin([event_time - 1, event_time, event_time + 1]) AS s
                            FROM system.query_log
                            WHERE cost_gate = 0
                                AND event_date = toDate('2026-11-23')
                                AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-24 00:00:00', 'UTC')
                                AND type = 'QueryFinish'
                                AND startsWith(query, 'SELECT source, uniqExact(subject_kind'))))
)
ORDER BY log_table, window_name;

-- S1: one row. WANT runs_before >= 1 and runs_after >= 1 (else STOP, limit a). before_* = the last run before 09:01Z (the 08:56 run),
--   after_* = the first run at or after 09:01Z (the 09:02 run). read_counters entry = (event name, before value, after value) for every
--   ProfileEvents name of either run that names a select, read, rows, mark, range, part, filter, prewhere, index, condition, granule, join, sort, merge or replacing counter (numbers only).
--   Control that can fail: runs_before and runs_after (0 = wrong day, prefix or type).
SELECT runs_before, runs_after, before_minute, after_minute,
       before_read_rows, after_read_rows, before_read_bytes, after_read_bytes, before_result_rows, after_result_rows,
       before_memory, after_memory, before_duration_ms, after_duration_ms,
       arrayMap(k -> tuple(k, before_events[k], after_events[k]),
                arrayFilter(k -> match(k, '(?i)select|read|rows|mark|range|part|filter|prewhere|index|condition|granule|join|sort|merg|replac'),
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

-- S3: one row per run of the statement from 08:00Z to the read time (run_kind 'run', run_time = its finish second), plus one 'now' row.
--   Per run: read_rows, memory,
--   Selected* counters, and the parts of work_items (wi_) and project_membership_transitions (pmt_) at the run start: count, levels,
--   rows per part, and per part other than the biggest part of the table now: overlapping = its org key range intersects the biggest
--   part's org key range (part index, parts still active now); disjoint = active now and no intersection (or no org rows);
--   unknown = gone now (merged), so its key range is not readable. Time basis: part_log create / merge / remove events of the day and
--   system.parts modification_time / remove_time (as PLAN-2 S3a); empty parts are left out.
-- PREDICTION (pm, a WANT that can fail): wi_state = 'overlap' (a small work_items part intersects the big part) predicts read_rows
--   >= 186,000 (the after level 209-214k); wi_state = 'none' predicts read_rows < 186,000 (the before level 163k). theory_ok = 1 when the
--   run matches, 0 when it does not, NULL when wi_state = 'unknown'. Any theory_ok = 0 = the overlap theory FAILS for that run.
--   Control that can fail: the 'now' row: wi_parts = wi_active_now (else part_log on this node misses events: states are not exact).
--   Gates: part_log day parts 6,000,000 rows / 3,000,000,000 bytes; query_log from_0800 window 1,000,000 rows; data table marks 100,000.
SELECT r.run_kind AS run_kind, r.run_time AS run_time, r.read_rows AS read_rows, r.memory AS memory,
       r.sel_parts AS selected_parts, r.sel_marks AS selected_marks, r.sel_ranges AS selected_ranges, r.sel_rows AS selected_rows,
       countIf(u.part_table = 'work_items') AS wi_parts,
       arraySort(groupArrayIf(u.level, u.part_table = 'work_items')) AS wi_levels,
       arrayReverseSort(groupArrayIf(u.part_rows, u.part_table = 'work_items')) AS wi_rows_per_part,
       countIf(u.part_table = 'work_items' AND u.overlap = 3) AS wi_big,
       countIf(u.part_table = 'work_items' AND u.overlap = 1) AS wi_small_overlapping,
       countIf(u.part_table = 'work_items' AND u.overlap = 0) AS wi_small_disjoint,
       countIf(u.part_table = 'work_items' AND u.overlap = 2) AS wi_small_unknown,
       toStartOfMinute(argMaxIf(u.created_at, u.max_block, u.part_table = 'work_items')) AS wi_newest_created_minute,
       countIf(u.part_table = 'project_membership_transitions') AS pmt_parts,
       arraySort(groupArrayIf(u.level, u.part_table = 'project_membership_transitions')) AS pmt_levels,
       arrayReverseSort(groupArrayIf(u.part_rows, u.part_table = 'project_membership_transitions')) AS pmt_rows_per_part,
       countIf(u.part_table = 'project_membership_transitions' AND u.overlap = 3) AS pmt_big,
       countIf(u.part_table = 'project_membership_transitions' AND u.overlap = 1) AS pmt_small_overlapping,
       countIf(u.part_table = 'project_membership_transitions' AND u.overlap = 0) AS pmt_small_disjoint,
       countIf(u.part_table = 'project_membership_transitions' AND u.overlap = 2) AS pmt_small_unknown,
       toStartOfMinute(argMaxIf(u.created_at, u.max_block, u.part_table = 'project_membership_transitions')) AS pmt_newest_created_minute,
       multiIf(wi_big = 0, 'unknown', wi_small_overlapping > 0, 'overlap', wi_small_unknown > 0, 'unknown', 'none') AS wi_state,
       if(wi_state = 'unknown' OR run_kind = 'now', NULL, (wi_state = 'overlap') = (read_rows >= 186000)) AS theory_ok,
       if(run_kind = 'now', (SELECT count() FROM system.parts WHERE active AND rows > 0 AND database = currentDatabase() AND table = 'work_items'), NULL) AS wi_active_now
FROM (
  SELECT 'run' AS run_kind, query_start_time AS run_start, event_time AS run_time, read_rows, toInt64(memory_usage) AS memory,
         ProfileEvents['SelectedParts'] AS sel_parts, ProfileEvents['SelectedMarks'] AS sel_marks,
         ProfileEvents['SelectedRanges'] AS sel_ranges, ProfileEvents['SelectedRows'] AS sel_rows
  FROM (
    WITH (SELECT throwIf(
      (SELECT sum(rows_in_granule) FROM (
         WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
         SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
                leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
         FROM mergeTreeIndex('system', 'query_log')
         WHERE marks_gate = 0)
       WHERE rows_in_granule > 0
         AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-24 00:00:00', 'UTC'))
         AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:00:00', 'UTC')))) > 1000000
      OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
      'STOP-COST-GATE')) AS cost_gate
    SELECT query_start_time, event_time, read_rows, memory_usage, ProfileEvents
    FROM system.query_log
    WHERE cost_gate = 0
        AND event_date = toDate('2026-11-23')
        AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-24 00:00:00', 'UTC')
        AND type = 'QueryFinish'
        AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
  )
  UNION ALL
  SELECT 'now', now(), now(), toUInt64(0), toInt64(0), toUInt64(0), toUInt64(0), toUInt64(0), toUInt64(0)
) AS r
CROSS JOIN (
  SELECT u0.part_table AS part_table, u0.name AS name, u0.created_at AS created_at, u0.removed_at AS removed_at,
         u0.part_rows AS part_rows, u0.level AS level, u0.max_block AS max_block,
         multiIf(ix.ix_found = 0, 2, ix.ix_big, 3, ix.ix_intersects, 1, 0) AS overlap
  FROM (
    SELECT part_table, name,
           coalesce(minIfOrNull(t, kind = 'create'), minIfOrNull(t, kind = 'part'), toDateTime(0)) AS created_at,
           minIfOrNull(t, kind = 'remove') AS removed_at,
           coalesce(maxIfOrNull(n, kind = 'part'), maxIfOrNull(n, kind = 'create'), 0) AS part_rows,
           countIf(kind IN ('part', 'create')) > 0 AS rows_known,
           toUInt32OrZero(splitByChar('_', name)[4]) AS level,
           toUInt64OrZero(splitByChar('_', name)[3]) AS max_block
    FROM (
      SELECT event_table AS part_table, tupleElement(e, 1) AS name, tupleElement(e, 2) AS kind, tupleElement(e, 3) AS t, tupleElement(e, 4) AS n
      FROM (
        WITH (SELECT throwIf(
          (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 6000000
          OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 3000000000,
          'STOP-COST-GATE')) AS cost_gate
        SELECT toString(table) AS event_table, arrayConcat(
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
      SELECT toString(table) AS p_table, tupleElement(e, 1) AS p_name, tupleElement(e, 2) AS p_kind, tupleElement(e, 3) AS p_t, tupleElement(e, 4) AS p_n
      FROM system.parts
      ARRAY JOIN arrayConcat([tuple(name, 'part', modification_time, rows)],
                             if(NOT active AND remove_time > toDateTime(0), [tuple(name, 'remove', remove_time, toUInt64(0))], [])) AS e
      WHERE database = currentDatabase() AND table IN ('project_membership_transitions', 'work_items')
    )
    GROUP BY part_table, name
    HAVING rows_known = 0 OR part_rows > 0
  ) AS u0
  LEFT JOIN (
    SELECT 'work_items' AS ix_table, part_name AS ix_name, toUInt8(1) AS ix_found, is_big AS ix_big,
           has_org AND org_lo <= big_hi AND org_hi >= big_lo AS ix_intersects
    FROM (
      SELECT part_name, has_org, org_lo, org_hi, part_rows_all = max(part_rows_all) OVER () AS is_big,
             argMax(org_lo, part_rows_all) OVER () AS big_lo, argMax(org_hi, part_rows_all) OVER () AS big_hi
      FROM (
        SELECT part_name, sum(rows_in_granule) AS part_rows_all,
               countIf(rows_in_granule > 0 AND k_lo <= k_hi) > 0 AS has_org,
               minIf(k_lo, rows_in_granule > 0 AND k_lo <= k_hi) AS org_lo,
               maxIf(k_hi, rows_in_granule > 0 AND k_lo <= k_hi) AS org_hi
        FROM (
          SELECT part_name, rows_in_granule,
                 if(k > CAST(tuple('f0f0f0f0', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), k, CAST(tuple('f0f0f0f0', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)')) AS k_lo,
                 if(is_last OR k_next > CAST(tuple('f0f0f0f1', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), CAST(tuple('f0f0f0f1', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), k_next) AS k_hi
          FROM (
            WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'work_items') > 100000, 'STOP-COST-GATE')) AS cost_gate
            SELECT part_name, rows_in_granule, tuple(org_id, repo_id, work_item_id) AS k,
                   leadInFrame(tuple(org_id, repo_id, work_item_id)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                   mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
            FROM mergeTreeIndex(currentDatabase(), 'work_items')
            WHERE cost_gate = 0))
        GROUP BY part_name))
    UNION ALL
    SELECT 'project_membership_transitions' AS ix_table, part_name AS ix_name, toUInt8(1) AS ix_found, is_big AS ix_big,
           has_org AND org_lo <= big_hi AND org_hi >= big_lo AS ix_intersects
    FROM (
      SELECT part_name, has_org, org_lo, org_hi, part_rows_all = max(part_rows_all) OVER () AS is_big,
             argMax(org_lo, part_rows_all) OVER () AS big_lo, argMax(org_hi, part_rows_all) OVER () AS big_hi
      FROM (
        SELECT part_name, sum(rows_in_granule) AS part_rows_all,
               countIf(rows_in_granule > 0 AND k_lo <= k_hi) > 0 AS has_org,
               minIf(k_lo, rows_in_granule > 0 AND k_lo <= k_hi) AS org_lo,
               maxIf(k_hi, rows_in_granule > 0 AND k_lo <= k_hi) AS org_hi
        FROM (
          SELECT part_name, rows_in_granule,
                 if(k > CAST(tuple('f0f0f0f0', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), k, CAST(tuple('f0f0f0f0', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)')) AS k_lo,
                 if(is_last OR k_next > CAST(tuple('f0f0f0f1', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), CAST(tuple('f0f0f0f1', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), k_next) AS k_hi
          FROM (
            WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'project_membership_transitions') > 100000, 'STOP-COST-GATE')) AS cost_gate
            SELECT part_name, rows_in_granule, tuple(org_id, toString(subject_kind), repo_id, subject_id, occurred_at, event_id) AS k,
                   leadInFrame(tuple(org_id, toString(subject_kind), repo_id, subject_id, occurred_at, event_id)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                   mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
            FROM mergeTreeIndex(currentDatabase(), 'project_membership_transitions')
            WHERE cost_gate = 0))
        GROUP BY part_name))
  ) AS ix ON u0.part_table = ix.ix_table AND u0.name = ix.ix_name
) AS u
WHERE u.created_at <= r.run_start AND (u.removed_at IS NULL OR u.removed_at > r.run_start)
GROUP BY r.run_kind, r.run_start, r.run_time, r.read_rows, r.memory, r.sel_parts, r.sel_marks, r.sel_ranges, r.sel_rows
ORDER BY r.run_start, run_kind;

-- S4: per run from 08:00Z (run_time = finish second, as S3) and per read table: the FINAL read path from processors_profile_log. merge_processors > 0 = that table was
--   read through the FINAL merge path (ReplacingSorted / FilterSortedStreamByRange); 0 = the no-merge path. select_output_rows =
--   MergeTreeSelect output rows. The processors read is limited by the primary key to the finish second (+-1 s) of each run; the query_id
--   join stays inside. WANT: one work_items row and one project_membership_transitions row per run.
-- PREDICTION (pm, a WANT that can fail, no unknowns): on the work_items row, theory_ok = 1 when the merge path (merge_processors > 0)
--   comes exactly with a high read (run_read_rows >= 186,000), 0 when not. Any theory_ok = 0 = the overlap theory FAILS for that run. Gates: processors_profile_log run_seconds granules 2,000,000 rows;
--   query_log from_0800 window 1,000,000 rows (3 reads).
SELECT r.run_time AS run_time, r.run_read_rows AS run_read_rows, p.plan_step_description AS step_table,
       uniqExact(p.plan_step) AS steps,
       countIf(startsWith(p.name, 'MergeTreeSelect')) AS select_processors,
       sumIf(p.output_rows, startsWith(p.name, 'MergeTreeSelect')) AS select_output_rows,
       countIf(p.name IN ('ReplacingSorted', 'FilterSortedStreamByRange')) AS merge_processors,
       sumIf(p.input_rows, p.name = 'ReplacingSorted') AS replacing_input_rows,
       if(endsWith(step_table, '.work_items'), (merge_processors > 0) = (run_read_rows >= 186000), NULL) AS theory_ok
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows_in_granule) FROM (
       WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
       SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
              leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
              mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
       FROM mergeTreeIndex('system', 'processors_profile_log')
       WHERE marks_gate = 0)
     WHERE rows_in_granule > 0
       AND arrayExists(s -> k <= tuple(toDate(s), s) AND (is_last OR k_next >= tuple(toDate(s), s)),
                        (SELECT groupArray(s) FROM (
                          WITH (SELECT throwIf(
                            (SELECT sum(rows_in_granule) FROM (
                               WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
                               SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
                                      leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                                      mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
                               FROM mergeTreeIndex('system', 'query_log')
                               WHERE marks_gate = 0)
                             WHERE rows_in_granule > 0
                               AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-24 00:00:00', 'UTC'))
                               AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:00:00', 'UTC')))) > 1000000
                            OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
                            'STOP-COST-GATE')) AS cost_gate
                          SELECT arrayJoin([event_time - 1, event_time, event_time + 1]) AS s
                          FROM system.query_log
                          WHERE cost_gate = 0
                              AND event_date = toDate('2026-11-23')
                              AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-24 00:00:00', 'UTC')
                              AND type = 'QueryFinish'
                              AND startsWith(query, 'SELECT source, uniqExact(subject_kind'))))) > 2000000
    OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'processors_profile_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
    'STOP-COST-GATE')) AS cost_gate
  SELECT query_id, name, plan_step, plan_step_description, input_rows, output_rows
  FROM system.processors_profile_log
  WHERE cost_gate = 0
    AND event_date = toDate('2026-11-23')
    AND event_time IN (
      SELECT arrayJoin([event_time - 1, event_time, event_time + 1])
      FROM (
        WITH (SELECT throwIf(
          (SELECT sum(rows_in_granule) FROM (
             WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
             SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
                    leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                    mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
             FROM mergeTreeIndex('system', 'query_log')
             WHERE marks_gate = 0)
           WHERE rows_in_granule > 0
             AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-24 00:00:00', 'UTC'))
             AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:00:00', 'UTC')))) > 1000000
          OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
          'STOP-COST-GATE')) AS cost_gate
        SELECT event_time
        FROM system.query_log
        WHERE cost_gate = 0
            AND event_date = toDate('2026-11-23')
            AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-24 00:00:00', 'UTC')
            AND type = 'QueryFinish'
            AND startsWith(query, 'SELECT source, uniqExact(subject_kind')))
    AND plan_step_name = 'ReadFromMergeTree'
) AS p
INNER JOIN (
  SELECT query_id, event_time AS run_time, read_rows AS run_read_rows
  FROM (
    WITH (SELECT throwIf(
      (SELECT sum(rows_in_granule) FROM (
         WITH (SELECT throwIf((SELECT sum(marks) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 400000, 'STOP-COST-GATE')) AS marks_gate
         SELECT part_name, rows_in_granule, tuple(event_date, event_time) AS k,
                leadInFrame(tuple(event_date, event_time)) OVER (PARTITION BY part_name ORDER BY mark_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS k_next,
                mark_number = max(mark_number) OVER (PARTITION BY part_name) AS is_last
         FROM mergeTreeIndex('system', 'query_log')
         WHERE marks_gate = 0)
       WHERE rows_in_granule > 0
         AND k <= tuple(toDate('2026-11-23'), toDateTime('2026-11-24 00:00:00', 'UTC'))
         AND (is_last OR k_next >= tuple(toDate('2026-11-23'), toDateTime('2026-11-23 08:00:00', 'UTC')))) > 1000000
      OR (SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log' AND startsWith(sorting_key, 'event_date, event_time')) != 1,
      'STOP-COST-GATE')) AS cost_gate
    SELECT query_id, event_time, read_rows
    FROM system.query_log
    WHERE cost_gate = 0
        AND event_date = toDate('2026-11-23')
        AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-24 00:00:00', 'UTC')
        AND type = 'QueryFinish'
        AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
  )
) AS r ON p.query_id = r.query_id
GROUP BY run_time, run_read_rows, step_table
ORDER BY run_time, step_table;

-- S5 (= PLAN-2 S3b): now, per data table: the parts whose granules hold org-range keys, the newest of them (highest max block), and how many OTHER parts
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
               if(k > CAST(tuple('f0f0f0f0', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), k,
                  CAST(tuple('f0f0f0f0', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)')) AS k_lo,
               if(is_last OR k_next > CAST(tuple('f0f0f0f1', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'),
                  CAST(tuple('f0f0f0f1', '00000000-0000-0000-0000-000000000000', ''), 'Tuple(String, UUID, String)'), k_next) AS k_hi
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
               if(k > CAST(tuple('f0f0f0f0', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), k,
                  CAST(tuple('f0f0f0f0', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)')) AS k_lo,
               if(is_last OR k_next > CAST(tuple('f0f0f0f1', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'),
                  CAST(tuple('f0f0f0f1', '', '00000000-0000-0000-0000-000000000000', '', '1970-01-01 00:00:00.000', ''), 'Tuple(String, String, UUID, String, DateTime64(3), String)'), k_next) AS k_hi
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
	p3x14548OrgA = "f0f0f0f0-0000-4000-8000-000000014548"
	p3x14548OrgB = "14548000-0000-4000-8000-0000000000bb"
)

// Synthetic rows. work_items: one merged part (org B 2,000 rows, then org A 40,900 rows over 40 repos; only repos 0-2 have a project),
// then merges stop and a 21-row org A part lands after every org A key (repo ffff...). transitions: one 20-row org A part.
const p3x14548Fixture = `
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
const p3x14548Overlap = `INSERT INTO work_items (org_id, repo_id, work_item_id, provider, project_id, created_at, updated_at, last_synced)
SELECT 'ORG_A', toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(n % 40), 12, '0'))), concat('wi-', leftPad(toString(n), 6, '0')),
       'linear', if(n % 40 < 3, 'p1', ''), toDateTime64('2026-11-01 00:00:00', 3), toDateTime64('2026-11-23 09:00:00', 3),
       toDateTime64('2026-11-23 09:00:00', 3)
FROM (SELECT 2000 + number * 381 AS n FROM numbers(107))`

// The same first token as the projector statement (edge_counts.go:50); a transitions arm and a work_items arm, both FINAL.
const p3x14548Like = `SELECT source, uniqExact(subject_kind, subject_id) AS edge_count
FROM (
  SELECT 'transition' AS source, subject_kind, subject_id FROM project_membership_transitions FINAL WHERE org_id = 'ORG_A'
  UNION ALL
  SELECT 'work_item_column' AS source, 'work_item' AS subject_kind, work_item_id AS subject_id FROM work_items FINAL
  WHERE org_id = 'ORG_A' AND notEmpty(project_id) AND provider != 'gitlab'
)
GROUP BY source`

const p3x14548LabelList = "S0,S1,S2,S3,S4,S5"

type p3x14548Statement struct{ label, text string }

func p3x14548Orgs(text string) string {
	return strings.NewReplacer("'ORG_A'", "'"+p3x14548OrgA+"'", "'ORG_B'", "'"+p3x14548OrgB+"'").Replace(text)
}

func p3x14548Split(body string) []p3x14548Statement {
	labels := strings.Split(p3x14548LabelList, ",")
	var out []p3x14548Statement
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
		out = append(out, p3x14548Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func p3x14548Deep(value reflect.Value, dbType string) string {
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
			parts[i] = p3x14548Deep(value.Index(i), "")
		}
		return "[" + strings.Join(parts, " ") + "]"
	case reflect.Map:
		keys := value.MapKeys()
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, p3x14548Deep(k, "")+":"+p3x14548Deep(value.MapIndex(k), ""))
		}
		sort.Strings(parts)
		return "{" + strings.Join(parts, " ") + "}"
	}
	return fmt.Sprint(value.Interface())
}

func p3x14548Run(ctx context.Context, conn driver.Conn, text, comment string, extra clickhouse.Settings) ([]map[string]string, error) {
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
			row[name] = p3x14548Deep(reflect.ValueOf(cells[i]), types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func p3x14548Log(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
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

func p3x14548Count(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func p3x14548Uint(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// p3x14548Caps returns the byte offsets of every cap literal "> N" (N a cap of this form) in text.
func p3x14548Caps(text string) [][2]int {
	caps := map[string]bool{"400000": true, "1000000": true, "2000000": true, "4000000": true, "6000000": true, "3000000000": true, "100000": true}
	var out [][2]int
	for _, m := range regexp.MustCompile(`> (\d+)\b`).FindAllStringSubmatchIndex(text, -1) {
		if caps[text[m[2]:m[3]]] {
			out = append(out, [2]int{m[0], m[1]})
		}
	}
	return out
}

type p3x14548Logged struct {
	comment, id, phase                                string
	readRows, readBytes, resultRows, memory, duration uint64
	finish                                            time.Time
	events                                            map[string]uint64
}

func TestReadPlan3Form14548ExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(p3x14548Form))
	if got := hex.EncodeToString(sum[:]); got != p3x14548FormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, p3x14548FormSHA256)
	}
	statements := p3x14548Split(p3x14548Form)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != p3x14548LabelList {
		t.Fatalf("form statements = %v", labels)
	}
	code := chmigrate.StripLineComments(p3x14548Form)
	for _, forbidden := range []string{`(?i)\b(FORMAT|EXPLAIN|INSERT|FINAL|LIMIT|OPTIMIZE|ALTER)\b`, `(?i)\bSETTINGS\s+\w+\s*=`, `(?im)^\s*SET\b`} {
		if regexp.MustCompile(forbidden).MatchString(code) {
			t.Fatalf("form holds a forbidden keyword (%s)", forbidden)
		}
	}
	// Static gate shape: cap literals, throw sites, log-table reads and part index reads per statement.
	wantCaps := map[string]map[string]int{
		"S0": {"400000": 5, "1000000": 1},
		"S1": {"400000": 1, "1000000": 1},
		"S2": {"400000": 2, "4000000": 1, "1000000": 1},
		"S3": {"6000000": 1, "3000000000": 1, "400000": 1, "1000000": 1, "100000": 2},
		"S4": {"400000": 4, "2000000": 1, "1000000": 3},
		"S5": {"100000": 2},
	}
	wantThrows := map[string]int{"S0": 6, "S1": 2, "S2": 4, "S3": 5, "S4": 8, "S5": 2}
	wantReads := map[string]map[string]int{
		"S0": {"query_log": 1, "processors_profile_log": 0, "part_log": 0},
		"S1": {"query_log": 1, "processors_profile_log": 0, "part_log": 0},
		"S2": {"query_log": 1, "processors_profile_log": 1, "part_log": 0},
		"S3": {"query_log": 1, "processors_profile_log": 0, "part_log": 1},
		"S4": {"query_log": 3, "processors_profile_log": 1, "part_log": 0},
		"S5": {"query_log": 0, "processors_profile_log": 0, "part_log": 0},
	}
	wantIndex := map[string]int{"S0": 5, "S1": 1, "S2": 2, "S3": 3, "S4": 4, "S5": 2}
	wantFires := 0
	for _, s := range statements {
		got := map[string]int{}
		for _, c := range p3x14548Caps(s.text) {
			got[strings.TrimPrefix(s.text[c[0]:c[1]], "> ")]++
			wantFires++
		}
		if !reflect.DeepEqual(got, wantCaps[s.label]) {
			t.Fatalf("%s: cap literals %v, want %v", s.label, got, wantCaps[s.label])
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != wantThrows[s.label] {
			t.Fatalf("%s: %d throw sites, want %d", s.label, n, wantThrows[s.label])
		}
		for table, want := range wantReads[s.label] {
			if n := strings.Count(s.text, "FROM system."+table+"\n"); n != want {
				t.Fatalf("%s: %d reads of system.%s, want %d", s.label, n, table, want)
			}
		}
		if n := strings.Count(s.text, "FROM mergeTreeIndex("); n != wantIndex[s.label] {
			t.Fatalf("%s: %d part index reads, want %d", s.label, n, wantIndex[s.label])
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

	// Filler: many processors_profile_log rows at other seconds, so the run-second read must prune by the primary key to stay in bound.
	for i := 0; i < 1500; i++ {
		rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_processors_profiles": 1, "max_threads": 4})),
			"SELECT sum(number) FROM numbers(20000)")
		if err != nil {
			t.Fatalf("filler: %v", err)
		}
		rows.Close()
	}
	time.Sleep(1100 * time.Millisecond)

	for _, statement := range chmigrate.SplitStatements(p3x14548Orgs(p3x14548Fixture)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}
	like := p3x14548Orgs(p3x14548Like)
	stamp := time.Now().UnixNano()
	phases := map[string][]string{}
	runPhase := func(phase string) {
		for i := 1; i <= 3; i++ {
			time.Sleep(1100 * time.Millisecond)
			comment := fmt.Sprintf("p3x14548-like-%s%d-%d", phase, i, stamp)
			if _, err := p3x14548Run(ctx, conn, like, comment, clickhouse.Settings{"log_processors_profiles": 1}); err != nil {
				t.Fatalf("like run %s%d: %v", phase, i, err)
			}
			phases[phase] = append(phases[phase], comment)
		}
	}
	overlapName := func() string {
		var name string
		if err := conn.QueryRow(ctx, "SELECT name FROM system.parts WHERE active AND database = currentDatabase() AND table = 'work_items' AND rows = 107").Scan(&name); err != nil {
			t.Fatalf("overlapping part: %v", err)
		}
		return name
	}
	// A: big part + a part after every org key. cut. B: + an overlapping part X. C: X dropped. D: + an overlapping part Y (kept).
	runPhase("a")
	time.Sleep(1100 * time.Millisecond)
	var cut, day, nextDay string
	if err := conn.QueryRow(ctx, "SELECT toString(now('UTC')), toString(toDate(now('UTC'))), toString(toDate(now('UTC')) + 1)").Scan(&cut, &day, &nextDay); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := conn.Exec(ctx, p3x14548Orgs(p3x14548Overlap)); err != nil {
		t.Fatalf("overlap part X: %v", err)
	}
	runPhase("b")
	time.Sleep(1100 * time.Millisecond) // part changes and run starts never share a second (the part times are seconds)
	if err := conn.Exec(ctx, "ALTER TABLE work_items DROP PART '"+overlapName()+"'"); err != nil {
		t.Fatalf("drop X: %v", err)
	}
	runPhase("c")
	time.Sleep(1100 * time.Millisecond)
	if err := conn.Exec(ctx, p3x14548Orgs(p3x14548Overlap)); err != nil {
		t.Fatalf("overlap part Y: %v", err)
	}
	runPhase("d")

	dated := func(text string) string {
		text = strings.ReplaceAll(text, "2026-11-23 08:50:00", day+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 09:10:00", nextDay+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 08:00:00", day+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-24 00:00:00", nextDay+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 09:01:00", cut)
		text = strings.ReplaceAll(text, ">= 186000", ">= 47000")
		return strings.ReplaceAll(text, "2026-11-23", day)
	}
	t.Logf("dated copies: windows %s 00:00:00 .. %s 00:00:00 UTC, cut %s, theory threshold 47000 (the CI clock and fixture, not prod)", day, nextDay, cut)

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the logs are not flushable by this user: %v", err)
	}
	loadLike := func(comment string) p3x14548Logged {
		r := p3x14548Logged{comment: comment, events: map[string]uint64{}}
		if err := conn.QueryRow(ctx, `SELECT query_id, read_rows, read_bytes, result_rows, toUInt64(memory_usage), query_duration_ms, event_time, ProfileEvents
			FROM system.query_log WHERE log_comment = ? AND type = 'QueryFinish'`, comment).Scan(&r.id, &r.readRows, &r.readBytes, &r.resultRows, &r.memory, &r.duration, &r.finish, &r.events); err != nil {
			t.Fatalf("like run %s in query_log: %v", comment, err)
		}
		return r
	}
	runs := map[string][]p3x14548Logged{}
	var all []p3x14548Logged
	for _, phase := range []string{"a", "b", "c", "d"} {
		for _, c := range phases[phase] {
			r := loadLike(c)
			r.phase = phase
			t.Logf("REPRO %s: read_rows=%d memory=%d SelectedRows=%d SelectedMarks=%d SelectedParts=%d SelectedRanges=%d finish=%s",
				strings.Split(c, "-")[2], r.readRows, r.memory, r.events["SelectedRows"], r.events["SelectedMarks"], r.events["SelectedParts"], r.events["SelectedRanges"], r.finish.UTC().Format("15:04:05"))
			runs[phase] = append(runs[phase], r)
			all = append(all, r)
		}
	}
	finishes := map[string]bool{}
	for _, r := range all {
		finishes[r.finish.UTC().Format("2006-01-02 15:04:05")] = true
	}
	if len(finishes) != len(all) {
		t.Fatalf("runs share a finish second (%d seconds for %d runs): the per-run cells need one run per second", len(finishes), len(all))
	}
	before, after := runs["a"][2], runs["b"][0]
	low, high := runs["a"][0].readRows, runs["b"][0].readRows
	for _, r := range all {
		want := low
		if r.phase == "b" || r.phase == "d" {
			want = high
		}
		if r.readRows != want {
			t.Errorf("fixture: run %s read_rows %d, want %d (A/C %d, B/D %d)", r.comment, r.readRows, want, low, high)
		}
	}
	if !(low < 47000 && high >= 47000) {
		t.Fatalf("fixture: low %d / high %d do not straddle the CI threshold 47000", low, high)
	}

	passComments := map[string]string{}
	passRows := map[string][]map[string]string{}
	for _, s := range statements {
		passComments[s.label] = fmt.Sprintf("p3x14548-pass-%s-%d", s.label, time.Now().UnixNano())
		got, err := p3x14548Run(ctx, conn, dated(s.text), passComments[s.label], nil)
		if err != nil {
			t.Errorf("PASS %s: SQL error: %v", s.label, err)
			continue
		}
		passRows[s.label] = got
		t.Logf("PASS %s ROWS %v", s.label, got)
	}

	// S0: 4 windows, key ok, rows > 0; the run-second window is a small share of the processors log (the filler).
	ppDay := p3x14548Count(ctx, t, conn, "SELECT count() FROM system.processors_profile_log WHERE event_date = toDate(?)", day)
	s0 := map[string]map[string]string{}
	for _, r := range passRows["S0"] {
		s0[r["log_table"]+"|"+r["window_name"]] = r
	}
	for _, k := range []string{"processors_profile_log|0850_0910", "processors_profile_log|run_seconds", "query_log|0850_0910", "query_log|from_0800"} {
		r := s0[k]
		if r == nil || r["key_ok"] != "1" || p3x14548Uint(r["window_rows"]) == 0 || p3x14548Uint(r["marks_all"]) == 0 {
			t.Errorf("S0 %s: %v, want key_ok 1, window_rows > 0, marks_all > 0", k, r)
		}
	}
	if len(passRows["S0"]) != 4 {
		t.Errorf("S0: %d rows, want 4", len(passRows["S0"]))
	}
	wRS := p3x14548Uint(s0["processors_profile_log|run_seconds"]["window_rows"])
	t.Logf("S0 run_seconds window %d rows of %d processors_profile_log rows today", wRS, ppDay)
	if wRS == 0 || 2*wRS > ppDay {
		t.Errorf("S0 run_seconds window %d is not a small share of %d rows: the bound cell would prove nothing", wRS, ppDay)
	}

	// S1, S2: as PLAN-2 (before = last A run, after = first B run).
	counterName := regexp.MustCompile(`(?i)select|read|rows|mark|range|part|filter|prewhere|index|condition|granule|join|sort|merg|replac`)
	if r := passRows["S1"]; len(r) != 1 {
		t.Errorf("S1: %d rows, want 1", len(r))
	} else {
		s1 := r[0]
		want := map[string]string{
			"runs_before": "3", "runs_after": "9",
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
	}
	stepKey := func(table, step, processor string) string { return table + "|" + step + "|" + processor }
	processorsOf := func(id string) map[string][4]uint64 {
		rows, err := conn.Query(ctx, `SELECT plan_step_description, toString(plan_step), name, count(), sum(input_rows), sum(output_rows), sum(output_bytes)
			FROM system.processors_profile_log WHERE query_id = ? AND plan_step_name = 'ReadFromMergeTree'
			GROUP BY plan_step_description, plan_step, name`, id)
		if err != nil {
			t.Fatalf("processors of %s: %v", id, err)
		}
		defer rows.Close()
		out := map[string][4]uint64{}
		for rows.Next() {
			var table, step, processor string
			var v [4]uint64
			if err := rows.Scan(&table, &step, &processor, &v[0], &v[1], &v[2], &v[3]); err != nil {
				t.Fatal(err)
			}
			out[stepKey(table, step, processor)] = v
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
		want := map[string]string{}
		for k, v := range processorsOf(id) {
			want[k] = fmt.Sprintf("%d %d %d %d", v[0], v[1], v[2], v[3])
		}
		if !reflect.DeepEqual(gotSteps[side], want) {
			t.Errorf("S2 %s = %v, want %v", side, gotSteps[side], want)
		}
	}

	// S3: one row per run + 'now'. Run values = the runs' own log values; wi_state / theory_ok by phase.
	wantState := map[string]string{"a": "none 1", "b": "unknown NULL", "c": "none 1", "d": "overlap 1"}
	var gotS3, wantS3 []string
	for _, r := range all {
		wantS3 = append(wantS3, fmt.Sprintf("%s %d %d %d %d %d %d %s", r.finish.UTC().Format("2006-01-02 15:04:05"), r.readRows, r.memory,
			r.events["SelectedParts"], r.events["SelectedMarks"], r.events["SelectedRanges"], r.events["SelectedRows"], wantState[r.phase]))
	}
	nowRows := 0
	for _, r := range passRows["S3"] {
		if r["run_kind"] == "now" {
			nowRows++
			if r["wi_parts"] != r["wi_active_now"] || r["wi_parts"] != "3" {
				t.Errorf("S3 now: wi_parts %s, wi_active_now %s, want both 3 (big, far, Y)", r["wi_parts"], r["wi_active_now"])
			}
			continue
		}
		if m := r["wi_newest_created_minute"]; m == "" || strings.HasPrefix(m, "1970") {
			t.Errorf("S3 run %s: wi_newest_created_minute %q", r["run_time"], m)
		}
		gotS3 = append(gotS3, strings.Join([]string{r["run_time"], r["read_rows"], r["memory"], r["selected_parts"], r["selected_marks"],
			r["selected_ranges"], r["selected_rows"], r["wi_state"], r["theory_ok"]}, " "))
	}
	sort.Strings(gotS3)
	sort.Strings(wantS3)
	if nowRows != 1 || !reflect.DeepEqual(gotS3, wantS3) {
		t.Errorf("S3 runs (now rows %d) = %v, want %v", nowRows, gotS3, wantS3)
	}

	// S4: per run and table; work_items merge path exactly on B and D; outputs = independent processors read per run.
	byFinish := map[string]p3x14548Logged{}
	for _, r := range all {
		byFinish[r.finish.UTC().Format("2006-01-02 15:04:05")] = r
	}
	s4Tables := map[string]int{}
	for _, r := range passRows["S4"] {
		run, ok := byFinish[r["run_time"]]
		if !ok {
			t.Errorf("S4 row for an unknown run %s", r["run_time"])
			continue
		}
		table := r["step_table"][strings.LastIndex(r["step_table"], ".")+1:]
		s4Tables[table]++
		var steps = map[string]bool{}
		var selectN, selectRows, mergeN, replacingIn uint64
		for k, v := range processorsOf(run.id) {
			parts := strings.SplitN(k, "|", 3)
			if parts[0] != r["step_table"] {
				continue
			}
			steps[parts[1]] = true
			switch {
			case strings.HasPrefix(parts[2], "MergeTreeSelect"):
				selectN += v[0]
				selectRows += v[2]
			case parts[2] == "ReplacingSorted" || parts[2] == "FilterSortedStreamByRange":
				mergeN += v[0]
			}
			if parts[2] == "ReplacingSorted" {
				replacingIn += v[1]
			}
		}
		theory := "NULL"
		if table == "work_items" {
			theory = "1"
			if (mergeN > 0) != (run.readRows >= 47000) {
				theory = "0"
			}
			if wantMerge := run.phase == "b" || run.phase == "d"; (mergeN > 0) != wantMerge {
				t.Errorf("S4 fixture: run %s phase %s work_items merge processors %d", r["run_time"], run.phase, mergeN)
			}
		}
		want := fmt.Sprintf("%d %d %d %d %d %d %s", run.readRows, len(steps), selectN, selectRows, mergeN, replacingIn, theory)
		got := strings.Join([]string{r["run_read_rows"], r["steps"], r["select_processors"], r["select_output_rows"], r["merge_processors"], r["replacing_input_rows"], r["theory_ok"]}, " ")
		if got != want {
			t.Errorf("S4 %s %s = %s, want %s", r["run_time"], table, got, want)
		}
	}
	if s4Tables["work_items"] != len(all) || s4Tables["project_membership_transitions"] != len(all) {
		t.Errorf("S4 rows per table %v, want %d each", s4Tables, len(all))
	}

	// S5: the newest part Y intersects the big part only.
	gotS5 := map[string]string{}
	for _, r := range passRows["S5"] {
		gotS5[r["data_table"]] = strings.Join([]string{r["org_parts"], r["newest_parts"], r["newest_org_rows"], r["other_parts_intersecting"], r["other_rows_intersecting"], r["other_org_rows"]}, " ")
	}
	if want := map[string]string{"project_membership_transitions": "1 1 20 0 0 0", "work_items": "3 1 107 1 42900 42921"}; !reflect.DeepEqual(gotS5, want) {
		t.Errorf("S5 = %v, want %v", gotS5, want)
	}

	// Mutants: each must change the result so its WANT fails.
	mutateWith := func(label, text string) []map[string]string {
		got, err := p3x14548Run(ctx, conn, text, fmt.Sprintf("p3x14548-mutate-%s-%d", label, time.Now().UnixNano()), nil)
		if err != nil {
			t.Fatalf("MUTATE %s: %v", label, err)
		}
		t.Logf("MUTATE %s ROWS %v", label, got)
		return got
	}
	mutants := 0
	replaced := func(label, from, to string) string {
		text := dated(byLabel[label])
		mutated := strings.ReplaceAll(text, from, to)
		if mutated == text {
			t.Fatalf("MUTATE %s: %q not found", label, from)
		}
		mutants++
		return mutated
	}
	pastDay := func(label string) string {
		mutants++
		return strings.ReplaceAll(strings.ReplaceAll(dated(byLabel[label]), nextDay, "2000-01-02"), day, "2000-01-01")
	}
	orgSwap := func(label string) string {
		// a range above every key: no granule spans into it (a range between two orgs would meet the granule that spans them)
		return strings.ReplaceAll(replaced(label, "'f0f0f0f0'", "'fff0f0f0'"), "'f0f0f0f1'", "'fff0f0f1'")
	}
	prefix, wrongPrefix := "'SELECT source, uniqExact(subject_kind'", "'SELECT zource, uniqExact(subject_kind'"
	flip := func(label string) string { return replaced(label, ">= 47000", ">= 1") }
	anyZero := func(rows []map[string]string) bool {
		for _, r := range rows {
			if r["theory_ok"] == "0" {
				return true
			}
		}
		return false
	}
	for _, r := range mutateWith("S0-past-day", pastDay("S0")) {
		if r["window_rows"] != "0" {
			t.Errorf("MUTATE S0 past day: %v, want window_rows 0", r)
		}
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
	if got := mutateWith("S1-cut", replaced("S1", "toDateTime('"+cut+"', 'UTC')", "toDateTime('"+day+" 00:00:00', 'UTC')")); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "12" {
		t.Errorf("MUTATE S1 cut at the window start: %v, want runs 0 and 12", got)
	}
	for name, text := range map[string]string{
		"S2-past-day": pastDay("S2"),
		"S2-prefix":   replaced("S2", prefix, wrongPrefix),
		"S2-step":     replaced("S2", "plan_step_name = 'ReadFromMergeTree'", "plan_step_name = 'ReadFromMergeTreez'"),
		"S4-past-day": pastDay("S4"),
		"S4-prefix":   replaced("S4", prefix, wrongPrefix),
		"S4-step":     replaced("S4", "plan_step_name = 'ReadFromMergeTree'", "plan_step_name = 'ReadFromMergeTreez'"),
	} {
		if got := mutateWith(name, text); len(got) != 0 {
			t.Errorf("MUTATE %s: %d rows, want 0", name, len(got))
		}
	}
	for name, text := range map[string]string{
		"S3-past-day": pastDay("S3"),
		"S3-prefix":   replaced("S3", prefix, wrongPrefix),
		"S3-type":     replaced("S3", "type = 'QueryFinish'", "type = 'ExceptionWhileProcessing'"),
	} {
		if got := mutateWith(name, text); len(got) != 1 || got[0]["run_kind"] != "now" {
			t.Errorf("MUTATE %s: %d rows, want only the 'now' row", name, len(got))
		}
	}
	noRemove := dated(byLabel["S3"])
	for _, from := range []string{"tuple(x, 'remove',", "tuple(part_name, 'remove',", "tuple(name, 'remove',"} {
		if !strings.Contains(noRemove, from) {
			t.Fatalf("MUTATE S3: %q not found", from)
		}
		noRemove = strings.ReplaceAll(noRemove, from, strings.Replace(from, "'remove'", "'removez'", 1))
	}
	mutants++
	for _, r := range mutateWith("S3-no-removal", noRemove) {
		if r["run_kind"] == "now" && r["wi_parts"] == r["wi_active_now"] {
			t.Errorf("MUTATE S3 no removal: now wi_parts %s = wi_active_now, want unequal", r["wi_parts"])
		}
	}
	if !anyZero(mutateWith("S3-threshold", flip("S3"))) {
		t.Errorf("MUTATE S3 threshold: no theory_ok 0, the prediction cannot fail")
	}
	if !anyZero(mutateWith("S3-org", orgSwap("S3"))) {
		t.Errorf("MUTATE S3 org range: no theory_ok 0")
	}
	if !anyZero(mutateWith("S4-threshold", flip("S4"))) {
		t.Errorf("MUTATE S4 threshold: no theory_ok 0, the prediction cannot fail")
	}
	if got := mutateWith("S5-org", orgSwap("S5")); len(got) != 2 || got[0]["org_parts"] != "0" || got[1]["org_parts"] != "0" {
		t.Errorf("MUTATE S5 org range: %v, want org_parts 0 and 0", got)
	}
	t.Logf("MUTANTS: %d", mutants)

	// PASS bounds: read_rows <= window rows + part index rows + system table rows (from S0 after the runs).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	sizes, err := p3x14548Run(ctx, conn, dated(byLabel["S0"]), fmt.Sprintf("p3x14548-sizes-%d", time.Now().UnixNano()), nil)
	if err != nil || len(sizes) != 4 {
		t.Fatalf("sizes: %v %v", sizes, err)
	}
	size := map[string]uint64{}
	for _, r := range sizes {
		size[r["log_table"]+"|"+r["window_name"]] = p3x14548Uint(r["window_rows"])
		size[r["log_table"]+"|marks"] = p3x14548Uint(r["marks_all"]) + p3x14548Uint(r["parts_all"])
	}
	wQ2, wQD, wP2, wRS2 := size["query_log|0850_0910"], size["query_log|from_0800"], size["processors_profile_log|0850_0910"], size["processors_profile_log|run_seconds"]
	mQ, mP := size["query_log|marks"], size["processors_profile_log|marks"]
	plToday := p3x14548Count(ctx, t, conn, "SELECT count() FROM system.part_log WHERE event_date = toDate(?)", day)
	mData := p3x14548Count(ctx, t, conn, "SELECT sum(marks) + count() FROM system.parts WHERE database = currentDatabase() AND table IN ('work_items', 'project_membership_transitions')")
	pAll := p3x14548Count(ctx, t, conn, "SELECT count() FROM system.parts WHERE (database = 'system' AND table IN ('query_log', 'processors_profile_log', 'part_log')) OR (database = currentDatabase() AND table IN ('work_items', 'project_membership_transitions'))")
	slack := 20*pAll + 5000
	bounds := map[string]uint64{
		"S0": 2*mP + 3*mQ + wQD + slack,
		"S1": wQ2 + mQ + slack,
		"S2": wP2 + wQ2 + mP + mQ + slack,
		"S3": plToday + wQD + mQ + mData + slack,
		"S4": wRS2 + 3*(wQD+mQ) + mP + slack,
		"S5": mData + slack,
	}
	t.Logf("sizes: ql 0850_0910 %d, ql from_0800 %d, pp 0850_0910 %d, pp run_seconds %d, ql marks+parts %d, pp marks+parts %d, part_log today %d, data %d, parts rows %d",
		wQ2, wQD, wP2, wRS2, mQ, mP, plToday, mData, pAll)
	for _, s := range statements {
		if _, ok := passRows[s.label]; !ok {
			continue
		}
		types, read := p3x14548Log(ctx, t, conn, passComments[s.label])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", s.label, types)
		}
		t.Logf("PASS %s bound: 1 <= read_rows %d <= %d", s.label, read, bounds[s.label])
		if read < 1 || read > bounds[s.label] {
			t.Errorf("PASS %s: read_rows %d outside [1, %d]", s.label, read, bounds[s.label])
		}
	}
	// The run-second filter alone: a primary-key read of processors_profile_log reads no more than the run_seconds window.
	var secs []string
	for f := range finishes {
		ts, _ := time.Parse("2006-01-02 15:04:05", f)
		for _, d := range []time.Duration{-time.Second, 0, time.Second} {
			secs = append(secs, "'"+ts.Add(d).Format("2006-01-02 15:04:05")+"'")
		}
	}
	pruneComment := fmt.Sprintf("p3x14548-prune-%d", time.Now().UnixNano())
	if _, err := p3x14548Run(ctx, conn, "SELECT count() FROM system.processors_profile_log WHERE event_date = toDate('"+day+"') AND event_time IN ("+strings.Join(secs, ", ")+")", pruneComment, nil); err != nil {
		t.Fatalf("prune cell: %v", err)
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	_, pruneRead := p3x14548Log(ctx, t, conn, pruneComment)
	t.Logf("PRUNE: processors_profile_log read %d rows for the run seconds; run_seconds window %d; rows today %d", pruneRead, wRS2, ppDay)
	if pruneRead > wRS2 || 2*pruneRead > ppDay {
		t.Errorf("PRUNE: read %d rows, want <= run_seconds window %d and < half of %d", pruneRead, wRS2, ppDay)
	}

	// Gate fires: each cap literal lowered to 0. WANT code 395 STOP-COST-GATE, ExceptionBeforeStart, small read.
	fires := 0
	for _, s := range statements {
		for i, c := range p3x14548Caps(s.text) {
			lowered := s.text[:c[0]] + "> 0" + s.text[c[1]:]
			comment := fmt.Sprintf("p3x14548-gate-%s-%d-%d", s.label, i, time.Now().UnixNano())
			_, err := p3x14548Run(ctx, conn, dated(lowered), comment, nil)
			var ex *clickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s #%d %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", s.label, i, s.text[c[0]:c[1]], err)
				continue
			}
			if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
				t.Fatal(err)
			}
			types, read := p3x14548Log(ctx, t, conn, comment)
			bound := 3*(mP+mQ) + wQD + mData + slack
			t.Logf("GATE %s #%d %s: code %d, types=%v read_rows=%d bound=%d", s.label, i, s.text[c[0]:c[1]], ex.Code, types, read, bound)
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
				t.Errorf("GATE %s #%d: query_log types %v, want [ExceptionBeforeStart]", s.label, i, types)
			}
			if read > bound {
				t.Errorf("GATE %s #%d: read_rows %d over %d", s.label, i, read, bound)
			}
			fires++
		}
	}
	if fires != wantFires || wantFires != 28 {
		t.Errorf("gates fired %d times, want %d (28)", fires, wantFires)
	}

	// Output cell: no query text / id, comment, org id, key value or part name in any output.
	secrets := []string{p3x14548OrgA, p3x14548OrgB, "uniqExact(subject_kind", "wi-0", "wz-0", "ffffffff-ffff", "00000000-0000-4000-8000-0000000000"}
	for _, r := range all {
		secrets = append(secrets, r.id, r.comment)
	}
	partName := regexp.MustCompile(`all_\d+_\d+_\d+`)
	leaks := func(label, text string) []string {
		got, err := p3x14548Run(ctx, conn, dated(text), fmt.Sprintf("p3x14548-output-%s-%d", label, time.Now().UnixNano()), nil)
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
				if partName.MatchString(value) {
					bad = append(bad, name+" holds a part name")
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
			t.Logf("OUTPUT %s: no query text, query_id, org id, key value or part name", s.label)
		}
	}
	outMutants := map[string]string{}
	m := strings.Replace(byLabel["S2"], "SELECT r.run_side AS run_side,", "SELECT r.run_side AS run_side, p.query_id AS leaked,", 1)
	outMutants["S2-query-id"] = strings.Replace(m, "GROUP BY run_side,", "GROUP BY leaked, run_side,", 1)
	m = strings.Replace(byLabel["S1"], "SELECT runs_before, runs_after,", "SELECT runs_before, runs_after, q_text,", 1)
	outMutants["S1-query-text"] = strings.Replace(m, "  SELECT countIf(", "  SELECT any(query) AS q_text, countIf(", 1)
	outMutants["S3-part-name"] = strings.Replace(byLabel["S3"], "SELECT r.run_kind AS run_kind,", "SELECT any(u.name) AS leaked, r.run_kind AS run_kind,", 1)
	m = strings.Replace(byLabel["S5"], "SELECT data_table, org_parts,", "SELECT data_table, leaked_key, org_parts,", 1)
	m = strings.Replace(m, "  SELECT 'work_items' AS data_table,", "  SELECT 'work_items' AS data_table, toString(any(k_lo)) AS leaked_key,", 1)
	outMutants["S5-key"] = strings.Replace(m, "  SELECT 'project_membership_transitions' AS data_table,", "  SELECT 'project_membership_transitions' AS data_table, toString(any(k_lo)) AS leaked_key,", 1)
	m = strings.Replace(byLabel["S4"], "SELECT r.run_time AS run_time,", "SELECT any(p.query_id) AS leaked, r.run_time AS run_time,", 1)
	outMutants["S4-query-id"] = m
	for name, text := range outMutants {
		if text == byLabel[strings.SplitN(name, "-", 2)[0]] {
			t.Fatalf("OUTPUT %s: mutant edit not applied", name)
		}
		if bad := leaks(name, text); len(bad) == 0 {
			t.Errorf("OUTPUT %s: nothing found, the cell cannot fail", name)
		} else {
			t.Logf("OUTPUT %s caught: %d findings", name, len(bad))
		}
	}
	t.Logf("REPRO RESULT: A (big + far part) %d rows; B (+ overlapping X) %d; C (X dropped) %d; D (+ overlapping Y) %d",
		runs["a"][2].readRows, runs["b"][0].readRows, runs["c"][2].readRows, runs["d"][0].readRows)
}
