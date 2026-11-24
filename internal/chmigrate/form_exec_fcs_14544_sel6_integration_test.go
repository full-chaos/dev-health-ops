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

// DO NOT MERGE. This file executes the prod read FORM-14544-SEL-6.sql byte for byte with every prod literal swapped to a
// synthetic value (the sha256 below is the sha256 of the swapped file), on the head schema with a synthetic fixture. S0 .. S6
// are asserted as the SEL-4 cell asserted them (phases 0 .. 3). S7 (source -> repo cut confirm) runs with its placeholder
// in every phase (NOT EXERCISED) and, in phase 4, filled the way the card's step M3 fills it: pass, shared cut, 0 repos,
// a renamed repo, a takeover, org-2, and one mutant per new clause. Every cost gate cap is fired once (FORM-TEMPLATE-5).
const formExecSel6x14544SHA256 = "c1757a1f697d70e872f74055534c293954710bb423d2b893bb5482e1eeada61e"

const formExecSel6x14544SQL = `-- FORM-14544-SEL-6 (lane fcs-14544sel5, 2026-11-24, ops pin 3207ac6258c0). ClickHouse prod, SELECT only. Card CARD-14544-14562-6.
-- = FORM-14544-SEL-5 (a76879ba4872) byte for byte on lines 4-343 (S0 .. S6) + a new S7 that fails closed on a takeover (acp P2-a).
-- Shape: pm-guard/FORM-TEMPLATE-5.md (7fb32b16d35d). WANT text and work math: FORM-14544-SEL-6.md.
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix f0f0f0f0 by startsWith on every FROM with org_id.
-- Each data statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- only), and every query level that reads a table, and the outer WHERE, tests the gate.
-- Run only the statements the card names, one per call, in its order. A SQL error (STOP-COST-GATE included) = STOP and report.
-- No comment line holds a statement end. The card gives each statement's line range and its sha12.
-- Named parameters (one literal each): T332 = 2026-11-24 11:01:20 UTC (STEP 332 roll START, the lower bound of every
-- retraction row). SKEW_BOUND = 300 s (FRESH needs computed_at >= mark + 300 s, the product's own skew bound,
-- postSyncTouchedClockMargin, touched_days.go:45-49, acp ruling). MARGIN = 300 s (a run has its OWN post-sync record when
-- every suite day of it holds a 'touched' event at or after its stamp - 300 s, the record's own bound, touched_days.go:447).

-- S0 INFO cost inputs, metadata only (system.one + system.parts). WANT 5 rows, table_rows > 0 in each (a wrong table
-- name gives a row with 0).
SELECT l.t AS table_name, p.table_rows AS table_rows, p.disk_bytes AS disk_bytes, p.active_parts AS active_parts
FROM (SELECT arrayJoin(['daily_metrics_touched_days', 'repos', 'test_suite_results', 'testops_release_confidence', 'testops_test_metrics_daily']) AS t FROM system.one) AS l
LEFT JOIN (
    SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
    FROM system.parts
    WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days', 'repos', 'test_suite_results', 'testops_release_confidence', 'testops_test_metrics_daily')
    GROUP BY table
) AS p ON p.table = l.t
ORDER BY table_name;

-- S1 ORG-1 on test_suite_results (FORM-332-RETR-3 stmt0 form). WANT orgs = 1, org_rows > 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM test_suite_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

-- S2 the verb's own selection (daily.RetiredSlotRepoDaysSQL, testops_retired_slot_days.go:24-36), rendered by the pin's cell
-- TestRetiredSlotFormRendersFromTheVerbConstant (sha12 eca4f85de80a) at its render params: --from 2026-11-17 --to 2026-11-23
-- --through 2026-11-24, row cap 201. The text below this comment is that render byte for byte. WANT: it completes. INFO
-- repo_days = the verb's retired_repo_days for those args. On the write path it is rendered again at the real F/T/TH.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 6000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS repo_days, uniqExact(repo) AS repos, toString(min(day)) AS first_day, toString(max(day)) AS last_day
FROM (
SELECT toString(repo_id) AS repo, toDate(coalesce(started_at, finished_at), 'UTC') AS day
FROM test_suite_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND coalesce(started_at, finished_at) IS NOT NULL
  AND toDate(coalesce(started_at, finished_at), 'UTC') <= toDate('2026-11-24')
  AND (repo_id, run_id) IN (
      SELECT repo_id, run_id
      FROM (SELECT repo_id, run_id, retracted, started_at, finished_at FROM test_suite_results FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0)
      WHERE retracted = 1
        AND toDate(coalesce(started_at, finished_at), 'UTC') BETWEEN toDate('2026-11-17') AND toDate('2026-11-23')
        AND 1)
GROUP BY repo, day
ORDER BY repo, day
LIMIT 201
);

-- S3 DIAGNOSTIC split of the newest-retracted keys (not a gate: last_synced is the walk's first-attempt clock, not the write
-- time). WANT newest_keys > 0 (control: the read sees the org's suite keys). INFO the rest.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count()                                                                                  AS newest_keys,
       countIf(retracted = 1)                                                                   AS retracted_keys,
       countIf(retracted = 1 AND last_synced < toDateTime64('2026-11-24 11:01:20', 3, 'UTC'))   AS stamped_before_t332_keys,
       uniqExactIf((repo_id, run_id), retracted = 1)                                            AS retracted_runs,
       uniqExactIf(repo_id, retracted = 1)                                                      AS retracted_repos,
       toString(minIf(day, retracted = 1))                                                      AS first_retracted_day,
       toString(maxIf(day, retracted = 1))                                                      AS last_retracted_day,
       toString(minIf(toDate(last_synced, 'UTC'), retracted = 1))                               AS first_stamp_day,
       toString(maxIf(toDate(last_synced, 'UTC'), retracted = 1))                               AS last_stamp_day
FROM (
    SELECT repo_id, run_id, retracted, last_synced, toDate(coalesce(started_at, finished_at), 'UTC') AS day
    FROM test_suite_results FINAL
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
)
WHERE cost_gate = 0;

-- S4 COVERAGE GATE on testops_test_metrics_daily. Every run with a newest-retracted suite key (stamp = the run's newest retraction
-- last_synced, worker clock, the walk's first attempt). The run has its OWN record when every suite day D of it holds a
-- 'touched' event at or after stamp - MARGIN (touched_days_clickhouse.go:394-407 writes one for every suite day at one time).
-- Expected repo-days X = D .. min(D+29, cut): cut = the day of the run's first such 'touched' event (the record's own cut,
-- touched_days_clickhouse.go:436), or today when the run has no own record (so least(D+29, today)). Per key: mark = the LAST
-- 'touched' or 'touched_testops_window' event of the repo-day (ClickHouse clock), last_compute = the newest computed_at of
-- the table (worker clock, stamped before the compute reads). Class TEST ORDER (each key in exactly one class):
--   NOROW        no stored row of the key (the compute wrote none: missing stays missing, nothing served, nothing stale)
--   MISSING-MARK a run of the key has no own record, or the key's newest mark is older than the stamp (rows written after
--                the last mark: the record of that write failed)
--   FRESH        last_compute >= mark + SKEW_BOUND
--   PENDING      a mark, no such compute (re-read after the org's next nightly run)
--   UNMARKED     no mark (the run's record wrote 'touched' but no window event)
-- WANT pending = 0 AND unmarked = 0 AND missing_mark = 0 (0 expected keys is a PASS). NOROW is INFO, out of W and out of
-- the WANT. W = every key that is not FRESH and not NOROW. w_* print the verb args.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days', 'test_suite_results', 'testops_test_metrics_daily')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days', 'test_suite_results', 'testops_test_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count()                                                  AS expected_keys,
       uniqExact(repo_id)                                       AS repos,
       countIf(cls = 'norow')                                   AS norow,
       countIf(cls = 'missing_mark')                            AS missing_mark,
       countIf(cls = 'fresh')                                   AS fresh,
       countIf(cls = 'pending')                                 AS pending,
       countIf(cls = 'unmarked')                                AS unmarked,
       countIf(cls IN ('missing_mark', 'pending', 'unmarked'))  AS w_keys,
       uniqExactIf(repo_id, cls IN ('missing_mark', 'pending', 'unmarked')) AS w_repos,
       toString(minIf(rday_min, cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_from,
       toString(maxIf(rday_max, cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_to,
       toString(maxIf(day, cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_through,
       arraySort(groupUniqArrayIf(substring(toString(repo_id), 1, 8), cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_repo_cuts
FROM (
SELECT k.repo_id AS repo_id, k.day AS day, k.rday_min AS rday_min, k.rday_max AS rday_max, c.last_compute AS last_compute,
       if(empty(m.mats), toDateTime64(0, 3, 'UTC'), arrayMax(m.mats)) AS mark_at,
       multiIf(last_compute = toDateTime(0, 'UTC'), 'norow',
               k.no_record = 1 OR (mark_at > toDateTime64(0, 3, 'UTC') AND mark_at < k.landed), 'missing_mark',
               mark_at > toDateTime64(0, 3, 'UTC') AND toDateTime64(last_compute, 3, 'UTC') >= mark_at + toIntervalSecond(300), 'fresh',
               mark_at > toDateTime64(0, 3, 'UTC'), 'pending',
               'unmarked') AS cls
FROM (
    SELECT repo_id, addDays(base_day, o) AS day, max(run_landed) AS landed, min(run_rday_min) AS rday_min, max(run_rday_max) AS rday_max,
           max(run_no_record) AS no_record
    FROM (
        SELECT repo_id, groupArray(base_day) AS days, any(landed) AS run_landed, any(rday_min) AS run_rday_min, any(rday_max) AS run_rday_max,
               1 - min(has_rec) AS run_no_record,
               if(min(has_rec) = 1, toDate(min(rec_at), 'UTC'), toDate(now64(3), 'UTC')) AS cut_day
        FROM (
            SELECT b.repo_id AS repo_id, b.run_id AS run_id, b.base_day AS base_day, b.landed AS landed,
                   b.rday_min AS rday_min, b.rday_max AS rday_max,
                   arrayFilter(a -> a >= b.landed - toIntervalSecond(300), t.tats) AS tf,
                   toUInt8(notEmpty(tf)) AS has_rec,
                   if(empty(tf), toDateTime64(0, 3, 'UTC'), arrayMin(tf)) AS rec_at
            FROM (
                SELECT DISTINCT s.repo_id AS repo_id, s.run_id AS run_id, toDate(coalesce(s.started_at, s.finished_at), 'UTC') AS base_day,
                       r.landed AS landed, r.rday_min AS rday_min, r.rday_max AS rday_max
                FROM test_suite_results AS s
                INNER JOIN (
                    SELECT repo_id, run_id, max(last_synced) AS landed,
                           min(toDate(coalesce(started_at, finished_at), 'UTC')) AS rday_min,
                           max(toDate(coalesce(started_at, finished_at), 'UTC')) AS rday_max
                    FROM (SELECT repo_id, run_id, retracted, last_synced, started_at, finished_at FROM test_suite_results FINAL
                          WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0)
                    WHERE retracted = 1 AND cost_gate = 0
                    GROUP BY repo_id, run_id
                ) AS r ON r.repo_id = s.repo_id AND r.run_id = s.run_id
                WHERE startsWith(s.org_id, 'f0f0f0f0') AND cost_gate = 0 AND coalesce(s.started_at, s.finished_at) IS NOT NULL
            ) AS b
            LEFT JOIN (
                SELECT repo_id, day, groupArray(at) AS tats
                FROM daily_metrics_touched_days
                WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND kind = 'touched'
                GROUP BY repo_id, day
            ) AS t ON t.repo_id = b.repo_id AND t.day = b.base_day
            WHERE cost_gate = 0
        )
        GROUP BY repo_id, run_id
    )
    ARRAY JOIN days AS base_day
    ARRAY JOIN range(0, 30) AS o
    WHERE addDays(base_day, o) <= cut_day AND cost_gate = 0
    GROUP BY repo_id, day
) AS k
LEFT JOIN (
    SELECT repo_id, day, groupArray(at) AS mats
    FROM daily_metrics_touched_days
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND kind IN ('touched', 'touched_testops_window')
    GROUP BY repo_id, day
) AS m ON m.repo_id = k.repo_id AND m.day = k.day
LEFT JOIN (
    SELECT repo_id, day, max(computed_at) AS last_compute
    FROM testops_test_metrics_daily
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
    GROUP BY repo_id, day
) AS c ON c.repo_id = k.repo_id AND c.day = k.day
WHERE cost_gate = 0
)
WHERE cost_gate = 0;

-- S5 COVERAGE GATE on testops_release_confidence (the risk pass writes it with testops_quality_drag and
-- testops_pipeline_stability, testops_risk_native_clickhouse.go:977, :1007, :1033). S4 with that table. Every run with a newest-retracted suite key (stamp = the run's newest retraction
-- last_synced, worker clock, the walk's first attempt). The run has its OWN record when every suite day D of it holds a
-- 'touched' event at or after stamp - MARGIN (touched_days_clickhouse.go:394-407 writes one for every suite day at one time).
-- Expected repo-days X = D .. min(D+29, cut): cut = the day of the run's first such 'touched' event (the record's own cut,
-- touched_days_clickhouse.go:436), or today when the run has no own record (so least(D+29, today)). Per key: mark = the LAST
-- 'touched' or 'touched_testops_window' event of the repo-day (ClickHouse clock), last_compute = the newest computed_at of
-- the table (worker clock, stamped before the compute reads). Class TEST ORDER (each key in exactly one class):
--   NOROW        no stored row of the key (the compute wrote none: missing stays missing, nothing served, nothing stale)
--   MISSING-MARK a run of the key has no own record, or the key's newest mark is older than the stamp (rows written after
--                the last mark: the record of that write failed)
--   FRESH        last_compute >= mark + SKEW_BOUND
--   PENDING      a mark, no such compute (re-read after the org's next nightly run)
--   UNMARKED     no mark (the run's record wrote 'touched' but no window event)
-- WANT pending = 0 AND unmarked = 0 AND missing_mark = 0 (0 expected keys is a PASS). NOROW is INFO, out of W and out of
-- the WANT. W = every key that is not FRESH and not NOROW. w_* print the verb args.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days', 'test_suite_results', 'testops_release_confidence')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days', 'test_suite_results', 'testops_release_confidence')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count()                                                  AS expected_keys,
       uniqExact(repo_id)                                       AS repos,
       countIf(cls = 'norow')                                   AS norow,
       countIf(cls = 'missing_mark')                            AS missing_mark,
       countIf(cls = 'fresh')                                   AS fresh,
       countIf(cls = 'pending')                                 AS pending,
       countIf(cls = 'unmarked')                                AS unmarked,
       countIf(cls IN ('missing_mark', 'pending', 'unmarked'))  AS w_keys,
       uniqExactIf(repo_id, cls IN ('missing_mark', 'pending', 'unmarked')) AS w_repos,
       toString(minIf(rday_min, cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_from,
       toString(maxIf(rday_max, cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_to,
       toString(maxIf(day, cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_through,
       arraySort(groupUniqArrayIf(substring(toString(repo_id), 1, 8), cls IN ('missing_mark', 'pending', 'unmarked'))) AS w_repo_cuts
FROM (
SELECT k.repo_id AS repo_id, k.day AS day, k.rday_min AS rday_min, k.rday_max AS rday_max, c.last_compute AS last_compute,
       if(empty(m.mats), toDateTime64(0, 3, 'UTC'), arrayMax(m.mats)) AS mark_at,
       multiIf(last_compute = toDateTime(0, 'UTC'), 'norow',
               k.no_record = 1 OR (mark_at > toDateTime64(0, 3, 'UTC') AND mark_at < k.landed), 'missing_mark',
               mark_at > toDateTime64(0, 3, 'UTC') AND toDateTime64(last_compute, 3, 'UTC') >= mark_at + toIntervalSecond(300), 'fresh',
               mark_at > toDateTime64(0, 3, 'UTC'), 'pending',
               'unmarked') AS cls
FROM (
    SELECT repo_id, addDays(base_day, o) AS day, max(run_landed) AS landed, min(run_rday_min) AS rday_min, max(run_rday_max) AS rday_max,
           max(run_no_record) AS no_record
    FROM (
        SELECT repo_id, groupArray(base_day) AS days, any(landed) AS run_landed, any(rday_min) AS run_rday_min, any(rday_max) AS run_rday_max,
               1 - min(has_rec) AS run_no_record,
               if(min(has_rec) = 1, toDate(min(rec_at), 'UTC'), toDate(now64(3), 'UTC')) AS cut_day
        FROM (
            SELECT b.repo_id AS repo_id, b.run_id AS run_id, b.base_day AS base_day, b.landed AS landed,
                   b.rday_min AS rday_min, b.rday_max AS rday_max,
                   arrayFilter(a -> a >= b.landed - toIntervalSecond(300), t.tats) AS tf,
                   toUInt8(notEmpty(tf)) AS has_rec,
                   if(empty(tf), toDateTime64(0, 3, 'UTC'), arrayMin(tf)) AS rec_at
            FROM (
                SELECT DISTINCT s.repo_id AS repo_id, s.run_id AS run_id, toDate(coalesce(s.started_at, s.finished_at), 'UTC') AS base_day,
                       r.landed AS landed, r.rday_min AS rday_min, r.rday_max AS rday_max
                FROM test_suite_results AS s
                INNER JOIN (
                    SELECT repo_id, run_id, max(last_synced) AS landed,
                           min(toDate(coalesce(started_at, finished_at), 'UTC')) AS rday_min,
                           max(toDate(coalesce(started_at, finished_at), 'UTC')) AS rday_max
                    FROM (SELECT repo_id, run_id, retracted, last_synced, started_at, finished_at FROM test_suite_results FINAL
                          WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0)
                    WHERE retracted = 1 AND cost_gate = 0
                    GROUP BY repo_id, run_id
                ) AS r ON r.repo_id = s.repo_id AND r.run_id = s.run_id
                WHERE startsWith(s.org_id, 'f0f0f0f0') AND cost_gate = 0 AND coalesce(s.started_at, s.finished_at) IS NOT NULL
            ) AS b
            LEFT JOIN (
                SELECT repo_id, day, groupArray(at) AS tats
                FROM daily_metrics_touched_days
                WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND kind = 'touched'
                GROUP BY repo_id, day
            ) AS t ON t.repo_id = b.repo_id AND t.day = b.base_day
            WHERE cost_gate = 0
        )
        GROUP BY repo_id, run_id
    )
    ARRAY JOIN days AS base_day
    ARRAY JOIN range(0, 30) AS o
    WHERE addDays(base_day, o) <= cut_day AND cost_gate = 0
    GROUP BY repo_id, day
) AS k
LEFT JOIN (
    SELECT repo_id, day, groupArray(at) AS mats
    FROM daily_metrics_touched_days
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND kind IN ('touched', 'touched_testops_window')
    GROUP BY repo_id, day
) AS m ON m.repo_id = k.repo_id AND m.day = k.day
LEFT JOIN (
    SELECT repo_id, day, max(computed_at) AS last_compute
    FROM testops_release_confidence
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
    GROUP BY repo_id, day
) AS c ON c.repo_id = k.repo_id AND c.day = k.day
WHERE cost_gate = 0
)
WHERE cost_gate = 0;

-- S6 SIZING of the stale slots no new-image walk retracts (FORM-14259-SIZE-3 S2 ec41eafa0a7b, its slot rule, grouped by
-- run day). A slot is a candidate when every suite head of its newest attempt occurs in the run's same attempt at least
-- twice as often as in the slot (the aggregate-of-parts rule, an UPPER bound: the parts of such a slot count too). A slot
-- whose newest rows are retraction rows has its own all-zero heads and drops out. One row per run day (the slot's first
-- suite day, the touched-day expression), and one control row: kind 'control', runs = the org's github repos (WANT 11, as
-- SIZE-3 S1 read on 2026-11-23), slots = the org's newest slots (WANT > 0). WANT (option (b) of the
-- W11-EFB-1118 open point): the day row 2026-11-18, if present, holds no repo cut of source e0e0e0e0's repo (pm names that
-- cut). Else STOP and a pm/acp ruling. repo_cuts = 8-char repo cuts, pm maps
-- them to the sync source (W11-EFB-1118 walked source e0e0e0e0 only). No data gate inside: the parts gate bounds it.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('repos', 'test_suite_results')) > 1600000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('repos', 'test_suite_results')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate,
gh AS (
    SELECT id FROM repos FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND provider = 'github'
),
sa AS (
    SELECT repo_id, run_id, report_slot AS slot, run_attempt AS att, last_synced AS ls,
           toDate(coalesce(started_at, finished_at), 'UTC') AS sday,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt))) AS hk
    FROM test_suite_results FINAL
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND report_slot != '' AND repo_id IN (SELECT id FROM gh)
),
nw AS (
    SELECT repo_id, run_id, slot, max(att) AS att FROM sa GROUP BY repo_id, run_id, slot
),
hk_slot AS (
    SELECT repo_id, run_id, slot, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, slot, att, hk
),
hk_run AS (
    SELECT repo_id, run_id, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, att, hk
),
uslots AS (
    SELECT k.repo_id AS repo_id, k.run_id AS run_id, k.slot AS slot, k.att AS att
    FROM hk_slot AS k
    INNER JOIN hk_run AS r ON r.repo_id = k.repo_id AND r.run_id = k.run_id AND r.att = k.att AND r.hk = k.hk
    WHERE (k.repo_id, k.run_id, k.slot, k.att) IN (SELECT repo_id, run_id, slot, att FROM nw)
    GROUP BY k.repo_id, k.run_id, k.slot, k.att
    HAVING min(r.n >= 2 * k.n) = 1
),
slot_day AS (
    SELECT repo_id, run_id, slot, att, min(sday) AS sday, max(ls) AS ls FROM sa GROUP BY repo_id, run_id, slot, att
)
SELECT kind, day, runs, slots, slots_before_14045_commit, repo_cuts
FROM (
    SELECT 'day' AS kind, toString(d.sday) AS day, uniqExact(u.repo_id, u.run_id) AS runs, count() AS slots,
           countIf(d.ls < toDateTime64('2026-11-20 02:45:39', 3, 'UTC')) AS slots_before_14045_commit,
           arraySort(groupUniqArray(substring(toString(u.repo_id), 1, 8))) AS repo_cuts
    FROM uslots AS u
    INNER JOIN slot_day AS d ON d.repo_id = u.repo_id AND d.run_id = u.run_id AND d.slot = u.slot AND d.att = u.att
    WHERE cost_gate = 0
    GROUP BY d.sday
    UNION ALL
    SELECT 'control' AS kind, '' AS day, (SELECT count() FROM gh) AS runs, (SELECT count() FROM nw) AS slots,
           toUInt64(0) AS slots_before_14045_commit, CAST([], 'Array(String)') AS repo_cuts
)
WHERE cost_gate = 0
ORDER BY kind, day;

-- stmt7
-- S7 SOURCE -> REPO CUT CONFIRM, fail closed (acp option (P) + READ-CARD-14544-14562-5-1 P2-a). The sync source e0e0e0e0 is
-- a Postgres row only (integration_sources). FORM-14544-SRC-1 stmt1 prints repo_cut = the first 8 hex of
-- sha256(lower(btrim(full_name))), the repos.id a GitHub route mints from that path (github_repository_route.go:287-306).
-- repos.repo = the current name, repos.identity_path = the name the id was minted from (github_repository_route.go:199-211).
-- A held identity keeps an old id after a rename (gitlab_repo_identity.go:184-195). A takeover leaves the OLD holder on the
-- minted id and gives the new repo the id of "name@N" (gitlab_repo_identity.go:196-209). So S7 also counts takeover ids in
-- the org's github repos and the matched rows whose CURRENT name mints the same cut (the same rule, sha256 of lower trim).
-- Named parameters: SRC_CUT = that repo_cut. Card step M3 writes it in place of the 8-char placeholder literal of the
-- src_cut line (the only change, M3 checks it). F6_1118_CUTS = the repo cuts of the S6 day row 2026-11-18 in pm's -4 out
-- (card step M6 checks the literal against that out).
-- WANT github_repos > 0 (control, the org's github repos, -4 F6 read 11), repos_with_cut = 1, cut_name_matches = 1,
-- takeover_ids = 0, cut_in_f6_1118 = 0 (option (b) of the W11-EFB-1118 open point holds). repos_with_cut = 0 is NOT
-- EXERCISED = STOP, never PASS. Any other miss = STOP and a pm/acp ruling. One FINAL read of repos, the parts gate bounds it.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('repos')) > 1000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('repos')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate,
'SRC1CUT8' AS src_cut,
['a1a1a1a1', 'b2b2b2b2'] AS f6_1118_cuts
SELECT src_cut,
       count()                                                                AS github_repos,
       countIf(startsWith(toString(id), src_cut))                             AS repos_with_cut,
       countIf(startsWith(toString(id), src_cut)
               AND startsWith(lower(hex(SHA256(lower(trimBoth(repo))))), src_cut)) AS cut_name_matches,
       countIf(position(identity_path, '@') > 0)                              AS takeover_ids,
       toUInt8(has(f6_1118_cuts, src_cut))                                    AS cut_in_f6_1118
FROM repos FINAL
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND provider = 'github';
`

const formExecSel6x14544Placeholder = "'SRC1CUT8' AS src_cut"

type formExecSel6x14544Result struct {
	columns []string
	rows    [][]string
}

func formExecSel6x14544Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	statementID := regexp.MustCompile(`^S[0-9]+$`)
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
			t.Fatalf("a statement has no \"-- S<n>\" line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	return ids, statements
}

func formExecSel6x14544Query(ctx context.Context, conn driver.Conn, statement string) (formExecSel6x14544Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecSel6x14544Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecSel6x14544Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecSel6x14544Result{}, fmt.Errorf("scan: %w", err)
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

func formExecSel6x14544Row(t *testing.T, result formExecSel6x14544Result, label string, row int, want map[string]string) {
	t.Helper()
	if row >= len(result.rows) {
		t.Errorf("%s: %d row(s), want a row %d", label, len(result.rows), row)
		return
	}
	for column, value := range want {
		found := false
		for index, name := range result.columns {
			if name != column {
				continue
			}
			found = true
			got := result.rows[row][index]
			if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
				if !strings.HasPrefix(got, prefix) {
					t.Errorf("%s row %d %s = %s, want prefix %s", label, row, column, got, prefix)
				}
			} else if got != value {
				t.Errorf("%s row %d %s = %s, want %s", label, row, column, got, value)
			}
		}
		if !found {
			t.Errorf("%s: no column %s in %v", label, column, result.columns)
		}
	}
}

func formExecSel6x14544QueryLog(ctx context.Context, t *testing.T, conn driver.Conn, label, comment string) ([]string, uint64, []string) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows, arrayStringConcat(tables, ',')
		FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	defer func() { _ = rows.Close() }()
	var types, tableLists []string
	var read uint64
	for rows.Next() {
		var typ, tables string
		var readRows uint64
		if err := rows.Scan(&typ, &readRows, &tables); err != nil {
			t.Fatalf("%s: query log scan: %v", label, err)
		}
		types = append(types, typ)
		tableLists = append(tableLists, tables)
		read += readRows
		t.Logf("%s type=%s read_rows=%d tables=%s", label, typ, readRows, tables)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	return types, read, tableLists
}

// formExecSel6x14544Fill is the card's step M3: the placeholder literal of the src_cut line replaced by the cut, once.
// The check M3 runs on the Mac is here too: the filled text with the cut put back is the statement as written.
func formExecSel6x14544Fill(t *testing.T, statement, cut string) string {
	t.Helper()
	if !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(cut) {
		t.Fatalf("cut %q is not 8 hex chars (M3 refuses it)", cut)
	}
	if got := strings.Count(statement, formExecSel6x14544Placeholder); got != 1 {
		t.Fatalf("S7 holds the placeholder %d time(s), want 1", got)
	}
	filled := strings.Replace(statement, formExecSel6x14544Placeholder, "'"+cut+"' AS src_cut", 1)
	if restored := strings.Replace(filled, "'"+cut+"' AS src_cut", formExecSel6x14544Placeholder, 1); restored != statement {
		t.Fatalf("M3 restore check: the filled S7 differs from S7 beyond the cut")
	}
	return filled
}

// formExecSel6x14544Mint mints a repos.id the way the GitHub routes do (repositoryIdentity): the first 16 bytes of
// sha256(lower(trim(name))) as a UUID.
func formExecSel6x14544Mint(name string) string {
	digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(name))))
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

// The fixture of the SEL-4 cell (phases 0 .. 3, the same rows and WANTs), then phase 4 for S7.
//
//	phase0 .. phase3: the SEL-4 cell's fixture: no retraction, retractions in every class, records and recomputes, a
//	        slot retracted on the new image. S7 runs with its placeholder: github_repos 3, every S7 count 0.
//	phase4: ids minted the way the GitHub routes mint them (formExecSel6x14544Mint). Rows of the org (github): S (minted
//	        from its own name) and X (cut a1a1a1a1, one of the synthetic F6 day 2026-11-18 cuts); a gitlab row of the org
//	        and a github row of org d0d0d0d0 with S's cut (not counted).
//	        pass       S's cut: github_repos 5, repos_with_cut 1, cut_name_matches 1, takeover_ids 0, cut_in_f6_1118 0.
//	        shared     cut a1a1a1a1: cut_in_f6_1118 1 = option (b) fails.
//	        0 repos    cut 9f9f9f9f: repos_with_cut 0 = NOT EXERCISED.
//	        renamed    R minted from an old name, repo = its new name: cut_name_matches 0 = STOP. Mutant without the
//	                   current-name clause: the WANT holds (the clause is the guard).
//	        takeover   A holds the id minted from a name, B (same name, another repository) holds the id of "name@N":
//	                   takeover_ids 1 = STOP (also for S's cut: org-wide, fail closed). Mutant without the takeover clause:
//	                   the WANT holds.
//	        org-2      a github row with S's cut in a second org with the prefix: repos_with_cut 2 = the WANT misses.
//	org mutant S1: a second org with the same prefix: orgs = 2, its WANT misses (as the SEL-4 cell).
func TestFormExecFcs14544Sel6(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
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
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	const (
		org    = "f0f0f0f0-1454-4000-8000-000000000001"
		org2   = "f0f0f0f0-1454-4000-8000-000000000009"
		other  = "d0d0d0d0-1454-4000-8000-000000000002"
		repoA  = "11111111-1454-4000-8000-0000000000a1"
		repoB  = "22222222-1454-4000-8000-0000000000b1"
		repoC  = "33333333-1454-4000-8000-0000000000c1"
		repoGl = "44444444-1454-4000-8000-0000000000d1"
		repoE  = "55555555-1454-4000-8000-0000000000e1"
		repoD  = "66666666-1454-4000-8000-0000000000f1"
		repoF  = "77777777-1454-4000-8000-0000000000f2"
		repoG  = "88888888-1454-4000-8000-0000000000f3"
		repoH  = "99999999-1454-4000-8000-0000000000f4"
		repoX  = "a1a1a1a1-1454-4000-8000-0000000000a8"
		d24    = "2026-11-24 "
	)
	for _, table := range []string{"test_suite_results", "daily_metrics_touched_days", "testops_test_metrics_daily", "testops_release_confidence", "repos"} {
		exec("SYSTEM STOP MERGES " + table)
	}
	suite := func(orgID, repo, run, suiteID, slot, name string, total, passed, failed int, day, stamp string, retracted int) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, '%s 10:00:00.000', '%s 10:05:00.000', '%s', 1, %d)",
			orgID, repo, run, suiteID, slot, name, total, passed, failed, day, day, stamp, retracted)
	}
	suites := `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, report_slot, suite_name, total_count, passed_count, failed_count, started_at, finished_at, last_synced, run_attempt, retracted) VALUES `
	mark := func(orgID, repo, day, kind, at string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')", orgID, day, repo, kind, at)
	}
	marks := `INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at) VALUES `
	metric := func(orgID, repo, day, computed string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s')", orgID, repo, day, computed)
	}
	tmd := `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, computed_at) VALUES `
	trc := `INSERT INTO testops_release_confidence (org_id, repo_id, day, computed_at) VALUES `
	days := func(from string, n int) []string {
		start, err := time.Parse(time.DateOnly, from)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, start.AddDate(0, 0, i).Format(time.DateOnly))
		}
		return out
	}
	window := func(repo string, from string, n int, kind, at string) []string {
		var out []string
		for _, day := range days(from, n) {
			out = append(out, mark(org, repo, day, kind, at))
		}
		return out
	}
	computes := func(repo string, from string, n int, computed string) []string {
		var out []string
		for _, day := range days(from, n) {
			out = append(out, metric(org, repo, day, computed))
		}
		return out
	}
	join := func(parts ...[]string) string {
		var all []string
		for _, part := range parts {
			all = append(all, part...)
		}
		return strings.Join(all, ", ")
	}
	repo := func(orgID, id, provider string) string {
		return fmt.Sprintf("('%s', 'fx/%s', '2026-01-01 00:00:00.000', '2026-11-01 00:00:00.000', '%s', '%s')", id, id[:8], orgID, provider)
	}
	reposInsert := `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES `
	exec(reposInsert + strings.Join([]string{
		repo(org, repoA, "github"), repo(org, repoB, "github"), repo(org, repoC, "github"), repo(org, repoGl, "gitlab"),
		repo(other, repoE, "github"),
	}, ", "))

	sum := sha256.Sum256([]byte(formExecSel6x14544SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecSel6x14544SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14544-SEL-6 synthetic copy)", got, formExecSel6x14544SHA256)
	}
	ids, statements := formExecSel6x14544Statements(t, formExecSel6x14544SQL)
	if got := strings.Join(ids, ","); got != "S0,S1,S2,S3,S4,S5,S6,S7" {
		t.Fatalf("statements = %s, want S0,S1,S2,S3,S4,S5,S6,S7", got)
	}
	lines := strings.Split(formExecSel6x14544SQL, "\n")
	for index, line := range lines {
		if strings.HasPrefix(line, "--") && strings.Contains(line, ";") {
			t.Errorf("comment line %d holds a ';': %s", index+1, line)
		}
	}
	if len(lines) < 345 || lines[344] != "-- stmt7" {
		t.Errorf("line 345 is not the \"-- stmt7\" marker")
	}
	gates := []struct{ id, rowsCap, bytesCap string }{
		{"S1", "> 20000000", "> 4000000000"},
		{"S2", "> 6000000", "> 4000000000"},
		{"S3", "> 6000000", "> 4294967296"},
		{"S4", "> 6000000", "> 4294967296"},
		{"S5", "> 6000000", "> 4294967296"},
		{"S6", "> 1600000", "> 4294967296"},
		{"S7", "> 1000000", "> 4294967296"},
	}
	for _, gate := range gates {
		text := statements[gate.id]
		for _, literal := range []string{gate.rowsCap, gate.bytesCap, "WITH (SELECT throwIf(", "'STOP-COST-GATE'"} {
			if got := strings.Count(text, literal); got != 1 {
				t.Errorf("%s holds %q %d time(s), want 1", gate.id, literal, got)
			}
		}
	}
	gateTables := map[string][]string{}
	tableList := regexp.MustCompile(`table IN \(([^)]*)\)`)
	for _, gate := range gates {
		match := tableList.FindStringSubmatch(statements[gate.id])
		if match == nil {
			t.Fatalf("%s: no gate table list", gate.id)
		}
		for _, item := range strings.Split(match[1], ",") {
			gateTables[gate.id] = append(gateTables[gate.id], strings.Trim(strings.TrimSpace(item), "'"))
		}
	}
	if got := strings.Join(gateTables["S7"], ","); got != "repos" {
		t.Errorf("S7 gate tables = %s, want repos (the only table S7 reads)", got)
	}
	var s0SQL strings.Builder
	for _, line := range strings.Split(statements["S0"], "\n") {
		if !strings.HasPrefix(line, "--") {
			s0SQL.WriteString(line + "\n")
		}
	}
	if strings.Contains(s0SQL.String(), "throwIf") || strings.Contains(s0SQL.String(), "FROM test_suite_results") {
		t.Errorf("S0 must read metadata only")
	}
	s5 := strings.ReplaceAll(statements["S5"], "testops_release_confidence", "testops_test_metrics_daily")
	cut := func(text string) string { return text[strings.Index(text, "WITH (SELECT throwIf("):] }
	if cut(s5) != cut(statements["S4"]) {
		t.Errorf("S5 is not S4 with testops_release_confidence in place of testops_test_metrics_daily")
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse version %s", version)

	query := func(label, statement string) (formExecSel6x14544Result, string, error) {
		comment := fmt.Sprintf("form14544sel6-%s-%d", label, time.Now().UnixNano())
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		result, err := formExecSel6x14544Query(qctx, conn, statement)
		if err == nil {
			t.Logf("FORM-14544-SEL-6 %s columns %v", label, result.columns)
			for index, row := range result.rows {
				t.Logf("FORM-14544-SEL-6 %s row %d %v", label, index, row)
			}
		}
		return result, comment, err
	}
	run := func(phase string) (map[string]formExecSel6x14544Result, map[string]string) {
		results, comments := map[string]formExecSel6x14544Result{}, map[string]string{}
		for _, id := range ids {
			result, comment, err := query(phase+"-"+id, statements[id])
			comments[id] = comment
			if err != nil {
				t.Errorf("FORM-14544-SEL-6 %s %s: %v", phase, id, err)
				continue
			}
			results[id] = result
		}
		return results, comments
	}
	rowCount := func(results map[string]formExecSel6x14544Result, label, id string, want int) {
		t.Helper()
		if got := len(results[id].rows); got != want {
			t.Errorf("%s %s: %d row(s), want %d", label, id, got, want)
		}
	}
	cells := func(results map[string]formExecSel6x14544Result, id string) map[string]string {
		cell := map[string]string{}
		result, ok := results[id]
		if !ok || len(result.rows) != 1 {
			return cell
		}
		for index, name := range result.columns {
			cell[name] = result.rows[0][index]
		}
		return cell
	}
	gatePass := func(results map[string]formExecSel6x14544Result, label, id string) bool {
		t.Helper()
		cell := cells(results, id)
		held := len(cell) > 0 && cell["pending"] == "0" && cell["unmarked"] == "0" && cell["missing_mark"] == "0"
		t.Logf("%s %s: coverage WANT (pending 0, unmarked 0, missing_mark 0) held = %v", label, id, held)
		return held
	}
	partition := func(results map[string]formExecSel6x14544Result, label, id string) {
		t.Helper()
		cell := cells(results, id)
		var total, w int
		for _, name := range []string{"norow", "missing_mark", "fresh", "pending", "unmarked"} {
			var n int
			fmt.Sscan(cell[name], &n)
			total += n
			if name == "missing_mark" || name == "pending" || name == "unmarked" {
				w += n
			}
		}
		var expected, wKeys int
		fmt.Sscan(cell["expected_keys"], &expected)
		fmt.Sscan(cell["w_keys"], &wKeys)
		if total != expected || w != wKeys {
			t.Errorf("%s %s: classes sum %d (expected_keys %d), W classes %d (w_keys %d): want every key in exactly one class", label, id, total, expected, w, wKeys)
		}
	}
	// s7Want is S7's decision: option (b) holds only on github_repos > 0, repos_with_cut 1, cut_name_matches 1,
	// takeover_ids 0 and cut_in_f6_1118 0.
	s7Want := func(result formExecSel6x14544Result, label string) bool {
		t.Helper()
		cell := cells(map[string]formExecSel6x14544Result{"S7": result}, "S7")
		held := len(cell) > 0 && cell["github_repos"] != "0" && cell["repos_with_cut"] == "1" && cell["cut_name_matches"] == "1" &&
			cell["takeover_ids"] == "0" && cell["cut_in_f6_1118"] == "0"
		t.Logf("%s S7: WANT (github_repos > 0, repos_with_cut 1, cut_name_matches 1, takeover_ids 0, cut_in_f6_1118 0) held = %v", label, held)
		return held
	}
	s7Placeholder := func(results map[string]formExecSel6x14544Result, label string) {
		t.Helper()
		rowCount(results, label, "S7", 1)
		formExecSel6x14544Row(t, results["S7"], label+" S7", 0, map[string]string{
			"src_cut": "SRC1CUT8", "github_repos": "3", "repos_with_cut": "0", "cut_name_matches": "0", "takeover_ids": "0", "cut_in_f6_1118": "0"})
		if s7Want(results["S7"], label+" placeholder") {
			t.Errorf("%s S7: the WANT held with the unfilled placeholder, want NOT EXERCISED (it must fail)", label)
		}
	}

	// Phase 0: no retraction. The coverage gates PASS on 0 expected keys.
	exec(suites + suite(org, repoA, "a0", "a0-s1", "p1", "s1", 4, 4, 0, "2026-11-20", d24+"11:30:00.000", 0))
	exec(tmd + metric(org, repoA, "2026-11-20", "2026-11-21 00:00:00"))
	results, _ := run("phase0")
	rowCount(results, "phase0", "S0", 5)
	for index, table := range []string{"daily_metrics_touched_days", "repos", "test_suite_results", "testops_release_confidence", "testops_test_metrics_daily"} {
		formExecSel6x14544Row(t, results["S0"], "phase0 S0", index, map[string]string{"table_name": table})
	}
	formExecSel6x14544Row(t, results["S1"], "phase0 S1", 0, map[string]string{"orgs": "1", "org_rows": "1"})
	formExecSel6x14544Row(t, results["S2"], "phase0 S2", 0, map[string]string{"repo_days": "0", "repos": "0"})
	formExecSel6x14544Row(t, results["S3"], "phase0 S3", 0, map[string]string{"newest_keys": "1", "retracted_keys": "0", "stamped_before_t332_keys": "0"})
	for _, id := range []string{"S4", "S5"} {
		formExecSel6x14544Row(t, results[id], "phase0 "+id, 0, map[string]string{"expected_keys": "0", "norow": "0", "missing_mark": "0", "fresh": "0", "pending": "0", "unmarked": "0"})
		partition(results, "phase0", id)
		if !gatePass(results, "phase0", id) {
			t.Errorf("phase0 %s: the coverage WANT failed on 0 retractions, want PASS", id)
		}
	}
	rowCount(results, "phase0", "S6", 1)
	formExecSel6x14544Row(t, results["S6"], "phase0 S6", 0, map[string]string{"kind": "control", "runs": "3", "slots": "1"})
	s7Placeholder(results, "phase0")

	// Phase 1.
	exec(suites + strings.Join([]string{
		suite(org, repoA, "aR", "aR-k1", "agg", "k1", 5, 3, 2, "2026-11-20", d24+"11:20:00.000", 0),
		suite(org, repoA, "aR", "aR-k2", "p1", "k2", 5, 5, 0, "2026-11-21", d24+"11:20:00.000", 0),
		suite(org, repoC, "cR", "cR-k1", "p1", "k1", 2, 2, 0, "2026-11-21", d24+"11:40:00.000", 1),
		suite(org, repoA, "aS", "aS-p1", "p1", "s1", 3, 3, 0, "2026-11-18", "2026-11-19 00:00:00.000", 0),
		suite(org, repoA, "aS", "aS-p2", "p2", "s2", 2, 2, 0, "2026-11-18", "2026-11-19 00:00:00.000", 0),
		suite(org, repoA, "aS", "aS-agg1", "agg", "s1", 3, 3, 0, "2026-11-18", "2026-11-19 00:00:00.000", 0),
		suite(org, repoA, "aS", "aS-agg2", "agg", "s2", 2, 2, 0, "2026-11-18", "2026-11-19 00:00:00.000", 0),
	}, ", "))
	exec(suites + strings.Join([]string{
		suite(org, repoA, "aR", "aR-k1", "agg", "k1", 0, 0, 0, "2026-11-20", d24+"11:40:00.000", 1),
		suite(org, repoB, "bR", "bR-k1", "agg", "k1", 0, 0, 0, "2026-10-01", d24+"10:50:00.000", 1),
		suite(org, repoC, "cR", "cR-k1", "p1", "k1", 2, 2, 0, "2026-11-21", d24+"11:50:00.000", 0),
		suite(other, repoE, "eR", "eR-k1", "agg", "k1", 0, 0, 0, "2026-11-20", d24+"11:40:00.000", 1),
		suite(org, repoD, "dR", "dR-k1", "agg", "k1", 0, 0, 0, "2026-11-21", d24+"12:00:00.000", 1),
		suite(org, repoF, "fR", "fR-k1", "agg", "k1", 0, 0, 0, "2026-10-05", d24+"12:30:00.000", 1),
		suite(org, repoG, "gR", "gR-k1", "agg", "k1", 0, 0, 0, "2026-11-19", d24+"13:00:00.000", 1),
		suite(org, repoH, "hR", "hR-k1", "agg", "k1", 0, 0, 0, "2026-11-15", d24+"12:00:00.000", 1),
	}, ", "))
	exec(marks + join(
		[]string{
			mark(org, repoA, "2026-11-20", "touched", d24+"12:00:00.000"),
			mark(org, repoA, "2026-11-21", "touched", d24+"12:00:00.000"),
			mark(org, repoA, "2026-11-24", "touched", d24+"09:00:00.000"),
			mark(org, repoA, "2026-11-22", "touched", d24+"12:15:00.000"),
			mark(other, repoE, "2026-11-20", "touched", d24+"12:00:00.000"),
			mark(org, repoD, "2026-11-21", "touched", d24+"12:00:30.000"),
			mark(org, repoF, "2026-10-05", "touched", d24+"12:20:00.000"),
			mark(org, repoF, "2026-10-06", "touched_testops_window", d24+"12:20:00.000"),
			mark(org, repoG, "2026-11-19", "touched", d24+"12:58:00.000"),
			mark(org, repoH, "2026-11-15", "touched", d24+"12:05:00.000"),
		},
		window(repoA, "2026-11-21", 4, "touched_testops_window", d24+"12:00:00.000"),
		window(repoD, "2026-11-22", 3, "touched_testops_window", d24+"12:00:30.000"),
		window(repoG, "2026-11-20", 5, "touched_testops_window", d24+"12:58:00.000"),
	))
	exec(tmd + join(
		[]string{
			metric(org, repoA, "2026-11-20", d24+"12:05:01"), metric(org, repoA, "2026-11-21", d24+"12:01:00"),
			metric(org, repoA, "2026-11-22", d24+"12:30:00"), metric(org, repoA, "2026-11-23", d24+"11:59:00"),
			metric(org, repoB, "2026-10-01", "2026-11-23 00:00:00"), metric(org, repoB, "2026-10-02", "2026-11-23 00:00:00"),
			metric(other, repoE, "2026-11-21", "2026-11-23 00:00:00"),
		},
		computes(repoD, "2026-11-21", 4, d24+"11:59:30"),
		computes(repoF, "2026-10-05", 3, d24+"12:25:00"),
		computes(repoG, "2026-11-19", 6, d24+"13:10:00"),
		computes(repoH, "2026-11-15", 10, d24+"12:20:00"),
	))
	exec(trc + join(computes(repoA, "2026-11-20", 5, d24+"12:10:00"), computes(repoD, "2026-11-21", 4, d24+"11:59:30")))
	results, passComments := run("phase1")
	formExecSel6x14544Row(t, results["S1"], "phase1 S1", 0, map[string]string{"orgs": "1", "org_rows": "15"})
	formExecSel6x14544Row(t, results["S2"], "phase1 S2", 0, map[string]string{
		"repo_days": "4", "repos": "3", "first_day": "2026-11-19", "last_day": "2026-11-21"})
	formExecSel6x14544Row(t, results["S3"], "phase1 S3", 0, map[string]string{
		"newest_keys": "13", "retracted_keys": "6", "stamped_before_t332_keys": "1", "retracted_runs": "6", "retracted_repos": "6",
		"first_retracted_day": "2026-10-01", "last_retracted_day": "2026-11-21", "first_stamp_day": "2026-11-24", "last_stamp_day": "2026-11-24"})
	formExecSel6x14544Row(t, results["S4"], "phase1 S4", 0, map[string]string{
		"expected_keys": "85", "repos": "6", "norow": "56", "missing_mark": "11", "fresh": "3", "pending": "6", "unmarked": "9",
		"w_keys": "26", "w_repos": "6", "w_from": "2026-10-01", "w_to": "2026-11-21", "w_through": "2026-11-24",
		"w_repo_cuts": "[11111111 22222222 66666666 77777777 88888888 99999999]"})
	formExecSel6x14544Row(t, results["S5"], "phase1 S5", 0, map[string]string{
		"expected_keys": "85", "repos": "6", "norow": "76", "missing_mark": "0", "fresh": "4", "pending": "5", "unmarked": "0",
		"w_keys": "5", "w_repos": "2", "w_from": "2026-11-20", "w_to": "2026-11-21", "w_through": "2026-11-24",
		"w_repo_cuts": "[11111111 66666666]"})
	for _, id := range []string{"S4", "S5"} {
		partition(results, "phase1", id)
		if gatePass(results, "phase1", id) {
			t.Errorf("phase1 %s: the coverage WANT held with pending/unmarked/missing keys, want it to fail", id)
		}
	}
	rowCount(results, "phase1", "S6", 2)
	formExecSel6x14544Row(t, results["S6"], "phase1 S6", 0, map[string]string{"kind": "control", "runs": "3", "slots": "8"})
	formExecSel6x14544Row(t, results["S6"], "phase1 S6", 1, map[string]string{
		"kind": "day", "day": "2026-11-18", "runs": "1", "slots": "3", "slots_before_14045_commit": "3", "repo_cuts": "[11111111]"})
	s7Placeholder(results, "phase1")

	// The live parts of this database: the fixture sizes of the work math and P of each gate (acp READ-CARD-14544-14562-4-1 G1).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	type partSize struct{ parts, rows uint64 }
	livePartSizes := func(label string) map[string]partSize {
		t.Helper()
		sizes := map[string]partSize{}
		partRows, err := conn.Query(ctx, `SELECT table, count(), sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
			AND table IN ('test_suite_results', 'daily_metrics_touched_days', 'testops_test_metrics_daily', 'testops_release_confidence', 'repos') GROUP BY table ORDER BY table`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = partRows.Close() }()
		for partRows.Next() {
			var table string
			var count, rows uint64
			if err := partRows.Scan(&table, &count, &rows); err != nil {
				t.Fatal(err)
			}
			sizes[table] = partSize{count, rows}
			t.Logf("fixture %s %s parts=%d rows=%d", label, table, count, rows)
		}
		return sizes
	}
	sizes := livePartSizes("phase1")
	gateParts := func(id string) uint64 {
		var p uint64
		for _, table := range gateTables[id] {
			p += sizes[table].parts
		}
		return p
	}

	// The gates fire: each cap literal lowered to 0 in a test copy only. WANT code 395 STOP-COST-GATE, query_log
	// ExceptionBeforeStart, system.parts in the tables list (a control that can fail), no data table, and
	// read_rows <= 2P + 1 with P the live parts of the gate's tables in this database.
	type fired struct{ id, cap, comment string }
	var fires []fired
	for _, gate := range gates {
		for _, literal := range []string{gate.rowsCap, gate.bytesCap} {
			lowered := strings.Replace(statements[gate.id], literal, "> 0", 1)
			_, comment, err := query("gate-"+gate.id+"-"+strings.TrimPrefix(literal, "> "), lowered)
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", gate.id, literal, err)
				continue
			}
			fires = append(fires, fired{gate.id, literal, comment})
		}
	}
	// Negative mutant (acp G1 hole 2): a gate-free data scalar placed in front of S1's gate, cap 0. The check must
	// report test_suite_results (a data table read before the stop).
	negative := strings.Replace(statements["S1"], "WITH (SELECT throwIf(\n", "WITH (SELECT throwIf((SELECT count() FROM test_suite_results) >= 0 AND (\n", 1)
	negative = strings.Replace(negative, "> 4000000000,\n  'STOP-COST-GATE'))", "> 4000000000),\n  'STOP-COST-GATE'))", 1)
	negative = strings.Replace(negative, "> 20000000", "> 0", 1)
	if negative == statements["S1"] || !strings.Contains(negative, "> 4000000000),") {
		t.Fatalf("negative mutant: S1 text not rewritten")
	}
	_, negativeComment, negativeErr := query("gate-negative-S1", negative)
	if negativeErr == nil || !strings.Contains(negativeErr.Error(), "STOP-COST-GATE") {
		t.Errorf("negative mutant: err = %v, want STOP-COST-GATE", negativeErr)
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	gateCheck := func(label string, types []string, read uint64, tableLists []string, p uint64) (dataTables []string) {
		t.Helper()
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("%s: query_log types %v, want [ExceptionBeforeStart]", label, types)
		}
		hasParts := false
		for _, list := range tableLists {
			for _, table := range strings.Split(list, ",") {
				switch {
				case table == "system.parts":
					hasParts = true
				case table != "" && !strings.HasPrefix(table, "system."):
					dataTables = append(dataTables, table)
				}
			}
		}
		if !hasParts {
			t.Errorf("%s: system.parts is not in the tables list %v (the control of the check)", label, tableLists)
		}
		if read > 2*p+1 {
			t.Errorf("%s: read_rows %d > 2P+1 = %d (P = %d live parts)", label, read, 2*p+1, p)
		}
		return dataTables
	}
	gateRead := map[string]uint64{}
	for _, f := range fires {
		label := "GATE " + f.id + " " + f.cap
		types, read, tableLists := formExecSel6x14544QueryLog(ctx, t, conn, label, f.comment)
		if data := gateCheck(label, types, read, tableLists, gateParts(f.id)); len(data) > 0 {
			t.Errorf("%s: read data table(s) %v before the stop, want system tables only", label, data)
		}
		if read > gateRead[f.id] {
			gateRead[f.id] = read
		}
		t.Logf("%s read_rows=%d 2P+1=%d", label, read, 2*gateParts(f.id)+1)
	}
	if len(fires) != 2*len(gates) {
		t.Errorf("gates fired %d times, want %d", len(fires), 2*len(gates))
	}
	{
		types, _, tableLists := formExecSel6x14544QueryLog(ctx, t, conn, "GATE negative S1", negativeComment)
		flagged := false
		for _, list := range tableLists {
			for _, table := range strings.Split(list, ",") {
				if strings.HasSuffix(table, "test_suite_results") {
					flagged = true
				}
			}
		}
		if len(types) != 1 || !flagged {
			t.Errorf("negative mutant: types %v tables %v, want one row whose tables report test_suite_results", types, tableLists)
		} else {
			t.Logf("negative mutant: the check reports test_suite_results (tables %v)", tableLists)
		}
	}

	// PASS-path read_rows: the data work math on this fixture from the live sizes (a FINAL read 2R + 2P + 1, a plain read
	// R) plus the statement's own gate read (2P + 1 of its tables).
	finalRead := func(table string) uint64 { return 2*sizes[table].rows + 2*sizes[table].parts + 1 }
	plain := func(table string) uint64 { return sizes[table].rows }
	dataBounds := map[string]uint64{
		"S1": plain("test_suite_results"),
		"S2": plain("test_suite_results") + finalRead("test_suite_results"),
		"S3": finalRead("test_suite_results"),
		"S4": plain("test_suite_results") + finalRead("test_suite_results") + 2*plain("daily_metrics_touched_days") + plain("testops_test_metrics_daily"),
		"S5": plain("test_suite_results") + finalRead("test_suite_results") + 2*plain("daily_metrics_touched_days") + plain("testops_release_confidence"),
		"S6": 5*finalRead("test_suite_results") + 6*finalRead("repos"),
		"S7": finalRead("repos"),
	}
	for _, gate := range gates {
		types, read, _ := formExecSel6x14544QueryLog(ctx, t, conn, "PASS "+gate.id, passComments[gate.id])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", gate.id, types)
		}
		bound := dataBounds[gate.id] + 2*gateParts(gate.id) + 1
		if read == 0 || read > bound {
			t.Errorf("PASS %s: query_log read_rows %d, want 1 .. %d (the work math on this fixture)", gate.id, read, bound)
		}
		t.Logf("PASS %s read_rows=%d bound=%d", gate.id, read, bound)
	}

	// Phase 2: every run gets its record or window events, and recomputes 300 s or more after them.
	exec(marks + join(
		[]string{
			mark(org, repoB, "2026-10-01", "touched", d24+"12:20:00.000"),
			mark(org, repoF, "2026-10-05", "touched", d24+"12:40:00.000"),
			mark(org, repoG, "2026-11-19", "touched", d24+"13:05:00.000"),
		},
		window(repoB, "2026-10-02", 29, "touched_testops_window", d24+"12:20:00.000"),
		window(repoF, "2026-10-06", 29, "touched_testops_window", d24+"12:40:00.000"),
		window(repoG, "2026-11-20", 5, "touched_testops_window", d24+"13:05:00.000"),
		window(repoH, "2026-11-16", 9, "touched_testops_window", d24+"12:30:00.000"),
	))
	exec(tmd + join(
		[]string{metric(org, repoA, "2026-11-21", d24+"12:10:00"), metric(org, repoA, "2026-11-23", d24+"12:10:00")},
		computes(repoD, "2026-11-21", 4, d24+"12:06:00"),
		computes(repoB, "2026-10-01", 2, d24+"12:30:00"),
		computes(repoF, "2026-10-05", 3, d24+"12:50:00"),
		computes(repoG, "2026-11-19", 6, d24+"13:15:00"),
		computes(repoH, "2026-11-16", 9, d24+"12:40:00"),
	))
	results, _ = run("phase2")
	formExecSel6x14544Row(t, results["S4"], "phase2 S4", 0, map[string]string{
		"expected_keys": "85", "norow": "56", "missing_mark": "0", "fresh": "29", "pending": "0", "unmarked": "0", "w_keys": "0", "w_repos": "0"})
	partition(results, "phase2", "S4")
	if !gatePass(results, "phase2", "S4") {
		t.Errorf("phase2 S4: the coverage WANT failed with every key covered or NOROW, want PASS")
	}
	formExecSel6x14544Row(t, results["S5"], "phase2 S5", 0, map[string]string{
		"expected_keys": "85", "norow": "76", "missing_mark": "0", "fresh": "4", "pending": "5", "unmarked": "0", "w_repo_cuts": "[11111111 66666666]"})
	partition(results, "phase2", "S5")
	if gatePass(results, "phase2", "S5") {
		t.Errorf("phase2 S5: the coverage WANT held with release confidence not recomputed, want it to fail")
	}
	s7Placeholder(results, "phase2")

	// Phase 3: the agg slot of run aS retracted on the new image.
	exec(suites + strings.Join([]string{
		suite(org, repoA, "aS", "aS-agg1", "agg", "s1", 0, 0, 0, "2026-11-18", d24+"12:40:00.000", 1),
		suite(org, repoA, "aS", "aS-agg2", "agg", "s2", 0, 0, 0, "2026-11-18", d24+"12:40:00.000", 1),
	}, ", "))
	results, _ = run("phase3")
	rowCount(results, "phase3", "S6", 1)
	formExecSel6x14544Row(t, results["S6"], "phase3 S6", 0, map[string]string{"kind": "control", "runs": "3", "slots": "8"})
	formExecSel6x14544Row(t, results["S3"], "phase3 S3", 0, map[string]string{"retracted_keys": "8", "retracted_runs": "7"})
	s7Placeholder(results, "phase3")

	// Phase 4: S7 filled as the card's step M3 fills it, on rows with ids minted like the GitHub routes.
	reposID := `INSERT INTO repos (id, repo, identity_path, created_at, last_synced, org_id, provider) VALUES `
	row := func(orgID, id, name, identityPath, provider string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '2026-01-01 00:00:00.000', '2026-11-01 00:00:00.000', '%s', '%s')", id, name, identityPath, orgID, provider)
	}
	const (
		nameS = "Fx-Org/Form-Repo-Pass"
		nameY = "fx-org/form-repo-old"
		nameZ = "fx-org/form-repo-new"
		nameX = "fx-org/form-repo-taken"
	)
	idS := formExecSel6x14544Mint(nameS)
	cutS := idS[:8]
	exec(reposID + strings.Join([]string{
		row(org, idS, nameS, nameS, "github"),
		row(org, repoX, "fx/a1-repo", "fx/a1-repo", "github"),
		row(org, cutS+"-1454-4000-8000-0000000000a9", nameS, nameS, "gitlab"),
		row(other, cutS+"-1454-4000-8000-0000000000aa", nameS, nameS, "github"),
	}, ", "))
	s7Text := statements["S7"]
	s7 := func(label, text, cutValue string) (formExecSel6x14544Result, string) {
		t.Helper()
		result, comment, err := query("phase4-"+label, formExecSel6x14544Fill(t, text, cutValue))
		if err != nil {
			t.Fatalf("phase4 %s S7: %v", label, err)
		}
		rowCount(map[string]formExecSel6x14544Result{"S7": result}, "phase4 "+label, "S7", 1)
		return result, comment
	}
	pass, passS7Comment := s7("pass", s7Text, cutS)
	formExecSel6x14544Row(t, pass, "phase4 pass S7", 0, map[string]string{
		"src_cut": cutS, "github_repos": "5", "repos_with_cut": "1", "cut_name_matches": "1", "takeover_ids": "0", "cut_in_f6_1118": "0"})
	if !s7Want(pass, "phase4 pass") {
		t.Errorf("phase4 pass S7: the WANT failed (one github repo of the org minted from its current name, no takeover, cut not in the 11-18 cuts), want PASS")
	}
	shared, _ := s7("shared-cut", s7Text, "a1a1a1a1")
	formExecSel6x14544Row(t, shared, "phase4 shared-cut S7", 0, map[string]string{
		"src_cut": "a1a1a1a1", "github_repos": "5", "repos_with_cut": "1", "takeover_ids": "0", "cut_in_f6_1118": "1"})
	if s7Want(shared, "phase4 shared-cut") {
		t.Errorf("phase4 shared-cut S7: the WANT held with the cut in the 11-18 cuts, want option (b) to fail")
	}
	none, _ := s7("zero-repos", s7Text, "9f9f9f9f")
	formExecSel6x14544Row(t, none, "phase4 zero-repos S7", 0, map[string]string{
		"src_cut": "9f9f9f9f", "github_repos": "5", "repos_with_cut": "0", "cut_name_matches": "0", "takeover_ids": "0", "cut_in_f6_1118": "0"})
	if s7Want(none, "phase4 zero-repos") {
		t.Errorf("phase4 zero-repos S7: the WANT held with no repo for the cut, want NOT EXERCISED (STOP)")
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	sizes = livePartSizes("phase4")
	{
		types, read, _ := formExecSel6x14544QueryLog(ctx, t, conn, "PASS phase4 S7", passS7Comment)
		bound := finalRead("repos") + 2*sizes["repos"].parts + 1
		if len(types) != 1 || types[0] != "QueryFinish" || read == 0 || read > bound {
			t.Errorf("PASS phase4 S7: types %v read_rows %d, want [QueryFinish] and 1 .. %d", types, read, bound)
		}
		t.Logf("PASS phase4 S7 read_rows=%d bound=%d", read, bound)
	}

	// The mutants drop one new clause each, in a test copy only: the case that clause guards then passes.
	nameClause := "\n               AND startsWith(lower(hex(SHA256(lower(trimBoth(repo))))), src_cut))"
	takeoverClause := "countIf(position(identity_path, '@') > 0)"
	for _, clause := range []string{nameClause, takeoverClause} {
		if got := strings.Count(s7Text, clause); got != 1 {
			t.Fatalf("S7 holds the clause %q %d time(s), want 1", clause, got)
		}
	}
	withoutName := strings.Replace(s7Text, nameClause, ")", 1)
	withoutTakeover := strings.Replace(s7Text, takeoverClause, "countIf(0)", 1)

	// Renamed: R holds the id minted from its old name, repos.repo is its new name.
	idY := formExecSel6x14544Mint(nameY)
	exec(reposID + row(org, idY, nameZ, nameY, "github"))
	renamed, _ := s7("renamed", s7Text, idY[:8])
	formExecSel6x14544Row(t, renamed, "phase4 renamed S7", 0, map[string]string{
		"github_repos": "6", "repos_with_cut": "1", "cut_name_matches": "0", "takeover_ids": "0", "cut_in_f6_1118": "0"})
	if s7Want(renamed, "phase4 renamed") {
		t.Errorf("phase4 renamed S7: the WANT held on a repo whose current name mints another cut, want STOP")
	}
	renamedMutant, _ := s7("renamed-mutant-no-name-clause", withoutName, idY[:8])
	formExecSel6x14544Row(t, renamedMutant, "phase4 renamed mutant S7", 0, map[string]string{"cut_name_matches": "1"})
	if !s7Want(renamedMutant, "phase4 renamed mutant") {
		t.Errorf("phase4 renamed mutant: without the current-name clause the WANT still failed, want it to hold (the clause must be the guard)")
	}
	again, _ := s7("pass-after-rename", s7Text, cutS)
	if !s7Want(again, "phase4 pass after the renamed row") {
		t.Errorf("phase4 pass S7 after the renamed row: the WANT failed, want PASS")
	}

	// Takeover: A holds the id minted from nameX; B, another repository now at nameX, holds the id of "nameX@777".
	idA := formExecSel6x14544Mint(nameX)
	idB := formExecSel6x14544Mint(nameX + "@777")
	exec(reposID + strings.Join([]string{row(org, idA, nameX, nameX, "github"), row(org, idB, nameX, nameX+"@777", "github")}, ", "))
	takeover, _ := s7("takeover", s7Text, idA[:8])
	formExecSel6x14544Row(t, takeover, "phase4 takeover S7", 0, map[string]string{
		"github_repos": "8", "repos_with_cut": "1", "cut_name_matches": "1", "takeover_ids": "1", "cut_in_f6_1118": "0"})
	if s7Want(takeover, "phase4 takeover") {
		t.Errorf("phase4 takeover S7: the WANT held on the old holder of a taken-over name, want STOP")
	}
	takeoverMutant, _ := s7("takeover-mutant-no-takeover-clause", withoutTakeover, idA[:8])
	formExecSel6x14544Row(t, takeoverMutant, "phase4 takeover mutant S7", 0, map[string]string{"takeover_ids": "0"})
	if !s7Want(takeoverMutant, "phase4 takeover mutant") {
		t.Errorf("phase4 takeover mutant: without the takeover clause the WANT still failed, want it to hold (the clause must be the guard)")
	}
	orgWide, _ := s7("pass-after-takeover", s7Text, cutS)
	formExecSel6x14544Row(t, orgWide, "phase4 pass after takeover S7", 0, map[string]string{"repos_with_cut": "1", "cut_name_matches": "1", "takeover_ids": "1"})
	if s7Want(orgWide, "phase4 pass after takeover") {
		t.Errorf("phase4 S's cut after a takeover elsewhere in the org: the WANT held, want STOP (takeover_ids is org-wide, fail closed)")
	}

	// Org-2: a github row with S's cut in a second org with the prefix.
	exec(reposID + row(org2, cutS+"-1454-4000-8000-0000000000ab", nameS, nameS, "github"))
	orgTwo, _ := s7("org-2", s7Text, cutS)
	formExecSel6x14544Row(t, orgTwo, "phase4 org-2 S7", 0, map[string]string{"repos_with_cut": "2", "cut_name_matches": "2", "github_repos": "9"})
	if s7Want(orgTwo, "phase4 org-2") {
		t.Errorf("phase4 org-2 S7: the WANT held with a second org's repo under the prefix, want it to fail")
	}

	// Org mutant: a second org with the same prefix (a test copy only). S1's WANT orgs = 1 misses.
	exec(suites + suite(org2, repoA, "z1", "z1-s1", "p1", "s1", 1, 1, 0, "2026-11-20", d24+"12:00:00.000", 0))
	mutant, _, err := query("orgmutant-S1", statements["S1"])
	if err != nil {
		t.Fatalf("org mutant S1: %v", err)
	}
	formExecSel6x14544Row(t, mutant, "org mutant S1", 0, map[string]string{"orgs": "2"})
}
