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

// DO NOT MERGE. This file executes the prod read FORM-14544-SEL-4.sql byte for byte, with its org prefix swapped to
// f0f0f0f0 (the sha256 below is the sha256 of the swapped file), on the head schema with a synthetic fixture in phases:
// no retraction (the prod state at the card's writing), retractions in every class (NOROW, MISSING-MARK, FRESH, PENDING, UNMARKED), the marks and
// recomputes that cover them, and a slot retracted on the new image. It checks each WANT per phase, logs the pass-path
// read_rows of each data statement, and fires each cost gate cap once (pm-guard FORM-TEMPLATE-4).
const formExecSel4x14544SHA256 = "e9501aaeda4aaa1897c2deb621585f1e60bb7019121ad97f4949ca7833e53b7d"

const formExecSel4x14544SQL = `-- FORM-14544-SEL-4 (lane fcs-14544card, 2026-11-24, ops pin 3207ac6258c0). ClickHouse prod, SELECT only. Card CARD-14544-14562-4.
-- Supersedes FORM-14544-SEL-2 and SEL-3 (frozen). acp READ-CARD-14544-14562-1-1 (62b3bee838e2) + -1-2 (99b7c2d46d9d) folded.
-- Shape: pm-guard/FORM-TEMPLATE-4.md (4f9c5f10ec18). WANT text and work math: FORM-14544-SEL-4.md.
-- No SET, SETTINGS, EXPLAIN, FORMAT or write. Org prefix f0f0f0f0 by startsWith on every FROM with org_id.
-- Each data statement starts with WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables it reads (system.parts
-- only), and every query level that reads a table, and the outer WHERE, tests the gate.
-- Run each statement once, in order S0 .. S6, one per call. A SQL error (STOP-COST-GATE included) = STOP and report.
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
-- W11-EFB-1118 open point): the day row 2026-11-18, if present, holds no repo cut of source efb5436f's repo (pm names that
-- cut). Else STOP and a pm/acp ruling. repo_cuts = 8-char repo cuts, pm maps
-- them to the sync source (W11-EFB-1118 walked source efb5436f only). No data gate inside: the parts gate bounds it.
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
`

type formExecSel4x14544Result struct {
	columns []string
	rows    [][]string
}

func formExecSel4x14544Statements(t *testing.T, text string) ([]string, map[string]string) {
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

func formExecSel4x14544Query(ctx context.Context, conn driver.Conn, statement string) (formExecSel4x14544Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecSel4x14544Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecSel4x14544Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecSel4x14544Result{}, fmt.Errorf("scan: %w", err)
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

func formExecSel4x14544Row(t *testing.T, result formExecSel4x14544Result, label string, row int, want map[string]string) {
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

func formExecSel4x14544QueryLog(ctx context.Context, t *testing.T, conn driver.Conn, label, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows, arrayStringConcat(tables, ',')
		FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	defer func() { _ = rows.Close() }()
	var types []string
	var read uint64
	for rows.Next() {
		var typ, tables string
		var readRows uint64
		if err := rows.Scan(&typ, &readRows, &tables); err != nil {
			t.Fatalf("%s: query log scan: %v", label, err)
		}
		types = append(types, typ)
		read += readRows
		t.Logf("%s type=%s read_rows=%d tables=%s", label, typ, readRows, tables)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	return types, read
}

// The fixture. Org f0f0f0f0-...01 is the org the swapped FORM reads. Org e0e0e0e0-... holds a retraction, marks and
// metrics that must not count. Repos A, B, C (github) and 44444444 (gitlab) have repos rows (S6 reads github repos only).
// Times are on 2026-11-24 unless named. "today" = the CI day: the runs with no own record sit in October, so their window
// D .. D+29 ends before today whatever day the cell runs.
//
//	phase0: one live suite of A (run a0, day 11-20). No retraction (the prod state at the card's writing).
//	phase1: run aR of A: key k1 (11-20) live, then retracted (stamp 11:40), key k2 (11-21) live. Own record: 'touched'
//	        12:00 on 11-20 and 11-21, window 12:00 on 11-21 .. 11-24, an old 'touched' 09:00 on 11-24, another sync's
//	        'touched' 12:15 on 11-22. Metrics: 11-20 at 12:05:01 (301 s after the mark: FRESH), 11-21 at 12:01:00 (60 s
//	        after: PENDING), 11-22 at 12:30 (300 s after the LAST mark 12:15: FRESH), 11-23 at 11:59 (before the mark:
//	        PENDING), 11-24 no row (a day with no suites: NOROW).
//	        run bR of B: key k1 (10-01) retracted, stamp 10:50 (a walk whose first attempt predates T332), no record:
//	        30 keys 10-01 .. 10-30, rows on 10-01 and 10-02 (MISSING-MARK: no own record), 28 NOROW.
//	        run dR of D: key k1 (11-21) retracted, stamp 12:00:00, record 12:00:30, metrics at 11:59:30 (60 s before the
//	        mark, rows stamped 30 s before it): 4 PENDING.
//	        run fR of F: key k1 (10-05) retracted, stamp 12:30, marks only at 12:20 (older than stamp - MARGIN: no own
//	        record): 30 keys, rows on 10-05 .. 10-07 (MISSING-MARK), 27 NOROW.
//	        run gR of G: key k1 (11-19) retracted, stamp 13:00, record 12:58 (inside MARGIN: an own record, cut 11-24) but
//	        every mark older than the stamp: 6 MISSING-MARK, though the rows at 13:10 are 300 s after the mark.
//	        run hR of H: key k1 (11-15) retracted, stamp 12:00, 'touched' 12:05 on 11-15 only (its window insert failed):
//	        11-15 FRESH (rows 12:20), 11-16 .. 11-24 UNMARKED.
//	        run cR of C: retracted, then live again (revived): not selected. Run aS of A (11-18): slots p1 {s1}, p2 {s2},
//	        agg {s1, s2} with equal heads: 3 candidate slots in S6.
//	        release confidence: A 11-20 .. 11-24 at 12:10, D 11-21 .. 11-24 at 11:59:30, nothing else.
//	phase2: every run gets its record or window events, and recomputes 300 s or more after them: S4 all FRESH or NOROW.
//	phase3: the agg slot of aS retracted on the new image: its candidates leave S6.
//	org mutant: a second org with the same prefix (a test copy only): S1 orgs = 2, its WANT misses.
func TestFormExecFcs14544Sel4(t *testing.T) {
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
		other  = "e0e0e0e0-1454-4000-8000-000000000002"
		repoA  = "11111111-1454-4000-8000-0000000000a1"
		repoB  = "22222222-1454-4000-8000-0000000000b1"
		repoC  = "33333333-1454-4000-8000-0000000000c1"
		repoGl = "44444444-1454-4000-8000-0000000000d1"
		repoE  = "55555555-1454-4000-8000-0000000000e1"
		repoD  = "66666666-1454-4000-8000-0000000000f1"
		repoF  = "77777777-1454-4000-8000-0000000000f2"
		repoG  = "88888888-1454-4000-8000-0000000000f3"
		repoH  = "99999999-1454-4000-8000-0000000000f4"
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
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ` + strings.Join([]string{
		repo(org, repoA, "github"), repo(org, repoB, "github"), repo(org, repoC, "github"), repo(org, repoGl, "gitlab"),
		repo(other, repoE, "github"),
	}, ", "))

	sum := sha256.Sum256([]byte(formExecSel4x14544SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecSel4x14544SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14544-SEL-4 swapped)", got, formExecSel4x14544SHA256)
	}
	ids, statements := formExecSel4x14544Statements(t, formExecSel4x14544SQL)
	if got := strings.Join(ids, ","); got != "S0,S1,S2,S3,S4,S5,S6" {
		t.Fatalf("statements = %s, want S0,S1,S2,S3,S4,S5,S6", got)
	}
	if strings.Contains(formExecSel4x14544SQL, "c6a38355") {
		t.Fatalf("the embedded text holds the real org prefix")
	}
	for index, line := range strings.Split(formExecSel4x14544SQL, "\n") {
		if strings.HasPrefix(line, "--") && strings.Contains(line, ";") {
			t.Errorf("comment line %d holds a ';': %s", index+1, line)
		}
	}
	gates := []struct{ id, rowsCap, bytesCap string }{
		{"S1", "> 20000000", "> 4000000000"},
		{"S2", "> 6000000", "> 4000000000"},
		{"S3", "> 6000000", "> 4294967296"},
		{"S4", "> 6000000", "> 4294967296"},
		{"S5", "> 6000000", "> 4294967296"},
		{"S6", "> 1600000", "> 4294967296"},
	}
	for _, gate := range gates {
		text := statements[gate.id]
		for _, literal := range []string{gate.rowsCap, gate.bytesCap, "WITH (SELECT throwIf(", "'STOP-COST-GATE'"} {
			if got := strings.Count(text, literal); got != 1 {
				t.Errorf("%s holds %q %d time(s), want 1", gate.id, literal, got)
			}
		}
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

	run := func(phase string) (map[string]formExecSel4x14544Result, map[string]string) {
		results, comments := map[string]formExecSel4x14544Result{}, map[string]string{}
		for _, id := range ids {
			comment := fmt.Sprintf("form14544sel4-%s-%s-%d", phase, id, time.Now().UnixNano())
			comments[id] = comment
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			result, err := formExecSel4x14544Query(qctx, conn, statements[id])
			if err != nil {
				t.Errorf("FORM-14544-SEL-4 %s %s: %v", phase, id, err)
				continue
			}
			t.Logf("FORM-14544-SEL-4 %s %s columns %v", phase, id, result.columns)
			for index, row := range result.rows {
				t.Logf("FORM-14544-SEL-4 %s %s row %d %v", phase, id, index, row)
			}
			results[id] = result
		}
		return results, comments
	}
	rowCount := func(results map[string]formExecSel4x14544Result, label, id string, want int) {
		t.Helper()
		if got := len(results[id].rows); got != want {
			t.Errorf("%s %s: %d row(s), want %d", label, id, got, want)
		}
	}
	cells := func(results map[string]formExecSel4x14544Result, id string) map[string]string {
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
	// gatePass is the coverage WANT; partition checks that every expected key is in exactly one class.
	gatePass := func(results map[string]formExecSel4x14544Result, label, id string) bool {
		t.Helper()
		cell := cells(results, id)
		held := len(cell) > 0 && cell["pending"] == "0" && cell["unmarked"] == "0" && cell["missing_mark"] == "0"
		t.Logf("%s %s: coverage WANT (pending 0, unmarked 0, missing_mark 0) held = %v", label, id, held)
		return held
	}
	partition := func(results map[string]formExecSel4x14544Result, label, id string) {
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

	// Phase 0: no retraction. The coverage gates PASS on 0 expected keys.
	exec(suites + suite(org, repoA, "a0", "a0-s1", "p1", "s1", 4, 4, 0, "2026-11-20", d24+"11:30:00.000", 0))
	exec(tmd + metric(org, repoA, "2026-11-20", "2026-11-21 00:00:00"))
	results, _ := run("phase0")
	rowCount(results, "phase0", "S0", 5)
	for index, table := range []string{"daily_metrics_touched_days", "repos", "test_suite_results", "testops_release_confidence", "testops_test_metrics_daily"} {
		formExecSel4x14544Row(t, results["S0"], "phase0 S0", index, map[string]string{"table_name": table})
	}
	formExecSel4x14544Row(t, results["S1"], "phase0 S1", 0, map[string]string{"orgs": "1", "org_rows": "1"})
	formExecSel4x14544Row(t, results["S2"], "phase0 S2", 0, map[string]string{"repo_days": "0", "repos": "0"})
	formExecSel4x14544Row(t, results["S3"], "phase0 S3", 0, map[string]string{"newest_keys": "1", "retracted_keys": "0", "stamped_before_t332_keys": "0"})
	for _, id := range []string{"S4", "S5"} {
		formExecSel4x14544Row(t, results[id], "phase0 "+id, 0, map[string]string{"expected_keys": "0", "norow": "0", "missing_mark": "0", "fresh": "0", "pending": "0", "unmarked": "0"})
		partition(results, "phase0", id)
		if !gatePass(results, "phase0", id) {
			t.Errorf("phase0 %s: the coverage WANT failed on 0 retractions, want PASS", id)
		}
	}
	rowCount(results, "phase0", "S6", 1)
	formExecSel4x14544Row(t, results["S6"], "phase0 S6", 0, map[string]string{"kind": "control", "runs": "3", "slots": "1"})

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
	formExecSel4x14544Row(t, results["S1"], "phase1 S1", 0, map[string]string{"orgs": "1", "org_rows": "15"})
	formExecSel4x14544Row(t, results["S2"], "phase1 S2", 0, map[string]string{
		"repo_days": "4", "repos": "3", "first_day": "2026-11-19", "last_day": "2026-11-21"})
	formExecSel4x14544Row(t, results["S3"], "phase1 S3", 0, map[string]string{
		"newest_keys": "13", "retracted_keys": "6", "stamped_before_t332_keys": "1", "retracted_runs": "6", "retracted_repos": "6",
		"first_retracted_day": "2026-10-01", "last_retracted_day": "2026-11-21", "first_stamp_day": "2026-11-24", "last_stamp_day": "2026-11-24"})
	formExecSel4x14544Row(t, results["S4"], "phase1 S4", 0, map[string]string{
		"expected_keys": "85", "repos": "6", "norow": "56", "missing_mark": "11", "fresh": "3", "pending": "6", "unmarked": "9",
		"w_keys": "26", "w_repos": "6", "w_from": "2026-10-01", "w_to": "2026-11-21", "w_through": "2026-11-24",
		"w_repo_cuts": "[11111111 22222222 66666666 77777777 88888888 99999999]"})
	formExecSel4x14544Row(t, results["S5"], "phase1 S5", 0, map[string]string{
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
	formExecSel4x14544Row(t, results["S6"], "phase1 S6", 0, map[string]string{"kind": "control", "runs": "3", "slots": "8"})
	formExecSel4x14544Row(t, results["S6"], "phase1 S6", 1, map[string]string{
		"kind": "day", "day": "2026-11-18", "runs": "1", "slots": "3", "slots_before_14045_commit": "3", "repo_cuts": "[11111111]"})

	// PASS-path read_rows against the work math on this fixture (FORM-14544-SEL-4.md): test_suite_results R 16 rows in
	// P 3 parts, daily_metrics_touched_days 22 / 1, testops_test_metrics_daily 31 / 2, testops_release_confidence 9 / 1,
	// repos 5 / 1. A FINAL read 2R + 2P + 1, a plain read R, the gate 2 x (parts of its tables) + 1.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	partRows, err := conn.Query(ctx, `SELECT table, count(), sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
		AND table IN ('test_suite_results', 'daily_metrics_touched_days', 'testops_test_metrics_daily', 'testops_release_confidence', 'repos') GROUP BY table ORDER BY table`)
	if err != nil {
		t.Fatal(err)
	}
	for partRows.Next() {
		var table string
		var count, rows uint64
		if err := partRows.Scan(&table, &count, &rows); err != nil {
			t.Fatal(err)
		}
		t.Logf("fixture phase1 %s parts=%d rows=%d", table, count, rows)
	}
	_ = partRows.Close()
	bounds := map[string]uint64{
		"S1": 16 + (2*3 + 1),
		"S2": (3*16 + 2*3 + 1) + (2*3 + 1),
		"S3": (2*16 + 2*3 + 1) + (2*3 + 1),
		"S4": (3*16 + 2*3 + 1) + 2*22 + 31 + (2*6 + 1),
		"S5": (3*16 + 2*3 + 1) + 2*22 + 9 + (2*5 + 1),
		"S6": 5*(2*16+2*3+1) + 6*(2*5+2*1+1) + (2*4 + 1),
	}
	for _, gate := range gates {
		types, read := formExecSel4x14544QueryLog(ctx, t, conn, "PASS "+gate.id, passComments[gate.id])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", gate.id, types)
		}
		if read == 0 || read > bounds[gate.id] {
			t.Errorf("PASS %s: query_log read_rows %d, want 1 .. %d (the work math on this fixture)", gate.id, read, bounds[gate.id])
		}
		t.Logf("PASS %s read_rows=%d bound=%d", gate.id, read, bounds[gate.id])
	}

	// The gates fire: each cap literal lowered to 0 in a test copy only. WANT code 395 STOP-COST-GATE, query_log
	// ExceptionBeforeStart, read_rows <= 2 x P + 1.
	gateReadMax := map[string]uint64{"S1": 2*3 + 1, "S2": 2*3 + 1, "S3": 2*3 + 1, "S4": 2*6 + 1, "S5": 2*5 + 1, "S6": 2*4 + 1}
	type fired struct{ id, cap, comment string }
	var fires []fired
	for _, gate := range gates {
		for _, literal := range []string{gate.rowsCap, gate.bytesCap} {
			lowered := strings.Replace(statements[gate.id], literal, "> 0", 1)
			comment := fmt.Sprintf("form14544sel4-gate-%s-%s-%d", gate.id, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			_, err := formExecSel4x14544Query(qctx, conn, lowered)
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", gate.id, literal, err)
				continue
			}
			fires = append(fires, fired{gate.id, literal, comment})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	for _, f := range fires {
		types, read := formExecSel4x14544QueryLog(ctx, t, conn, "GATE "+f.id+" "+f.cap, f.comment)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.id, f.cap, types)
		}
		if read > gateReadMax[f.id] {
			t.Errorf("GATE %s %s: query_log read_rows %d over 2 x P + 1 = %d", f.id, f.cap, read, gateReadMax[f.id])
		}
	}
	if len(fires) != 2*len(gates) {
		t.Errorf("gates fired %d times, want %d", len(fires), 2*len(gates))
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
	formExecSel4x14544Row(t, results["S4"], "phase2 S4", 0, map[string]string{
		"expected_keys": "85", "norow": "56", "missing_mark": "0", "fresh": "29", "pending": "0", "unmarked": "0", "w_keys": "0", "w_repos": "0"})
	partition(results, "phase2", "S4")
	if !gatePass(results, "phase2", "S4") {
		t.Errorf("phase2 S4: the coverage WANT failed with every key covered or NOROW, want PASS")
	}
	formExecSel4x14544Row(t, results["S5"], "phase2 S5", 0, map[string]string{
		"expected_keys": "85", "norow": "76", "missing_mark": "0", "fresh": "4", "pending": "5", "unmarked": "0", "w_repo_cuts": "[11111111 66666666]"})
	partition(results, "phase2", "S5")
	if gatePass(results, "phase2", "S5") {
		t.Errorf("phase2 S5: the coverage WANT held with release confidence not recomputed, want it to fail")
	}

	// Phase 3: the agg slot of run aS retracted on the new image.
	exec(suites + strings.Join([]string{
		suite(org, repoA, "aS", "aS-agg1", "agg", "s1", 0, 0, 0, "2026-11-18", d24+"12:40:00.000", 1),
		suite(org, repoA, "aS", "aS-agg2", "agg", "s2", 0, 0, 0, "2026-11-18", d24+"12:40:00.000", 1),
	}, ", "))
	results, _ = run("phase3")
	rowCount(results, "phase3", "S6", 1)
	formExecSel4x14544Row(t, results["S6"], "phase3 S6", 0, map[string]string{"kind": "control", "runs": "3", "slots": "8"})
	formExecSel4x14544Row(t, results["S3"], "phase3 S3", 0, map[string]string{"retracted_keys": "8", "retracted_runs": "7"})

	// Org mutant: a second org with the same prefix (a test copy only). S1's WANT orgs = 1 misses.
	exec(suites + suite(org2, repoA, "z1", "z1-s1", "p1", "s1", 1, 1, 0, "2026-11-20", d24+"12:00:00.000", 0))
	qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": fmt.Sprintf("form14544sel4-orgmutant-%d", time.Now().UnixNano())}))
	mutant, err := formExecSel4x14544Query(qctx, conn, statements["S1"])
	if err != nil {
		t.Fatalf("org mutant S1: %v", err)
	}
	formExecSel4x14544Row(t, mutant, "org mutant S1", 0, map[string]string{"orgs": "2"})
}
