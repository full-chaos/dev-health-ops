-- SIZING-13902-3 = SIZING-13902-2 (8f6b03b41cce, acp FORM copy) + cost gate + roll time, lane l13902-form3, 2026-11-23.
-- Prod: NOT EXECUTED. Executed once on CI (synthetic rows, schema at the chain head): EXEC-PROOF-13902-3.md.
-- Edits vs -2 (the -2 header edits E1-E9 stay, see that file):
--   G1 New stmt S0 first = cost gate: active parts of the 3 read tables from system.parts (rows, bytes on disk, parts).
--      Each later statement has a WANT read_rows bound from the S0 rows and a STOP-before rule (SIZING-13902-3.md).
--      No settings clause and no set statement anywhere: the gate is a read plus WANT lines, not a setting.
--   G2 {{ROLL_END_UTC}} filled = '2026-11-23 02:58:22' = creation time of the FIRST new-image worker pod of STEP 323
--      (RECORD 8ff7c60b985f, workers at ops 340c3e05, which carries the fix 9d88395188e6, rollout end 03:05:21Z).
--      Reason: an old-rule row is only a row computed BEFORE the first possible new-image write. A row computed in the
--      rollout window (02:58:22-03:05:21Z) can come from a new pod, so it counts as move_after_roll, never as old_checked.
--   G3 Every derived table has an alias (stmt1 cicd_keys / testops_keys, stmt3 run_rows). No alias equals an input
--      column name of its own SELECT (S0 reads rows / bytes_on_disk and names them table_rows / disk_bytes).
--   G4 stmt4 (raw span) removed: not needed for the pm ruling (ADDENDUM-1: stmt 2/3 decide).
-- Run rule: each statement once, in order. A SQL error = STOP. A STOP rule or a read_rows WANT miss = STOP (acp reads).

-- S0 COST GATE. WANT 3 rows (one per table), table_rows > 0 in each. Unit: all orgs (upper bound for the org reads).
SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')
GROUP BY table
ORDER BY table;

-- stmt0 ORG-1. WANT raw_orgs = 1, cicd_orgs = 1, testops_orgs = 1
SELECT
  (SELECT uniqExact(org_id) FROM ci_pipeline_runs WHERE startsWith(org_id, 'c6a38355')) AS raw_orgs,
  (SELECT uniqExact(org_id) FROM cicd_metrics_daily WHERE startsWith(org_id, 'c6a38355')) AS cicd_orgs,
  (SELECT uniqExact(org_id) FROM testops_pipeline_metrics_daily WHERE startsWith(org_id, 'c6a38355')) AS testops_orgs;

-- stmt1 RANGE of the stored live repo-days (= queue-table rows a full-range recompute rewrites).
-- WANT live_repo_days > 0 in both rows. INFO first_day, last_day, days.
SELECT 'cicd' AS t, count() AS live_repo_days, min(day) AS first_day, max(day) AS last_day, uniqExact(day) AS days
FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS r FROM cicd_metrics_daily
      WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS cicd_keys WHERE r = 0
UNION ALL
SELECT 'testops' AS t, count() AS live_repo_days, min(day) AS first_day, max(day) AS last_day, uniqExact(day) AS days
FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS r FROM testops_pipeline_metrics_daily
      WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS testops_keys WHERE r = 0;

-- stmt2 CICD. Raw = the cicd source (cicd/clickhouse.go:51-57): finished_at day, status not deleted, not running/queued.
-- WANT live_repo_days > 0, with_raw > 0, ctrl_checked > 0, ctrl_match = ctrl_checked.
-- WANT no_raw = 0 (a live key with no raw run gets a RETRACTION row at recompute, cicd/fence.go:66-69, :84-93).
-- WANT move_repo_days >= 1 (0 = NOT EXERCISED for cicd). WANT old_match = old_checked (a miss = acp reads).
-- INFO value_move (stored values that change), to_null, move_days, first/last, move_after_roll, move_raw_changed.
SELECT count() AS live_repo_days, countIf(ifNull(raw.runs, 0) > 0) AS with_raw,
  countIf(ifNull(raw.runs, 0) = 0) AS no_raw,
  minIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_first_day, maxIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_last_day,
  countIf(raw.rr > 0) AS move_repo_days, uniqExactIf(s.day, raw.rr > 0) AS move_days,
  minIf(s.day, raw.rr > 0) AS move_first_day, maxIf(s.day, raw.rr > 0) AS move_last_day,
  countIf(raw.rr > 0 AND raw.n_new = 0) AS to_null,
  countIf(raw.rr > 0 AND s.c >= toDateTime('2026-11-23 02:58:22', 'UTC')) AS move_after_roll,
  countIf(raw.rr > 0 AND raw.ls >= s.c) AS move_raw_changed,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c) AS old_checked,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS old_match,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND (raw.n_new = 0
               OR abs(raw.sum_old / raw.n_old - raw.sum_new / raw.n_new) > 1e-6 * greatest(1, raw.sum_new / raw.n_new))) AS value_move,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c) AS ctrl_checked,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS ctrl_match
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        tupleElement(argMax(tuple(avg_queue_minutes), computed_at), 1) AS q
      FROM cicd_metrics_daily WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT repo_id, toDate(assumeNotNull(finished_at)) AS day, count() AS runs, max(last_synced) AS ls,
    countIf(retry_count > 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS rr,
    countIf(queued_at IS NOT NULL AND started_at >= queued_at) AS n_old,
    sumIf(dateDiff('millisecond', queued_at, started_at) / 60000, queued_at IS NOT NULL AND started_at >= queued_at) AS sum_old,
    countIf(retry_count = 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS n_new,
    sumIf(dateDiff('millisecond', queued_at, started_at) / 60000, retry_count = 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS sum_new
  FROM ci_pipeline_runs FINAL
  WHERE startsWith(org_id, 'c6a38355') AND finished_at IS NOT NULL
    AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued')
  GROUP BY repo_id, day
) AS raw ON raw.repo_id = s.repo_id AND raw.day = s.day
WHERE s.r = 0;

-- stmt3 TESTOPS. Raw = the testops pipeline source (testops_native_clickhouse.go:126-130): started_at day, status not deleted.
-- Old sample = the raw arm text before the fix (queryapi/testopspooled/testopspooled.go:100-101 minus the retry_count guard).
-- WANT / INFO: as stmt2 (no_raw keys are retired by supersedeStaleTeamKeys, testops_native_executor.go:169-185).
SELECT count() AS live_repo_days, countIf(ifNull(raw.runs, 0) > 0) AS with_raw,
  countIf(ifNull(raw.runs, 0) = 0) AS no_raw,
  minIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_first_day, maxIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_last_day,
  countIf(raw.rr > 0) AS move_repo_days, uniqExactIf(s.day, raw.rr > 0) AS move_days,
  minIf(s.day, raw.rr > 0) AS move_first_day, maxIf(s.day, raw.rr > 0) AS move_last_day,
  countIf(raw.rr > 0 AND raw.n_new = 0) AS to_null,
  countIf(raw.rr > 0 AND s.c >= toDateTime('2026-11-23 02:58:22', 'UTC')) AS move_after_roll,
  countIf(raw.rr > 0 AND raw.ls >= s.c) AS move_raw_changed,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c) AS old_checked,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS old_match,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND (raw.n_new = 0
               OR abs(raw.sum_old / raw.n_old - raw.sum_new / raw.n_new) > 1e-6 * greatest(1, raw.sum_new / raw.n_new))) AS value_move,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c) AS ctrl_checked,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS ctrl_match
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        tupleElement(argMax(tuple(avg_queue_seconds), computed_at), 1) AS q
      FROM testops_pipeline_metrics_daily WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT repo_id, day, count() AS runs, max(run_ls) AS ls, countIf(retry_count > 0 AND qs IS NOT NULL) AS rr,
    count(qs) AS n_old, sum(qs) AS sum_old,
    countIf(retry_count = 0 AND qs IS NOT NULL) AS n_new, sumIf(qs, retry_count = 0) AS sum_new
  FROM (
    SELECT repo_id, toDate(started_at) AS day, last_synced AS run_ls, retry_count,
      if(queue_seconds >= 0, queue_seconds,
         if(started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 1000, NULL)) AS qs
    FROM ci_pipeline_runs FINAL
    WHERE startsWith(org_id, 'c6a38355') AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'
  ) AS run_rows
  GROUP BY repo_id, day
) AS raw ON raw.repo_id = s.repo_id AND raw.day = s.day
WHERE s.r = 0;
