//go:build integration

package daily

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
)

// Throwaway exec proof (CHAOS-13902, not for merge): the GONE statement (stmt4) of the prod read form FLOOR-13902-4 counts,
// per table, exactly the live keys that the real end of a run of the whole organization (RunStaleKeyRetractor.RetractStaleKeys)
// zeroes; the form's synthesized repository id equals icfinalize.SynthesizedRepoID; the form's canceled exclusion equals
// deploymentstatus.CanceledExclusionSQL. floor4Form is the form file with its org prefix (8 hex) replaced by "f0f0f0f0";
// its sha256 is pinned.
const floor4GoneSHA256 = "733f79a10335c5d34fc1616ba0ee87ad185d0da13e76a60921767e7bd23f4c97"

const floor4GoneForm = `-- FLOOR-13902-4: raw floor + gone-repository read of CARD-13902-recompute-14 (CHAOS-13902), every family the recompute can
--   ERASE. ClickHouse, SELECT only, FOUR statements (one per connector call), run by pm ONCE per chunk at the run minute, right
--   before 'chunk-start.sh <FROM> <TO> <DIGEST> --go' (card s4 step 1, (y)), and ONCE right before the pass-2 batch over
--   [2026-04-17, P2_TO_DAY] (chunk P2F; finalize-pass2.sh run --go checks it). Lane l13902 (lead fcdev), 2026-11-24.
--   Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-4.md. acp FORM read: OPEN. Supersedes FLOOR-13902-3 1a9ae3c8bf4d
--   (+ acp READ-CARD-13902-12-1 75c1b3e48f70 s3: stmt4 GONE, ic_landscape live rule + inputs, synth input,
--   repo_change_failure incidents + the loader's day rule, deploy canceled exclusion, incident org-scope note).
--   Code cites at ops 3207ac6258c0 (the run binary, STEP 332).
-- Placeholders: {{FROM}} {{TO}} = the chunk bounds; {{FILL_ID}} = 12 hex written by the fill (S-P2-1 a). Only
--   'chunk-start.sh --floor-sql <FROM> <TO>' fills them (ledger line FLOOR-FILL <C> fill=<id> sql_sha12=<..>).
-- Why: SIZING-14285-6.out (e0d4de33c436 :7) raw git_commits start 2025-11-01, stored daily rows from 2025-07-26. A family
--   that ERASEs (writes zeros or retracted = 1 over a live stored key it does not produce, card s13) turns a day with live
--   stored keys and no raw source into zeros. Floor(T, S) = max(card --from 2026-04-17, first day of raw or input table T
--   for scope S, all days, read now). S = the repo when T has a repo key, else the org.
-- Output columns, every row (18): stmt, family (stored table), raw_table, scope_id (repo id CUT to its first 8 hex, or 'org';
--   no full id), raw_first_day ('none' = no raw row), stored_first_day (first stored day in [FROM, TO]), stored_days,
--   read_rows_cap, orgs (ORG-1: uniqExact(org_id) of one table of the statement, WANT 1), fam_rows (the statement's family
--   rows), scopes_full / scopes_cut (distinct full ids / 8-hex cuts of those rows: WANT equal), read_at (server now(), UTC,
--   to the second), fill_id (the fill nonce), gone_keys, gone_repos, gone_first_day, empty_repo_keys (stmt4 only; 0 / 'none'
--   in stmt1-3). Plus ONE control row per statement (family '_stmt', raw_table '_cap'). Joins use the full ids.
-- ERASE families and their raw / input (card s13):
--   stmt1 repo_metrics_daily (git commits, PRs, reviews: reviews are no source of its retire rule
--   repo_user_commit_native_executor.go:241-248, kept as a safe false-STOP source), user_metrics_daily (same 3; the
--   ic_finalize synthesized keys left out, see stmt3), commit_metrics, team_metrics_daily (commits), ai_impact_metrics_daily
--   (PRs; reviews only load after PRs or incidents exist, ai_impact_native_executor.go:133: a safe false-STOP source; the
--   incident arm is stmt3 ai_impact_metrics_daily.org).
--   stmt2 cicd (ci_pipeline_runs finished: writer cicd/clickhouse.go:51-57, testops/status_export.go:15 "deleted", :20-21
--   "running"/"queued", :36 NotOpenRunSQL), testops pipeline / coverage / release_confidence / quality_drag /
--   pipeline_stability (started arm != 'deleted': testops_native_clickhouse.go:126-130), testops_test (the kept suite rows of
--   the loader, testops_native_clickhouse.go:140, queryapi/testopspooled/fragments.go:113-122, :165-188), deploy (deployed_at,
--   canceled excluded as the loader: deploy_native_clickhouse.go:23-28, deploymentstatus.go:58 CanceledStatuses, :85-112
--   CanceledExclusionSQL rendered here as text), repo_change_failure_daily (deployments: the change-failure loader
--   LoadWorkGraphEdgeDeployments, work_graph_edges_native_clickhouse.go:164-167, day = coalesce(deployed_at, finished_at,
--   started_at, last_synced), called repo_user_commit_native_executor.go:132; canceled is "not a deployment"
--   change_failure_native_clickhouse.go:229-239; and incidents: LoadIncidentsStarted :136, incident_native_clickhouse.go:79,
--   an org-scope raw = the org's first started_at day, is_deleted 0: a stored repo with nothing to count gets zeros,
--   repouser/changefailure.go:11-41).
--   stmt3 (org scope): incident (keys hold repo_id; the floor is org-wide = lower or equal to any per-repo mapping floor =
--   fewer STOPs; said here, acp P3); the 6 work-item tables; team_complexity; team_cognitive_load (input user_metrics_daily
--   only: the executor takes user OR team rows, team_cognitive_load_native_executor.go:88-99, so this floor can only be
--   higher = safe); compounding_risk team; ai_governance (input view ai_attribution_resolved, ai_governance_native_clickhouse.go
--   :44); work_item_measure_capability (live = newest item_count > 0 per (provider, measure, window_end); retirement rows
--   work_item_measure_capability.go:186-245); ic_landscape_rolling_30d (live = teamkeytables ICLandscapeRolling30d LiveHaving,
--   teamkeytables.go:234-245, :517-538; zeros over points not produced, ic_finalize_native_executor.go:51-63) vs
--   user_metrics_daily.live AND work_item_user_metrics_daily.live (icfinalize/loader.go:115, :167, :260); user_metrics_daily.synth
--   (synthesized repo_id = UUIDv5(1b4e28ba-2fa1-11d2-883f-b9a761bde3fb, org_id || 0x1F || author_email), icfinalize/executor.go
--   :130, :159-160; retracted :64-84, stale_team_keys.go:404-425) vs work_item_user_metrics_daily.live;
--   ai_impact_metrics_daily.org (incidents, ai_impact_native_executor.go:120-130); benchmarking (retireDay over the SIX output
--   tables when the input window holds no live key and the run is full_org, benchmarking_finalize_native_executor.go:198-230;
--   empty run :162-177 / benchmarking_empty_run_retire.go:148-183; retireDay :187-201, tables :46-62, live key :74-80; inputs
--   testops pipeline / test / coverage stored live, benchmarking/run.go:14-25 + comparisons.go:17-21 -> run.go:229-250
--   InputTables, benchmarking/clickhouse.go:372-428 LiveInputKeys).
--   stmt4 GONE: the 8 repository tables of the full-org finalize (stmt4 header).
-- No ERASE (no row here; card s13 with cites): file_metrics_daily, file_hotspot_daily, review_edges_daily,
--   compounding_risk_daily scope repo, ai_workflow_*, work_graph_*_edges, work_item_team_attributions, work_item_cycle_times
--   (retracts only items with an OPEN raw copy, work_item_cycle_retract.go:207-300), ai_policy_events (insert only,
--   ai_governance_native_clickhouse.go:435-478; in no stale-key table set).
-- Cost gate per statement (SIZING-14285-6 D1-D3 shape, acp READ-SIZING-14285-6-1 OK): read_rows_cap = f x ceil(1.25 x
--   (sum(marks x k) x 8192 + P)) + (1 + s) x (2P + 1) (s = the scalar subqueries that use cost_gate: 1 in stmt1-3 (ORG-1),
--   3 in stmt4); throwIf(read_rows_cap >= 20000000). A gate run reads P + Q + 1 metadata rows per scalar (Q = outdated parts
--   still listed), inside the cap headroom. pm WANT: read_rows <= read_rows_cap.

-- stmt1 GIT families (scope = repo).
-- Stored (live day of a repo): repo_metrics_daily and user_metrics_daily newest row per key not retracted (user_metrics_daily
--   without the ic_finalize synthesized keys: repo_id = UUIDv5(1b4e28ba-2fa1-11d2-883f-b9a761bde3fb, org_id || 0x1F ||
--   author_email), icfinalize/executor.go:124-160, executor.go:333; they are stmt3 family user_metrics_daily.synth); commit_metrics,
--   team_metrics_daily (repo_id != ''), ai_impact_metrics_daily every stored row (no retracted column).
-- Raw: git_commits FINAL toDate(committer_when); git_pull_requests FINAL toDate(created_at); git_pull_request_reviews
--   toDate(submitted_at) (raw, as the loader). Cost gate k repo_metrics_daily 2 (+ the ORG-1 scalar), every other table 1, f 2 (FINAL), s 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'repo_metrics_daily', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_impact_metrics_daily', 'commit_metrics', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'team_metrics_daily', 'user_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table,
  if(o.scope_full = 'org', 'org', substring(o.scope_full, 1, 8)) AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows,
  uniqExactIf(o.scope_full, o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_full,
  uniqExactIf(substring(o.scope_full, 1, 8), o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id,
  toUInt64(0) AS gone_keys, toUInt64(0) AS gone_repos, 'none' AS gone_first_day, toUInt64(0) AS empty_repo_keys
FROM (
  SELECT toUInt8(1) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_full,
    if(ifNull(r.present, 0) = 1, toString(r.f), 'none') AS raw_first_day, toString(p.p_sf) AS stored_first_day,
    toUInt64(p.p_sd) AS stored_days
  FROM (
    SELECT st.s_family AS p_family, st.s_scope AS p_scope, st.s_sf AS p_sf, st.s_sd AS p_sd, raw_name AS p_raw
    FROM (
    SELECT 'repo_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM repo_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'user_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT u.repo_id AS repo_id, u.day AS day, u.rt AS rt,
            toString(u.repo_id) = UUIDNumToString(toFixedString(concat(substring(u.h, 1, 6), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 7, 1)), 15), 80)),
              substring(u.h, 8, 1), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 9, 1)), 63), 128)), substring(u.h, 10, 7)), 16)) AS synth
          FROM (SELECT repo_id, author_email, day, argMax(retracted, computed_at) AS rt,
                  SHA1(concat(UUIDStringToNum('1b4e28ba-2fa1-11d2-883f-b9a761bde3fb'), any(org_id), '\x1F', author_email)) AS h
                FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id, author_email, day) AS u) AS k
    WHERE k.rt = 0 AND NOT k.synth GROUP BY k.repo_id
    UNION ALL
    SELECT 'commit_metrics' AS s_family, toString(repo_id) AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM commit_metrics WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id
    UNION ALL
    SELECT 'team_metrics_daily' AS s_family, toString(repo_id) AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM team_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') AND repo_id != '' GROUP BY repo_id
    UNION ALL
    SELECT 'ai_impact_metrics_daily' AS s_family, toString(repo_id) AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM ai_impact_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id
    ) AS st
    ARRAY JOIN multiIf(st.s_family = 'repo_metrics_daily', ['git_commits', 'git_pull_requests', 'git_pull_request_reviews'],
                     st.s_family = 'user_metrics_daily', ['git_commits', 'git_pull_requests', 'git_pull_request_reviews'],
                     st.s_family = 'commit_metrics', ['git_commits'],
                     st.s_family = 'team_metrics_daily', ['git_commits'],
                     st.s_family = 'ai_impact_metrics_daily', ['git_pull_requests', 'git_pull_request_reviews'],
                     ['?']) AS raw_name
  ) AS p
  LEFT JOIN (
    SELECT 'git_commits' AS r_raw, toString(repo_id) AS r_scope, min(toDate(committer_when)) AS f, toUInt8(1) AS present
    FROM git_commits FINAL WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id
    UNION ALL
    SELECT 'git_pull_requests' AS r_raw, toString(repo_id) AS r_scope, min(toDate(created_at)) AS f, toUInt8(1) AS present
    FROM git_pull_requests FINAL WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id
    UNION ALL
    SELECT 'git_pull_request_reviews' AS r_raw, toString(repo_id) AS r_scope, min(toDate(submitted_at)) AS f, toUInt8(1) AS present
    FROM git_pull_request_reviews WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id
  ) AS r ON r.r_raw = p.p_raw AND r.r_scope = p.p_scope
  UNION ALL
  SELECT toUInt8(1) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_full, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY family, raw_table, scope_id;

-- stmt2 CI / TESTOPS / DEPLOY families (scope = repo).
-- Stored: newest row per (repo_id, day) not retracted (cicd, testops pipeline/test/coverage, the 3 testops_risk tables,
--   deploy); repo_change_failure_daily every stored row (no retracted column).
-- Raw: ci_pipeline_runs FINAL .finished = toDate(finished_at) (finished, not deleted/running/queued) and .started =
--   toDate(started_at) (not deleted); test_suite_results FINAL toDate(coalesce(started_at, finished_at)) of the KEPT rows
--   only, one scan with window functions = the loader's rule at 3207ac6258c0 (testops_native_clickhouse.go:140
--   testopsKeptSuitesToday = SuiteWindowSource + SuiteRowsNewestAttempt; queryapi/testopspooled/fragments.go:165-188
--   rowsNewestAttempt: newest run_attempt of each report slot, retracted 0, slot not retired :113-115 (argMax(retracted,
--   (run_attempt, last_synced, retracted)) = 1), a legacy row (report_slot '') only when no newest-attempt slotted row of the
--   run lists its suite_id in legacy_suite_ids and no row of a retired slot lists it :119-122; CH 171 retracted column,
--   CHAOS-14259; the window bound of the loader is whole runs, so a run's rows are judged together as here); coverage_snapshots
--   FINAL joined to ci_pipeline_runs FINAL on (org_id, repo_id, run_id), day = toDate(started_at) of the run;
--   deployments FINAL, canceled excluded (deploymentstatus CanceledExclusionSQL as text): .deployed = toDate(deployed_at)
--   (deploy loader), .any = toDate(coalesce(deployed_at, finished_at, started_at, last_synced)) (the change-failure loader);
--   operational_incidents FINAL org first toDate(started_at), is_deleted 0 = the 2nd raw of repo_change_failure_daily (an
--   org-scope raw: the join takes 'org' for it, the out keeps the repo cut).
-- Cost gate k ci_pipeline_runs 3, deployments 2, cicd_metrics_daily 2 (+ ORG-1), every other table 1; f 2; s 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'ci_pipeline_runs', 3, table = 'cicd_metrics_daily', 2, table = 'deployments', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'coverage_snapshots', 'deploy_metrics_daily', 'deployments', 'operational_incidents', 'repo_change_failure_daily', 'test_suite_results', 'testops_coverage_metrics_daily', 'testops_pipeline_metrics_daily', 'testops_pipeline_stability', 'testops_quality_drag', 'testops_release_confidence', 'testops_test_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table,
  if(o.scope_full = 'org', 'org', substring(o.scope_full, 1, 8)) AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM cicd_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows,
  uniqExactIf(o.scope_full, o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_full,
  uniqExactIf(substring(o.scope_full, 1, 8), o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id,
  toUInt64(0) AS gone_keys, toUInt64(0) AS gone_repos, 'none' AS gone_first_day, toUInt64(0) AS empty_repo_keys
FROM (
  SELECT toUInt8(2) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_full,
    if(ifNull(r.present, 0) = 1, toString(r.f), 'none') AS raw_first_day, toString(p.p_sf) AS stored_first_day,
    toUInt64(p.p_sd) AS stored_days
  FROM (
    SELECT st.s_family AS p_family, st.s_scope AS p_scope, st.s_sf AS p_sf, st.s_sd AS p_sd, raw_name AS p_raw,
      if(raw_name = 'operational_incidents', 'org', st.s_scope) AS p_rscope
    FROM (
    SELECT 'cicd_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM cicd_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'testops_pipeline_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_pipeline_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'testops_test_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_test_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'testops_coverage_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_coverage_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'testops_release_confidence' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_release_confidence
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'testops_quality_drag' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_quality_drag
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'testops_pipeline_stability' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_pipeline_stability
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'deploy_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM deploy_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'repo_change_failure_daily' AS s_family, toString(repo_id) AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM repo_change_failure_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id
    ) AS st
    ARRAY JOIN multiIf(st.s_family = 'cicd_metrics_daily', ['ci_pipeline_runs.finished'],
                     st.s_family = 'testops_pipeline_metrics_daily', ['ci_pipeline_runs.started'],
                     st.s_family = 'testops_test_metrics_daily', ['test_suite_results'],
                     st.s_family = 'testops_coverage_metrics_daily', ['ci_pipeline_runs.started', 'coverage_snapshots'],
                     st.s_family = 'testops_release_confidence', ['ci_pipeline_runs.started'],
                     st.s_family = 'testops_quality_drag', ['ci_pipeline_runs.started'],
                     st.s_family = 'testops_pipeline_stability', ['ci_pipeline_runs.started'],
                     st.s_family = 'deploy_metrics_daily', ['deployments.deployed'],
                     st.s_family = 'repo_change_failure_daily', ['deployments.any', 'operational_incidents'],
                     ['?']) AS raw_name
  ) AS p
  LEFT JOIN (
    SELECT 'ci_pipeline_runs.finished' AS r_raw, toString(repo_id) AS r_scope, min(toDate(assumeNotNull(finished_at))) AS f, toUInt8(1) AS present
    FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND finished_at IS NOT NULL AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued') GROUP BY repo_id
    UNION ALL
    SELECT 'ci_pipeline_runs.started' AS r_raw, toString(repo_id) AS r_scope, min(toDate(started_at)) AS f, toUInt8(1) AS present
    FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted' GROUP BY repo_id
    UNION ALL
    SELECT 'test_suite_results' AS r_raw, toString(w.repo_id) AS r_scope, min(w.d) AS f, toUInt8(1) AS present
    FROM (SELECT v.repo_id AS repo_id, v.d AS d, v.has_day AS has_day, v.report_slot AS report_slot, v.suite_id AS suite_id,
            v.run_attempt AS run_attempt, v.retracted AS retracted, v.slot_max_attempt AS slot_max_attempt, v.slot_retired AS slot_retired,
            groupUniqArrayArrayIf(v.legacy_suite_ids, v.report_slot != '' AND v.run_attempt = v.slot_max_attempt) OVER (PARTITION BY v.repo_id, v.run_id) AS listed_newest,
            groupUniqArrayArrayIf(v.legacy_suite_ids, v.report_slot != '' AND v.slot_retired = 1) OVER (PARTITION BY v.repo_id, v.run_id) AS listed_retired
          FROM (SELECT repo_id, run_id, report_slot, suite_id, run_attempt, retracted, legacy_suite_ids,
                  toDate(assumeNotNull(coalesce(started_at, finished_at))) AS d, coalesce(started_at, finished_at) IS NOT NULL AS has_day,
                  max(run_attempt) OVER (PARTITION BY repo_id, run_id, report_slot) AS slot_max_attempt,
                  argMax(retracted, (run_attempt, last_synced, retracted)) OVER (PARTITION BY repo_id, run_id, report_slot) AS slot_retired
                FROM test_suite_results FINAL WHERE startsWith(org_id, 'f0f0f0f0')) AS v) AS w
    WHERE w.has_day AND ((w.report_slot != '' AND w.run_attempt = w.slot_max_attempt AND w.retracted = 0 AND w.slot_retired = 0)
      OR (w.report_slot = '' AND NOT has(w.listed_newest, w.suite_id) AND NOT has(w.listed_retired, w.suite_id)))
    GROUP BY w.repo_id
    UNION ALL
    SELECT 'coverage_snapshots' AS r_raw, toString(cs.c_repo) AS r_scope, min(toDate(pr.p_started)) AS f, toUInt8(1) AS present
    FROM (SELECT org_id AS c_org, repo_id AS c_repo, run_id AS c_run FROM coverage_snapshots FINAL WHERE startsWith(org_id, 'f0f0f0f0')) AS cs
    INNER JOIN (SELECT org_id AS p_org, repo_id AS p_repo, run_id AS p_run, started_at AS p_started FROM ci_pipeline_runs FINAL
                WHERE startsWith(org_id, 'f0f0f0f0')) AS pr ON pr.p_org = cs.c_org AND pr.p_repo = cs.c_repo AND pr.p_run = cs.c_run
    GROUP BY cs.c_repo
    UNION ALL
    SELECT 'deployments.deployed' AS r_raw, toString(repo_id) AS r_scope, min(toDate(assumeNotNull(deployed_at))) AS f, toUInt8(1) AS present
    FROM deployments FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND deployed_at IS NOT NULL
      AND lowerUTF8(replaceRegexpAll(coalesce(status, ''), '^(?:\\x09|\\x0a|\\x0b|\\x0c|\\x0d|\\x20|\xC2\x85|\xC2\xA0|\xE1\x9A\x80|\xE2\x80\x80|\xE2\x80\x81|\xE2\x80\x82|\xE2\x80\x83|\xE2\x80\x84|\xE2\x80\x85|\xE2\x80\x86|\xE2\x80\x87|\xE2\x80\x88|\xE2\x80\x89|\xE2\x80\x8A|\xE2\x80\xA8|\xE2\x80\xA9|\xE2\x80\xAF|\xE2\x81\x9F|\xE3\x80\x80)+|(?:\\x09|\\x0a|\\x0b|\\x0c|\\x0d|\\x20|\xC2\x85|\xC2\xA0|\xE1\x9A\x80|\xE2\x80\x80|\xE2\x80\x81|\xE2\x80\x82|\xE2\x80\x83|\xE2\x80\x84|\xE2\x80\x85|\xE2\x80\x86|\xE2\x80\x87|\xE2\x80\x88|\xE2\x80\x89|\xE2\x80\x8A|\xE2\x80\xA8|\xE2\x80\xA9|\xE2\x80\xAF|\xE2\x81\x9F|\xE3\x80\x80)+$', '')) NOT IN ('canceled', 'cancelled')
    GROUP BY repo_id
    UNION ALL
    SELECT 'deployments.any' AS r_raw, toString(repo_id) AS r_scope, min(toDate(coalesce(deployed_at, finished_at, started_at, last_synced))) AS f, toUInt8(1) AS present
    FROM deployments FINAL WHERE startsWith(org_id, 'f0f0f0f0')
      AND lowerUTF8(replaceRegexpAll(coalesce(status, ''), '^(?:\\x09|\\x0a|\\x0b|\\x0c|\\x0d|\\x20|\xC2\x85|\xC2\xA0|\xE1\x9A\x80|\xE2\x80\x80|\xE2\x80\x81|\xE2\x80\x82|\xE2\x80\x83|\xE2\x80\x84|\xE2\x80\x85|\xE2\x80\x86|\xE2\x80\x87|\xE2\x80\x88|\xE2\x80\x89|\xE2\x80\x8A|\xE2\x80\xA8|\xE2\x80\xA9|\xE2\x80\xAF|\xE2\x81\x9F|\xE3\x80\x80)+|(?:\\x09|\\x0a|\\x0b|\\x0c|\\x0d|\\x20|\xC2\x85|\xC2\xA0|\xE1\x9A\x80|\xE2\x80\x80|\xE2\x80\x81|\xE2\x80\x82|\xE2\x80\x83|\xE2\x80\x84|\xE2\x80\x85|\xE2\x80\x86|\xE2\x80\x87|\xE2\x80\x88|\xE2\x80\x89|\xE2\x80\x8A|\xE2\x80\xA8|\xE2\x80\xA9|\xE2\x80\xAF|\xE2\x81\x9F|\xE3\x80\x80)+$', '')) NOT IN ('canceled', 'cancelled')
    GROUP BY repo_id
    UNION ALL
    SELECT 'operational_incidents' AS r_raw, 'org' AS r_scope, min(toDate(assumeNotNull(started_at))) AS f, toUInt8(1) AS present
    FROM operational_incidents FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND is_deleted = 0 AND started_at IS NOT NULL HAVING count() > 0
  ) AS r ON r.r_raw = p.p_raw AND r.r_scope = p.p_rscope
  UNION ALL
  SELECT toUInt8(2) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_full, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY family, raw_table, scope_id;

-- stmt3 ORG-SCOPE families (no repo key on the raw side, or a stored input).
-- Stored (org-wide first stored day in the chunk): incident_metrics_daily and work_item_user_metrics_daily newest row per
--   key not retracted; work_item_metrics_daily, estimate_coverage_metrics_daily, work_item_state_durations_daily,
--   issue_type_metrics_daily, work_item_completion_daily, team_complexity_daily, team_cognitive_load_daily,
--   compounding_risk_daily (scope 'team'), ai_governance_coverage_daily: every stored row.
-- Raw / input (org-wide first day, ALL days): operational_incidents FINAL toDate(started_at), is_deleted 0;
--   work_items FINAL toDate(created_at); repo_complexity_daily day (stored by a job this card does not run);
--   user_metrics_daily / repo_metrics_daily first LIVE day (the stored inputs of team_cognitive_load / compounding team);
--   ai_attribution FINAL toDate(observed_at), superseded_by IS NULL (= the view ai_attribution_resolved).
--   work_item_measure_capability (live = newest item_count > 0 per (provider, measure, window_end); day = window_end;
--   retirement rows work_item_measure_capability.go:186-245), ic_landscape_rolling_30d (day = as_of_day; live = the newest row
--   holds a measure (teamkeytables LiveHaving); zeros over points not produced, ic_finalize_native_executor.go:51-63) vs
--   user_metrics_daily.live AND work_item_user_metrics_daily.live (first day of a key whose newest row is not retracted), and
--   user_metrics_daily.synth (the ic_finalize synthesized keys, :64-84) vs work_item_user_metrics_daily.live.
--   benchmarking: the six output tables (day column per table, newest retracted 0 per key) vs the 3 testops input tables.
-- Cost gate k user_metrics_daily 3 (the .live input, the .synth family, the ORG-1 scalar), work_item_user_metrics_daily 2 (the
--   family + the .live input), every other table 1, f 2, s 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'user_metrics_daily', 3, table = 'work_item_user_metrics_daily', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_attribution', 'ai_governance_coverage_daily', 'ai_impact_metrics_daily', 'compounding_risk_daily', 'estimate_coverage_metrics_daily', 'ic_landscape_rolling_30d', 'incident_metrics_daily', 'issue_type_metrics_daily', 'operational_incidents', 'repo_complexity_daily', 'repo_metrics_daily', 'team_cognitive_load_daily', 'team_complexity_daily', 'testops_benchmark_insights', 'testops_coverage_metrics_daily', 'testops_maturity_bands', 'testops_metric_anomalies', 'testops_metric_baselines', 'testops_metric_correlations', 'testops_period_comparisons', 'testops_pipeline_metrics_daily', 'testops_test_metrics_daily', 'user_metrics_daily', 'work_item_completion_daily', 'work_item_measure_capability', 'work_item_metrics_daily', 'work_item_state_durations_daily', 'work_item_user_metrics_daily', 'work_items')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table,
  if(o.scope_full = 'org', 'org', substring(o.scope_full, 1, 8)) AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows,
  uniqExactIf(o.scope_full, o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_full,
  uniqExactIf(substring(o.scope_full, 1, 8), o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id,
  toUInt64(0) AS gone_keys, toUInt64(0) AS gone_repos, 'none' AS gone_first_day, toUInt64(0) AS empty_repo_keys
FROM (
  SELECT toUInt8(3) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_full,
    if(ifNull(r.present, 0) = 1, toString(r.f), 'none') AS raw_first_day, toString(p.p_sf) AS stored_first_day,
    toUInt64(p.p_sd) AS stored_days
  FROM (
    SELECT st.s_family AS p_family, st.s_scope AS p_scope, st.s_sf AS p_sf, st.s_sd AS p_sd, raw_name AS p_raw
    FROM (
    SELECT 'incident_metrics_daily' AS s_family, 'org' AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM incident_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 HAVING count() > 0
    UNION ALL
    SELECT 'work_item_metrics_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM work_item_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'work_item_user_metrics_daily' AS s_family, 'org' AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT provider, work_scope_id, user_identity, day, row_view, argMax(retracted, computed_at) AS rt FROM work_item_user_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY provider, work_scope_id, user_identity, day, row_view) AS k
    WHERE k.rt = 0 HAVING count() > 0
    UNION ALL
    SELECT 'estimate_coverage_metrics_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM estimate_coverage_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'work_item_state_durations_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM work_item_state_durations_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'issue_type_metrics_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM issue_type_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'work_item_completion_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM work_item_completion_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'team_complexity_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM team_complexity_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'team_cognitive_load_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM team_cognitive_load_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'compounding_risk_daily.team' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM compounding_risk_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') AND scope = 'team' HAVING count() > 0
    UNION ALL
    SELECT 'ai_governance_coverage_daily' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM ai_governance_coverage_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'work_item_measure_capability' AS s_family, 'org' AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT provider, measure, window_end AS day, argMax(item_count, computed_at) AS ic FROM work_item_measure_capability
          WHERE startsWith(org_id, 'f0f0f0f0') AND window_end >= toDate('{{FROM}}') AND window_end <= toDate('{{TO}}')
          GROUP BY provider, measure, window_end) AS k
    WHERE k.ic > 0 HAVING count() > 0
    UNION ALL
    SELECT 'ic_landscape_rolling_30d' AS s_family, 'org' AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT as_of_day AS day FROM ic_landscape_rolling_30d
          WHERE startsWith(org_id, 'f0f0f0f0') AND as_of_day >= toDate('{{FROM}}') AND as_of_day <= toDate('{{TO}}')
          GROUP BY repo_id, team_id, map_name, identity_id, as_of_day
          HAVING argMax(x_raw, computed_at) != 0 OR argMax(y_raw, computed_at) != 0 OR argMax(x_norm, computed_at) != 0 OR argMax(y_norm, computed_at) != 0 OR argMax(churn_loc_30d, computed_at) != 0 OR argMax(delivery_units_30d, computed_at) != 0 OR argMax(wip_max_30d, computed_at) != 0 OR isNotNull(tupleElement(argMax(tuple(cycle_p50_30d_hours), computed_at), 1))) AS k
    HAVING count() > 0
    UNION ALL
    SELECT 'user_metrics_daily.synth' AS s_family, 'org' AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT u.repo_id AS repo_id, u.day AS day, u.rt AS rt,
            toString(u.repo_id) = UUIDNumToString(toFixedString(concat(substring(u.h, 1, 6), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 7, 1)), 15), 80)),
              substring(u.h, 8, 1), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 9, 1)), 63), 128)), substring(u.h, 10, 7)), 16)) AS synth
          FROM (SELECT repo_id, author_email, day, argMax(retracted, computed_at) AS rt,
                  SHA1(concat(UUIDStringToNum('1b4e28ba-2fa1-11d2-883f-b9a761bde3fb'), any(org_id), '\x1F', author_email)) AS h
                FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id, author_email, day) AS u) AS k
    WHERE k.rt = 0 AND k.synth HAVING count() > 0
    UNION ALL
    SELECT 'ai_impact_metrics_daily.org' AS s_family, 'org' AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM ai_impact_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') HAVING count() > 0
    UNION ALL
    SELECT 'benchmarking' AS s_family, 'org' AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT period_end AS day, argMax(retracted, computed_at) AS rt FROM testops_metric_baselines
          WHERE startsWith(org_id, 'f0f0f0f0') AND period_end >= toDate('{{FROM}}') AND period_end <= toDate('{{TO}}') GROUP BY metric_name, scope_type, scope_key, rolling_window_days, period_end
          UNION ALL
          SELECT period_end AS day, argMax(retracted, computed_at) AS rt FROM testops_maturity_bands
          WHERE startsWith(org_id, 'f0f0f0f0') AND period_end >= toDate('{{FROM}}') AND period_end <= toDate('{{TO}}') GROUP BY metric_name, scope_type, scope_key, period_end
          UNION ALL
          SELECT day AS day, argMax(retracted, computed_at) AS rt FROM testops_metric_anomalies
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY metric_name, scope_type, scope_key, anomaly_type, day
          UNION ALL
          SELECT current_period_end AS day, argMax(retracted, computed_at) AS rt FROM testops_period_comparisons
          WHERE startsWith(org_id, 'f0f0f0f0') AND current_period_end >= toDate('{{FROM}}') AND current_period_end <= toDate('{{TO}}') GROUP BY metric_name, scope_type, scope_key, comparison_period_end, current_period_end
          UNION ALL
          SELECT period_end AS day, argMax(retracted, computed_at) AS rt FROM testops_metric_correlations
          WHERE startsWith(org_id, 'f0f0f0f0') AND period_end >= toDate('{{FROM}}') AND period_end <= toDate('{{TO}}') GROUP BY metric_name, paired_metric_name, scope_type, scope_key, period_end
          UNION ALL
          SELECT assumeNotNull(period_end) AS day, argMax(retracted, computed_at) AS rt FROM testops_benchmark_insights
          WHERE startsWith(org_id, 'f0f0f0f0') AND assumeNotNull(period_end) >= toDate('{{FROM}}') AND assumeNotNull(period_end) <= toDate('{{TO}}') GROUP BY insight_id, period_end) AS k
    WHERE k.rt = 0 HAVING count() > 0
    ) AS st
    ARRAY JOIN multiIf(st.s_family = 'incident_metrics_daily', ['operational_incidents'],
                     st.s_family = 'work_item_metrics_daily', ['work_items'],
                     st.s_family = 'work_item_user_metrics_daily', ['work_items'],
                     st.s_family = 'estimate_coverage_metrics_daily', ['work_items'],
                     st.s_family = 'work_item_state_durations_daily', ['work_items'],
                     st.s_family = 'issue_type_metrics_daily', ['work_items'],
                     st.s_family = 'work_item_completion_daily', ['work_items'],
                     st.s_family = 'team_complexity_daily', ['repo_complexity_daily'],
                     st.s_family = 'team_cognitive_load_daily', ['user_metrics_daily.live'],
                     st.s_family = 'compounding_risk_daily.team', ['repo_metrics_daily.live'],
                     st.s_family = 'ai_governance_coverage_daily', ['ai_attribution'],
                     st.s_family = 'work_item_measure_capability', ['work_items'],
                     st.s_family = 'ic_landscape_rolling_30d', ['user_metrics_daily.live', 'work_item_user_metrics_daily.live'],
                     st.s_family = 'user_metrics_daily.synth', ['work_item_user_metrics_daily.live'],
                     st.s_family = 'ai_impact_metrics_daily.org', ['operational_incidents'],
                     st.s_family = 'benchmarking', ['benchmarking.inputs'],
                     ['?']) AS raw_name
  ) AS p
  LEFT JOIN (
    SELECT 'operational_incidents' AS r_raw, 'org' AS r_scope, min(toDate(assumeNotNull(started_at))) AS f, toUInt8(1) AS present
    FROM operational_incidents FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND is_deleted = 0 AND started_at IS NOT NULL HAVING count() > 0
    UNION ALL
    SELECT 'work_items' AS r_raw, 'org' AS r_scope, min(toDate(created_at)) AS f, toUInt8(1) AS present
    FROM work_items FINAL WHERE startsWith(org_id, 'f0f0f0f0') HAVING count() > 0
    UNION ALL
    SELECT 'repo_complexity_daily' AS r_raw, 'org' AS r_scope, min(day) AS f, toUInt8(1) AS present
    FROM repo_complexity_daily WHERE startsWith(org_id, 'f0f0f0f0') HAVING count() > 0
    UNION ALL
    SELECT 'work_item_user_metrics_daily.live' AS r_raw, 'org' AS r_scope, min(w.day) AS f, toUInt8(1) AS present
    FROM (SELECT provider, work_scope_id, user_identity, day, row_view, argMax(retracted, computed_at) AS rt FROM work_item_user_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY provider, work_scope_id, user_identity, day, row_view) AS w WHERE w.rt = 0 HAVING count() > 0
    UNION ALL
    SELECT 'user_metrics_daily.live' AS r_raw, 'org' AS r_scope, min(u.day) AS f, toUInt8(1) AS present
    FROM (SELECT repo_id, author_email, day, argMax(retracted, computed_at) AS rt FROM user_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, author_email, day) AS u WHERE u.rt = 0 HAVING count() > 0
    UNION ALL
    SELECT 'repo_metrics_daily.live' AS r_raw, 'org' AS r_scope, min(m.day) AS f, toUInt8(1) AS present
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM repo_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, day) AS m WHERE m.rt = 0 HAVING count() > 0
    UNION ALL
    SELECT 'ai_attribution' AS r_raw, 'org' AS r_scope, min(toDate(observed_at)) AS f, toUInt8(1) AS present
    FROM ai_attribution_resolved WHERE startsWith(toString(org_id), 'f0f0f0f0') HAVING count() > 0
    UNION ALL
    SELECT 'benchmarking.inputs' AS r_raw, 'org' AS r_scope, min(b.day) AS f, toUInt8(1) AS present
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_pipeline_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, day
          UNION ALL
          SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_test_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, day
          UNION ALL
          SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_coverage_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, day) AS b
    WHERE b.rt = 0 HAVING count() > 0
  ) AS r ON r.r_raw = p.p_raw AND r.r_scope = p.p_scope
  UNION ALL
  SELECT toUInt8(3) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_full, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY family, raw_table, scope_id;

-- stmt4 GONE (acp READ-CARD-13902-12-1 s3 (3): the full-org finalize, stale_team_keys_run.go:83-93, :290-333).
-- A run of the whole organization (daily-start with no --repo-id: full_org, daily/postgres.go:308-314) ends with
--   retractStaleTeamKeysOutsideRun (stale_team_keys.go:672-760) for the 8 OrganizationRunStaleKeyTables: a LIVE key of the day
--   (teamkeytables LiveHaving: newest row retracted 0 for user + testops x5, a measure != 0 / a nullable measure NOT NULL for
--   team and ai_impact; teamkeytables.go:517-538, the table specs :180-210, :291-345) whose repository (ScopeTuple = the
--   repo_id text, :596-606) is in no partition of the run AND not in the org's repositories (present = ClickHouseRepositoryDiscoverer
--   .RepositoryIDs: every repos id of the org, any version, + the nil id when a work item has no repository; daily/clickhouse.go
--   :85-125, :226-240) gets a row of zeros. Synthesized user keys are skipped (stale_team_keys.go:714-718). Raw rows of a gone
--   repository do not save its keys. For a full-org run partitions = the same discovery (stale_team_keys_run.go:301-307), so a
--   key is ERASED when its repo_id is not in held_repos.
-- repo_id '' (legacy team_metrics_daily rows written before migration 080, wellbeing_native_clickhouse.go:276-284): '' is in
--   no partition and no repos id, so the rule zeroes such a live key too: counted as empty_repo_keys (lib: STOP, acp reads).
-- Rows: one per table (8, always), family gone:<table>, raw_table repos, scope org, raw_first_day 1970-01-01 (no day floor),
--   stored_first_day = first day of a live key in [FROM, TO] (the chunk FROM day when none), stored_days = live keys in [FROM, TO] (the
--   data control: their sum WANT > 0), gone_keys (WANT 0), gone_repos, gone_first_day, empty_repo_keys (WANT 0).
-- Cost gate k repos 2 (held_repos + ORG-1), every other table 1, f 1 (no FINAL), s 3 (ORG-1, held_repos, the nil-id check).
WITH (SELECT 1 * intDiv(5 * (sum(marks * multiIf(table = 'repos', 2, 1)) * 8192 + count()) + 3, 4) + 4 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_impact_metrics_daily', 'repos', 'team_metrics_daily', 'testops_coverage_metrics_daily', 'testops_pipeline_stability', 'testops_quality_drag', 'testops_release_confidence', 'testops_test_metrics_daily', 'user_metrics_daily', 'work_items')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate,
  (SELECT arrayConcat(groupUniqArray(toString(id)),
     if((SELECT count() FROM work_items WHERE startsWith(org_id, 'f0f0f0f0') AND repo_id = toUUID('00000000-0000-0000-0000-000000000000') AND cost_gate = 0) > 0, ['00000000-0000-0000-0000-000000000000'], []))
   FROM repos WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS held_repos
SELECT o.stmt AS stmt, o.family AS family, if(o.family = '_stmt', '_cap', 'repos') AS raw_table, 'org' AS scope_id,
  if(o.family = '_stmt', 'none', '1970-01-01') AS raw_first_day,
  if(o.live_keys > 0, toString(o.live_first), '{{FROM}}') AS stored_first_day, toUInt64(o.live_keys) AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM repos WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows, toUInt64(0) AS scopes_full, toUInt64(0) AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id,
  toUInt64(o.gone_keys) AS gone_keys, toUInt64(o.gone_repos) AS gone_repos,
  if(o.gone_keys > 0, toString(o.gone_first), 'none') AS gone_first_day, toUInt64(o.empty_keys) AS empty_repo_keys
FROM (
  SELECT toUInt8(4) AS stmt, a.family AS family, a.live_keys AS live_keys, a.gone_keys AS gone_keys, a.gone_repos AS gone_repos,
    a.gone_first AS gone_first, a.live_first AS live_first, a.empty_keys AS empty_keys
  FROM (
    SELECT 'gone:ai_impact_metrics_daily' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM ai_impact_metrics_daily
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY team_id, repo_id, work_type, attribution_bucket, day
                HAVING argMax(prs_total, computed_at) != 0 OR argMax(prs_merged, computed_at) != 0 OR argMax(ai_assisted_prs, computed_at) != 0 OR argMax(agent_created_prs, computed_at) != 0 OR argMax(human_prs, computed_at) != 0 OR argMax(unknown_prs, computed_at) != 0 OR argMax(agent_created_pr_count, computed_at) != 0 OR argMax(rework_prs, computed_at) != 0 OR argMax(followup_commits_count, computed_at) != 0 OR argMax(revert_prs, computed_at) != 0 OR argMax(incidents_count, computed_at) != 0 OR argMax(test_gap_prs, computed_at) != 0 OR argMax(leverage_prs_component, computed_at) != 0 OR isNotNull(tupleElement(argMax(tuple(ai_assisted_pr_ratio), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(cycle_time_avg_hours), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(baseline_cycle_time_avg_hours), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(ai_cycle_time_delta_hours), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(reviews_per_pr), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(baseline_reviews_per_pr), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(ai_review_amplification), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(changes_requested_per_pr), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(rework_drag_rate), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(revert_rate), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(incident_drag_rate), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(test_gap_rate), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(leverage_cycle_time_component), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(leverage_review_component), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(leverage_rework_component), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(leverage_test_component), computed_at), 1)) OR isNotNull(tupleElement(argMax(tuple(leverage_incident_component), computed_at), 1))) AS k) AS g
    UNION ALL
    SELECT 'gone:team_metrics_daily' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM team_metrics_daily
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY team_id, repo_id, day
                HAVING argMax(commits_count, computed_at) != 0 OR argMax(after_hours_commits_count, computed_at) != 0 OR argMax(weekend_commits_count, computed_at) != 0 OR argMax(after_hours_commit_ratio, computed_at) != 0 OR argMax(weekend_commit_ratio, computed_at) != 0) AS k) AS g
    UNION ALL
    SELECT 'gone:testops_coverage_metrics_daily' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM testops_coverage_metrics_daily
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY repo_id, day
                HAVING argMax(retracted, computed_at) = 0) AS k) AS g
    UNION ALL
    SELECT 'gone:testops_pipeline_stability' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM testops_pipeline_stability
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY repo_id, day
                HAVING argMax(retracted, computed_at) = 0) AS k) AS g
    UNION ALL
    SELECT 'gone:testops_quality_drag' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM testops_quality_drag
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY repo_id, day
                HAVING argMax(retracted, computed_at) = 0) AS k) AS g
    UNION ALL
    SELECT 'gone:testops_release_confidence' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM testops_release_confidence
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY repo_id, day
                HAVING argMax(retracted, computed_at) = 0) AS k) AS g
    UNION ALL
    SELECT 'gone:testops_test_metrics_daily' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(repo_id) AS repo, day FROM testops_test_metrics_daily
                WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
                GROUP BY repo_id, day
                HAVING argMax(retracted, computed_at) = 0) AS k) AS g
    UNION ALL
    SELECT 'gone:user_metrics_daily' AS family, count() AS live_keys, countIf(g.gone) AS gone_keys, uniqExactIf(g.repo, g.gone) AS gone_repos,
      minIf(g.day, g.gone) AS gone_first, min(g.day) AS live_first, countIf(g.repo = '') AS empty_keys
    FROM (SELECT k.repo AS repo, k.day AS day, k.repo != '' AND NOT has(held_repos, k.repo) AS gone
          FROM (SELECT toString(k.repo_id) AS repo, k.day AS day
              FROM (SELECT u.repo_id AS repo_id, u.day AS day, u.rt AS rt,
                      toString(u.repo_id) = UUIDNumToString(toFixedString(concat(substring(u.h, 1, 6), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 7, 1)), 15), 80)),
                        substring(u.h, 8, 1), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 9, 1)), 63), 128)), substring(u.h, 10, 7)), 16)) AS synth
                    FROM (SELECT repo_id, author_email, day, argMax(retracted, computed_at) AS rt,
                            SHA1(concat(UUIDStringToNum('1b4e28ba-2fa1-11d2-883f-b9a761bde3fb'), any(org_id), '\x1F', author_email)) AS h
                          FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id, author_email, day) AS u) AS k
                WHERE k.rt = 0 AND NOT k.synth) AS k) AS g
  ) AS a
  UNION ALL
  SELECT toUInt8(4) AS stmt, '_stmt' AS family, toUInt64(0) AS live_keys, toUInt64(0) AS gone_keys, toUInt64(0) AS gone_repos,
    toDate('1970-01-01') AS gone_first, toDate('1970-01-01') AS live_first, toUInt64(0) AS empty_keys
) AS o
WHERE cost_gate = 0
ORDER BY family, raw_table, scope_id;
`

func floor4Stmt(t *testing.T, marker string) string {
	t.Helper()
	i := strings.Index(floor4GoneForm, marker)
	if i < 0 || strings.Count(floor4GoneForm, marker) != 1 {
		t.Fatalf("marker %q not found once", marker)
	}
	var kept []string
	for _, line := range strings.Split(floor4GoneForm[i:], "\n") {
		kept = append(kept, line)
		if !strings.HasPrefix(strings.TrimSpace(line), "--") && strings.HasSuffix(strings.TrimSpace(line), ";") {
			return strings.TrimSuffix(strings.Join(kept, "\n"), ";")
		}
	}
	t.Fatalf("no end of %q", marker)
	return ""
}

func TestFloor13902v4GoneCountsWhatTheRunOfTheWholeOrganizationZeroes(t *testing.T) {
	sum := sha256.Sum256([]byte(floor4GoneForm))
	if got := hex.EncodeToString(sum[:]); got != floor4GoneSHA256 {
		t.Fatalf("FLOOR-13902-4 (prefix swapped) sha256 = %s, want %s", got, floor4GoneSHA256)
	}
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "f0f0f0f0-0000-4000-8000-0000000000d1"
	const other = "b0000000-0000-4000-8000-0000000000d2"
	day := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	stored := day.Add(30 * time.Hour)
	clock := stored.Add(10 * time.Hour)
	held := uuid.MustParse("11111111-0000-4000-8000-0000000000a1")
	gone := uuid.MustParse("22222222-0000-4000-8000-0000000000a2")
	synthID := "w@x"
	synth := icfinalize.SynthesizedRepoID(org, synthID)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	// 1. The synthesized repository id: the form's SQL text vs icfinalize.SynthesizedRepoID, 3 identities.
	stmt1 := floor4Stmt(t, "-- stmt1 GIT families")
	expr := stmt1[strings.Index(stmt1, "toString(u.repo_id) = ")+len("toString(u.repo_id) = "):]
	expr = expr[:strings.Index(expr, " AS synth")]
	if !strings.HasPrefix(expr, "UUIDNumToString(") || !strings.Contains(stmt1, "SHA1(concat(UUIDStringToNum('1b4e28ba-2fa1-11d2-883f-b9a761bde3fb'), any(org_id), '\\x1F', author_email)) AS h") {
		t.Fatalf("the synthesized-id text of the form is not the expected shape: %s", expr)
	}
	identities := []string{"w@x", "Zoë.Ünïcode@example.org", "first.last+tag@sub.example.com"}
	rows, err := conn.Query(ctx, "SELECT u.e, "+expr+" FROM (SELECT e, SHA1(concat(UUIDStringToNum('1b4e28ba-2fa1-11d2-883f-b9a761bde3fb'), ?, '\\x1F', e)) AS h FROM (SELECT arrayJoin(?) AS e)) AS u ORDER BY u.e",
		org, identities)
	if err != nil {
		t.Fatalf("synth query: %v", err)
	}
	seen := 0
	for rows.Next() {
		var e, got string
		if err := rows.Scan(&e, &got); err != nil {
			t.Fatal(err)
		}
		want := icfinalize.SynthesizedRepoID(org, e).String()
		if got != want {
			t.Errorf("SYNTH %q: form SQL %s, icfinalize.SynthesizedRepoID %s", e, got, want)
		} else {
			t.Logf("SYNTH %q: form SQL = icfinalize.SynthesizedRepoID = %s", e, got)
		}
		seen++
	}
	_ = rows.Close()
	if seen != len(identities) {
		t.Fatalf("synth query returned %d rows, want %d", seen, len(identities))
	}

	// 2. The canceled exclusion: the form's literal pattern string = the one deploymentstatus renders, and the status list.
	stmt2 := floor4Stmt(t, "-- stmt2 CI / TESTOPS / DEPLOY")
	const head = "lowerUTF8(replaceRegexpAll(coalesce(status, ''), "
	i := strings.Index(stmt2, head)
	formLit := stmt2[i+len(head):]
	formLit = formLit[:strings.Index(formLit, ", '')) NOT IN ('canceled', 'cancelled')")]
	goSQL := deploymentstatus.CanceledExclusionSQL("status")
	if !strings.HasPrefix(goSQL, head) || !strings.HasSuffix(goSQL, ", '')) NOT IN {canceled_statuses:Array(String)}") {
		t.Fatalf("CanceledExclusionSQL has another shape: %s", goSQL)
	}
	goLit := strings.TrimSuffix(strings.TrimPrefix(goSQL, head), ", '')) NOT IN {canceled_statuses:Array(String)}")
	var same uint8
	if err := conn.QueryRow(ctx, "SELECT "+formLit+" = "+goLit).Scan(&same); err != nil || same != 1 {
		t.Fatalf("the form's canceled pattern is not the deploymentstatus pattern (same=%d err=%v)", same, err)
	}
	if strings.Join(deploymentstatus.CanceledStatuses, ",") != "canceled,cancelled" {
		t.Fatalf("CanceledStatuses = %v, the form lists canceled, cancelled", deploymentstatus.CanceledStatuses)
	}
	t.Logf("CANCELED: the form's pattern literal evaluates equal to deploymentstatus.CanceledExclusionSQL's; status list canceled, cancelled")

	// 3. GONE vs the real end of a run of the whole organization.
	exec("repo held", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, 'acme/held', ?, 'github', ?)", held, org, stored)
	exec("repo of another org with the gone id", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, 'b/gone', ?, 'github', ?)", gone, other, stored)
	for _, repo := range []string{held.String(), gone.String(), ""} {
		exec("team row", `INSERT INTO team_metrics_daily (org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count, computed_at)
    VALUES (?, ?, 'ENG', 'ENG', ?, 7, 1, 0, ?)`, org, day, repo, stored)
	}
	for _, repo := range []uuid.UUID{held, gone} {
		exec("ai_impact row", `INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, human_prs, computed_at)
    VALUES (?, 'ENG', ?, 'feature', ?, 'human', 5, 5, 5, ?)`, org, repo, day, stored)
		exec("user row", `INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES (?, ?, 'a@x', ?, 3, ?, 0)`,
			org, repo, day, stored)
		for _, table := range []string{"testops_test_metrics_daily", "testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability", "testops_coverage_metrics_daily"} {
			exec(table+" row", "INSERT INTO "+table+" (org_id, repo_id, day, computed_at, retracted) VALUES (?, ?, ?, ?, 0)", org, repo, day, stored)
		}
	}
	exec("ai_impact row with no measure (not live)", `INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket, computed_at)
    VALUES (?, 'ENG', ?, 'bug', ?, 'human', ?)`, org, gone, day, stored)
	exec("synthesized user key", `INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES (?, ?, ?, ?, 0, ?, 0)`,
		org, synth, synthID, day, stored)
	exec("retracted user key of the gone repo", `INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES (?, ?, 'r@x', ?, 2, ?, 0), (?, ?, 'r@x', ?, 0, ?, 1)`,
		org, gone, day, stored, org, gone, day, stored.Add(time.Hour))

	stmt4 := strings.NewReplacer("{{FROM}}", day.Format(time.DateOnly), "{{TO}}", day.Format(time.DateOnly), "{{FILL_ID}}", "a1b2c3d4e5f6").Replace(floor4Stmt(t, "-- stmt4 GONE"))
	type goneRow struct{ live, gone, repos, empty uint64 }
	readForm := func(phase string) map[string]goneRow {
		t.Helper()
		out := map[string]goneRow{}
		rows, err := conn.Query(ctx, "SELECT family, stored_days, gone_keys, gone_repos, empty_repo_keys, orgs FROM ("+stmt4+")")
		if err != nil {
			t.Fatalf("%s: stmt4: %v", phase, err)
		}
		defer rows.Close()
		for rows.Next() {
			var family string
			var r goneRow
			var orgs uint64
			if err := rows.Scan(&family, &r.live, &r.gone, &r.repos, &r.empty, &orgs); err != nil {
				t.Fatal(err)
			}
			if orgs != 1 {
				t.Fatalf("%s: orgs %d, want 1", phase, orgs)
			}
			if family != "_stmt" {
				out[strings.TrimPrefix(family, "gone:")] = r
				t.Logf("%s FORM %s live %d gone %d gone_repos %d empty %d", phase, family, r.live, r.gone, r.repos, r.empty)
			}
		}
		return out
	}
	liveKeys := func(table StaleKeyTable) map[string]bool {
		t.Helper()
		out := map[string]bool{}
		rows, err := conn.Query(ctx, table.LiveKeysQuery(), org, day)
		if err != nil {
			t.Fatalf("live keys %s: %v", table.Table, err)
		}
		defer rows.Close()
		columns := len(table.Keys)
		for rows.Next() {
			values := make([]string, columns)
			dest := make([]any, columns)
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			out[strings.Join(values, "\x00")] = true
		}
		return out
	}
	before := readForm("BEFORE")
	liveBefore := map[string]map[string]bool{}
	for _, table := range OrganizationRunStaleKeyTables() {
		liveBefore[table.Table] = liveKeys(table)
	}
	if len(before) != 8 {
		t.Fatalf("stmt4 gave %d tables, want 8", len(before))
	}
	endStaleKeyRun(t, ctx, conn, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true,
		DiscoveredRepoIDs: []RepositoryID{RepositoryID(held.String())}}, clock)
	for _, table := range OrganizationRunStaleKeyTables() {
		after := liveKeys(table)
		zeroed, synthKeys := 0, 0
		for key := range liveBefore[table.Table] {
			if !after[key] {
				zeroed++
			}
			if table.Table == "user_metrics_daily" && strings.HasPrefix(key, synth.String()+"\x00") {
				synthKeys++
				if !after[key] {
					t.Errorf("user_metrics_daily: the synthesized key was zeroed by the run")
				}
			}
		}
		f := before[table.Table]
		if uint64(len(liveBefore[table.Table])-synthKeys) != f.live {
			t.Errorf("%s: the form counts %d live keys, the rule's live-key read %d (synthesized left out: %d)", table.Table, f.live, len(liveBefore[table.Table]), synthKeys)
		}
		if uint64(zeroed) != f.gone+f.empty {
			t.Errorf("%s: the run zeroed %d live keys, the form counts gone %d + repo_id '' %d", table.Table, zeroed, f.gone, f.empty)
		}
		if f.gone == 0 {
			t.Errorf("%s: no gone key in the fixture: the case is not set", table.Table)
		}
		t.Logf("EQUAL %s: live %d, the run zeroed %d = form gone %d + repo_id '' %d", table.Table, f.live, zeroed, f.gone, f.empty)
	}
	if before["team_metrics_daily"].empty != 1 {
		t.Errorf("team_metrics_daily: form empty_repo_keys %d, want 1 (the live repo_id '' key, which the run zeroes)", before["team_metrics_daily"].empty)
	}
	afterRun := readForm("AFTER")
	for table, r := range afterRun {
		if r.gone != 0 || r.empty != 0 {
			t.Errorf("%s after the run: gone %d empty %d, want 0 (every such key is zeroed now)", table, r.gone, r.empty)
		}
	}
}
