//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of the prod read forms FLOOR-13902-3, FORM-QZ1-13902-2, FORM-QZ3-13902-2 and FORM-QZ4-13902-2
// (CHAOS-13902): not for merge. Each const is the form file with its org prefix (8 hex) replaced by the synthetic
// "f0f0f0f0"; each sha256 is pinned. It reuses helpers of the FLOOR-13902-2 cell file (same package, same branch).
const (
	f3SHA256  = "5118d4142c6ebc356406fb72a8ea6b1f4317809c5152bcbbf2d7696ba4455c46"
	q1v2SHA   = "87304f5372b93098aa70ea0f6385984f3325f2aef3a0e611ee1028a5db1bb379"
	q3v2SHA   = "b8462fa34ff41210b14887d028a01b600577ea4f24977cbaa6f03b86abf348a3"
	q4v2SHA   = "2fafb8650f20d4ca52ac95b5ff06a4a21ad8e0e87d20d9872a983f1d66c1fc98"
	f3FillID  = "a1b2c3d4e5f6"
	f3RunDay1 = "2026-10-23"
	f3RunDayN = "2026-11-22"
)

const f3Form = `-- FLOOR-13902-3: raw floor read of CARD-13902-recompute-13 (CHAOS-13902), every family the recompute can ERASE from nothing.
--   ClickHouse, SELECT only, FOUR statements (one per connector call), run by pm ONCE per chunk at the run minute, right
--   before 'chunk-start.sh <FROM> <TO> <DIGEST> --go' (card s4 step 1, (y)). Lane l13902 (lead fcdev), 2026-11-24.
--   Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-3.md. acp FORM read: OPEN. Supersedes FLOOR-13902-2 486bd07cb397
--   (acp READ-CARD-13902-10-1 eac6ab209bed F-P2-1..F-P2-6, F-P3-1..F-P3-4 + the early INFO 3-5 + the 3-family ERASE add).
-- Placeholders: {{FROM}} {{TO}} = the chunk bounds; {{FILL_ID}} = 12 hex written by the fill (S-P2-1 a). Only
--   'chunk-start.sh --floor-sql <FROM> <TO>' fills them (ledger line FLOOR-FILL <C> fill=<id> sql_sha12=<..>).
-- Why: SIZING-14285-6.out (e0d4de33c436 :7) raw git_commits start 2025-11-01, stored daily rows from 2025-07-26. A family
--   that ERASEs (writes zeros or retracted = 1 over a live stored key it does not produce, card s13) turns a day with live
--   stored keys and no raw source into zeros. Floor(T, S) = max(card --from 2026-04-17, first day of raw or input table T
--   for scope S, all days, read now). S = the repo when T has a repo key, else the org.
-- Output columns, every row: stmt, family (stored table), raw_table, scope_id (repo id CUT to its first 8 hex, or 'org';
--   F-P2-2: no full id), raw_first_day ('none' = no raw row), stored_first_day (first stored day in [FROM, TO]),
--   stored_days, read_rows_cap, orgs (ORG-1: uniqExact(org_id) of one table of the statement, WANT 1; F-P2-1), fam_rows
--   (the statement's family rows), scopes_full / scopes_cut (distinct full ids / 8-hex cuts of those rows: WANT equal, no
--   cut collision), read_at (server now(), UTC, to the second), fill_id (the fill nonce). Plus ONE control row per statement
--   (family '_stmt', raw_table '_cap'). Joins inside the SQL use the full ids.
-- ERASE families and their raw / input (card s13; code at the run binary 3207ac6258c0):
--   stmt1 repo_metrics_daily (git commits, PRs, reviews: reviews are no source of its retire rule
--   repo_user_commit_native_executor.go:241-248, kept as a safe false-STOP source; F-P3-4), user_metrics_daily (same 3; the
--   ic_finalize synthesized keys left out, see stmt3), commit_metrics, team_metrics_daily (commits), ai_impact_metrics_daily
--   (PRs; reviews only load after PRs or incidents exist, ai_impact_native_executor.go:133: kept as a safe false-STOP source;
--   the incident arm is stmt3 ai_impact_metrics_daily.org, F-P2-4).
--   stmt2 cicd (ci_pipeline_runs finished: writer cicd/clickhouse.go:51-57, testops/status_export.go:15 "deleted", :20-21
--   "running"/"queued", :36 NotOpenRunSQL), testops pipeline / test / coverage / release_confidence / quality_drag /
--   pipeline_stability (started arm != 'deleted': testops_native_clickhouse.go:126-130; F-P3-1), deploy, change_failure.
--   stmt3 (org scope): incident; the 6 work-item tables; team_complexity; team_cognitive_load (input user_metrics_daily only:
--   the executor takes user OR team rows, team_cognitive_load_native_executor.go:88-99, so this floor can only be higher =
--   safe; F-P3-3); compounding_risk team; ai_governance (input view ai_attribution_resolved, as the loader
--   ai_governance_native_clickhouse.go:44; F-P3-2); work_item_measure_capability (retirement rows,
--   work_item_measure_capability.go:186-245); ic_landscape_rolling_30d and user_metrics_daily.synth (ic_finalize zeros /
--   retraction, ic_finalize_native_executor.go:51-84; synthesized repo_id = UUIDv5(1b4e28ba-2fa1-11d2-883f-b9a761bde3fb,
--   org_id || 0x1F || author_email), icfinalize/executor.go:124-160, :333); ai_impact_metrics_daily.org (incidents,
--   ai_impact_native_executor.go:120-130); benchmarking (testops_metric_baselines: retireDay when the input window holds no
--   live key and the run is full_org, benchmarking_finalize_native_executor.go:198-230; empty run :162-176 /
--   benchmarking_empty_run_retire.go:148-182; inputs testops pipeline / test / coverage stored live, benchmarking/run.go
--   DefaultBenchmarkMetrics, benchmarking/clickhouse.go:371-420 LiveInputKeys).
--   stmt4 the 8 repository tables of the full-org finalize (stale_team_keys_run.go:83-93, :300-330): repository held.
-- No ERASE (no row here; card s13 with cites): file_metrics_daily, file_hotspot_daily, review_edges_daily,
--   compounding_risk_daily scope repo, ai_workflow_*, work_graph_*_edges, work_item_team_attributions, work_item_cycle_times
--   (retracts only items with an OPEN raw copy, work_item_cycle_retract.go:203-231), ai_policy_events (insert only,
--   ai_governance_native_clickhouse.go:435-478; in no stale-key table set).
-- Cost gate per statement (SIZING-14285-6 D1-D3 shape, acp READ-SIZING-14285-6-1 OK): read_rows_cap = f x ceil(1.25 x
--   (sum(marks x k) x 8192 + P)) + 2 x (2P + 1) (s = 1: the ORG-1 scalar; its table counted once more in k);
--   throwIf(read_rows_cap >= 20000000). A gate run reads P + Q + 1 metadata rows (Q = outdated parts still listed), inside
--   the cap headroom. pm WANT: read_rows <= read_rows_cap.

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
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id
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
--   toDate(started_at) (not deleted); test_suite_results FINAL toDate(coalesce(started_at, finished_at)); coverage_snapshots
--   FINAL joined to ci_pipeline_runs FINAL on (org_id, repo_id, run_id), day = toDate(started_at) of the run;
--   deployments FINAL .deployed = toDate(deployed_at), .any = toDate(coalesce(deployed_at, finished_at, started_at)).
-- Cost gate k ci_pipeline_runs 3, deployments 2, cicd_metrics_daily 2 (+ ORG-1), every other table 1; f 2; s 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'ci_pipeline_runs', 3, table = 'cicd_metrics_daily', 2, table = 'deployments', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'coverage_snapshots', 'deploy_metrics_daily', 'deployments', 'repo_change_failure_daily', 'test_suite_results', 'testops_coverage_metrics_daily', 'testops_pipeline_metrics_daily', 'testops_pipeline_stability', 'testops_quality_drag', 'testops_release_confidence', 'testops_test_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table,
  if(o.scope_full = 'org', 'org', substring(o.scope_full, 1, 8)) AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM cicd_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows,
  uniqExactIf(o.scope_full, o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_full,
  uniqExactIf(substring(o.scope_full, 1, 8), o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id
FROM (
  SELECT toUInt8(2) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_full,
    if(ifNull(r.present, 0) = 1, toString(r.f), 'none') AS raw_first_day, toString(p.p_sf) AS stored_first_day,
    toUInt64(p.p_sd) AS stored_days
  FROM (
    SELECT st.s_family AS p_family, st.s_scope AS p_scope, st.s_sf AS p_sf, st.s_sd AS p_sd, raw_name AS p_raw
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
                     st.s_family = 'repo_change_failure_daily', ['deployments.any'],
                     ['?']) AS raw_name
  ) AS p
  LEFT JOIN (
    SELECT 'ci_pipeline_runs.finished' AS r_raw, toString(repo_id) AS r_scope, min(toDate(assumeNotNull(finished_at))) AS f, toUInt8(1) AS present
    FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND finished_at IS NOT NULL AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued') GROUP BY repo_id
    UNION ALL
    SELECT 'ci_pipeline_runs.started' AS r_raw, toString(repo_id) AS r_scope, min(toDate(started_at)) AS f, toUInt8(1) AS present
    FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted' GROUP BY repo_id
    UNION ALL
    SELECT 'test_suite_results' AS r_raw, toString(repo_id) AS r_scope, min(toDate(assumeNotNull(coalesce(started_at, finished_at)))) AS f, toUInt8(1) AS present
    FROM test_suite_results FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND coalesce(started_at, finished_at) IS NOT NULL GROUP BY repo_id
    UNION ALL
    SELECT 'coverage_snapshots' AS r_raw, toString(cs.c_repo) AS r_scope, min(toDate(pr.p_started)) AS f, toUInt8(1) AS present
    FROM (SELECT org_id AS c_org, repo_id AS c_repo, run_id AS c_run FROM coverage_snapshots FINAL WHERE startsWith(org_id, 'f0f0f0f0')) AS cs
    INNER JOIN (SELECT org_id AS p_org, repo_id AS p_repo, run_id AS p_run, started_at AS p_started FROM ci_pipeline_runs FINAL
                WHERE startsWith(org_id, 'f0f0f0f0')) AS pr ON pr.p_org = cs.c_org AND pr.p_repo = cs.c_repo AND pr.p_run = cs.c_run
    GROUP BY cs.c_repo
    UNION ALL
    SELECT 'deployments.deployed' AS r_raw, toString(repo_id) AS r_scope, min(toDate(assumeNotNull(deployed_at))) AS f, toUInt8(1) AS present
    FROM deployments FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND deployed_at IS NOT NULL GROUP BY repo_id
    UNION ALL
    SELECT 'deployments.any' AS r_raw, toString(repo_id) AS r_scope, min(toDate(assumeNotNull(coalesce(deployed_at, finished_at, started_at)))) AS f, toUInt8(1) AS present
    FROM deployments FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND coalesce(deployed_at, finished_at, started_at) IS NOT NULL GROUP BY repo_id
  ) AS r ON r.r_raw = p.p_raw AND r.r_scope = p.p_scope
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
--   retirement rows work_item_measure_capability.go:186-245), ic_landscape_rolling_30d (day = as_of_day; zeros over points
--   not produced, ic_finalize_native_executor.go:51-63) and user_metrics_daily.synth (the ic_finalize synthesized keys, :64-84):
--   inputs user_metrics_daily live (all days) and work_items.
-- Cost gate k user_metrics_daily 3 (the .live input, the .synth family, the ORG-1 scalar), every other table 1, f 2, s 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'user_metrics_daily', 3, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_attribution', 'ai_governance_coverage_daily', 'ai_impact_metrics_daily', 'compounding_risk_daily', 'estimate_coverage_metrics_daily', 'ic_landscape_rolling_30d', 'incident_metrics_daily', 'issue_type_metrics_daily', 'operational_incidents', 'repo_complexity_daily', 'repo_metrics_daily', 'team_cognitive_load_daily', 'team_complexity_daily', 'testops_coverage_metrics_daily', 'testops_metric_baselines', 'testops_pipeline_metrics_daily', 'testops_test_metrics_daily', 'user_metrics_daily', 'work_item_completion_daily', 'work_item_measure_capability', 'work_item_metrics_daily', 'work_item_state_durations_daily', 'work_item_user_metrics_daily', 'work_items')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table,
  if(o.scope_full = 'org', 'org', substring(o.scope_full, 1, 8)) AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows,
  uniqExactIf(o.scope_full, o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_full,
  uniqExactIf(substring(o.scope_full, 1, 8), o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id
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
    SELECT 'ic_landscape_rolling_30d' AS s_family, 'org' AS s_scope, min(as_of_day) AS s_sf, uniqExact(as_of_day) AS s_sd
    FROM ic_landscape_rolling_30d WHERE startsWith(org_id, 'f0f0f0f0') AND as_of_day >= toDate('{{FROM}}') AND as_of_day <= toDate('{{TO}}') HAVING count() > 0
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
    FROM (SELECT metric_name, scope_type, scope_key, period_end AS day, rolling_window_days, argMax(retracted, computed_at) AS rt
          FROM testops_metric_baselines WHERE startsWith(org_id, 'f0f0f0f0') AND period_end >= toDate('{{FROM}}') AND period_end <= toDate('{{TO}}')
          GROUP BY metric_name, scope_type, scope_key, period_end, rolling_window_days) AS k
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
                     st.s_family = 'ic_landscape_rolling_30d', ['user_metrics_daily.live', 'work_items'],
                     st.s_family = 'user_metrics_daily.synth', ['work_items'],
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

-- stmt4 REPOSITORY HELD (the full-org finalize, stale_team_keys_run.go:83-93, :300-330).
-- A run of the whole organization (daily-start with no --repo-id: full_org, daily/postgres.go:308-314) ends with a finalize
--   that writes rows of zeros over the live keys of 8 repository-scoped tables for a repository that is in no partition of
--   the run AND that the organization does not hold now (retractStaleTeamKeysOutsideRun, stale_team_keys.go). "Held" =
--   any repos row of the org (ClickHouseRepositoryDiscoverer.RepositoryIDs, daily/clickhouse.go:85-125, no filter), plus the
--   nil repository id when the org has a work item stored without a repository (:113-125).
-- Rows: family held:<table>, raw_table repos, scope = the stored repo; raw_first_day '1970-01-01' = held, 'none' = NOT
--   held (STOP: its keys would be zeroed). user_metrics_daily without the synthesized keys (stmt1 rule).
-- Cost gate k repos 2 (+ ORG-1), every other table 1, f 1 (no FINAL), s 1.
WITH (SELECT 1 * intDiv(5 * (sum(marks * multiIf(table = 'repos', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_impact_metrics_daily', 'repos', 'team_metrics_daily', 'testops_coverage_metrics_daily', 'testops_pipeline_stability', 'testops_quality_drag', 'testops_release_confidence', 'testops_test_metrics_daily', 'user_metrics_daily', 'work_items')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table,
  if(o.scope_full = 'org', 'org', substring(o.scope_full, 1, 8)) AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap,
  (SELECT uniqExact(org_id) FROM repos WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  countIf(o.family != '_stmt') OVER () AS fam_rows,
  uniqExactIf(o.scope_full, o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_full,
  uniqExactIf(substring(o.scope_full, 1, 8), o.family != '_stmt' AND o.scope_full != 'org') OVER () AS scopes_cut,
  toString(now('UTC')) AS read_at, '{{FILL_ID}}' AS fill_id
FROM (
  SELECT toUInt8(4) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_full,
    if(ifNull(r.present, 0) = 1, toString(r.f), 'none') AS raw_first_day, toString(p.p_sf) AS stored_first_day,
    toUInt64(p.p_sd) AS stored_days
  FROM (
    SELECT st.s_family AS p_family, st.s_scope AS p_scope, st.s_sf AS p_sf, st.s_sd AS p_sd, raw_name AS p_raw
    FROM (
    SELECT 'held:team_metrics_daily' AS s_family, toString(repo_id) AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM team_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') AND repo_id != '' GROUP BY repo_id
    UNION ALL
    SELECT 'held:ai_impact_metrics_daily' AS s_family, toString(repo_id) AS s_scope, min(day) AS s_sf, uniqExact(day) AS s_sd
    FROM ai_impact_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id
    UNION ALL
    SELECT 'held:user_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT u.repo_id AS repo_id, u.day AS day, u.rt AS rt,
            toString(u.repo_id) = UUIDNumToString(toFixedString(concat(substring(u.h, 1, 6), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 7, 1)), 15), 80)),
              substring(u.h, 8, 1), char(bitOr(bitAnd(reinterpretAsUInt8(substring(u.h, 9, 1)), 63), 128)), substring(u.h, 10, 7)), 16)) AS synth
          FROM (SELECT repo_id, author_email, day, argMax(retracted, computed_at) AS rt,
                  SHA1(concat(UUIDStringToNum('1b4e28ba-2fa1-11d2-883f-b9a761bde3fb'), any(org_id), '\x1F', author_email)) AS h
                FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}') GROUP BY repo_id, author_email, day) AS u) AS k
    WHERE k.rt = 0 AND NOT k.synth GROUP BY k.repo_id
    UNION ALL
    SELECT 'held:testops_test_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_test_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'held:testops_release_confidence' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_release_confidence
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'held:testops_quality_drag' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_quality_drag
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'held:testops_pipeline_stability' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_pipeline_stability
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    UNION ALL
    SELECT 'held:testops_coverage_metrics_daily' AS s_family, toString(k.repo_id) AS s_scope, min(k.day) AS s_sf, uniqExact(k.day) AS s_sd
    FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt FROM testops_coverage_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
    ) AS st
    ARRAY JOIN multiIf(st.s_family = 'held:team_metrics_daily', ['repos'],
                     st.s_family = 'held:ai_impact_metrics_daily', ['repos'],
                     st.s_family = 'held:user_metrics_daily', ['repos'],
                     st.s_family = 'held:testops_test_metrics_daily', ['repos'],
                     st.s_family = 'held:testops_release_confidence', ['repos'],
                     st.s_family = 'held:testops_quality_drag', ['repos'],
                     st.s_family = 'held:testops_pipeline_stability', ['repos'],
                     st.s_family = 'held:testops_coverage_metrics_daily', ['repos'],
                     ['?']) AS raw_name
  ) AS p
  LEFT JOIN (
    SELECT 'repos' AS r_raw, toString(id) AS r_scope, toDate('1970-01-01') AS f, toUInt8(1) AS present
    FROM repos WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY id
    UNION ALL
    SELECT 'repos' AS r_raw, '00000000-0000-0000-0000-000000000000' AS r_scope, toDate('1970-01-01') AS f, toUInt8(1) AS present
    FROM work_items WHERE startsWith(org_id, 'f0f0f0f0') AND repo_id = toUUID('00000000-0000-0000-0000-000000000000') HAVING count() > 0
  ) AS r ON r.r_raw = p.p_raw AND r.r_scope = p.p_scope
  UNION ALL
  SELECT toUInt8(4) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_full, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY family, raw_table, scope_id;
`

const q1v2Form = `-- FORM-QZ1-13902-2: Q-z1 of CARD-13902-recompute-13 (z) (= -1 17daee69fd42 + acp READ-CARD-13902-10-1 Q1-P2-1, Q1-P3-1): a TTL set on prod by hand (not by a migration) on a raw table the
--   recompute reads. ClickHouse, SELECT only, TWO statements, metadata only (system.tables, system.mutations). Lane l13902 (lead fcdev),
--   2026-11-24. Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-3.md. acp FORM read: OPEN.
-- stmt1 (TTL). Code side (card (z)): no TTL on these tables at the pin's chain (baseline/head.json :241 :346 :351 :361 :366 at
--   a7febdf50002, sql/145..170 no TTL / MODIFY TTL).
-- WANT: 11 rows (one per name; a missing row = a wrong name or a missing table: STOP); has_ttl = 0 in every row. has_ttl 1 =
--   the floor moves daily and stored days are orphaned every day: STOP, tell pm at once (pm binding rule 1).
-- Cost gate: throwIf over the system.tables row count of this database (metadata only); no data table is read. Fired in CI
--   by the TEMPLATE-3 :12 method (the one literal '> 100000' of the statement set to '> -1' in the cell copy; Q1-P2-1).
WITH (SELECT throwIf((SELECT count() FROM system.tables WHERE database = currentDatabase()) > 100000, 'STOP-COST-GATE')) AS cost_gate
SELECT name AS table_name, engine AS engine, toUInt8(positionCaseInsensitive(create_table_query, ' TTL ') > 0) AS has_ttl,
  partition_key AS partition_key, sorting_key AS sorting_key
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('ai_attribution', 'ci_pipeline_runs', 'coverage_snapshots', 'deployments', 'git_commit_stats', 'git_commits',
               'git_pull_request_reviews', 'git_pull_requests', 'operational_incidents', 'test_suite_results', 'work_items')
  AND cost_gate = 0
ORDER BY name;

-- stmt2 (hand DELETE; acp Q1-P3-1): mutations of the 11 tables on prod (system.mutations keeps the finished ones only up to
--   the table setting finished_mutations_to_keep, so an old DELETE can be gone: INFO). WANT delete_mutations = 0; > 0 = a
--   DELETE ran on a raw table: STOP, pm + acp read (the card's (z) "no deletion" finding is wrong on prod).
-- Cost gate: throwIf over the system.mutations row count of this database (metadata only), fired in CI as stmt1's.
WITH (SELECT throwIf((SELECT count() FROM system.mutations WHERE database = currentDatabase()) > 100000, 'STOP-COST-GATE')) AS cost_gate
SELECT count() AS mutations, countIf(positionCaseInsensitive(command, 'DELETE') > 0) AS delete_mutations,
  countIf(positionCaseInsensitive(command, 'DELETE') > 0 AND is_done = 0) AS delete_mutations_open,
  min(toDate(create_time)) AS first_mutation_day
FROM system.mutations
WHERE database = currentDatabase()
  AND table IN ('ai_attribution', 'ci_pipeline_runs', 'coverage_snapshots', 'deployments', 'git_commit_stats', 'git_commits',
                'git_pull_request_reviews', 'git_pull_requests', 'operational_incidents', 'test_suite_results', 'work_items')
  AND cost_gate = 0;
`

const q3v2Form = `-- FORM-QZ3-13902-2: Q-z3 of CARD-13902-recompute-13 (z) (= -1 dd868a360eb2 + acp Q3-P2-1 ORG-1): per repo, the first raw commit day vs the first LIVE stored
--   repo_metrics_daily day with commits_count > 0 (all days). ClickHouse, SELECT only, ONE statement. Lane l13902 (lead
--   fcdev), 2026-11-24. Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-3.md. acp FORM read: OPEN.
-- Rule: commits_count of a stored repo-day = the raw commits of that day (repouser/compute.go:614, from git_commits FINAL
--   committer_when [D, D+1), repouser/clickhouse.go:87-92). A stored commit day earlier than the first raw commit of the
--   repo = raw commits that existed at the compute and are gone now.
-- WANT: orgs = 1 (ORG-1: uniqExact(org_id) of repo_metrics_daily under the prefix); repos_with_commit_days > 0 (control); repos_stored_before_raw = 0; repos_commit_days_no_raw = 0. Either > 0 = a
--   deletion or a lost raw write: STOP, pm + acp read. INFO first_stored_commit_day, first_raw_commit_day (org-wide).
-- Cost gate: k repo_metrics_daily 2 (+ the ORG-1 scalar), git_commits 1, f 2 (FINAL), s 1: read_rows_cap = 2 x ceil(1.25 x
--   (sum(marks x k) x 8192 + P)) + 2 x (2P + 1).
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'repo_metrics_daily', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commits', 'repo_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT (SELECT uniqExact(org_id) FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs,
  count() AS repos_with_commit_days,
  countIf(ifNull(r.present, 0) = 1 AND s.sf < r.rf) AS repos_stored_before_raw,
  countIf(ifNull(r.present, 0) = 0) AS repos_commit_days_no_raw,
  min(s.sf) AS first_stored_commit_day,
  minIf(r.rf, ifNull(r.present, 0) = 1) AS first_raw_commit_day,
  read_rows_cap
FROM (SELECT k.repo_id AS repo_id, min(k.day) AS sf
      FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS rt, argMax(commits_count, computed_at) AS cc
            FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, day) AS k
      WHERE k.rt = 0 AND k.cc > 0 GROUP BY k.repo_id) AS s
LEFT JOIN (SELECT repo_id, min(toDate(committer_when)) AS rf, toUInt8(1) AS present
           FROM git_commits FINAL WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id) AS r ON r.repo_id = s.repo_id
WHERE cost_gate = 0;
`

const q4v2Form = `-- FORM-QZ4-13902-2: Q-z4 of CARD-13902-recompute-13 (z) (= -1 fa3a6e583362 + acp Q4-P2-1 ORG-1, Q4-P2-2 bound): data loss or
--   not before the raw floor 2025-11-01. pm ask (binding):
--   per stored daily table, the count of stored rows dated before 2025-11-01 (no lower bound: Q4-P2-2) with ANY
--   commit-derived column > 0.
--   ClickHouse, SELECT only, ONE statement. Lane l13902 (lead fcdev), 2026-11-24. Prod: NOT EXECUTED.
--   CI exec: EXEC-PROOF-FLOOR-13902-3.md (org + rule mutants, a b0 row in every table). acp FORM read: OPEN.
-- Commit-derived columns = the WRITER's values computed from git_commits / git_commit_stats (cites git show a7febdf50002;
--   D = internal/jobs/metrics/daily, R = D/repouser; map SUBAGENT-QZ4-COLUMNS-1.md, spot-checked R/compute.go:612-618,
--   :463-470, D/wellbeing_native_clickhouse.go:203-210):
--   repo_metrics_daily: same day commits_count, total_loc_touched, avg_commit_size_loc, large_commit_ratio (R/compute.go:
--     614-617, written R/clickhouse.go:516-562); 30-day BACKWARD window rework_churn_ratio_30d, single_owner_file_ratio_30d,
--     bus_factor, code_ownership_gini (D/repo_user_commit_native_executor.go:96-123, R/quality.go:14, :48, :101, :141).
--     A window ending on or before 2025-10-31 holds only days before the floor.
--   user_metrics_daily: commits_count, loc_added, loc_deleted, files_changed, large_commits_count, avg_commit_size_loc
--     (R/compute.go:465-470), loc_touched (icfinalize/merge.go:236). NOT active_hours / weekend_days (also from PRs).
--   commit_metrics: every row is one raw commit-day (R/compute.go:654-663): any row counts.
--   team_metrics_daily: commits_count, after_hours_commits_count, weekend_commits_count (numerical/wellbeing.go:221-223;
--     source git_commits only, D/wellbeing_native_clickhouse.go:205-208).
--   file_metrics_daily: churn, commits_count (filehotspots/compute.go:139-165; 30-day backward window).
--   file_hotspot_daily: churn_commits_30d, churn_loc_30d (filehotspots/compute.go:229-230, :341). NOT risk_score.
--   ic_landscape_rolling_30d (day col as_of_day): churn_loc_30d (icfinalize/loader.go:57-66, :119; backward window).
--   team_cognitive_load_daily: after_hours_commit_ratio, weekend_commit_ratio > 0 (D/team_cognitive_load_clickhouse.go:
--     305-329; a 0.0 can occur with no commit, so only > 0 counts).
--   Not commit-derived (no row here): ai_impact (PR-linked paths only), compounding_risk (reads repo_metrics_daily, covered),
--     repo_change_failure_daily, work_item*, cicd, testops*, deploy, incident.
-- Columns out per table: rows_in_range (all stored versions), keys_in_range, commit_rows_any_version (versions with a
--   commit-derived value > 0), commit_keys_newest (keys whose NEWEST version has one), first_day (INFO: the first stored
--   day of the table before the bound), read_rows_cap, orgs (ORG-1: uniqExact(org_id) of repo_metrics_daily, WANT 1).
-- WANT / reading (card s12 Q-z4):
--   control: orgs = 1 (ORG-1); repo_metrics_daily keys_in_range > 0 (stored rows before 2025-11-01; 0 = NOT EXERCISED, acp reads).
--   commit_rows_any_version = 0 AND commit_keys_newest = 0 in EVERY table = those days come only from PR / review rows (PRs
--     are cut at updated_at): a recompute there changes nothing; the floor is a safety STOP, not a loss case.
--   > 0 in ANY table = commit-derived values with no raw commit behind them: raw rows existed once and are gone; the card's
--     "no delete" finding is wrong somewhere: STOP and report (pm).
-- Cost gate (SIZING-14285-6 D1-D3 shape): k repo_metrics_daily 2 (+ the ORG-1 scalar), every other table 1, f 1 (no FINAL),
--   s 1: read_rows_cap = ceil(1.25 x (sum(marks x k) x 8192 + P)) + 2 x (2P + 1); throwIf(read_rows_cap >= 20000000). pm
--   WANT read_rows <= read_rows_cap. No SET, no SETTINGS.
WITH (SELECT 1 * intDiv(5 * (sum(marks * multiIf(table = 'repo_metrics_daily', 2, 1)) * 8192 + count()) + 3, 4) + 2 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'file_hotspot_daily', 'file_metrics_daily', 'ic_landscape_rolling_30d', 'repo_metrics_daily', 'team_cognitive_load_daily', 'team_metrics_daily', 'user_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.table_name AS table_name, o.rows_in_range AS rows_in_range, o.keys_in_range AS keys_in_range,
  o.commit_rows_any_version AS commit_rows_any_version, o.commit_keys_newest AS commit_keys_newest, o.first_day AS first_day, read_rows_cap,
  (SELECT uniqExact(org_id) FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs
FROM (
  SELECT 'repo_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT repo_id, day, day AS d, count() AS n, countIf(commits_count > 0 OR total_loc_touched > 0 OR avg_commit_size_loc > 0 OR large_commit_ratio > 0 OR rework_churn_ratio_30d > 0 OR single_owner_file_ratio_30d > 0 OR bus_factor > 0 OR code_ownership_gini > 0) AS c_any, argMax(toUInt8(commits_count > 0 OR total_loc_touched > 0 OR avg_commit_size_loc > 0 OR large_commit_ratio > 0 OR rework_churn_ratio_30d > 0 OR single_owner_file_ratio_30d > 0 OR bus_factor > 0 OR code_ownership_gini > 0), computed_at) AS c_new
        FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY repo_id, day) AS k
  UNION ALL
  SELECT 'user_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT repo_id, author_email, day, day AS d, count() AS n, countIf(commits_count > 0 OR loc_added > 0 OR loc_deleted > 0 OR files_changed > 0 OR large_commits_count > 0 OR avg_commit_size_loc > 0 OR loc_touched > 0) AS c_any, argMax(toUInt8(commits_count > 0 OR loc_added > 0 OR loc_deleted > 0 OR files_changed > 0 OR large_commits_count > 0 OR avg_commit_size_loc > 0 OR loc_touched > 0), computed_at) AS c_new
        FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY repo_id, author_email, day) AS k
  UNION ALL
  SELECT 'commit_metrics' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT repo_id, day, author_email, commit_hash, day AS d, count() AS n, countIf(1 = 1) AS c_any, argMax(toUInt8(1 = 1), computed_at) AS c_new
        FROM commit_metrics WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY repo_id, day, author_email, commit_hash) AS k
  UNION ALL
  SELECT 'team_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT team_id, repo_id, day, day AS d, count() AS n, countIf(commits_count > 0 OR after_hours_commits_count > 0 OR weekend_commits_count > 0) AS c_any, argMax(toUInt8(commits_count > 0 OR after_hours_commits_count > 0 OR weekend_commits_count > 0), computed_at) AS c_new
        FROM team_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY team_id, repo_id, day) AS k
  UNION ALL
  SELECT 'file_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT repo_id, day, path, day AS d, count() AS n, countIf(churn > 0 OR commits_count > 0) AS c_any, argMax(toUInt8(churn > 0 OR commits_count > 0), computed_at) AS c_new
        FROM file_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY repo_id, day, path) AS k
  UNION ALL
  SELECT 'file_hotspot_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT repo_id, day, file_path, day AS d, count() AS n, countIf(churn_commits_30d > 0 OR churn_loc_30d > 0) AS c_any, argMax(toUInt8(churn_commits_30d > 0 OR churn_loc_30d > 0), computed_at) AS c_new
        FROM file_hotspot_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY repo_id, day, file_path) AS k
  UNION ALL
  SELECT 'ic_landscape_rolling_30d' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT repo_id, team_id, map_name, as_of_day, identity_id, as_of_day AS d, count() AS n, countIf(churn_loc_30d > 0) AS c_any, argMax(toUInt8(churn_loc_30d > 0), computed_at) AS c_new
        FROM ic_landscape_rolling_30d WHERE startsWith(org_id, 'f0f0f0f0') AND as_of_day < toDate('2025-11-01')
        GROUP BY repo_id, team_id, map_name, as_of_day, identity_id) AS k
  UNION ALL
  SELECT 'team_cognitive_load_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest, min(k.d) AS first_day
  FROM (SELECT team_id, day, day AS d, count() AS n, countIf(ifNull(after_hours_commit_ratio, 0) > 0 OR ifNull(weekend_commit_ratio, 0) > 0) AS c_any, argMax(toUInt8(ifNull(after_hours_commit_ratio, 0) > 0 OR ifNull(weekend_commit_ratio, 0) > 0), computed_at) AS c_new
        FROM team_cognitive_load_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day < toDate('2025-11-01')
        GROUP BY team_id, day) AS k
) AS o
WHERE cost_gate = 0
ORDER BY o.table_name;
`

// Repos with distinct 8-hex cuts (the out holds the cut only).
const (
	q1 = "11111111-0000-4000-8000-000000000001"
	q2 = "22222222-0000-4000-8000-000000000002"
	q3 = "33333333-0000-4000-8000-000000000003"
	q9 = "99999999-0000-4000-8000-000000000009"
)

func f3Synth(org, identity string) string {
	return uuid.NewSHA1(uuid.MustParse("1b4e28ba-2fa1-11d2-883f-b9a761bde3fb"), []byte(org+"\x1f"+identity)).String()
}

// f3Fixture: the FLOOR-13902-2 cell fixture with repos r1/r2/r3 renamed q1/q2/q3, the -3 extra rows, and ONE foreign-org
// (b0) row in EVERY table a statement reads (acp X-P2-1): stored b0 rows on 2026-10-23 (before every f0 stored first day),
// raw b0 rows in 2024 (before every f0 raw first day), same repo q1, so a missing org filter moves an exact PASS value.
func f3Fixture(synth string) string {
	base := strings.NewReplacer(r1, q1, r2, q2, r3, q3).Replace(floor2Fixture)
	b := func(sql string) string { return sql + "\n" }
	extra := b(`INSERT INTO repos (org_id, id, repo, created_at, last_synced) VALUES
('`+fOrg+`', '`+q1+`', 'r1', '2025-01-01 00:00:00', '2025-01-01 00:00:00'),
('`+bOrg+`', '`+q1+`', 'b1', '2024-01-01 00:00:00', '2024-01-01 00:00:00'),
('`+bOrg+`', '`+q9+`', 'b9', '2024-01-01 00:00:00', '2024-01-01 00:00:00');`) +
		b(`INSERT INTO work_item_measure_capability (org_id, provider, measure, tracked, item_count, window_start, window_end, computed_at) VALUES
('`+fOrg+`', 'jira', 'm1', 1, 3, '2026-08-02', '2026-10-30', '2026-10-31 01:00:00'),
('`+fOrg+`', 'jira', 'm2', 1, 2, '2026-07-27', '2026-10-24', '2026-10-25 01:00:00'),
('`+bOrg+`', 'jira', 'm1', 1, 3, '2026-07-26', '2026-10-23', '2026-10-24 01:00:00');`) +
		b(`INSERT INTO work_item_measure_capability (org_id, provider, measure, tracked, item_count, window_start, window_end, computed_at) VALUES
('`+fOrg+`', 'jira', 'm2', 0, 0, '2026-07-27', '2026-10-24', '2026-10-26 01:00:00');`) +
		b(`INSERT INTO ic_landscape_rolling_30d (org_id, repo_id, team_id, map_name, as_of_day, identity_id, computed_at) VALUES
('`+fOrg+`', '`+q1+`', 't1', 'm', '2026-10-29', 'i1', '2026-10-30 01:00:00'),
('`+bOrg+`', '`+q1+`', 't1', 'm', '2026-10-23', 'i1', '2026-10-24 01:00:00');`) +
		b(`INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES
('`+fOrg+`', '`+synth+`', 'u9@x', '2026-10-28', 0, '2026-10-29 01:00:00', 0),
('`+bOrg+`', '`+q1+`', 'b@x', '2026-10-23', 1, '2026-10-24 01:00:00', 0);`) +
		b(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('`+fOrg+`', '`+q3+`', '2026-10-30', '2026-10-31 01:00:00', 0),
('`+bOrg+`', '`+q1+`', '2026-10-23', '2026-10-24 01:00:00', 0);`) +
		b(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('`+fOrg+`', '`+q3+`', '2026-10-30', '2026-11-01 01:00:00', 1);`) +
		b(`INSERT INTO testops_metric_baselines (org_id, metric_name, scope_type, scope_key, period_end, rolling_window_days, computed_at, retracted) VALUES
('`+fOrg+`', 'pass_rate', 'repo', 'k1', '2026-10-29', 30, '2026-10-30 01:00:00', 0),
('`+bOrg+`', 'pass_rate', 'repo', 'k1', '2026-10-23', 30, '2026-10-24 01:00:00', 0);`) +
		b(`INSERT INTO git_pull_request_reviews (org_id, repo_id, number, review_id, reviewer, state, submitted_at, last_synced) VALUES
('`+bOrg+`', '`+q1+`', 9, 'rvb', 'y', 'APPROVED', '2024-01-02 09:00:00', '2024-01-02 10:00:00');`) +
		b(`INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, computed_at) VALUES
('`+bOrg+`', '`+q1+`', 'hb', '2026-10-23', 'b@x', 5, '2026-10-24 01:00:00');`) +
		b(`INSERT INTO team_metrics_daily (org_id, team_id, repo_id, day, commits_count, computed_at) VALUES
('`+bOrg+`', 't1', '`+q1+`', '2026-10-23', 1, '2026-10-24 01:00:00');`) +
		b(`INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket, computed_at) VALUES
('`+bOrg+`', 't1', '`+q1+`', 'feature', '2026-10-23', 'human', '2026-10-24 01:00:00');`) +
		b(`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, started_at, finished_at, last_synced) VALUES
('`+bOrg+`', '`+q1+`', 'runB', 'sb', 'unit', '2024-01-03 10:00:00', '2024-01-03 10:10:00', '2024-01-03 11:00:00');`) +
		b(`INSERT INTO coverage_snapshots (org_id, repo_id, run_id, snapshot_id, last_synced) VALUES
('`+bOrg+`', '`+q1+`', 'runB', 'cb', '2024-06-01 11:00:00');`) +
		b(`INSERT INTO deployments (org_id, repo_id, deployment_id, deployed_at, finished_at, last_synced) VALUES
('`+bOrg+`', '`+q1+`', 'db', '2024-01-04 10:00:00', '2024-01-04 10:00:00', '2024-01-04 11:00:00');`) +
		b(`INSERT INTO operational_incidents (org_id, provider, id, started_at, is_deleted, observed_at, ordering_contract) VALUES
('`+bOrg+`', 'pagerduty', 'incb', '2024-01-05 10:00:00', 0, '2024-01-05 10:00:00', 2);`) +
		b(`INSERT INTO repo_complexity_daily (org_id, repo_id, day, computed_at) VALUES
('`+bOrg+`', '`+q1+`', '2024-01-06', '2024-01-07 01:00:00');`) +
		b(`INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, kind, source, observed_at, ingested_at, computed_at, superseded_by) VALUES
(generateUUIDv4(), '`+bOrg+`', 'github', 'pr', 'pb', 'k', 's', '2024-01-07 10:00:00', '2024-01-07 10:00:00', '2024-01-07 10:00:00', NULL);`)
	for _, t := range []string{"testops_pipeline_metrics_daily", "testops_coverage_metrics_daily", "testops_release_confidence", "testops_quality_drag",
		"testops_pipeline_stability", "deploy_metrics_daily", "incident_metrics_daily"} {
		extra += b(`INSERT INTO ` + t + ` (org_id, repo_id, day, computed_at, retracted) VALUES ('` + bOrg + `', '` + q1 + `', '2026-10-23', '2026-10-24 01:00:00', 0);`)
	}
	extra += b(`INSERT INTO repo_change_failure_daily (org_id, repo_id, day, computed_at) VALUES ('` + bOrg + `', '` + q1 + `', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO work_item_user_metrics_daily (org_id, provider, work_scope_id, user_identity, row_view, day, computed_at, retracted) VALUES ('` + bOrg + `', 'jira', 'P', 'u1', 'team', '2026-10-23', '2026-10-24 01:00:00', 0);`)
	extra += b(`INSERT INTO estimate_coverage_metrics_daily (org_id, provider, work_scope_id, day, computed_at) VALUES ('` + bOrg + `', 'jira', 'P', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO work_item_state_durations_daily (org_id, provider, work_scope_id, team_id, status, row_view, day, computed_at) VALUES ('` + bOrg + `', 'jira', 'P', 't1', 'todo', 'team', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO issue_type_metrics_daily (org_id, provider, team_id, issue_type_norm, day, computed_at) VALUES ('` + bOrg + `', 'jira', 't1', 'bug', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO work_item_completion_daily (org_id, team_id, day, computed_at) VALUES ('` + bOrg + `', 't1', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO team_complexity_daily (org_id, team_id, day, computed_at) VALUES ('` + bOrg + `', 't1', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO team_cognitive_load_daily (org_id, team_id, day, computed_at) VALUES ('` + bOrg + `', 't1', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO compounding_risk_daily (org_id, scope, scope_id, day, computed_at) VALUES ('` + bOrg + `', 'team', 't1', '2026-10-23', '2026-10-24 01:00:00');`)
	extra += b(`INSERT INTO ai_governance_coverage_daily (org_id, day, computed_at) VALUES ('` + bOrg + `', '2026-10-23', '2026-10-24 01:00:00');`)
	return base + extra
}

// f3VariantHeld: live f0 user keys of repo q9, which has no f0 repos row (only a b0 one) and no raw row.
const f3VariantHeld = `
INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES
('` + fOrg + `', '` + q9 + `', 'z@x', '2026-10-27', 1, '2026-10-28 01:00:00', 0);
`

type f3Spec struct {
	reads   map[string]uint64
	f, s    uint64
	orgsTab string
}

func (sp f3Spec) gate() string {
	names := make([]string, 0, len(sp.reads))
	var arms []string
	for n := range sp.reads {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if sp.reads[n] > 1 {
			arms = append(arms, fmt.Sprintf("table = '%s', %d", n, sp.reads[n]))
		}
	}
	weight := ""
	if len(arms) > 0 {
		weight = " * multiIf(" + strings.Join(arms, ", ") + ", 1)"
	}
	return fmt.Sprintf("WITH (SELECT %d * intDiv(5 * (sum(marks%s) * 8192 + count()) + 3, 4) + %d * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()\n"+
		"      AND table IN ('%s')) AS read_rows_cap,\n"+
		"  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate\n", sp.f, weight, 1+sp.s, strings.Join(names, "', '"))
}

func (sp f3Spec) derive(live map[string]floor2Parts) (uint64, uint64) {
	var w, p uint64
	for table, k := range sp.reads {
		w += k * live[table].marks
		p += live[table].parts
	}
	return sp.f*((5*(w*8192+p)+3)/4) + (1+sp.s)*(2*p+1), p
}

type f3Run struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	specs map[string]f3Spec
	log   []f3Logged
}

type f3Logged struct {
	phase, label, comment string
	capRows, p            uint64
	fire                  bool
}

func (r *f3Run) query(phase, label, text string) ([]map[string]string, error) {
	t := r.t
	comment := fmt.Sprintf("f3-%s-%s-%d", phase, label, time.Now().UnixNano())
	sp, known := r.specs[label]
	var capRows, p uint64
	if known {
		capRows, p = sp.derive(floor2LiveParts(t, r.ctx, r.conn))
	}
	rows, err := r.conn.Query(clickhouse.Context(r.ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), text)
	var got []map[string]string
	if err == nil {
		cols := rows.Columns()
		types := rows.ColumnTypes()
		for rows.Next() {
			cells := make([]any, len(types))
			for i, ct := range types {
				cells[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(cells...); err != nil {
				t.Fatalf("%s %s: scan: %v", phase, label, err)
			}
			row := map[string]string{}
			parts := make([]string, 0, len(cols))
			for i, name := range cols {
				v := cells[i]
				row[name] = floor2Cell(v)
				parts = append(parts, name+"="+row[name])
			}
			t.Logf("%s %s ROW %s", phase, label, strings.Join(parts, " "))
			if known {
				if c, ok := row["read_rows_cap"]; ok && c != fmt.Sprint(capRows) {
					t.Errorf("%s %s: read_rows_cap %s, want %d (Go formula on live system.parts)", phase, label, c, capRows)
				}
			}
			got = append(got, row)
		}
		err = rows.Err()
		_ = rows.Close()
	}
	if known {
		r.log = append(r.log, f3Logged{phase, label, comment, capRows, p, err != nil})
	}
	return got, err
}

func (r *f3Run) checkLog() map[string]bool {
	t := r.t
	if err := r.conn.Exec(r.ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	fired := map[string]bool{}
	for _, c := range r.log {
		types, read := floor2QueryLog(t, r.ctx, r.conn, c.comment)
		if c.fire {
			fired[c.label] = true
			t.Logf("%s %s query_log types=%v read_rows=%d bound 2(2P+1)=%d", c.phase, c.label, types, read, 2*(2*c.p+1))
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > 2*(2*c.p+1) {
				t.Errorf("%s %s: query_log %v read_rows %d, want [ExceptionBeforeStart] and <= %d", c.phase, c.label, types, read, 2*(2*c.p+1))
			}
			continue
		}
		t.Logf("%s %s query_log types=%v read_rows=%d read_rows_cap=%d", c.phase, c.label, types, read, c.capRows)
		if len(types) != 1 || types[0] != "QueryFinish" || read > c.capRows {
			t.Errorf("%s %s: query_log %v read_rows %d, want [QueryFinish] and <= %d", c.phase, c.label, types, read, c.capRows)
		}
	}
	return fired
}

func f3Rows(got []map[string]string) []string {
	var out []string
	for _, r := range got {
		out = append(out, strings.Join([]string{r["family"], r["raw_table"], r["scope_id"], r["raw_first_day"], r["stored_first_day"], r["stored_days"]}, " "))
	}
	sort.Strings(out)
	return out
}

func f3Sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// f3Verdict = the l13902-lib.sh floor_check rule (scripts-11) on the rows of one chunk (proven on the lib by HARNESS-12).
func f3Verdict(rows []map[string]string) []string {
	const runFirstDay = "2026-04-17"
	var reasons []string
	perStmt := map[string]int{"1": 0, "2": 0, "3": 0, "4": 0}
	for _, r := range rows {
		if r["family"] == "_stmt" {
			continue
		}
		perStmt[r["stmt"]]++
		switch {
		case r["raw_first_day"] == "none" && r["raw_table"] == "repos":
			reasons = append(reasons, fmt.Sprintf("%s scope %s: repository not held", r["family"], r["scope_id"]))
		case r["raw_first_day"] == "none":
			reasons = append(reasons, fmt.Sprintf("%s %s scope %s: no raw row", r["family"], r["raw_table"], r["scope_id"]))
		default:
			floor := r["raw_first_day"]
			if runFirstDay > floor {
				floor = runFirstDay
			}
			if r["stored_first_day"] < floor {
				reasons = append(reasons, fmt.Sprintf("%s %s scope %s: stored day %s below the floor %s", r["family"], r["raw_table"], r["scope_id"], r["stored_first_day"], floor))
			}
		}
	}
	for _, s := range []string{"1", "2", "3", "4"} {
		if perStmt[s] == 0 {
			reasons = append(reasons, "stmt"+s+" 0 family rows")
		}
	}
	sort.Strings(reasons)
	return reasons
}

func f3Schema(t *testing.T, ctx context.Context, tables map[string]bool) driver.Conn {
	t.Helper()
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
	for table := range tables {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges %s: %v", table, err)
		}
	}
	return conn
}

func f3Exec(t *testing.T, ctx context.Context, conn driver.Conn, sql string) {
	t.Helper()
	for _, statement := range chmigrate.SplitStatements(sql) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}
}

func f3Pin(t *testing.T, name, text, want string) {
	t.Helper()
	sum := sha256.Sum256([]byte(text))
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("%s (prefix swapped) sha256 = %s, want %s", name, got, want)
	}
}

func TestFloor13902v3FormExecutesMutantsAndEveryStopFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	f3Pin(t, "FLOOR-13902-3", f3Form, f3SHA256)
	if n := strings.Count(chmigrate.StripLineComments(f3Form), "'f0f0f0f0'"); n != 63 {
		t.Fatalf("%d org prefixes in the SQL, want 63", n)
	}
	specs := map[string]f3Spec{
		"stmt1": {f: 2, s: 1, orgsTab: "repo_metrics_daily", reads: map[string]uint64{"ai_impact_metrics_daily": 1, "commit_metrics": 1, "git_commits": 1,
			"git_pull_request_reviews": 1, "git_pull_requests": 1, "repo_metrics_daily": 2, "team_metrics_daily": 1, "user_metrics_daily": 1}},
		"stmt2": {f: 2, s: 1, orgsTab: "cicd_metrics_daily", reads: map[string]uint64{"ci_pipeline_runs": 3, "cicd_metrics_daily": 2, "coverage_snapshots": 1,
			"deploy_metrics_daily": 1, "deployments": 2, "repo_change_failure_daily": 1, "test_suite_results": 1, "testops_coverage_metrics_daily": 1,
			"testops_pipeline_metrics_daily": 1, "testops_pipeline_stability": 1, "testops_quality_drag": 1, "testops_release_confidence": 1,
			"testops_test_metrics_daily": 1}},
		"stmt3": {f: 2, s: 1, orgsTab: "user_metrics_daily", reads: map[string]uint64{"ai_attribution": 1, "ai_governance_coverage_daily": 1,
			"ai_impact_metrics_daily": 1, "compounding_risk_daily": 1, "estimate_coverage_metrics_daily": 1, "ic_landscape_rolling_30d": 1,
			"incident_metrics_daily": 1, "issue_type_metrics_daily": 1, "operational_incidents": 1, "repo_complexity_daily": 1, "repo_metrics_daily": 1,
			"team_cognitive_load_daily": 1, "team_complexity_daily": 1, "testops_coverage_metrics_daily": 1, "testops_metric_baselines": 1,
			"testops_pipeline_metrics_daily": 1, "testops_test_metrics_daily": 1, "user_metrics_daily": 3, "work_item_completion_daily": 1,
			"work_item_measure_capability": 1, "work_item_metrics_daily": 1, "work_item_state_durations_daily": 1, "work_item_user_metrics_daily": 1,
			"work_items": 1}},
		"stmt4": {f: 1, s: 1, orgsTab: "repos", reads: map[string]uint64{"ai_impact_metrics_daily": 1, "repos": 2, "team_metrics_daily": 1,
			"testops_coverage_metrics_daily": 1, "testops_pipeline_stability": 1, "testops_quality_drag": 1, "testops_release_confidence": 1,
			"testops_test_metrics_daily": 1, "user_metrics_daily": 1, "work_items": 1}},
	}
	statements := floor2Split(f3Form)
	if len(statements) != 4 {
		t.Fatalf("FLOOR-13902-3: %d statements, want 4", len(statements))
	}
	forms := map[string]string{}
	for i, st := range statements {
		label := fmt.Sprintf("stmt%d", i+1)
		body := chmigrate.StripLineComments(st.text)
		if st.label != label || strings.Count(st.text, specs[label].gate()) != 1 {
			t.Fatalf("%s: label %q or gate text not found:\n%s", label, st.label, specs[label].gate())
		}
		for _, one := range []string{"'STOP-COST-GATE'", ">= 20000000", "system.parts", "WHERE cost_gate = 0", "AND cost_gate = 0) AS orgs"} {
			if strings.Count(body, one) != 1 {
				t.Fatalf("%s: %q not exactly once", label, one)
			}
		}
		if !strings.Contains(body, "(SELECT uniqExact(org_id) FROM "+specs[label].orgsTab+" WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS orgs") {
			t.Fatalf("%s: ORG-1 scalar not on %s", label, specs[label].orgsTab)
		}
		if strings.Contains(body, " FINAL ") != (specs[label].f == 2) {
			t.Fatalf("%s: FINAL vs f %d", label, specs[label].f)
		}
		forms[label] = st.text
	}
	fill := func(text, from, to string) string {
		out := strings.NewReplacer("{{FROM}}", from, "{{TO}}", to, "{{FILL_ID}}", f3FillID).Replace(text)
		if strings.Contains(out, "{{") {
			t.Fatalf("a placeholder is left after the fill")
		}
		return out
	}
	tables := map[string]bool{}
	for _, sp := range specs {
		for table := range sp.reads {
			tables[table] = true
		}
	}
	conn := f3Schema(t, ctx, tables)
	run := &f3Run{t: t, ctx: ctx, conn: conn, specs: specs}
	synth := f3Synth(fOrg, "u9@x")
	t.Logf("synthesized repo id of (f0 org, u9@x) = %s cut %s", synth, synth[:8])
	f3Exec(t, ctx, conn, f3Fixture(synth))

	c1 := q1[:8]
	ctl := func(from string) string { return "_stmt _cap org none " + from + " 0" }
	s1 := []string{ctl(f3RunDay1),
		"ai_impact_metrics_daily git_pull_request_reviews " + c1 + " 2025-08-02 2026-10-26 1",
		"ai_impact_metrics_daily git_pull_requests " + c1 + " 2025-08-01 2026-10-26 1",
		"commit_metrics git_commits " + c1 + " 2026-01-10 2026-10-25 1",
		"repo_metrics_daily git_commits " + c1 + " 2026-01-10 2026-10-25 2",
		"repo_metrics_daily git_pull_request_reviews " + c1 + " 2025-08-02 2026-10-25 2",
		"repo_metrics_daily git_pull_requests " + c1 + " 2025-08-01 2026-10-25 2",
		"team_metrics_daily git_commits " + c1 + " 2026-01-10 2026-10-25 1",
		"user_metrics_daily git_commits " + c1 + " 2026-01-10 2026-10-25 1",
		"user_metrics_daily git_pull_request_reviews " + c1 + " 2025-08-02 2026-10-25 1",
		"user_metrics_daily git_pull_requests " + c1 + " 2025-08-01 2026-10-25 1"}
	s2 := []string{ctl(f3RunDay1),
		"cicd_metrics_daily ci_pipeline_runs.finished " + c1 + " 2026-02-01 2026-10-25 1",
		"deploy_metrics_daily deployments.deployed " + c1 + " 2026-03-01 2026-10-28 1",
		"repo_change_failure_daily deployments.any " + c1 + " 2026-02-15 2026-10-28 1",
		"testops_coverage_metrics_daily ci_pipeline_runs.started " + c1 + " 2025-12-01 2026-10-26 1",
		"testops_coverage_metrics_daily coverage_snapshots " + c1 + " 2026-02-01 2026-10-26 1",
		"testops_pipeline_metrics_daily ci_pipeline_runs.started " + c1 + " 2025-12-01 2026-10-25 1",
		"testops_pipeline_stability ci_pipeline_runs.started " + c1 + " 2025-12-01 2026-10-27 1",
		"testops_quality_drag ci_pipeline_runs.started " + c1 + " 2025-12-01 2026-10-27 1",
		"testops_release_confidence ci_pipeline_runs.started " + c1 + " 2025-12-01 2026-10-27 1",
		"testops_test_metrics_daily test_suite_results " + c1 + " 2026-02-02 2026-10-26 1"}
	s3 := []string{ctl(f3RunDay1),
		"ai_governance_coverage_daily ai_attribution org 2026-01-06 2026-10-29 1",
		"ai_impact_metrics_daily.org operational_incidents org 2026-01-05 2026-10-26 1",
		"benchmarking benchmarking.inputs org 2026-10-25 2026-10-29 1",
		"compounding_risk_daily.team repo_metrics_daily.live org 2025-08-15 2026-10-26 1",
		"estimate_coverage_metrics_daily work_items org 2026-01-03 2026-10-29 1",
		"ic_landscape_rolling_30d user_metrics_daily.live org 2026-10-25 2026-10-29 1",
		"ic_landscape_rolling_30d work_items org 2026-01-03 2026-10-29 1",
		"incident_metrics_daily operational_incidents org 2026-01-05 2026-10-29 1",
		"issue_type_metrics_daily work_items org 2026-01-03 2026-10-29 1",
		"team_cognitive_load_daily user_metrics_daily.live org 2026-10-25 2026-10-25 1",
		"team_complexity_daily repo_complexity_daily org 2026-01-04 2026-10-29 1",
		"user_metrics_daily.synth work_items org 2026-01-03 2026-10-28 1",
		"work_item_completion_daily work_items org 2026-01-03 2026-10-29 1",
		"work_item_measure_capability work_items org 2026-01-03 2026-10-30 1",
		"work_item_metrics_daily work_items org 2026-01-03 2026-10-29 1",
		"work_item_state_durations_daily work_items org 2026-01-03 2026-10-29 1",
		"work_item_user_metrics_daily work_items org 2026-01-03 2026-10-29 1"}
	s4 := []string{ctl(f3RunDay1),
		"held:ai_impact_metrics_daily repos " + c1 + " 1970-01-01 2026-10-26 1",
		"held:team_metrics_daily repos " + c1 + " 1970-01-01 2026-10-25 1",
		"held:testops_coverage_metrics_daily repos " + c1 + " 1970-01-01 2026-10-26 1",
		"held:testops_pipeline_stability repos " + c1 + " 1970-01-01 2026-10-27 1",
		"held:testops_quality_drag repos " + c1 + " 1970-01-01 2026-10-27 1",
		"held:testops_release_confidence repos " + c1 + " 1970-01-01 2026-10-27 1",
		"held:testops_test_metrics_daily repos " + c1 + " 1970-01-01 2026-10-26 1",
		"held:user_metrics_daily repos " + c1 + " 1970-01-01 2026-10-25 1"}
	// stmt1 rule mutant (no newest-row retracted filter): the retracted 2026-10-23 repo-day counts as live.
	var s1RetractedLive []string
	for _, r := range s1 {
		if strings.HasPrefix(r, "repo_metrics_daily ") {
			r = strings.Replace(r, " 2026-10-25 2", " 2026-10-23 3", 1)
		}
		s1RetractedLive = append(s1RetractedLive, r)
	}
	passWant := map[string][]string{"stmt1": s1, "stmt2": s2, "stmt3": s3, "stmt4": s4}
	want := func(phase, label string, got, w []string) {
		if strings.Join(got, "\n") != strings.Join(f3Sorted(w), "\n") {
			t.Errorf("%s %s rows:\n got  %s\n want %s", phase, label, strings.Join(got, "\n      "), strings.Join(f3Sorted(w), "\n      "))
		}
	}
	// controls: every row of a statement carries ORG-1, its family-row count, the cut check, the server time and the nonce
	controls := func(phase, label string, got []map[string]string, start time.Time) {
		fam, cuts := 0, map[string]bool{}
		for _, r := range got {
			if r["family"] != "_stmt" {
				fam++
				if r["scope_id"] != "org" {
					cuts[r["scope_id"]] = true
				}
			}
		}
		for _, r := range got {
			if r["orgs"] != "1" || r["fam_rows"] != fmt.Sprint(fam) || r["scopes_full"] != r["scopes_cut"] || r["scopes_cut"] != fmt.Sprint(len(cuts)) || r["fill_id"] != f3FillID {
				t.Errorf("%s %s: control columns %v (want orgs 1, fam_rows %d, scopes %d = cut, fill %s)", phase, label, r, fam, len(cuts), f3FillID)
				return
			}
			at, err := time.Parse("2006-01-02 15:04:05", r["read_at"])
			if err != nil || at.Before(start.Add(-2*time.Second)) || at.After(time.Now().UTC().Add(2*time.Second)) {
				t.Errorf("%s %s: read_at %q not the server time of the read", phase, label, r["read_at"])
				return
			}
		}
	}
	var passRows []map[string]string
	for _, label := range []string{"stmt1", "stmt2", "stmt3", "stmt4"} {
		start := time.Now().UTC()
		got, err := run.query("PASS", label, fill(forms[label], f3RunDay1, f3RunDayN))
		if err != nil {
			t.Fatalf("PASS %s: %v", label, err)
		}
		want("PASS", label, f3Rows(got), passWant[label])
		controls("PASS", label, got, start)
		passRows = append(passRows, got...)
	}
	if v := f3Verdict(passRows); len(v) != 0 {
		t.Errorf("PASS verdict = %v, want PASS", v)
	} else {
		t.Logf("VERDICT PASS C1: 0 reasons; b0 rows in every table read did not move any exact value (every org filter executed)")
	}

	mutate := func(text, from, to string, places int) string {
		if n := strings.Count(text, from); n != places {
			t.Fatalf("mutant pattern %q found %d times, want %d", from, n, places)
		}
		return strings.ReplaceAll(text, from, to)
	}
	ttLive := "FROM testops_test_metrics_daily\n          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')\n          GROUP BY repo_id, day) AS k\n    WHERE k.rt = 0 GROUP BY k.repo_id"
	synthCut := synth[:8]
	type mutant struct {
		label, kind, from, to string
		places                int
		want                  []string // nil = assert changed only
	}
	mutants := []mutant{
		{"stmt1", "ORG", "'f0f0f0f0'", "'b0000000'", 9, nil},
		{"stmt1", "RULE", "GROUP BY repo_id, day) AS k\n    WHERE k.rt = 0 GROUP BY k.repo_id", "GROUP BY repo_id, day) AS k\n    GROUP BY k.repo_id", 1, s1RetractedLive},
		{"stmt1", "RULE-SYNTH", " AND NOT k.synth", "", 1, append(append([]string(nil), s1...),
			"user_metrics_daily git_commits "+synthCut+" none 2026-10-28 1",
			"user_metrics_daily git_pull_request_reviews "+synthCut+" none 2026-10-28 1",
			"user_metrics_daily git_pull_requests "+synthCut+" none 2026-10-28 1")},
		{"stmt2", "ORG", "'f0f0f0f0'", "'b0000000'", 17, nil},
		{"stmt2", "RULE", " AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued')", "", 1,
			replaceRow(s2, "cicd_metrics_daily ci_pipeline_runs.finished "+c1+" 2026-02-01 2026-10-25 1",
				"cicd_metrics_daily ci_pipeline_runs.finished "+c1+" 2025-12-01 2026-10-25 1")},
		{"stmt3", "ORG", "'f0f0f0f0'", "'b0000000'", 26, nil},
		{"stmt3", "RULE", " AND is_deleted = 0", "", 1,
			replaceRow(replaceRow(s3, "incident_metrics_daily operational_incidents org 2026-01-05 2026-10-29 1",
				"incident_metrics_daily operational_incidents org 2025-12-20 2026-10-29 1"),
				"ai_impact_metrics_daily.org operational_incidents org 2026-01-05 2026-10-26 1",
				"ai_impact_metrics_daily.org operational_incidents org 2025-12-20 2026-10-26 1")},
		{"stmt3", "RULE-RETIRED", "    WHERE k.ic > 0 HAVING count() > 0", "    HAVING count() > 0", 1,
			replaceRow(s3, "work_item_measure_capability work_items org 2026-01-03 2026-10-30 1",
				"work_item_measure_capability work_items org 2026-01-03 2026-10-24 2")},
		{"stmt4", "ORG", "'f0f0f0f0'", "'b0000000'", 11, nil},
		{"stmt4", "RULE", ttLive, strings.Replace(ttLive, "    WHERE k.rt = 0 GROUP BY k.repo_id", "    GROUP BY k.repo_id", 1), 1,
			append(append([]string(nil), s4...), "held:testops_test_metrics_daily repos "+q3[:8]+" none 2026-10-30 1")},
	}
	for _, m := range mutants {
		got, err := run.query("MUTANT-"+m.kind, m.label, fill(mutate(forms[m.label], m.from, m.to, m.places), f3RunDay1, f3RunDayN))
		if err != nil {
			t.Fatalf("MUTANT-%s %s: SQL error: %v", m.kind, m.label, err)
		}
		g := f3Rows(got)
		if strings.Join(g, "\n") == strings.Join(f3Sorted(passWant[m.label]), "\n") {
			t.Errorf("MUTANT-%s %s did not change the output", m.kind, m.label)
		}
		if m.want != nil {
			want("MUTANT-"+m.kind, m.label, g, m.want)
		}
		t.Logf("MUTANT-%s %s CHANGED (%d places)", m.kind, m.label, m.places)
	}

	f3Exec(t, ctx, conn, f3VariantHeld)
	var varRows []map[string]string
	for _, label := range []string{"stmt1", "stmt2", "stmt3", "stmt4"} {
		got, err := run.query("VARIANT", label, fill(forms[label], f3RunDay1, f3RunDayN))
		if err != nil {
			t.Fatalf("VARIANT %s: %v", label, err)
		}
		varRows = append(varRows, got...)
	}
	c9 := q9[:8]
	wantReasons := f3Sorted([]string{
		"held:user_metrics_daily scope " + c9 + ": repository not held",
		"user_metrics_daily git_commits scope " + c9 + ": no raw row",
		"user_metrics_daily git_pull_request_reviews scope " + c9 + ": no raw row",
		"user_metrics_daily git_pull_requests scope " + c9 + ": no raw row",
	})
	if v := f3Verdict(varRows); strings.Join(v, "\n") != strings.Join(wantReasons, "\n") {
		t.Errorf("VARIANT verdict:\n got  %s\n want %s", strings.Join(v, "\n      "), strings.Join(wantReasons, "\n      "))
	} else {
		t.Logf("VERDICT STOP C1 (repo q9 not held: a b0 repos row for q9 does not count): %s", strings.Join(v, "; "))
	}
	var zeroRows []map[string]string
	for _, label := range []string{"stmt1", "stmt2", "stmt3", "stmt4"} {
		got, err := run.query("ZERO", label, fill(forms[label], "2026-04-17", "2026-04-19"))
		if err != nil {
			t.Fatalf("ZERO %s: %v", label, err)
		}
		want("ZERO", label, f3Rows(got), []string{ctl("2026-04-17")})
		zeroRows = append(zeroRows, got...)
	}
	if v := f3Verdict(zeroRows); strings.Join(v, "|") != "stmt1 0 family rows|stmt2 0 family rows|stmt3 0 family rows|stmt4 0 family rows" {
		t.Errorf("ZERO verdict = %v", v)
	} else {
		t.Logf("VERDICT STOP C8 (each statement 0 family rows)")
	}

	gate := func(phase string, fires map[string]bool) {
		live := floor2LiveParts(t, ctx, conn)
		for _, label := range []string{"stmt1", "stmt2", "stmt3", "stmt4"} {
			capRows, _ := specs[label].derive(live)
			if (capRows >= 20000000) != fires[label] {
				t.Fatalf("%s %s: derived read_rows_cap %d, the fixture must make fire = %v", phase, label, capRows, fires[label])
			}
			_, err := run.query(phase, label, fill(forms[label], f3RunDay1, f3RunDayN))
			if fires[label] {
				if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
					t.Errorf("%s %s: err = %v, want code 395 STOP-COST-GATE", phase, label, err)
				} else {
					t.Logf("%s %s FIRED at derived read_rows_cap %d", phase, label, capRows)
				}
			} else if err != nil {
				t.Errorf("%s %s: SQL error %v (derived cap %d < 20M: must pass)", phase, label, err, capRows)
			} else {
				t.Logf("%s %s passes at derived read_rows_cap %d", phase, label, capRows)
			}
		}
	}
	insert := func(sql string) {
		start := time.Now()
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("fire rows: %v", err)
		}
		t.Logf("fire rows inserted in %s", time.Since(start).Round(time.Millisecond))
	}
	// Fire rows of org e0, each row its own key (optimize_on_insert collapses equal keys in one block).
	insert(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, last_synced)
SELECT '` + eOrg + `', toUUID('00000000-0000-4000-8000-000000000009'), concat('fire-', toString(number)), 'github_actions', 'success',
  toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), toDateTime64('2026-01-01 00:00:00', 3, 'UTC') FROM numbers(2800000)`)
	gate("FIRE-A", map[string]bool{"stmt2": true})
	insert(`INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at)
SELECT '` + eOrg + `', generateUUIDv4(number), toDate('2026-01-01') + (number % 300), toDateTime('2026-01-02 00:00:00', 'UTC') FROM numbers(8200000)`)
	gate("FIRE-B", map[string]bool{"stmt1": true, "stmt2": true, "stmt3": true})
	insert(`INSERT INTO repos (org_id, id, repo, created_at, last_synced)
SELECT '` + eOrg + `', generateUUIDv4(number), 'x', toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), toDateTime64('2026-01-01 00:00:00', 3, 'UTC') FROM numbers(8200000)`)
	gate("FIRE-C", map[string]bool{"stmt1": true, "stmt2": true, "stmt3": true, "stmt4": true})
	fired := run.checkLog()
	for _, label := range []string{"stmt1", "stmt2", "stmt3", "stmt4"} {
		if !fired[label] {
			t.Errorf("%s: the cost gate never fired", label)
		}
	}
}

func TestQZ13902v2FormsExecuteMutantsAndEveryGateFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	f3Pin(t, "FORM-QZ1-13902-2", q1v2Form, q1v2SHA)
	f3Pin(t, "FORM-QZ3-13902-2", q3v2Form, q3v2SHA)
	f3Pin(t, "FORM-QZ4-13902-2", q4v2Form, q4v2SHA)
	q4Tables := []string{"commit_metrics", "file_hotspot_daily", "file_metrics_daily", "ic_landscape_rolling_30d", "repo_metrics_daily",
		"team_cognitive_load_daily", "team_metrics_daily", "user_metrics_daily"}
	q4Reads := map[string]uint64{}
	for _, tb := range q4Tables {
		q4Reads[tb] = 1
	}
	q4Reads["repo_metrics_daily"] = 2
	specs := map[string]f3Spec{
		"qz3": {f: 2, s: 1, orgsTab: "repo_metrics_daily", reads: map[string]uint64{"git_commits": 1, "repo_metrics_daily": 2}},
		"qz4": {f: 1, s: 1, orgsTab: "repo_metrics_daily", reads: q4Reads},
	}
	trim := func(s string) string { return strings.TrimSuffix(strings.TrimRight(s, " \n"), ";") }
	q1s := floor2Split(q1v2Form)
	if len(q1s) != 2 {
		t.Fatalf("FORM-QZ1-13902-2: %d statements, want 2", len(q1s))
	}
	for label, text := range map[string]string{"qz3": trim(q3v2Form), "qz4": trim(q4v2Form)} {
		if strings.Count(text, specs[label].gate()) != 1 || strings.Count(chmigrate.StripLineComments(text), "AND cost_gate = 0) AS orgs") != 1 {
			t.Fatalf("%s: gate text or ORG-1 scalar not found:\n%s", label, specs[label].gate())
		}
	}
	tables := map[string]bool{"git_commits": true}
	for _, tb := range q4Tables {
		tables[tb] = true
	}
	conn := f3Schema(t, ctx, tables)
	run := &f3Run{t: t, ctx: ctx, conn: conn, specs: specs}
	// f0 + b0 rows in EVERY table the QZ forms read (acp X-P2-1).
	fx := ""
	for _, org := range []string{fOrg, bOrg} {
		cc := "0"
		if org == bOrg {
			cc = "5"
		}
		fx += `
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES ('` + org + `', '` + q1 + `', 'h1', 'a', 'a@x', '2026-01-10 10:00:00', '2026-01-10 11:00:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, computed_at, retracted) VALUES ('` + org + `', '` + q1 + `', '2025-08-15', ` + cc + `, '2025-08-16 01:00:00', 0), ('` + org + `', '` + q1 + `', '2026-10-25', 2, '2026-10-26 01:00:00', 0);
INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES ('` + org + `', '` + q1 + `', 'a@x', '2025-08-15', ` + cc + `, '2025-08-16 01:00:00', 0);
INSERT INTO team_cognitive_load_daily (org_id, team_id, day, computed_at) VALUES ('` + org + `', 't1', '2025-08-15', '2025-08-16 01:00:00');
INSERT INTO file_metrics_daily (org_id, repo_id, day, path, churn, commits_count, computed_at) VALUES ('` + org + `', '` + q1 + `', '2025-08-15', 'a.go', ` + cc + `, ` + cc + `, '2025-08-16 01:00:00');
INSERT INTO file_hotspot_daily (org_id, repo_id, day, file_path, churn_loc_30d, churn_commits_30d, computed_at) VALUES ('` + org + `', '` + q1 + `', '2025-08-15', 'a.go', ` + cc + `, ` + cc + `, '2025-08-16 01:00:00');
INSERT INTO ic_landscape_rolling_30d (org_id, repo_id, team_id, map_name, as_of_day, identity_id, churn_loc_30d, computed_at) VALUES ('` + org + `', '` + q1 + `', 't1', 'm', '2025-08-15', 'i1', ` + cc + `, '2025-08-16 01:00:00');
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, computed_at) VALUES ('` + org + `', '` + q1 + `', 'h1', '2026-01-10', 'a@x', 5, '2026-01-11 01:00:00');
`
	}
	f3Exec(t, ctx, conn, fx)
	q4Line := func(phase, text string) []string {
		got, err := run.query(phase, "qz4", text)
		if err != nil {
			t.Fatalf("%s qz4: %v", phase, err)
		}
		var out []string
		for _, r := range got {
			if r["orgs"] != "1" {
				t.Errorf("%s qz4: orgs %s, want 1", phase, r["orgs"])
			}
			out = append(out, strings.Join([]string{r["table_name"], r["rows_in_range"], r["keys_in_range"], r["commit_rows_any_version"], r["commit_keys_newest"], r["first_day"]}, " "))
		}
		sort.Strings(out)
		return out
	}
	q4Pass := []string{"commit_metrics 0 0 0 0 1970-01-01", "file_hotspot_daily 1 1 0 0 2025-08-15", "file_metrics_daily 1 1 0 0 2025-08-15",
		"ic_landscape_rolling_30d 1 1 0 0 2025-08-15", "repo_metrics_daily 1 1 0 0 2025-08-15", "team_cognitive_load_daily 1 1 0 0 2025-08-15",
		"team_metrics_daily 0 0 0 0 1970-01-01", "user_metrics_daily 1 1 0 0 2025-08-15"}
	eq := func(phase, label string, got, want []string) {
		if strings.Join(got, "\n") != strings.Join(f3Sorted(want), "\n") {
			t.Errorf("%s %s:\n got  %s\n want %s", phase, label, strings.Join(got, "\n      "), strings.Join(f3Sorted(want), "\n      "))
		}
	}
	eq("PASS", "qz4", q4Line("PASS", trim(q4v2Form)), q4Pass)
	q4Org := []string{"commit_metrics 0 0 0 0 1970-01-01", "file_hotspot_daily 1 1 1 1 2025-08-15", "file_metrics_daily 1 1 1 1 2025-08-15",
		"ic_landscape_rolling_30d 1 1 1 1 2025-08-15", "repo_metrics_daily 1 1 1 1 2025-08-15", "team_cognitive_load_daily 1 1 0 0 2025-08-15",
		"team_metrics_daily 0 0 0 0 1970-01-01", "user_metrics_daily 1 1 1 1 2025-08-15"}
	eq("MUTANT-ORG", "qz4", q4Line("MUTANT-ORG", strings.ReplaceAll(trim(q4v2Form), "'f0f0f0f0'", "'b0000000'")), q4Org)
	f3Exec(t, ctx, conn, `INSERT INTO team_metrics_daily (org_id, team_id, repo_id, day, commits_count, computed_at) VALUES ('`+fOrg+`', 't1', '`+q1+`', '2025-09-01', 1, '2025-09-02 01:00:00');`)
	eq("LOSS", "qz4", q4Line("LOSS", trim(q4v2Form)), replaceRow(q4Pass, "team_metrics_daily 0 0 0 0 1970-01-01", "team_metrics_daily 1 1 1 1 2025-09-01"))
	rule := strings.ReplaceAll(trim(q4v2Form), "commits_count > 0 OR after_hours_commits_count > 0 OR ", "")
	if strings.Count(trim(q4v2Form), "commits_count > 0 OR after_hours_commits_count > 0 OR ") != 2 {
		t.Fatalf("qz4 rule mutant pattern")
	}
	eq("MUTANT-RULE", "qz4", q4Line("MUTANT-RULE", rule), replaceRow(q4Pass, "team_metrics_daily 0 0 0 0 1970-01-01", "team_metrics_daily 1 1 0 0 2025-09-01"))

	q3Line := func(phase, text string) string {
		got, err := run.query(phase, "qz3", text)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s qz3: %v (%d rows)", phase, err, len(got))
		}
		r := got[0]
		return strings.Join([]string{r["orgs"], r["repos_with_commit_days"], r["repos_stored_before_raw"], r["repos_commit_days_no_raw"], r["first_stored_commit_day"], r["first_raw_commit_day"]}, " ")
	}
	for _, c := range []struct{ phase, text, want string }{
		{"PASS", trim(q3v2Form), "1 1 0 0 2026-10-25 2026-01-10"},
		{"MUTANT-ORG", strings.ReplaceAll(trim(q3v2Form), "'f0f0f0f0'", "'b0000000'"), "1 1 1 0 2025-08-15 2026-01-10"},
		{"MUTANT-RULE", strings.Replace(trim(q3v2Form), " AND k.cc > 0", "", 1), "1 1 1 0 2025-08-15 2026-01-10"},
	} {
		if got := q3Line(c.phase, c.text); got != c.want {
			t.Errorf("%s qz3 = %s, want %s", c.phase, got, c.want)
		} else {
			t.Logf("%s qz3 = %s", c.phase, got)
		}
	}

	// QZ1: TTL + DELETE mutations, can fail, org-free metadata; gate fired by the TEMPLATE-3 :12 method (one literal).
	q1Run := func(phase string, idx int, text string) ([]map[string]string, error) {
		return run.query(phase, fmt.Sprintf("qz1-s%d", idx+1), text)
	}
	q1Pass, err := q1Run("PASS", 0, q1s[0].text)
	if err != nil || len(q1Pass) != 11 {
		t.Fatalf("PASS qz1 stmt1: %v, %d rows (want 11)", err, len(q1Pass))
	}
	for _, r := range q1Pass {
		if r["has_ttl"] != "0" {
			t.Errorf("PASS qz1 stmt1: %s has_ttl %s", r["table_name"], r["has_ttl"])
		}
	}
	m2, err := q1Run("PASS", 1, q1s[1].text)
	if err != nil || len(m2) != 1 || m2[0]["delete_mutations"] != "0" {
		t.Fatalf("PASS qz1 stmt2: %v %v", err, m2)
	}
	if err := conn.Exec(ctx, "ALTER TABLE ai_attribution MODIFY TTL toDateTime(observed_at) + INTERVAL 10 YEAR"); err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if err := conn.Exec(ctx, "ALTER TABLE git_commits DELETE WHERE org_id = 'nobody'"); err != nil {
		t.Fatalf("delete mutation: %v", err)
	}
	ttl, _ := q1Run("CANFAIL", 0, q1s[0].text)
	hits := 0
	for _, r := range ttl {
		if r["has_ttl"] == "1" {
			hits++
			t.Logf("CANFAIL qz1 stmt1: %s has_ttl 1", r["table_name"])
		}
	}
	if hits != 1 {
		t.Errorf("CANFAIL qz1 stmt1: %d tables with has_ttl 1, want 1 (ai_attribution)", hits)
	}
	del, _ := q1Run("CANFAIL", 1, q1s[1].text)
	if len(del) != 1 || del[0]["delete_mutations"] != "1" {
		t.Errorf("CANFAIL qz1 stmt2: %v, want delete_mutations 1", del)
	}
	ruleDel, _ := q1Run("MUTANT-RULE", 1, strings.Replace(q1s[1].text, "positionCaseInsensitive(command, 'DELETE') > 0) AS delete_mutations,", "positionCaseInsensitive(command, 'UPDATE') > 0) AS delete_mutations,", 1))
	if len(ruleDel) != 1 || ruleDel[0]["delete_mutations"] != "0" {
		t.Errorf("MUTANT-RULE qz1 stmt2: %v, want delete_mutations 0", ruleDel)
	}
	ruleT, _ := q1Run("MUTANT-RULE", 0, strings.Replace(q1s[0].text, "'git_commits',", "'git_commitz',", 1))
	if len(ruleT) != 10 {
		t.Errorf("MUTANT-RULE qz1 stmt1: %d rows, want 10", len(ruleT))
	}
	for i, st := range q1s {
		const capLit = "> 100000, 'STOP-COST-GATE'"
		if strings.Count(st.text, capLit) != 1 || strings.Count(chmigrate.StripLineComments(st.text), "> 100000") != 1 {
			t.Fatalf("qz1 stmt%d: the cap literal '> 100000' is not exactly once in the SQL", i+1)
		}
		low := strings.Replace(st.text, capLit, "> -1, 'STOP-COST-GATE'", 1)
		comment := fmt.Sprintf("qz1-fire-%d-%d", i, time.Now().UnixNano())
		frows, ferr := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), low)
		if ferr == nil {
			_ = frows.Close()
		}
		if ferr == nil || !strings.Contains(ferr.Error(), "STOP-COST-GATE") || !strings.Contains(ferr.Error(), "395") {
			t.Errorf("FIRE qz1 stmt%d ('> 100000' -> '> -1'): err = %v, want code 395 STOP-COST-GATE", i+1, ferr)
			continue
		}
		if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatal(err)
		}
		types, read := floor2QueryLog(t, ctx, conn, comment)
		t.Logf("FIRE qz1 stmt%d: code 395 STOP-COST-GATE, query_log %v read_rows %d", i+1, types, read)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("FIRE qz1 stmt%d: query_log %v, want [ExceptionBeforeStart]", i+1, types)
		}
	}

	// QZ3 + QZ4 gates fired by fixture: repo_metrics_daily rows of org e0 (k 2 in both; QZ3 f 2, QZ4 f 1).
	if err := conn.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at)
SELECT '`+eOrg+`', generateUUIDv4(number), toDate('2025-01-01') + (number % 300), toDateTime('2025-01-02 00:00:00', 'UTC') FROM numbers(8200000)`); err != nil {
		t.Fatalf("fire rows: %v", err)
	}
	live := floor2LiveParts(t, ctx, conn)
	for label, text := range map[string]string{"qz3": trim(q3v2Form), "qz4": trim(q4v2Form)} {
		capRows, _ := specs[label].derive(live)
		_, err := run.query("FIRE", label, text)
		if capRows < 20000000 || err == nil || !strings.Contains(err.Error(), "395") {
			t.Errorf("FIRE %s: derived cap %d, err %v (want >= 20M and code 395)", label, capRows, err)
		} else {
			t.Logf("FIRE %s FIRED at derived read_rows_cap %d", label, capRows)
		}
	}
	fired := run.checkLog()
	for _, label := range []string{"qz3", "qz4"} {
		if !fired[label] {
			t.Errorf("%s: the cost gate never fired", label)
		}
	}
}
