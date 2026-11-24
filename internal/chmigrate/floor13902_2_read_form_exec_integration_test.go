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

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of the prod read forms FLOOR-13902-2, FORM-QZ4-13902-1, FORM-QZ3-13902-1 and FORM-QZ1-13902-1
// (CHAOS-13902): not for merge. Each const is the form file with its org prefix (8 hex) replaced by the synthetic
// "f0f0f0f0"; each sha256 is pinned.
const (
	floor2SHA256 = "74b8f431a56ffa720e9e4198e033a267fc7e58ee277315c3f4bf38b4f46aeb87"
	qz4SHA256    = "6c3bb23699e144d32ddf5c18f6fc1c13e5b6eb92558fcc4b7480aa319563760c"
	qz3SHA256    = "0ffe9362db20b04b5ef936205109fc7662bf282cf39270d5182c4d0bbe0a1d18"
	qz1SHA256    = "17daee69fd42bae5462fcddffb523eeaf601a6ad7ed57ce9eb936eafc4344d33"
)

const floor2Form = `-- FLOOR-13902-2: raw floor read of CARD-13902-recompute-10 (CHAOS-13902), every family the recompute can ERASE from nothing.
--   ClickHouse, SELECT only, THREE statements (one per connector call), run by pm ONCE per chunk at the run minute, right
--   before 'chunk-start.sh <FROM> <TO> <DIGEST> --go' (card s4 step 1, (y)). Lane l13902 (lead fcdev), 2026-11-24.
--   Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-2.md. acp FORM read: OPEN. Supersedes FLOOR-13902-1 b56cb11c9d39.
-- Placeholders {{FROM}} {{TO}} = the chunk bounds; only 'chunk-start.sh --floor-sql <FROM> <TO>' fills them.
-- Why: SIZING-14285-6.out (6ac8b917087c) raw git_commits start 2025-11-01, stored daily rows from 2025-07-26. A family
--   that ERASEs (writes zeros or retracted = 1 over a live stored key it does not produce, card s10 family table) turns a
--   day with live stored keys and no raw source into zeros. Floor(T, S) = max(card --from 2026-04-17, first day of raw or
--   input table T for scope S, all days, read now). S = the repo when T has a repo key, else the org.
-- One row per (family = stored table, raw_table, scope_id) for every scope with a stored row in [FROM, TO], plus ONE control
--   row per statement (family '_stmt', raw_table '_cap'): it carries read_rows_cap even when no family has a row.
--   raw_first_day = the first raw day or 'none'; stored_first_day = first stored day in [FROM, TO]; stored_days = days.
-- Families with no ERASE (a missing raw day writes nothing, the stored row stays; card s10): file_metrics_daily,
--   file_hotspot_daily, review_edges_daily, compounding_risk_daily scope repo, ai_workflow_*, work_graph_*_edges,
--   work_item_team_attributions, work_item_cycle_times (reopen rows only), benchmarking (retires a day only with no live
--   input in [D-89, D]), work_item_measure_capability (90-day window, org proof first), ic_finalize (follows
--   user_metrics_daily, checked in stmt1). They have no row here.
-- Cost gate per statement (SIZING-14285-6 D1-D3 shape, acp READ-SIZING-14285-6-1 OK): read_rows_cap = 2 x ceil(1.25 x
--   (sum(marks x k) x 8192 + P)) + (2P + 1); throwIf(read_rows_cap >= 20000000). A gate run reads P + Q + 1 metadata rows
--   (Q = outdated parts still listed; acp P3-2), inside the cap headroom. pm WANT: read_rows <= read_rows_cap.

-- stmt1 GIT families (scope = repo).
-- Stored (live day of a repo): repo_metrics_daily and user_metrics_daily newest row per key not retracted; commit_metrics,
--   team_metrics_daily (repo_id != ''), ai_impact_metrics_daily every stored row (no retracted column).
-- Raw: git_commits FINAL toDate(committer_when); git_pull_requests FINAL toDate(created_at); git_pull_request_reviews
--   toDate(submitted_at) (raw, as the loader). Cost gate k 1 each, f 2 (FINAL), s 0.
WITH (SELECT 2 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_impact_metrics_daily', 'commit_metrics', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'team_metrics_daily', 'user_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table, o.scope_id AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap
FROM (
  SELECT toUInt8(1) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_id,
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
    FROM (SELECT repo_id, author_email, day, argMax(retracted, computed_at) AS rt FROM user_metrics_daily
          WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('{{FROM}}') AND day <= toDate('{{TO}}')
          GROUP BY repo_id, author_email, day) AS k
    WHERE k.rt = 0 GROUP BY k.repo_id
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
  SELECT toUInt8(1) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_id, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY o.family, o.raw_table, o.scope_id;

-- stmt2 CI / TESTOPS / DEPLOY families (scope = repo).
-- Stored: newest row per (repo_id, day) not retracted (cicd, testops pipeline/test/coverage, the 3 testops_risk tables,
--   deploy); repo_change_failure_daily every stored row (no retracted column).
-- Raw: ci_pipeline_runs FINAL .finished = toDate(finished_at) (finished, not deleted/running/queued) and .started =
--   toDate(started_at) (not deleted); test_suite_results FINAL toDate(coalesce(started_at, finished_at)); coverage_snapshots
--   FINAL joined to ci_pipeline_runs FINAL on (org_id, repo_id, run_id), day = toDate(started_at) of the run;
--   deployments FINAL .deployed = toDate(deployed_at), .any = toDate(coalesce(deployed_at, finished_at, started_at)).
-- Cost gate k ci_pipeline_runs 3, deployments 2, every other table 1; f 2; s 0.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'ci_pipeline_runs', 3, table = 'deployments', 2, 1)) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'coverage_snapshots', 'deploy_metrics_daily', 'deployments', 'repo_change_failure_daily', 'test_suite_results', 'testops_coverage_metrics_daily', 'testops_pipeline_metrics_daily', 'testops_pipeline_stability', 'testops_quality_drag', 'testops_release_confidence', 'testops_test_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table, o.scope_id AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap
FROM (
  SELECT toUInt8(2) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_id,
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
  SELECT toUInt8(2) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_id, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY o.family, o.raw_table, o.scope_id;

-- stmt3 ORG-SCOPE families (no repo key on the raw side, or a stored input).
-- Stored (org-wide first stored day in the chunk): incident_metrics_daily and work_item_user_metrics_daily newest row per
--   key not retracted; work_item_metrics_daily, estimate_coverage_metrics_daily, work_item_state_durations_daily,
--   issue_type_metrics_daily, work_item_completion_daily, team_complexity_daily, team_cognitive_load_daily,
--   compounding_risk_daily (scope 'team'), ai_governance_coverage_daily: every stored row.
-- Raw / input (org-wide first day, ALL days): operational_incidents FINAL toDate(started_at), is_deleted 0;
--   work_items FINAL toDate(created_at); repo_complexity_daily day (stored by a job this card does not run);
--   user_metrics_daily / repo_metrics_daily first LIVE day (the stored inputs of team_cognitive_load / compounding team);
--   ai_attribution FINAL toDate(observed_at), superseded_by IS NULL (= the view ai_attribution_resolved).
-- Cost gate k 1 each, f 2, s 0.
WITH (SELECT 2 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ai_attribution', 'ai_governance_coverage_daily', 'compounding_risk_daily', 'estimate_coverage_metrics_daily', 'incident_metrics_daily', 'issue_type_metrics_daily', 'operational_incidents', 'repo_complexity_daily', 'repo_metrics_daily', 'team_cognitive_load_daily', 'team_complexity_daily', 'user_metrics_daily', 'work_item_completion_daily', 'work_item_metrics_daily', 'work_item_state_durations_daily', 'work_item_user_metrics_daily', 'work_items')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.stmt AS stmt, o.family AS family, o.raw_table AS raw_table, o.scope_id AS scope_id, o.raw_first_day AS raw_first_day,
  o.stored_first_day AS stored_first_day, o.stored_days AS stored_days, read_rows_cap
FROM (
  SELECT toUInt8(3) AS stmt, p.p_family AS family, p.p_raw AS raw_table, p.p_scope AS scope_id,
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
    FROM ai_attribution FINAL WHERE startsWith(toString(org_id), 'f0f0f0f0') AND superseded_by IS NULL HAVING count() > 0
  ) AS r ON r.r_raw = p.p_raw AND r.r_scope = p.p_scope
  UNION ALL
  SELECT toUInt8(3) AS stmt, '_stmt' AS family, '_cap' AS raw_table, 'org' AS scope_id, 'none' AS raw_first_day,
    '{{FROM}}' AS stored_first_day, toUInt64(0) AS stored_days
) AS o
WHERE cost_gate = 0
ORDER BY o.family, o.raw_table, o.scope_id;
`

const qz4Form = `-- FORM-QZ4-13902-1: Q-z4 of CARD-13902-recompute-10 (z): data loss or not before the raw floor 2025-11-01. pm ask (binding):
--   per stored daily table, the count of stored rows dated 2025-07-26..2025-10-31 with ANY commit-derived column > 0.
--   ClickHouse, SELECT only, ONE statement. Lane l13902 (lead fcdev), 2026-11-24. Prod: NOT EXECUTED.
--   CI exec: EXEC-PROOF-FLOOR-13902-2.md (same cell, org + rule mutants). acp FORM read: OPEN.
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
--   commit-derived value > 0), commit_keys_newest (keys whose NEWEST version has one), read_rows_cap.
-- WANT / reading (card s12 Q-z4):
--   control: repo_metrics_daily keys_in_range > 0 (the range has stored rows; 0 = NOT EXERCISED, acp reads).
--   commit_rows_any_version = 0 AND commit_keys_newest = 0 in EVERY table = those days come only from PR / review rows (PRs
--     are cut at updated_at): a recompute there changes nothing; the floor is a safety STOP, not a loss case.
--   > 0 in ANY table = commit-derived values with no raw commit behind them: raw rows existed once and are gone; the card's
--     "no delete" finding is wrong somewhere: STOP and report (pm).
-- Cost gate (SIZING-14285-6 D1-D3 shape): k 1 each, f 1 (no FINAL), s 0: read_rows_cap = ceil(1.25 x (sum(marks) x 8192
--   + P)) + (2P + 1); throwIf(read_rows_cap >= 20000000). pm WANT read_rows <= read_rows_cap. No SET, no SETTINGS.
WITH (SELECT 1 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'file_hotspot_daily', 'file_metrics_daily', 'ic_landscape_rolling_30d', 'repo_metrics_daily', 'team_cognitive_load_daily', 'team_metrics_daily', 'user_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT o.table_name AS table_name, o.rows_in_range AS rows_in_range, o.keys_in_range AS keys_in_range,
  o.commit_rows_any_version AS commit_rows_any_version, o.commit_keys_newest AS commit_keys_newest, read_rows_cap
FROM (
  SELECT 'repo_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT repo_id, day, count() AS n, countIf(commits_count > 0 OR total_loc_touched > 0 OR avg_commit_size_loc > 0 OR large_commit_ratio > 0 OR rework_churn_ratio_30d > 0 OR single_owner_file_ratio_30d > 0 OR bus_factor > 0 OR code_ownership_gini > 0) AS c_any, argMax(toUInt8(commits_count > 0 OR total_loc_touched > 0 OR avg_commit_size_loc > 0 OR large_commit_ratio > 0 OR rework_churn_ratio_30d > 0 OR single_owner_file_ratio_30d > 0 OR bus_factor > 0 OR code_ownership_gini > 0), computed_at) AS c_new
        FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY repo_id, day) AS k
  UNION ALL
  SELECT 'user_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT repo_id, author_email, day, count() AS n, countIf(commits_count > 0 OR loc_added > 0 OR loc_deleted > 0 OR files_changed > 0 OR large_commits_count > 0 OR avg_commit_size_loc > 0 OR loc_touched > 0) AS c_any, argMax(toUInt8(commits_count > 0 OR loc_added > 0 OR loc_deleted > 0 OR files_changed > 0 OR large_commits_count > 0 OR avg_commit_size_loc > 0 OR loc_touched > 0), computed_at) AS c_new
        FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY repo_id, author_email, day) AS k
  UNION ALL
  SELECT 'commit_metrics' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT repo_id, day, author_email, commit_hash, count() AS n, countIf(1 = 1) AS c_any, argMax(toUInt8(1 = 1), computed_at) AS c_new
        FROM commit_metrics WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY repo_id, day, author_email, commit_hash) AS k
  UNION ALL
  SELECT 'team_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT team_id, repo_id, day, count() AS n, countIf(commits_count > 0 OR after_hours_commits_count > 0 OR weekend_commits_count > 0) AS c_any, argMax(toUInt8(commits_count > 0 OR after_hours_commits_count > 0 OR weekend_commits_count > 0), computed_at) AS c_new
        FROM team_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY team_id, repo_id, day) AS k
  UNION ALL
  SELECT 'file_metrics_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT repo_id, day, path, count() AS n, countIf(churn > 0 OR commits_count > 0) AS c_any, argMax(toUInt8(churn > 0 OR commits_count > 0), computed_at) AS c_new
        FROM file_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY repo_id, day, path) AS k
  UNION ALL
  SELECT 'file_hotspot_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT repo_id, day, file_path, count() AS n, countIf(churn_commits_30d > 0 OR churn_loc_30d > 0) AS c_any, argMax(toUInt8(churn_commits_30d > 0 OR churn_loc_30d > 0), computed_at) AS c_new
        FROM file_hotspot_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY repo_id, day, file_path) AS k
  UNION ALL
  SELECT 'ic_landscape_rolling_30d' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT repo_id, team_id, map_name, as_of_day, identity_id, count() AS n, countIf(churn_loc_30d > 0) AS c_any, argMax(toUInt8(churn_loc_30d > 0), computed_at) AS c_new
        FROM ic_landscape_rolling_30d WHERE startsWith(org_id, 'f0f0f0f0') AND as_of_day >= toDate('2025-07-26') AND as_of_day <= toDate('2025-10-31')
        GROUP BY repo_id, team_id, map_name, as_of_day, identity_id) AS k
  UNION ALL
  SELECT 'team_cognitive_load_daily' AS table_name, sum(k.n) AS rows_in_range, count() AS keys_in_range,
    sum(k.c_any) AS commit_rows_any_version, countIf(k.c_new = 1) AS commit_keys_newest
  FROM (SELECT team_id, day, count() AS n, countIf(ifNull(after_hours_commit_ratio, 0) > 0 OR ifNull(weekend_commit_ratio, 0) > 0) AS c_any, argMax(toUInt8(ifNull(after_hours_commit_ratio, 0) > 0 OR ifNull(weekend_commit_ratio, 0) > 0), computed_at) AS c_new
        FROM team_cognitive_load_daily WHERE startsWith(org_id, 'f0f0f0f0') AND day >= toDate('2025-07-26') AND day <= toDate('2025-10-31')
        GROUP BY team_id, day) AS k
) AS o
WHERE cost_gate = 0
ORDER BY o.table_name;
`

const qz3Form = `-- FORM-QZ3-13902-1: Q-z3 of CARD-13902-recompute-10 (z): per repo, the first raw commit day vs the first LIVE stored
--   repo_metrics_daily day with commits_count > 0 (all days). ClickHouse, SELECT only, ONE statement. Lane l13902 (lead
--   fcdev), 2026-11-24. Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-2.md. acp FORM read: OPEN.
-- Rule: commits_count of a stored repo-day = the raw commits of that day (repouser/compute.go:614, from git_commits FINAL
--   committer_when [D, D+1), repouser/clickhouse.go:87-92). A stored commit day earlier than the first raw commit of the
--   repo = raw commits that existed at the compute and are gone now.
-- WANT: repos_with_commit_days > 0 (control); repos_stored_before_raw = 0; repos_commit_days_no_raw = 0. Either > 0 = a
--   deletion or a lost raw write: STOP, pm + acp read. INFO first_stored_commit_day, first_raw_commit_day (org-wide).
-- Cost gate: k 1 each, f 2 (FINAL), s 0: read_rows_cap = 2 x ceil(1.25 x (sum(marks) x 8192 + P)) + (2P + 1).
WITH (SELECT 2 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commits', 'repo_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT count() AS repos_with_commit_days,
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

const qz1Form = `-- FORM-QZ1-13902-1: Q-z1 of CARD-13902-recompute-10 (z): a TTL set on prod by hand (not by a migration) on a raw table the
--   recompute reads. ClickHouse, SELECT only, ONE statement, metadata only (system.tables). Lane l13902 (lead fcdev),
--   2026-11-24. Prod: NOT EXECUTED. CI exec: EXEC-PROOF-FLOOR-13902-2.md. acp FORM read: OPEN.
-- Code side (card (z)): no TTL on these tables at the pin's chain (baseline/head.json :241 :346 :351 :361 :366 at
--   a7febdf50002, sql/145..170 no TTL / MODIFY TTL).
-- WANT: 11 rows (one per name; a missing row = a wrong name or a missing table: STOP); has_ttl = 0 in every row. has_ttl 1 =
--   the floor moves daily and stored days are orphaned every day: STOP, tell pm at once (pm binding rule 1).
-- Cost gate: throwIf over the system.tables row count of this database (metadata only); no data table is read.
WITH (SELECT throwIf((SELECT count() FROM system.tables WHERE database = currentDatabase()) > 100000, 'STOP-COST-GATE')) AS cost_gate
SELECT name AS table_name, engine AS engine, toUInt8(positionCaseInsensitive(create_table_query, ' TTL ') > 0) AS has_ttl,
  partition_key AS partition_key, sorting_key AS sorting_key
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('ai_attribution', 'ci_pipeline_runs', 'coverage_snapshots', 'deployments', 'git_commit_stats', 'git_commits',
               'git_pull_request_reviews', 'git_pull_requests', 'operational_incidents', 'test_suite_results', 'work_items')
  AND cost_gate = 0
ORDER BY name;
`

const (
	fOrg = "f0f0f0f0-0000-4000-8000-000000000000"
	bOrg = "b0000000-0000-4000-8000-000000000000"
	eOrg = "e0e0e0e0-0000-4000-8000-000000000000"
	r1   = "00000000-0000-4000-8000-000000000001"
	r2   = "00000000-0000-4000-8000-000000000002"
	r3   = "00000000-0000-4000-8000-000000000003"
)

// PASS fixture. f0f0f0f0 = the card's org (synthetic); b0000000 = another org (its rows must never show in the f0 output);
// repo r1 only. Chunk C1 = 2026-10-23..2026-11-22.
const floor2Fixture = `
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h1', 'a', 'a@x', '2026-01-10 10:00:00', '2026-01-10 11:00:00'),
('` + fOrg + `', '` + r1 + `', 'h2', 'a', 'a@x', '2026-10-25 10:00:00', '2026-10-25 11:00:00'),
('` + bOrg + `', '` + r1 + `', 'hb', 'b', 'b@x', '2025-01-01 10:00:00', '2025-01-01 11:00:00');
INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 1, '2025-08-01 09:00:00', '2025-08-01 10:00:00'),
('` + bOrg + `', '` + r1 + `', 9, '2024-01-01 09:00:00', '2024-01-01 10:00:00');
INSERT INTO git_pull_request_reviews (org_id, repo_id, number, review_id, reviewer, state, submitted_at, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 1, 'rv1', 'x', 'APPROVED', '2025-08-02 09:00:00', '2025-08-02 10:00:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-23', 0, '2026-10-24 01:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2026-10-25', 2, '2026-10-26 01:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2026-11-01', 0, '2026-11-02 01:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2025-08-15', 0, '2025-08-16 01:00:00', 0),
('` + bOrg + `', '` + r1 + `', '2026-10-24', 3, '2026-10-25 01:00:00', 0),
('` + bOrg + `', '` + r1 + `', '2025-08-20', 5, '2025-08-21 01:00:00', 0);
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-23', 0, '2026-10-30 01:00:00', 1);
INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, commits_count, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', 'a@x', '2026-10-25', 1, '2026-10-26 01:00:00', 0);
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, computed_at) VALUES
('` + fOrg + `', '` + r1 + `', 'h2', '2026-10-25', 'a@x', 5, '2026-10-26 01:00:00');
INSERT INTO team_metrics_daily (org_id, team_id, repo_id, day, commits_count, computed_at) VALUES
('` + fOrg + `', 't1', '` + r1 + `', '2026-10-25', 1, '2026-10-26 01:00:00'),
('` + fOrg + `', 't1', '', '2026-10-24', 1, '2026-10-25 01:00:00');
INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket, computed_at) VALUES
('` + fOrg + `', 't1', '` + r1 + `', 'feature', '2026-10-26', 'human', '2026-10-27 01:00:00');
INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, finished_at, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'run1', 'github_actions', 'success', '2026-02-01 10:00:00', '2026-02-01 10:30:00', '2026-02-01 11:00:00'),
('` + fOrg + `', '` + r1 + `', 'run0', 'github_actions', 'queued', '2025-12-01 10:00:00', '2025-12-01 10:30:00', '2025-12-01 11:00:00'),
('` + bOrg + `', '` + r1 + `', 'runB', 'github_actions', 'success', '2024-06-01 10:00:00', '2024-06-01 10:30:00', '2024-06-01 11:00:00');
INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, started_at, finished_at, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'run1', 's1', 'unit', '2026-02-02 10:00:00', '2026-02-02 10:10:00', '2026-02-02 11:00:00');
INSERT INTO coverage_snapshots (org_id, repo_id, run_id, snapshot_id, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'run1', 'c1', '2026-02-01 11:00:00');
INSERT INTO deployments (org_id, repo_id, deployment_id, deployed_at, finished_at, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'd1', '2026-03-01 10:00:00', '2026-03-01 10:00:00', '2026-03-01 11:00:00'),
('` + fOrg + `', '` + r1 + `', 'd0', NULL, '2026-02-15 10:00:00', '2026-02-15 11:00:00');
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-25', '2026-10-26 01:00:00', 0),
('` + bOrg + `', '` + r1 + `', '2026-10-24', '2026-10-25 01:00:00', 0);
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-25', '2026-10-26 01:00:00', 0);
INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-26', '2026-10-27 01:00:00', 0);
INSERT INTO testops_coverage_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-26', '2026-10-27 01:00:00', 0);
INSERT INTO testops_release_confidence (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-27', '2026-10-28 01:00:00', 0);
INSERT INTO testops_quality_drag (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-27', '2026-10-28 01:00:00', 0);
INSERT INTO testops_pipeline_stability (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-27', '2026-10-28 01:00:00', 0);
INSERT INTO deploy_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-28', '2026-10-29 01:00:00', 0);
INSERT INTO repo_change_failure_daily (org_id, repo_id, day, computed_at) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-28', '2026-10-29 01:00:00');
INSERT INTO operational_incidents (org_id, provider, id, started_at, is_deleted, observed_at, ordering_contract) VALUES
('` + fOrg + `', 'pagerduty', 'inc1', '2026-01-05 10:00:00', 0, '2026-01-05 10:00:00', 2),
('` + fOrg + `', 'pagerduty', 'inc0', '2025-12-20 10:00:00', 1, '2025-12-20 10:00:00', 2);
INSERT INTO work_items (org_id, repo_id, work_item_id, provider, status, created_at, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'wi1', 'jira', 'todo', '2026-01-03 10:00:00', '2026-01-03 11:00:00'),
('` + bOrg + `', '` + r1 + `', 'wiB', 'jira', 'todo', '2025-05-05 10:00:00', '2025-05-05 11:00:00');
INSERT INTO repo_complexity_daily (org_id, repo_id, day, computed_at) VALUES
('` + fOrg + `', '` + r1 + `', '2026-01-04', '2026-01-05 01:00:00');
INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, kind, source, observed_at, ingested_at, computed_at, superseded_by) VALUES
(generateUUIDv4(), '` + fOrg + `', 'github', 'pr', 'p1', 'k', 's', '2026-01-06 10:00:00', '2026-01-06 10:00:00', '2026-01-06 10:00:00', NULL),
(generateUUIDv4(), '` + fOrg + `', 'github', 'pr', 'p0', 'k', 's', '2025-11-11 10:00:00', '2025-11-11 10:00:00', '2025-11-11 10:00:00', generateUUIDv4());
INSERT INTO incident_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-29', '2026-10-30 01:00:00', 0);
INSERT INTO work_item_metrics_daily (org_id, provider, work_scope_id, team_id, row_view, day, computed_at) VALUES
('` + fOrg + `', 'jira', 'P', 't1', 'team', '2026-10-29', '2026-10-30 01:00:00'),
('` + bOrg + `', 'jira', 'P', 't1', 'team', '2026-10-24', '2026-10-25 01:00:00');
INSERT INTO work_item_user_metrics_daily (org_id, provider, work_scope_id, user_identity, row_view, day, computed_at, retracted) VALUES
('` + fOrg + `', 'jira', 'P', 'u1', 'team', '2026-10-29', '2026-10-30 01:00:00', 0);
INSERT INTO estimate_coverage_metrics_daily (org_id, provider, work_scope_id, day, computed_at) VALUES
('` + fOrg + `', 'jira', 'P', '2026-10-29', '2026-10-30 01:00:00');
INSERT INTO work_item_state_durations_daily (org_id, provider, work_scope_id, team_id, status, row_view, day, computed_at) VALUES
('` + fOrg + `', 'jira', 'P', 't1', 'todo', 'team', '2026-10-29', '2026-10-30 01:00:00');
INSERT INTO issue_type_metrics_daily (org_id, provider, team_id, issue_type_norm, day, computed_at) VALUES
('` + fOrg + `', 'jira', 't1', 'bug', '2026-10-29', '2026-10-30 01:00:00');
INSERT INTO work_item_completion_daily (org_id, team_id, day, computed_at) VALUES
('` + fOrg + `', 't1', '2026-10-29', '2026-10-30 01:00:00');
INSERT INTO team_complexity_daily (org_id, team_id, day, computed_at) VALUES
('` + fOrg + `', 't1', '2026-10-29', '2026-10-30 01:00:00');
INSERT INTO team_cognitive_load_daily (org_id, team_id, day, computed_at) VALUES
('` + fOrg + `', 't1', '2026-10-25', '2026-10-26 01:00:00');
INSERT INTO compounding_risk_daily (org_id, scope, scope_id, day, computed_at) VALUES
('` + fOrg + `', 'team', 't1', '2026-10-26', '2026-10-27 01:00:00');
INSERT INTO ai_governance_coverage_daily (org_id, day, computed_at) VALUES
('` + fOrg + `', '2026-10-29', '2026-10-30 01:00:00');
`

// Variant rows, added after the PASS path and the mutants (each must STOP the floor rule).
// BELOW: repo r2 has a live repo-day 2026-10-25 and its first raw commit is 2026-10-26 (no PR, no review).
const floor2VariantBelow = `
INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES ('` + fOrg + `', '` + r2 + `', '2026-10-25', '2026-10-26 01:00:00', 0);
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES ('` + fOrg + `', '` + r2 + `', 'h9', 'a', 'a@x', '2026-10-26 10:00:00', '2026-10-26 11:00:00');
`

// NORAW: repo r3 has a live cicd day 2026-11-01 and no ci_pipeline_runs row at all.
const floor2VariantNoRaw = `
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, computed_at, retracted) VALUES ('` + fOrg + `', '` + r3 + `', '2026-11-01', '2026-11-02 01:00:00', 0);
`

// LOSS (Q-z4 can fail): a stored team row on 2025-09-01 with a commit-derived value and no raw commit behind it.
const qz4VariantLoss = `
INSERT INTO team_metrics_daily (org_id, team_id, repo_id, day, commits_count, computed_at) VALUES ('` + fOrg + `', 't1', '` + r1 + `', '2025-09-01', 1, '2025-09-02 01:00:00');
`

type floor2Statement struct{ label, text string }

func floor2Split(body string) []floor2Statement {
	var out []floor2Statement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		if !strings.HasSuffix(strings.TrimSpace(chmigrate.StripLineComments(line)), ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		for _, l := range chunk {
			if t := strings.TrimSpace(l); strings.HasPrefix(t, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(t, "-- "))[0]
				break
			}
		}
		out = append(out, floor2Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func floor2Cell(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

type floor2Parts struct{ rows, marks, parts uint64 }

func floor2LiveParts(t *testing.T, ctx context.Context, conn driver.Conn) map[string]floor2Parts {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT table, sum(rows), sum(marks), count() FROM system.parts
		WHERE active AND database = currentDatabase() GROUP BY table`)
	if err != nil {
		t.Fatalf("system.parts: %v", err)
	}
	defer rows.Close()
	out := map[string]floor2Parts{}
	for rows.Next() {
		var table string
		var p floor2Parts
		if err := rows.Scan(&table, &p.rows, &p.marks, &p.parts); err != nil {
			t.Fatalf("system.parts scan: %v", err)
		}
		out[table] = p
	}
	return out
}

func floor2QueryLog(t *testing.T, ctx context.Context, conn driver.Conn, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
		WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("query log %s: %v", comment, err)
	}
	defer rows.Close()
	var types []string
	var read uint64
	for rows.Next() {
		var typ string
		var r uint64
		if err := rows.Scan(&typ, &r); err != nil {
			t.Fatalf("query log scan: %v", err)
		}
		types = append(types, typ)
		read += r
	}
	return types, read
}

// floor2Spec: the tables a statement reads (table -> k scans), f (2 = FINAL), and the check row format.
type floor2Spec struct {
	reads map[string]uint64
	f     uint64
}

func (s floor2Spec) gateText() string {
	names := make([]string, 0, len(s.reads))
	var arms []string
	for n := range s.reads {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if s.reads[n] > 1 {
			arms = append(arms, fmt.Sprintf("table = '%s', %d", n, s.reads[n]))
		}
	}
	weight := ""
	if len(arms) > 0 {
		weight = " * multiIf(" + strings.Join(arms, ", ") + ", 1)"
	}
	return fmt.Sprintf("WITH (SELECT %d * intDiv(5 * (sum(marks%s) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()\n"+
		"      AND table IN ('%s')) AS read_rows_cap,\n"+
		"  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate\n", s.f, weight, strings.Join(names, "', '"))
}

func (s floor2Spec) derive(live map[string]floor2Parts) (capRows, p uint64) {
	var w uint64
	for table, k := range s.reads {
		w += k * live[table].marks
		p += live[table].parts
	}
	return s.f*((5*(w*8192+p)+3)/4) + (2*p + 1), p
}

// floor2Verdict is the chunk-start.sh floor rule (l13902-lib.sh floor_check) on the rows of one chunk.
func floor2Verdict(rows []map[string]string, from string) []string {
	const runFirstDay = "2026-04-17"
	var reasons []string
	family := 0
	for _, r := range rows {
		if r["family"] == "_stmt" {
			continue
		}
		family++
		if r["raw_first_day"] == "none" {
			reasons = append(reasons, fmt.Sprintf("%s %s scope %s: no raw row", r["family"], r["raw_table"], r["scope_id"]))
			continue
		}
		floor := r["raw_first_day"]
		if runFirstDay > floor {
			floor = runFirstDay
		}
		if r["stored_first_day"] < floor {
			reasons = append(reasons, fmt.Sprintf("%s %s scope %s: stored day %s below the floor %s", r["family"], r["raw_table"], r["scope_id"], r["stored_first_day"], floor))
		}
	}
	if family == 0 {
		reasons = append(reasons, "0 family rows (NOT EXERCISED)")
	}
	sort.Strings(reasons)
	return reasons
}

func TestFloor13902FormsExecuteMutantsAndEveryStopFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	for _, f := range []struct{ name, text, sha string }{{"FLOOR-13902-2", floor2Form, floor2SHA256}, {"FORM-QZ4-13902-1", qz4Form, qz4SHA256},
		{"FORM-QZ3-13902-1", qz3Form, qz3SHA256}, {"FORM-QZ1-13902-1", qz1Form, qz1SHA256}} {
		sum := sha256.Sum256([]byte(f.text))
		if got := hex.EncodeToString(sum[:]); got != f.sha {
			t.Fatalf("%s (prefix swapped) sha256 = %s, want %s", f.name, got, f.sha)
		}
	}
	for _, c := range []struct {
		name, text string
		n          int
	}{{"FLOOR-13902-2", floor2Form, 41}, {"FORM-QZ4-13902-1", qz4Form, 8}, {"FORM-QZ3-13902-1", qz3Form, 2}, {"FORM-QZ1-13902-1", qz1Form, 0}} {
		if got := strings.Count(chmigrate.StripLineComments(c.text), "'f0f0f0f0')"); got != c.n {
			t.Fatalf("%s: %d org filters, want %d", c.name, got, c.n)
		}
	}

	specs := map[string]floor2Spec{
		"stmt1": {f: 2, reads: map[string]uint64{"ai_impact_metrics_daily": 1, "commit_metrics": 1, "git_commits": 1, "git_pull_request_reviews": 1,
			"git_pull_requests": 1, "repo_metrics_daily": 1, "team_metrics_daily": 1, "user_metrics_daily": 1}},
		"stmt2": {f: 2, reads: map[string]uint64{"ci_pipeline_runs": 3, "cicd_metrics_daily": 1, "coverage_snapshots": 1, "deploy_metrics_daily": 1,
			"deployments": 2, "repo_change_failure_daily": 1, "test_suite_results": 1, "testops_coverage_metrics_daily": 1,
			"testops_pipeline_metrics_daily": 1, "testops_pipeline_stability": 1, "testops_quality_drag": 1, "testops_release_confidence": 1,
			"testops_test_metrics_daily": 1}},
		"stmt3": {f: 2, reads: map[string]uint64{"ai_attribution": 1, "ai_governance_coverage_daily": 1, "compounding_risk_daily": 1,
			"estimate_coverage_metrics_daily": 1, "incident_metrics_daily": 1, "issue_type_metrics_daily": 1, "operational_incidents": 1,
			"repo_complexity_daily": 1, "repo_metrics_daily": 1, "team_cognitive_load_daily": 1, "team_complexity_daily": 1, "user_metrics_daily": 1,
			"work_item_completion_daily": 1, "work_item_metrics_daily": 1, "work_item_state_durations_daily": 1,
			"work_item_user_metrics_daily": 1, "work_items": 1}},
		"qz4": {f: 1, reads: map[string]uint64{"commit_metrics": 1, "file_hotspot_daily": 1, "file_metrics_daily": 1, "ic_landscape_rolling_30d": 1,
			"repo_metrics_daily": 1, "team_cognitive_load_daily": 1, "team_metrics_daily": 1, "user_metrics_daily": 1}},
		"qz3": {f: 2, reads: map[string]uint64{"git_commits": 1, "repo_metrics_daily": 1}},
	}
	statements := floor2Split(floor2Form)
	if len(statements) != 3 || statements[0].label != "stmt1" || statements[1].label != "stmt2" || statements[2].label != "stmt3" {
		t.Fatalf("FLOOR-13902-2 statements = %+v", len(statements))
	}
	forms := map[string]string{"stmt1": statements[0].text, "stmt2": statements[1].text, "stmt3": statements[2].text,
		"qz4": strings.TrimSuffix(strings.TrimRight(qz4Form, " \n"), ";"), "qz3": strings.TrimSuffix(strings.TrimRight(qz3Form, " \n"), ";")}
	for label, text := range forms {
		body := chmigrate.StripLineComments(text)
		if n := strings.Count(text, specs[label].gateText()); n != 1 {
			t.Fatalf("%s: gate text found %d times, want 1:\n%s", label, n, specs[label].gateText())
		}
		for _, one := range []string{"'STOP-COST-GATE'", ">= 20000000", "system.parts", "WHERE cost_gate = 0"} {
			if n := strings.Count(body, one); n != 1 {
				t.Fatalf("%s: %q found %d times, want 1", label, one, n)
			}
		}
		if hasFinal := strings.Contains(body, " FINAL "); hasFinal != (specs[label].f == 2) {
			t.Fatalf("%s: FINAL = %v but f = %d", label, hasFinal, specs[label].f)
		}
	}
	fill := func(text, from, to string) string {
		out := strings.ReplaceAll(strings.ReplaceAll(text, "{{FROM}}", from), "{{TO}}", to)
		if strings.Contains(out, "{{") {
			t.Fatalf("a placeholder is left after the fill")
		}
		return out
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
	all := map[string]bool{}
	for _, s := range specs {
		for table := range s.reads {
			all[table] = true
		}
	}
	for table := range all {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges %s: %v", table, err)
		}
	}
	for _, statement := range chmigrate.SplitStatements(floor2Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	type logged struct {
		phase, label, comment string
		capRows, p            uint64
		fire                  bool
	}
	var checks []logged
	// run executes one statement; it returns the rows (column -> value) and the error.
	run := func(phase, label, text string) ([]map[string]string, []string, error) {
		comment := fmt.Sprintf("floor13902-%s-%s-%d", phase, label, time.Now().UnixNano())
		rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), text)
		var got []map[string]string
		var cols []string
		if err == nil {
			cols = rows.Columns()
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
					row[name] = floor2Cell(cells[i])
					parts = append(parts, name+"="+row[name])
				}
				t.Logf("%s %s ROW %s", phase, label, strings.Join(parts, " "))
				got = append(got, row)
			}
			err = rows.Err()
			_ = rows.Close()
		}
		if _, ok := specs[label]; ok {
			capRows, p := specs[label].derive(floor2LiveParts(t, ctx, conn))
			checks = append(checks, logged{phase, label, comment, capRows, p, err != nil})
			for _, r := range got {
				if c, ok := r["read_rows_cap"]; ok && c != fmt.Sprint(capRows) {
					t.Errorf("%s %s: read_rows_cap %s, want %d (Go formula on live system.parts)", phase, label, c, capRows)
				}
			}
		}
		return got, cols, err
	}
	// rowsOf renders floor rows "family raw_table scope raw_first stored_first days" (stmt + cap checked elsewhere).
	rowsOf := func(got []map[string]string) []string {
		var out []string
		for _, r := range got {
			out = append(out, strings.Join([]string{r["family"], r["raw_table"], r["scope_id"], r["raw_first_day"], r["stored_first_day"], r["stored_days"]}, " "))
		}
		sort.Strings(out)
		return out
	}
	want := func(phase, label string, got, w []string) {
		sort.Strings(w)
		if strings.Join(got, "\n") != strings.Join(w, "\n") {
			t.Errorf("%s %s rows:\n got  %s\n want %s", phase, label, strings.Join(got, "\n      "), strings.Join(w, "\n      "))
		}
	}
	C1F, C1T := "2026-10-23", "2026-11-22"
	ctl := func(from string) string { return "_stmt _cap org none " + from + " 0" }
	passWant := map[string][]string{
		"stmt1": {ctl(C1F),
			"ai_impact_metrics_daily git_pull_request_reviews " + r1 + " 2025-08-02 2026-10-26 1",
			"ai_impact_metrics_daily git_pull_requests " + r1 + " 2025-08-01 2026-10-26 1",
			"commit_metrics git_commits " + r1 + " 2026-01-10 2026-10-25 1",
			"repo_metrics_daily git_commits " + r1 + " 2026-01-10 2026-10-25 2",
			"repo_metrics_daily git_pull_request_reviews " + r1 + " 2025-08-02 2026-10-25 2",
			"repo_metrics_daily git_pull_requests " + r1 + " 2025-08-01 2026-10-25 2",
			"team_metrics_daily git_commits " + r1 + " 2026-01-10 2026-10-25 1",
			"user_metrics_daily git_commits " + r1 + " 2026-01-10 2026-10-25 1",
			"user_metrics_daily git_pull_request_reviews " + r1 + " 2025-08-02 2026-10-25 1",
			"user_metrics_daily git_pull_requests " + r1 + " 2025-08-01 2026-10-25 1"},
		"stmt2": {ctl(C1F),
			"cicd_metrics_daily ci_pipeline_runs.finished " + r1 + " 2026-02-01 2026-10-25 1",
			"deploy_metrics_daily deployments.deployed " + r1 + " 2026-03-01 2026-10-28 1",
			"repo_change_failure_daily deployments.any " + r1 + " 2026-02-15 2026-10-28 1",
			"testops_coverage_metrics_daily ci_pipeline_runs.started " + r1 + " 2025-12-01 2026-10-26 1",
			"testops_coverage_metrics_daily coverage_snapshots " + r1 + " 2026-02-01 2026-10-26 1",
			"testops_pipeline_metrics_daily ci_pipeline_runs.started " + r1 + " 2025-12-01 2026-10-25 1",
			"testops_pipeline_stability ci_pipeline_runs.started " + r1 + " 2025-12-01 2026-10-27 1",
			"testops_quality_drag ci_pipeline_runs.started " + r1 + " 2025-12-01 2026-10-27 1",
			"testops_release_confidence ci_pipeline_runs.started " + r1 + " 2025-12-01 2026-10-27 1",
			"testops_test_metrics_daily test_suite_results " + r1 + " 2026-02-02 2026-10-26 1"},
		"stmt3": {ctl(C1F),
			"ai_governance_coverage_daily ai_attribution org 2026-01-06 2026-10-29 1",
			"compounding_risk_daily.team repo_metrics_daily.live org 2025-08-15 2026-10-26 1",
			"estimate_coverage_metrics_daily work_items org 2026-01-03 2026-10-29 1",
			"incident_metrics_daily operational_incidents org 2026-01-05 2026-10-29 1",
			"issue_type_metrics_daily work_items org 2026-01-03 2026-10-29 1",
			"team_cognitive_load_daily user_metrics_daily.live org 2026-10-25 2026-10-25 1",
			"team_complexity_daily repo_complexity_daily org 2026-01-04 2026-10-29 1",
			"work_item_completion_daily work_items org 2026-01-03 2026-10-29 1",
			"work_item_metrics_daily work_items org 2026-01-03 2026-10-29 1",
			"work_item_state_durations_daily work_items org 2026-01-03 2026-10-29 1",
			"work_item_user_metrics_daily work_items org 2026-01-03 2026-10-29 1"},
	}
	// PASS path: every statement finishes, the exact rows, the floor rule PASSes (the AT-floor row of team_cognitive_load
	// included), read_rows_cap = the Go formula.
	var passRows []map[string]string
	for _, label := range []string{"stmt1", "stmt2", "stmt3"} {
		got, _, err := run("PASS", label, fill(forms[label], C1F, C1T))
		if err != nil {
			t.Fatalf("PASS %s: %v", label, err)
		}
		want("PASS", label, rowsOf(got), append([]string(nil), passWant[label]...))
		passRows = append(passRows, got...)
	}
	if v := floor2Verdict(passRows, C1F); len(v) != 0 {
		t.Errorf("PASS fixture floor verdict = %v, want PASS", v)
	} else {
		t.Logf("VERDICT PASS C1: 0 reasons (team_cognitive_load_daily stored day 2026-10-25 = its floor 2026-10-25: runs)")
	}
	qz4Rows := func(phase string) []string {
		got, _, err := run(phase, "qz4", forms["qz4"])
		if err != nil {
			t.Fatalf("%s qz4: %v", phase, err)
		}
		var out []string
		for _, r := range got {
			out = append(out, strings.Join([]string{r["table_name"], r["rows_in_range"], r["keys_in_range"], r["commit_rows_any_version"], r["commit_keys_newest"]}, " "))
		}
		sort.Strings(out)
		return out
	}
	qz4Base := []string{"commit_metrics 0 0 0 0", "file_hotspot_daily 0 0 0 0", "file_metrics_daily 0 0 0 0", "ic_landscape_rolling_30d 0 0 0 0",
		"repo_metrics_daily 1 1 0 0", "team_cognitive_load_daily 0 0 0 0", "team_metrics_daily 0 0 0 0", "user_metrics_daily 0 0 0 0"}
	want("PASS", "qz4", qz4Rows("PASS"), append([]string(nil), qz4Base...))
	qz3Line := func(phase, text string) string {
		got, _, err := run(phase, "qz3", text)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s qz3: %v (%d rows)", phase, err, len(got))
		}
		r := got[0]
		return strings.Join([]string{r["repos_with_commit_days"], r["repos_stored_before_raw"], r["repos_commit_days_no_raw"], r["first_stored_commit_day"], r["first_raw_commit_day"]}, " ")
	}
	if got := qz3Line("PASS", forms["qz3"]); got != "1 0 0 2026-10-25 2026-01-10" {
		t.Errorf("PASS qz3 = %s, want 1 0 0 2026-10-25 2026-01-10", got)
	}
	qz1Text := strings.TrimSuffix(strings.TrimRight(qz1Form, " \n"), ";")
	qz1Lines := func(phase, text string) []string {
		got, _, err := run(phase, "qz1", text)
		if err != nil {
			t.Fatalf("%s qz1: %v", phase, err)
		}
		var out []string
		for _, r := range got {
			out = append(out, r["table_name"]+" ttl="+r["has_ttl"])
		}
		sort.Strings(out)
		return out
	}
	qz1Want := []string{}
	for _, n := range []string{"ai_attribution", "ci_pipeline_runs", "coverage_snapshots", "deployments", "git_commit_stats", "git_commits",
		"git_pull_request_reviews", "git_pull_requests", "operational_incidents", "test_suite_results", "work_items"} {
		qz1Want = append(qz1Want, n+" ttl=0")
	}
	want("PASS", "qz1", qz1Lines("PASS", qz1Text), qz1Want)

	// Org + rule mutants (acp standing rule, every statement). One change each; each must CHANGE the output (exact WANT).
	mutate := func(text, from, to string, places int) string {
		if n := strings.Count(text, from); n != places {
			t.Fatalf("mutant pattern %q found %d times, want %d", from, n, places)
		}
		return strings.ReplaceAll(text, from, to)
	}
	type mutant struct {
		label, kind, from, to string
		places                int
		want                  []string
	}
	floorMutants := []mutant{
		{"stmt1", "ORG", "'f0f0f0f0'", "'b0000000'", 8, []string{ctl(C1F),
			"repo_metrics_daily git_commits " + r1 + " 2025-01-01 2026-10-24 1",
			"repo_metrics_daily git_pull_request_reviews " + r1 + " none 2026-10-24 1",
			"repo_metrics_daily git_pull_requests " + r1 + " 2024-01-01 2026-10-24 1"}},
		{"stmt1", "RULE", "GROUP BY repo_id, day) AS k\n    WHERE k.rt = 0 GROUP BY k.repo_id", "GROUP BY repo_id, day) AS k\n    GROUP BY k.repo_id", 1,
			[]string{ctl(C1F),
				"ai_impact_metrics_daily git_pull_request_reviews " + r1 + " 2025-08-02 2026-10-26 1",
				"ai_impact_metrics_daily git_pull_requests " + r1 + " 2025-08-01 2026-10-26 1",
				"commit_metrics git_commits " + r1 + " 2026-01-10 2026-10-25 1",
				"repo_metrics_daily git_commits " + r1 + " 2026-01-10 2026-10-23 3",
				"repo_metrics_daily git_pull_request_reviews " + r1 + " 2025-08-02 2026-10-23 3",
				"repo_metrics_daily git_pull_requests " + r1 + " 2025-08-01 2026-10-23 3",
				"team_metrics_daily git_commits " + r1 + " 2026-01-10 2026-10-25 1",
				"user_metrics_daily git_commits " + r1 + " 2026-01-10 2026-10-25 1",
				"user_metrics_daily git_pull_request_reviews " + r1 + " 2025-08-02 2026-10-25 1",
				"user_metrics_daily git_pull_requests " + r1 + " 2025-08-01 2026-10-25 1"}},
		{"stmt2", "ORG", "'f0f0f0f0'", "'b0000000'", 16, []string{ctl(C1F),
			"cicd_metrics_daily ci_pipeline_runs.finished " + r1 + " 2024-06-01 2026-10-24 1"}},
		{"stmt2", "RULE", " AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued')", "", 1,
			replaceRow(passWant["stmt2"], "cicd_metrics_daily ci_pipeline_runs.finished "+r1+" 2026-02-01 2026-10-25 1",
				"cicd_metrics_daily ci_pipeline_runs.finished "+r1+" 2025-12-01 2026-10-25 1")},
		{"stmt3", "ORG", "'f0f0f0f0'", "'b0000000'", 17, []string{ctl(C1F),
			"work_item_metrics_daily work_items org 2025-05-05 2026-10-24 1"}},
		{"stmt3", "RULE", " AND is_deleted = 0", "", 1,
			replaceRow(passWant["stmt3"], "incident_metrics_daily operational_incidents org 2026-01-05 2026-10-29 1",
				"incident_metrics_daily operational_incidents org 2025-12-20 2026-10-29 1")},
	}
	for _, m := range floorMutants {
		got, _, err := run("MUTANT-"+m.kind, m.label, fill(mutate(forms[m.label], m.from, m.to, m.places), C1F, C1T))
		if err != nil {
			t.Fatalf("MUTANT-%s %s: SQL error (a mutant must change the output, not fire): %v", m.kind, m.label, err)
		}
		g := rowsOf(got)
		if strings.Join(g, "\n") == strings.Join(passWant[m.label], "\n") {
			t.Errorf("MUTANT-%s %s did not change the output", m.kind, m.label)
		}
		want("MUTANT-"+m.kind, m.label, g, append([]string(nil), m.want...))
		t.Logf("MUTANT-%s %s CHANGED (%d places %q -> %q)", m.kind, m.label, m.places, m.from, m.to)
	}
	// Q-z4 / Q-z3 / Q-z1 mutants.
	got4, _, err := run("MUTANT-ORG", "qz4", mutate(forms["qz4"], "'f0f0f0f0'", "'b0000000'", 8))
	if err != nil {
		t.Fatalf("MUTANT-ORG qz4: %v", err)
	}
	var g4 []string
	for _, r := range got4 {
		g4 = append(g4, strings.Join([]string{r["table_name"], r["rows_in_range"], r["keys_in_range"], r["commit_rows_any_version"], r["commit_keys_newest"]}, " "))
	}
	sort.Strings(g4)
	want("MUTANT-ORG", "qz4", g4, []string{"commit_metrics 0 0 0 0", "file_hotspot_daily 0 0 0 0", "file_metrics_daily 0 0 0 0", "ic_landscape_rolling_30d 0 0 0 0",
		"repo_metrics_daily 1 1 1 1", "team_cognitive_load_daily 0 0 0 0", "team_metrics_daily 0 0 0 0", "user_metrics_daily 0 0 0 0"})
	if got := qz3Line("MUTANT-ORG", mutate(forms["qz3"], "'f0f0f0f0'", "'b0000000'", 2)); got != "1 0 0 2025-08-20 2025-01-01" {
		t.Errorf("MUTANT-ORG qz3 = %s, want 1 0 0 2025-08-20 2025-01-01", got)
	}
	if got := qz3Line("MUTANT-RULE", mutate(forms["qz3"], " AND k.cc > 0", "", 1)); got != "1 1 0 2025-08-15 2026-01-10" {
		t.Errorf("MUTANT-RULE qz3 = %s, want 1 1 0 2025-08-15 2026-01-10", got)
	}
	want("MUTANT-RULE", "qz1", qz1Lines("MUTANT-RULE", mutate(qz1Text, "'git_commits',", "'git_commitz',", 1)), removeRow(qz1Want, "git_commits ttl=0"))

	// Variants: each STOP of the floor rule, and the Q-z4 loss reading.
	for _, statement := range chmigrate.SplitStatements(floor2VariantBelow + floor2VariantNoRaw + qz4VariantLoss) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("variant fixture: %v\n%s", err, statement)
		}
	}
	var varRows []map[string]string
	for _, label := range []string{"stmt1", "stmt2", "stmt3"} {
		got, _, err := run("VARIANT", label, fill(forms[label], C1F, C1T))
		if err != nil {
			t.Fatalf("VARIANT %s: %v", label, err)
		}
		varRows = append(varRows, got...)
	}
	wantReasons := []string{
		"cicd_metrics_daily ci_pipeline_runs.finished scope " + r3 + ": no raw row",
		"repo_metrics_daily git_commits scope " + r2 + ": stored day 2026-10-25 below the floor 2026-10-26",
		"repo_metrics_daily git_pull_request_reviews scope " + r2 + ": no raw row",
		"repo_metrics_daily git_pull_requests scope " + r2 + ": no raw row",
	}
	if v := floor2Verdict(varRows, C1F); strings.Join(v, "\n") != strings.Join(wantReasons, "\n") {
		t.Errorf("VARIANT verdict:\n got  %s\n want %s", strings.Join(v, "\n      "), strings.Join(wantReasons, "\n      "))
	} else {
		t.Logf("VERDICT STOP C1 (below floor + no raw): %s", strings.Join(v, "; "))
	}
	var zeroRows []map[string]string
	for _, label := range []string{"stmt1", "stmt2", "stmt3"} {
		got, _, err := run("ZERO", label, fill(forms[label], "2026-04-17", "2026-04-19"))
		if err != nil {
			t.Fatalf("ZERO %s: %v", label, err)
		}
		want("ZERO", label, rowsOf(got), []string{ctl("2026-04-17")})
		zeroRows = append(zeroRows, got...)
	}
	if v := floor2Verdict(zeroRows, "2026-04-17"); len(v) != 1 || v[0] != "0 family rows (NOT EXERCISED)" {
		t.Errorf("ZERO verdict = %v, want the 0-rows STOP", v)
	} else {
		t.Logf("VERDICT STOP C8 (0 family rows): %s", v[0])
	}
	want("LOSS", "qz4", qz4Rows("LOSS"), replaceRow(qz4Base, "team_metrics_daily 0 0 0 0", "team_metrics_daily 1 1 1 1"))
	got4r, _, err := run("MUTANT-RULE", "qz4", mutate(forms["qz4"], "commits_count > 0 OR after_hours_commits_count > 0 OR ", "", 2))
	if err != nil {
		t.Fatalf("MUTANT-RULE qz4: %v", err)
	}
	var g4r []string
	for _, r := range got4r {
		g4r = append(g4r, strings.Join([]string{r["table_name"], r["rows_in_range"], r["keys_in_range"], r["commit_rows_any_version"], r["commit_keys_newest"]}, " "))
	}
	sort.Strings(g4r)
	want("MUTANT-RULE", "qz4", g4r, replaceRow(qz4Base, "team_metrics_daily 0 0 0 0", "team_metrics_daily 1 1 0 0"))

	// Cost gate fires by fixture only (no SQL text change): rows of another org (e0e0e0e0) grow the live marks. Each fire row has
	// its own key: optimize_on_insert collapses equal ReplacingMergeTree keys inside one insert block.
	gate := func(phase string, fires map[string]bool) {
		live := floor2LiveParts(t, ctx, conn)
		for _, label := range []string{"stmt1", "stmt2", "stmt3", "qz4", "qz3"} {
			capRows, _ := specs[label].derive(live)
			if (capRows >= 20000000) != fires[label] {
				t.Fatalf("%s %s: derived read_rows_cap %d, the fixture must make fire = %v", phase, label, capRows, fires[label])
			}
			text := forms[label]
			if strings.HasPrefix(label, "stmt") {
				text = fill(text, C1F, C1T)
			}
			_, _, err := run(phase, label, text)
			if fires[label] {
				if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
					t.Errorf("%s %s: err = %v, want code 395 STOP-COST-GATE", phase, label, err)
				} else {
					t.Logf("%s %s FIRED at derived read_rows_cap %d: %s", phase, label, capRows, strings.SplitN(err.Error(), ":", 3)[0])
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
	insert(`INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, started_at, last_synced)
SELECT '` + eOrg + `', toUUID('00000000-0000-4000-8000-000000000009'), concat('fire-', toString(number)), 'github_actions', 'success',
  toDateTime64('2026-01-01 00:00:00', 3, 'UTC'), toDateTime64('2026-01-01 00:00:00', 3, 'UTC') FROM numbers(2800000)`)
	gate("FIRE-A", map[string]bool{"stmt2": true})
	insert(`INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at)
SELECT '` + eOrg + `', generateUUIDv4(number), toDate('2026-01-01') + (number % 300), toDateTime('2026-01-02 00:00:00', 'UTC') FROM numbers(8200000)`)
	gate("FIRE-B", map[string]bool{"stmt1": true, "stmt2": true, "stmt3": true, "qz3": true})
	insert(`INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at)
SELECT '` + eOrg + `', generateUUIDv4(number), toDate('2026-01-01') + (number % 300), toDateTime('2026-01-02 00:00:00', 'UTC') FROM numbers(8200000, 8100000)`)
	gate("FIRE-C", map[string]bool{"stmt1": true, "stmt2": true, "stmt3": true, "qz3": true, "qz4": true})

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	fired := map[string]bool{}
	for _, c := range checks {
		types, read := floor2QueryLog(t, ctx, conn, c.comment)
		if c.fire {
			fired[c.label] = true
			t.Logf("%s %s query_log types=%v read_rows=%d bound 2P+1=%d", c.phase, c.label, types, read, 2*c.p+1)
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > 2*c.p+1 {
				t.Errorf("%s %s: query_log %v read_rows %d, want [ExceptionBeforeStart] and <= %d", c.phase, c.label, types, read, 2*c.p+1)
			}
			continue
		}
		t.Logf("%s %s query_log types=%v read_rows=%d read_rows_cap=%d", c.phase, c.label, types, read, c.capRows)
		if len(types) != 1 || types[0] != "QueryFinish" || read > c.capRows {
			t.Errorf("%s %s: query_log %v read_rows %d, want [QueryFinish] and <= read_rows_cap %d", c.phase, c.label, types, read, c.capRows)
		}
	}
	for _, label := range []string{"stmt1", "stmt2", "stmt3", "qz4", "qz3"} {
		if !fired[label] {
			t.Errorf("%s: the cost gate never fired", label)
		}
	}
}

func replaceRow(rows []string, from, to string) []string {
	out := make([]string, 0, len(rows))
	found := false
	for _, r := range rows {
		if r == from {
			r, found = to, true
		}
		out = append(out, r)
	}
	if !found {
		panic("replaceRow: row not found: " + from)
	}
	return out
}

func removeRow(rows []string, drop string) []string {
	var out []string
	for _, r := range rows {
		if r != drop {
			out = append(out, r)
		}
	}
	return out
}
