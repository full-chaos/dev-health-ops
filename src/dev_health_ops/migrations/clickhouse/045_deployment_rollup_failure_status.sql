-- Migration 045: normalize deployment-failure classification in the canonical
-- deployment_daily_rollup materialized view (CHAOS-2395).
--
-- The MV (026_materialized_views.sql, rebuilt in 027_add_org_id_to_sorting_keys)
-- classified failures with countIf(status = 'failure') ONLY. That counts
-- GitHub's deployment state 'failure' but silently drops GitLab's deployment
-- status 'failed'/'canceled' (and 'error'), so deployment_daily_rollup
-- .failure_deployments undercounted failed deployments for GitLab orgs and any
-- DORA change-failure-rate derived from the rollup was biased toward 0. This
-- recreates the MV with the provider-agnostic failure union that
-- compute_deployments.DEPLOYMENT_FAILURE_STATUSES / compute_dora use, so the
-- rollup classifies failures identically to the Python daily-metrics path.
--
-- A ClickHouse materialized view only transforms rows at INSERT time, so
-- recreating it makes every future deployment sync correct. Historical
-- deployment_daily_rollup rows are intentionally left untouched: no in-app
-- reader consumes that rollup today (DORA computes change-failure-rate in
-- Python directly from the deployments table), and the next sync repopulates
-- current partitions. The MV definition below is byte-for-byte the org-scoped
-- 027 definition with only the failure countIf widened.
DROP VIEW IF EXISTS deployment_daily_rollup_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS deployment_daily_rollup_mv
TO deployment_daily_rollup
AS SELECT
    org_id,
    repo_id,
    toDate(coalesce(deployed_at, started_at)) AS day,
    toUInt64(count()) AS total_deployments,
    toUInt64(countIf(status = 'success')) AS success_deployments,
    toUInt64(countIf(status IN ('failure', 'failed', 'error', 'canceled'))) AS failure_deployments
FROM deployments
GROUP BY org_id, repo_id, toDate(coalesce(deployed_at, started_at));
