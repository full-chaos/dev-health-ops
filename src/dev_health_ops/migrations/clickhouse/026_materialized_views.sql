-- 026_materialized_views.sql
-- Materialized views for immutable event daily rollups.
--
-- SAFE for materialized views (immutable once inserted):
--   - git_commits: keyed by (repo_id, hash) — commits are immutable facts
--   - ci_pipeline_runs: keyed by (repo_id, run_id) — completed runs don't change
--   - deployments: keyed by (repo_id, deployment_id) — deployment records are final
--
-- NOT SAFE (excluded — mutable state that gets updated in place):
--   - git_pull_requests: state, merged_at, review counts mutate over PR lifecycle
--   - work_items: status, assignee, priority fields change continuously
--   - investment distributions: LLM-computed at compute-time, may be recomputed
--
-- Note: Source tables use ReplacingMergeTree but the events aggregated here
-- are immutable once inserted. The MV triggers on INSERT, so each event is
-- counted exactly once. ReplacingMergeTree deduplication in source tables
-- handles re-syncs, but the aggregated counts remain correct because the
-- underlying facts (commit happened, CI run completed, deployment finished)
-- do not change.

-- ============================================================================
-- A) Commit count daily rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS commit_daily_rollup (
    repo_id UUID,
    day Date,
    commit_count SimpleAggregateFunction(sum, UInt64),
    loc_added SimpleAggregateFunction(sum, UInt64),
    loc_deleted SimpleAggregateFunction(sum, UInt64),
    files_changed SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS commit_daily_rollup_mv
TO commit_daily_rollup
AS SELECT
    gc.repo_id AS repo_id,
    toDate(gc.author_when) AS day,
    toUInt64(count()) AS commit_count,
    toUInt64(sum(if(gcs.additions > 0, gcs.additions, 0))) AS loc_added,
    toUInt64(sum(if(gcs.deletions > 0, abs(gcs.deletions), 0))) AS loc_deleted,
    toUInt64(count(DISTINCT gcs.file_path)) AS files_changed
FROM git_commits gc
LEFT JOIN git_commit_stats gcs ON gc.repo_id = gcs.repo_id AND gc.hash = gcs.commit_hash
GROUP BY gc.repo_id, toDate(gc.author_when);

-- ============================================================================
-- B) CI pipeline daily rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS ci_daily_rollup (
    repo_id UUID,
    day Date,
    total_runs SimpleAggregateFunction(sum, UInt64),
    success_runs SimpleAggregateFunction(sum, UInt64),
    failure_runs SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS ci_daily_rollup_mv
TO ci_daily_rollup
AS SELECT
    repo_id,
    toDate(started_at) AS day,
    toUInt64(count()) AS total_runs,
    toUInt64(countIf(status = 'success')) AS success_runs,
    toUInt64(countIf(status = 'failure')) AS failure_runs
FROM ci_pipeline_runs
GROUP BY repo_id, toDate(started_at);

-- ============================================================================
-- C) Deployment daily rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS deployment_daily_rollup (
    repo_id UUID,
    day Date,
    total_deployments SimpleAggregateFunction(sum, UInt64),
    success_deployments SimpleAggregateFunction(sum, UInt64),
    failure_deployments SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS deployment_daily_rollup_mv
TO deployment_daily_rollup
AS SELECT
    repo_id,
    toDate(coalesce(deployed_at, started_at)) AS day,
    toUInt64(count()) AS total_deployments,
    toUInt64(countIf(status = 'success')) AS success_deployments,
    toUInt64(countIf(status = 'failure')) AS failure_deployments
FROM deployments
GROUP BY repo_id, toDate(coalesce(deployed_at, started_at));
