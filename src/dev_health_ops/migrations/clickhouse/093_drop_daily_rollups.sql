-- These three daily rollups are AggregatingMergeTree tables fed by
-- materialized views that fire once per INSERT block on top of
-- ReplacingMergeTree source tables (git_commits, ci_pipeline_runs,
-- deployments). A re-synced run/commit/deployment can arrive as multiple
-- insert blocks, so the MV recomputes and adds its aggregate again for
-- every block instead of once per logical row -- the rollups silently
-- double (or triple, ...) count on any re-sync. No Go or Python code reads
-- these tables; only test fixtures reference them. Rather than rework the
-- views to track state with uniqExact, drop the rollups and their views
-- outright until a reader actually needs them.
--
-- Views first (a view referencing a dropped table is not a live footgun,
-- but dropping the table underneath a live view first would leave a
-- dangling view against a nonexistent target on a running cluster).

DROP VIEW IF EXISTS commit_daily_rollup_mv;
DROP VIEW IF EXISTS ci_daily_rollup_mv;
DROP VIEW IF EXISTS deployment_daily_rollup_mv;

DROP TABLE IF EXISTS commit_daily_rollup;
DROP TABLE IF EXISTS ci_daily_rollup;
DROP TABLE IF EXISTS deployment_daily_rollup;
