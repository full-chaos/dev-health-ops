-- CHAOS-4365 item 2 (4347-C): team-keyed cognitive load, OWNERSHIP-scoped.
--
-- CHAOS-4321 hard rule: team = project/repo ownership only, never
-- person->membership. This table is written by aggregating
-- user_metrics_daily / team_metrics_daily rows BY repo_id, then mapping
-- repo_id -> team via team_repo_ownership merged over teams.repo_patterns
-- (the same ownership sources CHAOS-4365 item 1 wired up in
-- providers/teams.py::load_team_repo_ownership_map) -- never via either
-- source table's own team_id column, which CHAOS-4396 found falls back to
-- author-membership resolution when repo-ownership resolution misses.
--
-- Append-only MergeTree (root AGENTS.md contract): re-computations insert
-- new rows with a newer computed_at; readers dedup per (org_id, team_id,
-- day) via argMax(<col>, computed_at), matching compounding_risk_daily
-- (040_compounding_risk_daily.sql) and every other daily rollup in this
-- schema. Never ReplacingMergeTree.
--
-- Written EXACTLY ONCE per (org_id, day) from run_daily_metrics_finalize
-- (job_daily.py), never per-repo -- a row here must always reflect every
-- repo the team owns, not one partition's slice of them (CHAOS-4365: a
-- per-repo write silently dropped every owned repo but the last one
-- written once argMax(computed_at) dedup collapsed the redundant rows).
--
-- Column types are also pinned in full-chaos/dev-health-go's schema.go
-- (ProductionColumns["team_cognitive_load_daily"] / EngineFull) with a
-- test asserting they match this DDL byte-for-byte -- a column added,
-- renamed, or retyped here without updating that map breaks that repo's
-- CI, not this one's.

CREATE TABLE IF NOT EXISTS team_cognitive_load_daily
(
    org_id                    String,
    team_id                   String,
    day                       Date,

    -- Summed across every author on every repo this team owns (mirrors
    -- user_metrics_daily's own UInt32 counters, widened to Float64 here
    -- since this is a cross-repo SUM, not a single row's raw count).
    pr_interruption_load      Float64,
    context_spread_count      Float64,
    review_request_load       Float64,

    -- Recomputed from SUMMED after_hours/weekend commit counts across every
    -- repo this team owns (never averaged directly across repos -- a ratio
    -- is not additive; see append-only-daily-tables reader-dedup contract).
    -- NULL when no team_metrics_daily row exists for any owned repo this
    -- day -- distinct from a measured 0.0.
    after_hours_commit_ratio  Nullable(Float64),
    weekend_commit_ratio      Nullable(Float64),

    -- Diagnosability: how many distinct owned repos contributed a signal,
    -- and how many distinct authors rolled up into this team, this day.
    contributing_repo_count   UInt32,
    sample_author_count       UInt32,

    computed_at               DateTime64(6, 'UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, team_id, day);
