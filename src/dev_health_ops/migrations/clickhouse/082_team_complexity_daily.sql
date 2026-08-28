-- CHAOS-4365 item 3 (4347-C): team-keyed cyclomatic-complexity rollup,
-- OWNERSHIP-scoped.
--
-- CHAOS-4321 hard rule: team = project/repo ownership only, never
-- person->membership. This table is written by aggregating
-- repo_complexity_daily rows BY repo_id, then mapping repo_id -> team via
-- team_repo_ownership merged over teams.repo_patterns (the same ownership
-- sources CHAOS-4365 item 1 wired up in
-- providers/teams.py::load_team_repo_ownership_map) -- repo_complexity_daily
-- carries no team_id column of its own to fall back on, so there is no
-- CHAOS-4396-style taint risk here, but the resolution path is identical to
-- item 1/2 for consistency and so a single ownership-map load is reusable
-- across all three finalize-step producers.
--
-- Append-only MergeTree (root AGENTS.md contract): re-computations insert
-- new rows with a newer computed_at -- readers dedup per (org_id, team_id,
-- day) via argMax(<col>, computed_at), matching compounding_risk_daily
-- (040_compounding_risk_daily.sql) and team_cognitive_load_daily
-- (081_team_cognitive_load_daily.sql). Never ReplacingMergeTree.
--
-- Written EXACTLY ONCE per (org_id, day) from run_daily_metrics_finalize
-- (job_daily.py), never per-repo -- same CHAOS-4399 discipline items 1/2
-- already enforce: a per-repo write inside the daily partition loop would
-- let argMax(computed_at) dedup silently keep only the last-processed
-- repo's numbers for a multi-repo team.
--
-- loc_total / cyclomatic_total / high_complexity_functions /
-- very_high_complexity_functions are SUMMED across every repo_complexity_
-- daily row this team owns (absolute counts, additive). cyclomatic_per_kloc
-- is RECOMPUTED from those summed totals
-- (sum(cyclomatic_total) / (sum(loc_total) / 1000)) -- a loc-weighted
-- ratio, never a naive average of each owned repo's own
-- cyclomatic_per_kloc: a ratio is not additive, the same rule
-- team_cognitive_load_daily's after_hours_commit_ratio/weekend_commit_ratio
-- follow (a straight mean of per-repo ratios would let a low-LOC repo's
-- noisy ratio skew the team number as much as the team's largest repo).
--
-- Column types are also pinned in full-chaos/dev-health-go's schema.go
-- (ProductionColumns["team_complexity_daily"] / EngineFull) with a test
-- asserting they match this DDL byte-for-byte -- a column added, renamed,
-- or retyped here without updating that map breaks that repo's CI, not
-- this one's.

-- Column order mirrors repo_complexity_daily (007_complexity_investment_
-- issues.sql) exactly -- loc_total, cyclomatic_total, cyclomatic_per_kloc,
-- high_complexity_functions, very_high_complexity_functions -- plus
-- team_id/contributing_repo_count. This exact order is also what
-- dev-health-go's schema.go pins byte-for-byte -- keep both in sync.
CREATE TABLE IF NOT EXISTS team_complexity_daily
(
    org_id                          String,
    team_id                         String,
    day                             Date,

    -- Summed across every repo_complexity_daily row this team owns.
    loc_total                       UInt64,
    cyclomatic_total                UInt64,

    -- Recomputed from the summed totals above (loc-weighted), never
    -- averaged directly across owned repos' own ratios.
    cyclomatic_per_kloc             Float64,

    high_complexity_functions       UInt64,
    very_high_complexity_functions  UInt64,

    -- Diagnosability: how many distinct owned repos contributed a
    -- repo_complexity_daily row this day.
    contributing_repo_count         UInt32,

    computed_at                     DateTime64(6, 'UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, team_id, day);
