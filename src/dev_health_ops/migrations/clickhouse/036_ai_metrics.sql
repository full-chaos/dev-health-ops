-- Migration 036: AI workflow impact metric rollups.
--
-- Append-only daily rollups computed from ai_attribution_resolved and PR facts.
-- Read latest values with argMax(..., computed_at).  Null attribution is kept
-- as attribution_bucket='unknown' and is never folded into human baselines.

CREATE TABLE IF NOT EXISTS ai_impact_metrics_daily
(
    org_id String,
    team_id Nullable(String),
    repo_id UUID,
    work_type LowCardinality(String),
    day Date,
    attribution_bucket LowCardinality(String), -- ai_assisted|agent_created|ai_review|human|unknown

    prs_total UInt32,
    prs_merged UInt32,
    ai_assisted_prs UInt32,
    agent_created_prs UInt32,
    human_prs UInt32,
    unknown_prs UInt32,
    ai_assisted_pr_ratio Nullable(Float64),
    agent_created_pr_count UInt32,

    cycle_time_avg_hours Nullable(Float64),
    baseline_cycle_time_avg_hours Nullable(Float64),
    ai_cycle_time_delta_hours Nullable(Float64),

    reviews_per_pr Nullable(Float64),
    baseline_reviews_per_pr Nullable(Float64),
    ai_review_amplification Nullable(Float64),
    changes_requested_per_pr Nullable(Float64),

    rework_prs UInt32,
    rework_drag_rate Nullable(Float64),
    followup_commits_count UInt32,
    revert_prs UInt32,
    revert_rate Nullable(Float64),
    incidents_count UInt32,
    incident_drag_rate Nullable(Float64),

    test_gap_prs UInt32,
    test_gap_rate Nullable(Float64),

    leverage_prs_component Float64,
    leverage_cycle_time_component Nullable(Float64),
    leverage_review_component Nullable(Float64),
    leverage_rework_component Nullable(Float64),
    leverage_test_component Nullable(Float64),
    leverage_incident_component Nullable(Float64),

    computed_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, team_id, repo_id, work_type, day, attribution_bucket)
SETTINGS index_granularity = 8192;
