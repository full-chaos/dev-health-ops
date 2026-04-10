-- TestOps risk model tables (CHAOS-1079).

CREATE TABLE IF NOT EXISTS testops_release_confidence (
    repo_id UUID,
    day Date,
    confidence_score Float64,
    pipeline_success_factor Float64,
    test_pass_factor Float64,
    coverage_factor Float64,
    flake_penalty Float64,
    regression_penalty Float64,
    factors_json String DEFAULT '{}',
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE TABLE IF NOT EXISTS testops_quality_drag (
    repo_id UUID,
    day Date,
    drag_hours Float64,
    failure_rework_hours Float64,
    flake_investigation_hours Float64,
    queue_wait_hours Float64,
    retry_overhead_hours Float64,
    factors_json String DEFAULT '{}',
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE TABLE IF NOT EXISTS testops_pipeline_stability (
    repo_id UUID,
    day Date,
    stability_index Float64,
    success_rate_7d Float64,
    success_rate_trend Float64,
    failure_clustering_score Float64,
    median_recovery_time_seconds Nullable(Float64),
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);
