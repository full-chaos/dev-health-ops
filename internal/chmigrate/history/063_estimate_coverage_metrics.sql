CREATE TABLE IF NOT EXISTS estimate_coverage_metrics_daily (
    day Date,
    provider String,
    work_scope_id String,
    team_id Nullable(String),
    team_name Nullable(String),
    estimated_count UInt32,
    unestimated_count UInt32,
    backlog_size UInt32,
    ratio Nullable(Float64),
    computed_at DateTime64(3, 'UTC'),
    org_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, provider, work_scope_id, ifNull(team_id, ''))
SETTINGS index_granularity = 8192;
