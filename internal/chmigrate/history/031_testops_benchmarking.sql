-- Benchmarking + insight pipeline tables (CHAOS-1170).

CREATE TABLE IF NOT EXISTS testops_period_comparisons (
    metric_name LowCardinality(String),
    scope_type LowCardinality(String),
    scope_key String,
    current_period_start Date,
    current_period_end Date,
    comparison_period_start Date,
    comparison_period_end Date,
    current_value Float64,
    comparison_value Float64,
    absolute_delta Float64,
    percentage_change Nullable(Float64),
    trend_direction LowCardinality(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(current_period_end)
ORDER BY (metric_name, scope_type, scope_key, current_period_end, comparison_period_end);

CREATE TABLE IF NOT EXISTS testops_metric_baselines (
    metric_name LowCardinality(String),
    scope_type LowCardinality(String),
    scope_key String,
    period_start Date,
    period_end Date,
    rolling_window_days UInt16,
    current_value Float64,
    baseline_value Float64,
    percentile_rank Float64,
    p25_value Float64,
    p50_value Float64,
    p75_value Float64,
    p90_value Float64,
    sample_size UInt32,
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(period_end)
ORDER BY (metric_name, scope_type, scope_key, period_end, rolling_window_days);

CREATE TABLE IF NOT EXISTS testops_maturity_bands (
    metric_name LowCardinality(String),
    scope_type LowCardinality(String),
    scope_key String,
    period_start Date,
    period_end Date,
    value Float64,
    percentile_rank Float64,
    maturity_band LowCardinality(String),
    confidence Float64,
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(period_end)
ORDER BY (metric_name, scope_type, scope_key, period_end);

CREATE TABLE IF NOT EXISTS testops_metric_anomalies (
    metric_name LowCardinality(String),
    scope_type LowCardinality(String),
    scope_key String,
    day Date,
    value Float64,
    baseline_value Float64,
    z_score Float64,
    anomaly_type LowCardinality(String),
    direction LowCardinality(String),
    severity LowCardinality(String),
    volatility_score Float64,
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (metric_name, scope_type, scope_key, day, anomaly_type);

CREATE TABLE IF NOT EXISTS testops_metric_correlations (
    metric_name LowCardinality(String),
    paired_metric_name LowCardinality(String),
    scope_type LowCardinality(String),
    scope_key String,
    period_start Date,
    period_end Date,
    coefficient Float64,
    p_value Float64,
    sample_size UInt32,
    is_significant UInt8,
    interpretation String,
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(period_end)
ORDER BY (metric_name, paired_metric_name, scope_type, scope_key, period_end);

CREATE TABLE IF NOT EXISTS testops_benchmark_insights (
    insight_id String,
    insight_type LowCardinality(String),
    scope_type LowCardinality(String),
    scope_key String,
    metric_name LowCardinality(String),
    paired_metric_name Nullable(String),
    period_start Nullable(Date),
    period_end Nullable(Date),
    severity LowCardinality(String),
    summary String,
    evidence_json String DEFAULT '{}',
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(coalesce(period_end, toDate(computed_at)))
ORDER BY (insight_id, computed_at);
