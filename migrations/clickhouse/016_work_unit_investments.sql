-- Work unit investment materialization tables

CREATE TABLE IF NOT EXISTS work_unit_investments (
    work_unit_id String,
    from_ts DateTime64(3, 'UTC'),
    to_ts DateTime64(3, 'UTC'),
    repo_id Nullable(UUID),
    provider Nullable(String),
    effort_metric LowCardinality(String),
    effort_value Float64,
    theme_distribution_json Map(String, Float32),
    subcategory_distribution_json Map(String, Float32),
    structural_evidence_json String,
    evidence_quality Float32,
    evidence_quality_band LowCardinality(String),
    categorization_status LowCardinality(String),
    categorization_errors_json String,
    categorization_model_version String,
    categorization_input_hash String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    INDEX idx_theme_keys mapKeys(theme_distribution_json) TYPE set(0) GRANULARITY 1,
    INDEX idx_subcategory_keys mapKeys(subcategory_distribution_json) TYPE set(0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(from_ts)
ORDER BY (toDate(from_ts), repo_id, work_unit_id, categorization_run_id)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS work_unit_investment_quotes (
    work_unit_id String,
    quote String,
    source_type LowCardinality(String),
    source_id String,
    computed_at DateTime64(3, 'UTC'),
    categorization_run_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (work_unit_id, source_type, source_id, quote);
