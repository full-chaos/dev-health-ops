-- Bring pre-existing work_unit_investments tables up to current schema.
-- ClickHouse CREATE TABLE IF NOT EXISTS does not add new columns, so we apply
-- additive ALTERs to support upgrades from earlier prototypes.

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

ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS from_ts DateTime64(3, 'UTC') AFTER work_unit_id;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS to_ts DateTime64(3, 'UTC') AFTER from_ts;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS repo_id Nullable(UUID) AFTER to_ts;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS provider Nullable(String) AFTER repo_id;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS effort_metric LowCardinality(String) AFTER provider;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS effort_value Float64 AFTER effort_metric;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS theme_distribution_json Map(String, Float32) AFTER effort_value;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS subcategory_distribution_json Map(String, Float32) AFTER theme_distribution_json;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS structural_evidence_json String AFTER subcategory_distribution_json;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS evidence_quality Float32 AFTER structural_evidence_json;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS evidence_quality_band LowCardinality(String) AFTER evidence_quality;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS categorization_status LowCardinality(String) AFTER evidence_quality_band;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS categorization_errors_json String AFTER categorization_status;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS categorization_model_version String AFTER categorization_errors_json;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS categorization_input_hash String AFTER categorization_model_version;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS categorization_run_id String AFTER categorization_input_hash;
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS computed_at DateTime64(3, 'UTC') AFTER categorization_run_id;

CREATE TABLE IF NOT EXISTS work_unit_investment_quotes (
    work_unit_id String,
    quote String,
    source_type LowCardinality(String),
    source_id String,
    computed_at DateTime64(3, 'UTC'),
    categorization_run_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (work_unit_id, source_type, source_id, quote);

ALTER TABLE work_unit_investment_quotes
    ADD COLUMN IF NOT EXISTS computed_at DateTime64(3, 'UTC');
ALTER TABLE work_unit_investment_quotes
    ADD COLUMN IF NOT EXISTS categorization_run_id String;
