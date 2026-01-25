-- Materialized investment tables
-- work_unit_investments stores the categorization results for each work unit (PR/Issue).
CREATE TABLE IF NOT EXISTS work_unit_investments (
    work_unit_id String,
    from_ts DateTime64(3, 'UTC'),
    to_ts DateTime64(3, 'UTC'),
    repo_id Nullable(UUID),
    provider Nullable(String),
    effort_metric String, -- 'fte_days', 'story_points', etc.
    effort_value Float64,
    theme_distribution_json Map(String, Float64), -- 'Theme': 0.8
    subcategory_distribution_json Map(String, Float64),
    structural_evidence_json String, -- Serialized JSON of structural signals
    evidence_quality Float64, -- 0.0 - 1.0
    evidence_quality_band String, -- 'high', 'medium', 'low'
    categorization_status String, -- 'success', 'error', 'partial'
    categorization_errors_json String,
    categorization_model_version String,
    categorization_input_hash String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (work_unit_id);

-- work_unit_investment_quotes stores extracted text snippets used for evidence.
CREATE TABLE IF NOT EXISTS work_unit_investment_quotes (
    work_unit_id String,
    quote String,
    source_type String, -- 'pr_body', 'commit_msg', 'issue_desc'
    source_id String,
    computed_at DateTime64(3, 'UTC'),
    categorization_run_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (work_unit_id, source_id, quote);
