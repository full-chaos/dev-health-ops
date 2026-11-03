-- Shadow categorization results (CHAOS-8868, parent CHAOS-8865). In shadow mode
-- the worker asks a candidate backend for the same bundle that the served
-- provider categorizes, and stores the answer here instead of in
-- work_unit_investments. No reader of the product reads this table: a served
-- reader, the skip-existing lookup and the daily tables never see a shadow row.
-- That holds by table, not by a filter a reader could forget.
--
-- One row for each (org_id, work_unit_id, categorization_input_hash,
-- shadow_config). shadow_config is the identity stamp of the candidate
-- (provider, api, model, taxonomy, rubric and its hash, adapter, map, level
-- rule), so a result of one configuration never matches another.
--
-- The writer only inserts. ReplacingMergeTree keeps the newest computed_at of a
-- key at merge time, and merges are eventual, so a reader never reads the rows
-- as they are: it takes argMax(..., computed_at) grouped by the sorting key.
-- Because the key has no run id, a repeat of the same unit, input and
-- configuration replaces the older row, so run-to-run stability is not
-- measurable from this table.
--
-- Typed columns only. There is no text column that could hold a prompt, a
-- response or a quote: the evidence is kept as a span id, a handle and a source
-- id. For a failure state the two distribution maps are empty, because a shadow
-- row must not look like a mix. theme_distribution_json is always the
-- deterministic roll-up of subcategory_distribution_json and never comes from
-- the backend. Despite the column names, both are Map(String, Float64) columns
-- like their work_unit_investments namesakes, not JSON text.
--
-- sufficiency_level is -1 when the question was not answered.
-- Retention: 90 days.
CREATE TABLE IF NOT EXISTS work_unit_investment_shadow (
    org_id String,
    work_unit_id String,
    categorization_input_hash String,
    shadow_config String,
    rubric_sha256 String,
    model_returned String,
    state LowCardinality(String),
    categorization_status LowCardinality(String),
    complete_strict UInt8,
    subcategory_distribution_json Map(String, Float64),
    theme_distribution_json Map(String, Float64),
    levels Map(String, UInt8),
    level_probabilities Map(String, Array(Float32)),
    sufficiency_level Int8,
    evidence_span_id String,
    evidence_handle String,
    evidence_source_type LowCardinality(String),
    evidence_source_id String,
    warnings Array(String),
    error_codes Array(String),
    served_run_id String,
    computed_at DateTime64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, work_unit_id, categorization_input_hash, shadow_config)
TTL toDateTime(computed_at) + INTERVAL 90 DAY;
