-- One row for each HTTP attempt of an LLM categorization call (CHAOS-8868,
-- parent CHAOS-8865). Scalars only: no prompt, no response, no source text.
-- In the shadow build only role = 'shadow' rows are written. Rows for the
-- served path come later in their own change.
--
-- Shadow usage is deliberately not written to llm_token_usage: the organization
-- spend reader groups that table by run_id, provider and model with no source
-- filter, so a shadow row would show as a cost the organization did not pay.
-- Shadow spend is the sum of the attempt rows with role = 'shadow'.
--
-- The sorting key is the identity of an attempt: (org_id, run_id, work_unit_id,
-- role, config, kind, attempt). A merge keeps ONE row for each identity, so two
-- attempts that differ in any of these fields must stay two rows: the same unit
-- and attempt number under two configurations, or a first send and a repair of
-- the same number, or a served and a fallback attempt. An earlier draft of this
-- file had no config and no kind in the key, and a merge then deleted one of two
-- such rows (executed: write two, OPTIMIZE FINAL, count 1). A sorting key
-- cannot be changed once a table exists, so the key was fixed here, in place:
-- the table was not deployed anywhere when it was changed.
--
-- The key holds run_id, and a run id is new for each run, so rows of different
-- runs never replace each other: the table is an append log. ReplacingMergeTree
-- only removes the repeat of one batch, and only at merge time, which is
-- eventual. Every reader dedups by the sorting key (argMax on computed_at)
-- BEFORE it sums, or it counts an unmerged duplicate two times.
--
-- role: served, shadow or fallback. config is the identity stamp of the
-- candidate for a shadow row. api_mode: responses or systemone. attempt is 1 for
-- the first send. kind: first, retry, repair or fallback. error_class is the
-- failure class of the attempt, or an empty string. state is the final state on
-- the last attempt of a classification and an empty string on the others.
-- billed_cost_usd is the reported usage times the rate table that rates_version
-- names. Neither provider API returns an invoice figure.
-- Retention: 400 days.
CREATE TABLE IF NOT EXISTS llm_categorization_attempts (
    org_id String,
    run_id String,
    work_unit_id String,
    role LowCardinality(String),
    config String,
    rubric_sha256 String,
    provider LowCardinality(String),
    api_mode LowCardinality(String),
    model_requested String,
    model_returned String,
    attempt UInt8,
    kind LowCardinality(String),
    http_status UInt16,
    error_class LowCardinality(String),
    state LowCardinality(String),
    request_id String,
    input_tokens UInt32,
    output_tokens UInt32,
    cached_input_tokens UInt32,
    billed_cost_usd Float64,
    rates_version LowCardinality(String),
    latency_ms UInt32,
    retry_wait_ms UInt32,
    computed_at DateTime64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, run_id, work_unit_id, role, config, kind, attempt)
TTL toDateTime(computed_at) + INTERVAL 400 DAY;
