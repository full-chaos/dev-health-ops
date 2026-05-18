-- Migration 035: AI attribution tables and resolved view.
--
-- Stores one row per detected AI signal per subject (PR, commit, issue, etc.).
-- Deduplication is by (org_id, provider, subject_type, subject_id, source) via
-- ReplacingMergeTree(computed_at) — latest insert wins within that key.
--
-- Source precedence (highest → lowest):
--   MANUAL > pr_label > bot_author > commit_trailer > ci_annotation > branch_name > pr_body
--
-- Write path: every detected signal is persisted raw.
-- Read path:  use `ai_attribution_resolved` to get the effective attribution per subject.

CREATE TABLE IF NOT EXISTS ai_attribution
(
    record_id      UUID,
    org_id         UUID,
    provider       LowCardinality(String),
    subject_type   LowCardinality(String),
    subject_id     String,
    repo_id        Nullable(UUID),
    kind           LowCardinality(String),
    source         LowCardinality(String),
    confidence     Float32,
    actor          Nullable(String),
    evidence       String,                          -- JSON-encoded evidence blob
    observed_at    DateTime64(3, 'UTC'),
    ingested_at    DateTime64(3, 'UTC'),
    superseded_by  Nullable(UUID),
    computed_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, provider, subject_type, subject_id, source)
SETTINGS index_granularity = 8192;

-- -------------------------------------------------------------------------
-- Resolved view: pick the highest-precedence, non-superseded record per
-- (org_id, subject_type, subject_id) using window functions.
--
-- Implemented as a plain VIEW (not an incremental MV) because precedence
-- resolution requires visibility across all rows for a subject, which
-- incremental ClickHouse MVs cannot provide.  The base table uses
-- ReplacingMergeTree(computed_at) for write-time deduplication per source;
-- this view handles read-time cross-source precedence resolution.
--
-- Source priority integers (lower = wins):
--   manual=1, pr_label=2, bot_author=3, commit_trailer=4,
--   ci_annotation=5, branch_name=6, pr_body=7
-- -------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS ai_attribution_resolved AS
SELECT
    record_id,
    org_id,
    provider,
    subject_type,
    subject_id,
    repo_id,
    kind,
    source,
    confidence,
    actor,
    evidence,
    observed_at,
    ingested_at,
    superseded_by,
    computed_at
FROM (
    SELECT
        *,
        multiIf(
            source = 'manual',          1,
            source = 'pr_label',        2,
            source = 'bot_author',      3,
            source = 'commit_trailer',  4,
            source = 'ci_annotation',   5,
            source = 'branch_name',     6,
            source = 'pr_body',         7,
            8
        ) AS _source_priority
    FROM ai_attribution FINAL
    WHERE superseded_by IS NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY org_id, subject_type, subject_id
    ORDER BY _source_priority ASC, confidence DESC
) = 1;
