-- Migration 039: recommendations_daily — append-only rule evaluation results.
--
-- Each row records one rule evaluation for a (team_id, rule_id) pair.
-- Re-evaluations INSERT new rows with a newer computed_at.
-- Read the latest result with: argMax(fired, computed_at).
--
-- ORDER BY includes window_end so range queries are efficient and
-- (team_id, rule_id, window_end) forms a natural "latest per window" key.

CREATE TABLE IF NOT EXISTS recommendations_daily
(
    team_id           LowCardinality(String),
    org_id            String,
    rule_id           LowCardinality(String),
    rule_version      LowCardinality(String)    DEFAULT '1.0.0',
    window_start      Date,
    window_end        Date,
    fired             Bool,
    severity          LowCardinality(String),   -- 'warning' | 'critical'
    title             String,
    rationale         String,
    success_criterion String,
    evidence_json     String,                   -- JSON list[EvidenceRef]
    computed_at       DateTime64(3, 'UTC')      DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(window_end)
ORDER BY (org_id, team_id, rule_id, window_end)
SETTINGS index_granularity = 8192;
