-- Migration 038: AI governance policy events and coverage rollups.
--
-- Governance is coverage-oriented, not surveillance-oriented: events reference
-- work artifact ids and JSON evidence metadata only.  No prompt/session/IDE
-- telemetry is captured.

CREATE TABLE IF NOT EXISTS ai_policy_events
(
    event_id     UUID,
    org_id       String,
    team_id      Nullable(String),
    repo_id      Nullable(UUID),
    rule_id      LowCardinality(String),
    severity     LowCardinality(String),
    subject_type LowCardinality(String),
    subject_id   String,
    observed_at  DateTime64(3, 'UTC'),
    evidence     String,                         -- JSON-encoded artifact references only
    computed_at  DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, ifNull(team_id, ''), ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')), rule_id, subject_type, subject_id, observed_at, event_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ai_governance_coverage_daily
(
    org_id              String,
    team_id             Nullable(String),
    repo_id             Nullable(UUID),
    day                 Date,
    ai_artifacts        UInt64,
    declared_artifacts  UInt64,
    human_reviewed_prs  UInt64,
    security_scanned_prs UInt64,
    in_policy_artifacts UInt64,
    computed_at         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, ifNull(team_id, ''), ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')), day)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ai_tool_allowlist
(
    org_id      String,
    tool_name   String,
    model_name  Nullable(String),
    status      LowCardinality(String),           -- allowed | disallowed | deprecated
    reason      Nullable(String),
    updated_at  DateTime64(3, 'UTC'),
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, tool_name, ifNull(model_name, ''))
SETTINGS index_granularity = 8192;
