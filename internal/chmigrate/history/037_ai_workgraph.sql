-- Migration 037: AI workflow evidence extensions for the Work Graph.
--
-- These tables are analytics-only ClickHouse state. They store AI workflow run
-- metadata and typed evidence edges without raw prompts, sessions, transcripts,
-- IDE telemetry, or keystroke data.

CREATE TABLE IF NOT EXISTS ai_workflow_runs
(
    run_id            String,
    org_id            UUID,
    provider          LowCardinality(String),
    run_kind          LowCardinality(String),
    status            Nullable(String),
    tool              Nullable(String),
    model             Nullable(String),
    actor             Nullable(String),
    repo_id           Nullable(UUID),
    prompts_redacted  Bool,
    prompt_hash       Nullable(String),
    prompt_length     Nullable(UInt32),
    started_at        Nullable(DateTime64(3, 'UTC')),
    completed_at      Nullable(DateTime64(3, 'UTC')),
    observed_at       DateTime64(3, 'UTC'),
    metadata          String,
    computed_at       DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, provider, run_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ai_workflow_issue_edges
(
    edge_id      String,
    org_id       UUID,
    issue_id     String,
    run_id       String,
    provider     LowCardinality(String),
    repo_id      Nullable(UUID),
    confidence   Float32,
    source       LowCardinality(String),
    evidence     String,
    observed_at  DateTime64(3, 'UTC'),
    computed_at  DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, issue_id, run_id, source)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ai_workflow_artifact_edges
(
    edge_id        String,
    org_id         UUID,
    run_id         String,
    artifact_type  LowCardinality(String),
    artifact_id    String,
    provider       LowCardinality(String),
    repo_id        Nullable(UUID),
    confidence     Float32,
    source         LowCardinality(String),
    evidence       String,
    observed_at    DateTime64(3, 'UTC'),
    computed_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, run_id, artifact_type, artifact_id, source)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS work_graph_pr_review_outcome_edges
(
    edge_id            String,
    org_id             UUID,
    pr_id              String,
    review_outcome_id  String,
    outcome            Nullable(String),
    provider           LowCardinality(String),
    repo_id            Nullable(UUID),
    confidence         Float32,
    source             LowCardinality(String),
    evidence           String,
    observed_at        DateTime64(3, 'UTC'),
    computed_at        DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, pr_id, review_outcome_id, source)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS work_graph_pr_deployment_edges
(
    edge_id        String,
    org_id         UUID,
    pr_id          String,
    deployment_id  String,
    provider       LowCardinality(String),
    repo_id        Nullable(UUID),
    confidence     Float32,
    source         LowCardinality(String),
    evidence       String,
    observed_at    DateTime64(3, 'UTC'),
    computed_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, pr_id, deployment_id, source)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS work_graph_deployment_incident_edges
(
    edge_id        String,
    org_id         UUID,
    deployment_id  String,
    incident_id    String,
    provider       LowCardinality(String),
    repo_id        Nullable(UUID),
    confidence     Float32,
    source         LowCardinality(String),
    evidence       String,
    observed_at    DateTime64(3, 'UTC'),
    computed_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, deployment_id, incident_id, source)
SETTINGS index_granularity = 8192;
