-- GAP-ASKDEV-CI-01: provider-neutral required CI/acceptance projection.
-- Missing provider policy and missing results stay explicit ``unknown`` values.
-- neither is rewritten as optional/passed. ReplacingMergeTree makes bounded
-- producer replays idempotent without publishing partial-run completeness.
CREATE TABLE IF NOT EXISTS ci_acceptance_checks (
    org_id LowCardinality(String),
    repo_id UUID,
    run_id String,
    check_key String,
    check_name String,
    provider LowCardinality(String),
    requirement LowCardinality(String),
    result LowCardinality(String),
    rule_version LowCardinality(String),
    provenance String,
    target_branch Nullable(String),
    pr_number Nullable(UInt32),
    source_url Nullable(String),
    observed_at DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC'),
    CONSTRAINT valid_ci_requirement CHECK requirement IN ('required', 'optional', 'unknown'),
    CONSTRAINT valid_ci_result CHECK result IN ('passed', 'failed', 'skipped', 'pending', 'unknown')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, run_id, check_key);
