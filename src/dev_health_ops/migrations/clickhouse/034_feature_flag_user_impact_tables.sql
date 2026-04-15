-- Migration 034: Add feature flag and user impact analytics tables.

CREATE TABLE IF NOT EXISTS feature_flag (
    org_id String DEFAULT 'default',
    provider String,
    flag_key String,
    project_key String,
    repo_id String,
    environment String,
    flag_type String,
    created_at DateTime64(3, 'UTC'),
    archived_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, provider, flag_key);

CREATE TABLE IF NOT EXISTS feature_flag_event (
    org_id String DEFAULT 'default',
    event_type String,
    flag_key String,
    environment String,
    repo_id String,
    actor_type String,
    prev_state String,
    next_state String,
    event_ts DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    source_event_id String,
    dedupe_key String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (org_id, flag_key, environment, event_ts)
TTL event_ts + INTERVAL 90 DAY DELETE;

CREATE TABLE IF NOT EXISTS feature_flag_link (
    org_id String DEFAULT 'default',
    flag_key String,
    target_type String,
    target_id String,
    provider String,
    link_source String,
    link_type String,
    evidence_type String,
    confidence Float32,
    valid_from Nullable(DateTime64(3, 'UTC')),
    valid_to Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, flag_key, target_type, target_id);

CREATE TABLE IF NOT EXISTS telemetry_signal_bucket (
    org_id String DEFAULT 'default',
    signal_type String,
    signal_count UInt64,
    session_count UInt64,
    unique_pseudonymous_count Nullable(UInt64),
    endpoint_group String,
    environment String,
    repo_id String,
    release_ref String,
    bucket_start DateTime64(3, 'UTC'),
    bucket_end DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    is_sampled UInt8 DEFAULT 0,
    schema_version String,
    dedupe_key String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (org_id, environment, repo_id, release_ref, bucket_start)
TTL bucket_start + INTERVAL 90 DAY DELETE;

CREATE TABLE IF NOT EXISTS release_impact_daily (
    org_id String DEFAULT 'default',
    day Date,
    release_ref String,
    environment String,
    repo_id String,
    release_user_friction_delta Nullable(Float64),
    release_post_friction_rate Nullable(Float64),
    release_error_rate_delta Nullable(Float64),
    release_post_error_rate Nullable(Float64),
    time_to_first_user_issue_after_release Nullable(Float64),
    release_impact_confidence_score Float32,
    release_impact_coverage_ratio Float32,
    flag_exposure_rate Nullable(Float64),
    flag_activation_rate Nullable(Float64),
    flag_reliability_guardrail Nullable(Float64),
    flag_friction_delta Nullable(Float64),
    flag_rollout_half_life Nullable(Float64),
    flag_churn_rate Nullable(Float64),
    issue_to_release_impact_link_rate Nullable(Float64),
    rollback_or_disable_after_impact_spike UInt32,
    coverage_ratio Float32,
    missing_required_fields_count UInt32,
    instrumentation_change_flag UInt8 DEFAULT 0,
    data_completeness Float32,
    concurrent_deploy_count UInt32,
    computed_at DateTime64(3, 'UTC')
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, release_ref, environment, day)
TTL day + INTERVAL 365 DAY DELETE;
