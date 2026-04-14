CREATE TABLE IF NOT EXISTS security_alerts (
    repo_id UUID,
    alert_id String,
    source String,
    severity Nullable(String),
    state Nullable(String),
    package_name Nullable(String),
    cve_id Nullable(String),
    url Nullable(String),
    title Nullable(String),
    description Nullable(String),
    created_at DateTime64(3, 'UTC'),
    fixed_at Nullable(DateTime64(3, 'UTC')),
    dismissed_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, alert_id);
