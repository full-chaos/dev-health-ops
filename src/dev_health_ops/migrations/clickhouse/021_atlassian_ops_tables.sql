CREATE TABLE IF NOT EXISTS atlassian_ops_incidents (
    id String,
    url Nullable(String),
    summary String,
    description Nullable(String),
    status String,
    severity String,
    created_at DateTime64(6, 'UTC'),
    provider_id Nullable(String),
    last_synced DateTime64(6, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS atlassian_ops_alerts (
    id String,
    status String,
    priority String,
    created_at DateTime64(6, 'UTC'),
    acknowledged_at Nullable(DateTime64(6, 'UTC')),
    snoozed_at Nullable(DateTime64(6, 'UTC')),
    closed_at Nullable(DateTime64(6, 'UTC')),
    last_synced DateTime64(6, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS atlassian_ops_schedules (
    id String,
    name String,
    timezone Nullable(String),
    last_synced DateTime64(6, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (id);
