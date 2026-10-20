CREATE TABLE IF NOT EXISTS product_telemetry_events (
    org_id_hash String DEFAULT '',
    event_id String,
    name LowCardinality(String),
    schema_version String,
    session_id String,
    anonymous_user_id String,
    route_pattern Nullable(String),
    payload_json String,
    occurred_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    source LowCardinality(String)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (org_id_hash, name, occurred_at, event_id)
TTL toDateTime(occurred_at) + INTERVAL 180 DAY DELETE;
