-- Provider API spend of Go provider-sync unit executions, for platform-admin
-- analytics. One row = one execution's calls on one transport since that
-- execution's previous flush, so every sum over rows counts each physical
-- send at most once. requests counts every write of a request to the wire
-- (retries and the transport's own re-sends included, whether or not a
-- response came back), and responses counts the calls that got one.
--
-- A row is built once and re-sent unchanged when a write is retried, and all
-- rows of one execution share window_started_at, so a duplicate always lands
-- in the same partition and collapses on the full sorting key. Readers sum
-- with FINAL.
CREATE TABLE IF NOT EXISTS provider_request_usage (
    org_id String,
    provider LowCardinality(String),
    dataset LowCardinality(String),
    integration_id String,
    sync_run_id String,
    unit_id String,
    execution_id String,
    attempt UInt32,
    flush_seq UInt32,
    transport LowCardinality(String),
    requests UInt64,
    responses UInt64,
    status_2xx UInt64,
    status_3xx UInt64,
    status_4xx UInt64,
    status_429 UInt64,
    status_5xx UInt64,
    latest_status Nullable(UInt16),
    rate_limit_remaining Nullable(Int64),
    rate_limit_limit Nullable(Int64),
    rate_limit_used Nullable(Int64),
    rate_limit_reset String DEFAULT '',
    rate_limit_resource LowCardinality(String) DEFAULT '',
    retry_after String DEFAULT '',
    window_started_at DateTime64(3, 'UTC'),
    recorded_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(recorded_at)
PARTITION BY toYYYYMM(window_started_at)
ORDER BY (org_id, provider, dataset, unit_id, execution_id, transport, flush_seq)
TTL toDateTime(window_started_at) + INTERVAL 395 DAY DELETE;
