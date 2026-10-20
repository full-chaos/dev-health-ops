CREATE TABLE IF NOT EXISTS worklogs (
    work_item_id String,
    provider String,
    worklog_id String,
    author Nullable(String),
    started_at DateTime64(6, 'UTC'),
    time_spent_seconds Int64,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    last_synced DateTime64(6, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (provider, worklog_id);
