CREATE TABLE IF NOT EXISTS backfill_log (
    job_id String,
    org_id String,
    chunk_index UInt32,
    chunk_since Date,
    chunk_before Date,
    provider String,
    items_synced UInt32,
    duration_ms UInt64,
    status String,
    error_message String DEFAULT '',
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (org_id, job_id, chunk_index);
