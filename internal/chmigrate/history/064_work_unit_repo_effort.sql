CREATE TABLE IF NOT EXISTS work_unit_repo_effort (
    work_unit_id String,
    repo_id Nullable(UUID),
    effort_metric String,
    effort_value Float64,
    allocation_weight Float64,
    allocation_source String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    org_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(computed_at)
ORDER BY (org_id, work_unit_id, ifNull(toString(repo_id), ''))
SETTINGS index_granularity = 8192;
