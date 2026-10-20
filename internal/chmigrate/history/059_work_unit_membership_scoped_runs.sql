CREATE TABLE IF NOT EXISTS work_unit_membership_scoped_runs (
    org_id       String,
    scope_kind   LowCardinality(String),
    scope_id     String,
    run_id       String,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, scope_kind, scope_id, run_id);
