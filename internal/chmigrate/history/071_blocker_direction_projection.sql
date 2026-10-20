-- GAP-ASKDEV-BLOCKER-DIRECTION-01: canonical source --blocks--> target semantics.
ALTER TABLE work_item_dependencies
    ADD COLUMN IF NOT EXISTS relationship_semantics_version String DEFAULT 'legacy.v1';

CREATE TABLE IF NOT EXISTS work_graph_projection_runs (
    org_id String,
    projection_name String,
    scope_repo_id Nullable(UUID),
    rule_version String,
    input_watermark Nullable(DateTime64(3, 'UTC')),
    row_count UInt64,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, projection_name, ifNull(scope_repo_id, toUUID('00000000-0000-0000-0000-000000000000')));
