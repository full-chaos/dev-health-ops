-- Migration 023b: Create dora_metrics_daily table.
-- This was originally 020_dora_metrics.sql but was placed in the repo-root
-- migrations/ directory instead of the package migrations/ directory used by
-- the runner. Numbered 023b so it sorts after 023_capacity_forecasts and
-- before 024_add_org_id (which ALTERs this table).

CREATE TABLE IF NOT EXISTS dora_metrics_daily (
  repo_id UUID,
  day Date,
  metric_name String,
  value Float64,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, metric_name);
