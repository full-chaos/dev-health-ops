CREATE TABLE IF NOT EXISTS dora_metrics_daily (
  repo_id UUID,
  day Date,
  metric_name String,
  value Float64,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, metric_name);
