-- Additional quality, delivery, and collaboration metrics.

ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_size_p50_loc Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_size_p90_loc Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_comments_per_100_loc Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_reviews_per_100_loc Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS rework_churn_ratio_30d Float64;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS single_owner_file_ratio_30d Float64;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS review_load_top_reviewer_ratio Float64;

CREATE TABLE IF NOT EXISTS review_edges_daily (
  repo_id UUID,
  day Date,
  reviewer String,
  author String,
  reviews_count UInt32,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, reviewer, author, day);

CREATE TABLE IF NOT EXISTS cicd_metrics_daily (
  repo_id UUID,
  day Date,
  pipelines_count UInt32,
  success_rate Float64,
  avg_duration_minutes Nullable(Float64),
  p90_duration_minutes Nullable(Float64),
  avg_queue_minutes Nullable(Float64),
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE TABLE IF NOT EXISTS deploy_metrics_daily (
  repo_id UUID,
  day Date,
  deployments_count UInt32,
  failed_deployments_count UInt32,
  deploy_time_p50_hours Nullable(Float64),
  lead_time_p50_hours Nullable(Float64),
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE TABLE IF NOT EXISTS incident_metrics_daily (
  repo_id UUID,
  day Date,
  incidents_count UInt32,
  mttr_p50_hours Nullable(Float64),
  mttr_p90_hours Nullable(Float64),
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);
