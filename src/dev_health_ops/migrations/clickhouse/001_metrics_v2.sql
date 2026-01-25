-- Derived metrics tables for Grafana (developer health).

CREATE TABLE IF NOT EXISTS repo_metrics_daily (
  repo_id UUID,
  day Date,
  commits_count UInt32,
  total_loc_touched UInt32,
  avg_commit_size_loc Float64,
  large_commit_ratio Float64,
  prs_merged UInt32,
  median_pr_cycle_hours Float64,
  pr_cycle_p75_hours Float64,
  pr_cycle_p90_hours Float64,
  prs_with_first_review UInt32,
  pr_first_review_p50_hours Nullable(Float64),
  pr_first_review_p90_hours Nullable(Float64),
  pr_review_time_p50_hours Nullable(Float64),
  pr_pickup_time_p50_hours Nullable(Float64),
  large_pr_ratio Float64,
  pr_rework_ratio Float64,
  mttr_hours Nullable(Float64),
  change_failure_rate Float64,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

CREATE TABLE IF NOT EXISTS user_metrics_daily (
  repo_id UUID,
  day Date,
  author_email String,
  commits_count UInt32,
  loc_added UInt32,
  loc_deleted UInt32,
  files_changed UInt32,
  large_commits_count UInt32,
  avg_commit_size_loc Float64,
  prs_authored UInt32,
  prs_merged UInt32,
  avg_pr_cycle_hours Float64,
  median_pr_cycle_hours Float64,
  pr_cycle_p75_hours Float64,
  pr_cycle_p90_hours Float64,
  prs_with_first_review UInt32,
  pr_first_review_p50_hours Nullable(Float64),
  pr_first_review_p90_hours Nullable(Float64),
  pr_review_time_p50_hours Nullable(Float64),
  pr_pickup_time_p50_hours Nullable(Float64),
  reviews_given UInt32,
  changes_requested_given UInt32,
  reviews_received UInt32,
  review_reciprocity Float64,
  team_id Nullable(String),
  team_name Nullable(String),
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, author_email, day);

CREATE TABLE IF NOT EXISTS commit_metrics (
  repo_id UUID,
  commit_hash String,
  day Date,
  author_email String,
  total_loc UInt32,
  files_changed UInt32,
  size_bucket LowCardinality(String),
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, author_email, commit_hash);

CREATE TABLE IF NOT EXISTS team_metrics_daily (
  day Date,
  team_id LowCardinality(String),
  team_name String,
  commits_count UInt32,
  after_hours_commits_count UInt32,
  weekend_commits_count UInt32,
  after_hours_commit_ratio Float64,
  weekend_commit_ratio Float64,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (team_id, day);

CREATE TABLE IF NOT EXISTS work_item_metrics_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  team_id LowCardinality(String),
  team_name String,
  items_started UInt32,
  items_completed UInt32,
  items_started_unassigned UInt32,
  items_completed_unassigned UInt32,
  wip_count_end_of_day UInt32,
  wip_unassigned_end_of_day UInt32,
  cycle_time_p50_hours Nullable(Float64),
  cycle_time_p90_hours Nullable(Float64),
  lead_time_p50_hours Nullable(Float64),
  lead_time_p90_hours Nullable(Float64),
  wip_age_p50_hours Nullable(Float64),
  wip_age_p90_hours Nullable(Float64),
  bug_completed_ratio Float64,
  story_points_completed Float64,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (
  provider,
  day,
  work_scope_id,
  team_id
);

CREATE TABLE IF NOT EXISTS work_item_user_metrics_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  user_identity String,
  team_id LowCardinality(String),
  team_name String,
  items_started UInt32,
  items_completed UInt32,
  wip_count_end_of_day UInt32,
  cycle_time_p50_hours Nullable(Float64),
  cycle_time_p90_hours Nullable(Float64),
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (provider, work_scope_id, user_identity, day);

CREATE TABLE IF NOT EXISTS work_item_cycle_times (
  work_item_id String,
  provider LowCardinality(String),
  day Date,
  work_scope_id LowCardinality(String),
  team_id Nullable(String),
  team_name Nullable(String),
  assignee Nullable(String),
  type LowCardinality(String),
  status LowCardinality(String),
  created_at DateTime('UTC'),
  started_at Nullable(DateTime('UTC')),
  completed_at Nullable(DateTime('UTC')),
  cycle_time_hours Nullable(Float64),
  lead_time_hours Nullable(Float64),
  computed_at DateTime('UTC')
) ENGINE ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (provider, work_item_id);

CREATE TABLE IF NOT EXISTS work_item_state_durations_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  team_id LowCardinality(String),
  team_name String,
  status LowCardinality(String),
  duration_hours Float64,
  items_touched UInt32,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (provider, work_scope_id, team_id, status, day);

CREATE TABLE IF NOT EXISTS file_metrics_daily (
  repo_id UUID,
  day Date,
  path String,
  churn UInt32,
  contributors UInt32,
  commits_count UInt32,
  hotspot_score Float64,
  computed_at DateTime('UTC')
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day, path);

-- Forward-compatible ALTERs for existing tables created by older versions.
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_cycle_p75_hours Float64;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_cycle_p90_hours Float64;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS prs_with_first_review UInt32;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_first_review_p50_hours Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_first_review_p90_hours Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_review_time_p50_hours Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_pickup_time_p50_hours Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS large_pr_ratio Float64;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS pr_rework_ratio Float64;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS mttr_hours Nullable(Float64);
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS change_failure_rate Float64;

ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_cycle_p75_hours Float64;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_cycle_p90_hours Float64;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS prs_with_first_review UInt32;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_first_review_p50_hours Nullable(Float64);
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_first_review_p90_hours Nullable(Float64);
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_review_time_p50_hours Nullable(Float64);
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_pickup_time_p50_hours Nullable(Float64);
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS reviews_given UInt32;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS changes_requested_given UInt32;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS reviews_received UInt32;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS review_reciprocity Float64;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS team_id Nullable(String);
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS team_name Nullable(String);

ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS work_scope_id LowCardinality(String);
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS items_started_unassigned UInt32;
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS items_completed_unassigned UInt32;
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS wip_unassigned_end_of_day UInt32;
ALTER TABLE work_item_user_metrics_daily ADD COLUMN IF NOT EXISTS work_scope_id LowCardinality(String);
ALTER TABLE work_item_cycle_times ADD COLUMN IF NOT EXISTS work_scope_id LowCardinality(String);
ALTER TABLE work_item_state_durations_daily ADD COLUMN IF NOT EXISTS work_scope_id LowCardinality(String);

-- Update git_pull_requests with metrics columns
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS additions Nullable(UInt32);
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS deletions Nullable(UInt32);
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS changed_files Nullable(UInt32);
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS first_review_at Nullable(DateTime64(3, 'UTC'));
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS first_comment_at Nullable(DateTime64(3, 'UTC'));
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS changes_requested_count UInt32 DEFAULT 0;
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS reviews_count UInt32 DEFAULT 0;
ALTER TABLE git_pull_requests ADD COLUMN IF NOT EXISTS comments_count UInt32 DEFAULT 0;
