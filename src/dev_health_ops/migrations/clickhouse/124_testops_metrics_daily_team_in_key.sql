-- Migration 124: team_id joins the sorting key of the three TestOps metrics tables (CHAOS-9468).
--
-- A repository with several main owners (a full ownership tie, D5990) counts whole in each owner in
-- every family whose daily table keys a row by team. testops_pipeline_metrics_daily,
-- testops_test_metrics_daily and testops_coverage_metrics_daily were ReplacingMergeTree(computed_at)
-- ORDER BY (org_id, repo_id, day) (migration 096): a second owner row of the same repository and day
-- was merged away, so a tie showed only the first owner. With team_id in the key every main owner keeps
-- its own row. team_id stays Nullable(String): a repository with no owner keeps one row with a NULL
-- team_id, its own key bucket, hence allow_nullable_key.
--
-- Rows of an earlier run that carry another team_id (the repository had another owner then) are not
-- collapsed by the engine any more, so every reader takes the newest computed_at of each
-- (org_id, repo_id, day) as a whole before it reads a team.
--
-- Rebuild: shadow table with the new key, copy, atomic EXCHANGE, drop the parked old table. A leftover
-- shadow of an interrupted run is dropped first. Rows written between the copy and the exchange are
-- lost for that run only: the daily family rewrites the day on its next run.
DROP TABLE IF EXISTS testops_pipeline_metrics_daily_new;

CREATE TABLE IF NOT EXISTS testops_pipeline_metrics_daily_new
(
    repo_id UUID,
    day Date,
    pipelines_count UInt32,
    success_count UInt32,
    failure_count UInt32,
    cancelled_count UInt32,
    success_rate Float64,
    failure_rate Float64,
    cancel_rate Float64,
    rerun_rate Float64,
    median_duration_seconds Nullable(Float64),
    p95_duration_seconds Nullable(Float64),
    avg_queue_seconds Nullable(Float64),
    p95_queue_seconds Nullable(Float64),
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, day, team_id)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO testops_pipeline_metrics_daily_new
(
    repo_id, day, pipelines_count, success_count, failure_count, cancelled_count,
    success_rate, failure_rate, cancel_rate, rerun_rate,
    median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds,
    team_id, service_id, org_id, computed_at
)
SELECT
    repo_id, day, pipelines_count, success_count, failure_count, cancelled_count,
    success_rate, failure_rate, cancel_rate, rerun_rate,
    median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds,
    team_id, service_id, org_id, computed_at
FROM testops_pipeline_metrics_daily;

EXCHANGE TABLES testops_pipeline_metrics_daily AND testops_pipeline_metrics_daily_new;

DROP TABLE IF EXISTS testops_pipeline_metrics_daily_new;

DROP TABLE IF EXISTS testops_test_metrics_daily_new;

CREATE TABLE IF NOT EXISTS testops_test_metrics_daily_new
(
    repo_id UUID,
    day Date,
    total_cases UInt32,
    passed_count UInt32,
    failed_count UInt32,
    skipped_count UInt32,
    quarantined_count UInt32,
    pass_rate Float64,
    failure_rate Float64,
    flake_rate Float64,
    retry_dependency_rate Float64,
    total_suites UInt32,
    suite_duration_p50_seconds Nullable(Float64),
    suite_duration_p95_seconds Nullable(Float64),
    failure_recurrence_score Float64,
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, day, team_id)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO testops_test_metrics_daily_new
(
    repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count,
    pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites,
    suite_duration_p50_seconds, suite_duration_p95_seconds, failure_recurrence_score,
    team_id, service_id, org_id, computed_at
)
SELECT
    repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count,
    pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites,
    suite_duration_p50_seconds, suite_duration_p95_seconds, failure_recurrence_score,
    team_id, service_id, org_id, computed_at
FROM testops_test_metrics_daily;

EXCHANGE TABLES testops_test_metrics_daily AND testops_test_metrics_daily_new;

DROP TABLE IF EXISTS testops_test_metrics_daily_new;

DROP TABLE IF EXISTS testops_coverage_metrics_daily_new;

CREATE TABLE IF NOT EXISTS testops_coverage_metrics_daily_new
(
    repo_id UUID,
    day Date,
    line_coverage_pct Nullable(Float64),
    branch_coverage_pct Nullable(Float64),
    lines_total Nullable(UInt32),
    lines_covered Nullable(UInt32),
    coverage_delta_pct Nullable(Float64),
    uncovered_files_count UInt32,
    coverage_regression_count UInt32,
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, day, team_id)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO testops_coverage_metrics_daily_new
(
    repo_id, day, line_coverage_pct, branch_coverage_pct, lines_total, lines_covered,
    coverage_delta_pct, uncovered_files_count, coverage_regression_count,
    team_id, service_id, org_id, computed_at
)
SELECT
    repo_id, day, line_coverage_pct, branch_coverage_pct, lines_total, lines_covered,
    coverage_delta_pct, uncovered_files_count, coverage_regression_count,
    team_id, service_id, org_id, computed_at
FROM testops_coverage_metrics_daily;

EXCHANGE TABLES testops_coverage_metrics_daily AND testops_coverage_metrics_daily_new;

DROP TABLE IF EXISTS testops_coverage_metrics_daily_new;
