-- TestOps Phase 0: Raw event tables and metrics tables.
-- Extends existing ci_pipeline_runs with TestOps-specific columns.

-- Extended pipeline run fields (ALTERs on existing table).
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS pipeline_name Nullable(String);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS provider LowCardinality(String) DEFAULT '';
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS duration_seconds Nullable(Float64);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS queue_seconds Nullable(Float64);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS retry_count UInt32 DEFAULT 0;
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS cancel_reason Nullable(String);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS trigger_source Nullable(String);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS commit_hash Nullable(String);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS branch Nullable(String);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS pr_number Nullable(UInt32);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS team_id Nullable(String);
ALTER TABLE ci_pipeline_runs ADD COLUMN IF NOT EXISTS service_id Nullable(String);

-- Job runs within a pipeline.
CREATE TABLE IF NOT EXISTS ci_job_runs (
    repo_id UUID,
    run_id String,
    job_id String,
    job_name String,
    stage Nullable(String),
    status Nullable(String),
    started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')),
    duration_seconds Nullable(Float64),
    runner_type Nullable(String),
    retry_attempt UInt32 DEFAULT 0,
    org_id LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, run_id, job_id);

-- Test suite results.
CREATE TABLE IF NOT EXISTS test_suite_results (
    repo_id UUID,
    run_id String,
    suite_id String,
    suite_name String,
    framework Nullable(String),
    environment Nullable(String),
    total_count UInt32,
    passed_count UInt32,
    failed_count UInt32,
    skipped_count UInt32,
    error_count UInt32 DEFAULT 0,
    quarantined_count UInt32 DEFAULT 0,
    retried_count UInt32 DEFAULT 0,
    duration_seconds Nullable(Float64),
    started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')),
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, run_id, suite_id);

-- Test case results.
CREATE TABLE IF NOT EXISTS test_case_results (
    repo_id UUID,
    run_id String,
    suite_id String,
    case_id String,
    case_name String,
    class_name Nullable(String),
    status LowCardinality(String),
    duration_seconds Nullable(Float64),
    retry_attempt UInt32 DEFAULT 0,
    failure_message Nullable(String),
    failure_type Nullable(String),
    stack_trace Nullable(String),
    is_quarantined UInt8 DEFAULT 0,
    org_id LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, run_id, suite_id, case_id);

-- Coverage snapshots.
CREATE TABLE IF NOT EXISTS coverage_snapshots (
    repo_id UUID,
    run_id String,
    snapshot_id String,
    report_format Nullable(String),
    lines_total Nullable(UInt32),
    lines_covered Nullable(UInt32),
    line_coverage_pct Nullable(Float64),
    branches_total Nullable(UInt32),
    branches_covered Nullable(UInt32),
    branch_coverage_pct Nullable(Float64),
    functions_total Nullable(UInt32),
    functions_covered Nullable(UInt32),
    commit_hash Nullable(String),
    branch Nullable(String),
    pr_number Nullable(UInt32),
    team_id Nullable(String),
    service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, run_id, snapshot_id);

-- TestOps pipeline metrics (daily rollups).
CREATE TABLE IF NOT EXISTS testops_pipeline_metrics_daily (
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
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

-- TestOps test reliability metrics (daily rollups).
CREATE TABLE IF NOT EXISTS testops_test_metrics_daily (
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
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

-- TestOps coverage metrics (daily rollups).
CREATE TABLE IF NOT EXISTS testops_coverage_metrics_daily (
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
) ENGINE MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (repo_id, day);

-- AI report plans (persisted for saved/scheduled reports).
CREATE TABLE IF NOT EXISTS report_plans (
    plan_id String,
    report_type LowCardinality(String),
    audience Nullable(String),
    scope_teams Array(String),
    scope_repos Array(String),
    scope_services Array(String),
    time_range_start Nullable(Date),
    time_range_end Nullable(Date),
    comparison_period Nullable(String),
    sections Array(String),
    requested_metrics Array(String),
    requested_charts Array(String),
    include_insights UInt8 DEFAULT 1,
    include_anomalies UInt8 DEFAULT 1,
    confidence_threshold LowCardinality(String) DEFAULT 'direct_fact',
    org_id LowCardinality(String) DEFAULT '',
    created_at DateTime('UTC')
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (org_id, plan_id);

-- Provenance records for generated artifacts.
CREATE TABLE IF NOT EXISTS report_provenance (
    provenance_id String,
    artifact_type LowCardinality(String),
    artifact_id String,
    plan_id String,
    data_sources Array(String),
    metrics_used Array(String),
    time_range_start Nullable(Date),
    time_range_end Nullable(Date),
    filters_applied String DEFAULT '{}',
    generator_version String DEFAULT '',
    org_id LowCardinality(String) DEFAULT '',
    generated_at DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(generated_at)
ORDER BY (plan_id, artifact_type, artifact_id);
