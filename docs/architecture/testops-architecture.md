# TestOps Architecture

## Overview
TestOps extends the Dev Health platform to provide deep visibility into CI/CD pipeline health, test reliability, and code coverage. It enables teams to identify bottlenecks in their delivery process, detect flaky tests before they impact productivity, and track coverage trends across services and repositories.

## Data Pipeline

### Ingestion Layer
The TestOps ingestion layer fetches raw execution data from CI/CD providers and parses test/coverage artifacts.

- **CI/CD Connectors** (`src/dev_health_ops/connectors/testops/`):
    - `github_actions.py`: Fetches workflow and job runs from GitHub Actions.
    - `gitlab_ci.py`: Fetches pipeline and job data from GitLab CI.
    - `base.py`: Defines the `BasePipelineAdapter` and `PipelineSyncBatch` contracts.
- **Test Result Processors** (`src/dev_health_ops/processors/testops_tests.py`):
    - Parses JUnit XML and other test report formats.
    - Normalizes test suites and cases into canonical models.
- **Coverage Processors** (`src/dev_health_ops/processors/testops_coverage.py`):
    - Parses Cobertura, LCOV, and JaCoCo reports.
    - Aggregates coverage metrics at the snapshot level.

### Storage Layer
TestOps data is persisted in ClickHouse for high-performance analytics. The schema is defined in `src/dev_health_ops/migrations/clickhouse/029_testops_tables.sql`.

- **Raw Event Tables**:
    - `ci_pipeline_runs`: Extended with TestOps fields (duration, queue time, trigger source).
    - `ci_job_runs`: Individual job/stage execution data.
    - `test_suite_results`: Aggregated suite-level outcomes.
    - `test_case_results`: Atomic test case outcomes, including failure messages and stack traces.
    - `coverage_snapshots`: Aggregate and per-file coverage data.

- **Daily Metrics Tables**:
    - `testops_pipeline_metrics_daily`: Daily rollups of pipeline performance.
    - `testops_test_metrics_daily`: Daily rollups of test reliability and flakiness.
    - `testops_coverage_metrics_daily`: Daily rollups of code coverage trends.

### Metrics Layer
Metrics are computed daily from raw event tables and persisted in the daily metrics tables.

- **Pipeline Health**: Success rates, failure rates, and cancellation rates.
- **Pipeline Performance**: Median and P95 duration, average and P95 queue time.
- **Test Reliability**: Pass rates, failure rates, and flake detection.
- **Flake Detection**: Identified when a test case flips between pass and fail within the same run window or exhibits inconsistent behavior across retries.
- **Coverage Trends**: Line and branch coverage percentages, coverage deltas, and regression tracking.

### Entity Resolution
TestOps uses path-based attribution to map test suites and coverage snapshots to services and teams.

- **Service Mapping**: The `attribute_service_from_path` logic in `testops_tests.py` inspects file paths (e.g., `services/auth-service/...`) to attribute work to specific services.
- **Ownership Attribution**: Services are linked to teams via the platform's central entity catalog, allowing TestOps metrics to roll up to team and organization levels.

## Metric Definitions

| Metric | Definition | Unit | Table | Computation |
| :--- | :--- | :--- | :--- | :--- |
| **Success Rate** | Share of pipeline runs that completed successfully. | ratio | `testops_pipeline_metrics_daily` | `success_count / pipelines_count` |
| **Median Duration** | Median time from pipeline start to finish. | seconds | `testops_pipeline_metrics_daily` | `quantile(0.5)(duration_seconds)` |
| **P95 Queue Time** | 95th percentile time spent waiting in queue. | seconds | `testops_pipeline_metrics_daily` | `quantile(0.95)(queue_seconds)` |
| **Flake Rate** | Share of test cases exhibiting inconsistent outcomes. | ratio | `testops_test_metrics_daily` | `flake_count / total_cases` |
| **Pass Rate** | Share of executed test cases that passed. | ratio | `testops_test_metrics_daily` | `passed_count / total_cases` |
| **Line Coverage** | Percentage of code lines covered by tests. | percent | `testops_coverage_metrics_daily` | `lines_covered / lines_total` |
| **Coverage Delta** | Change in coverage percentage from prior day. | percent | `testops_coverage_metrics_daily` | `current_pct - prior_pct` |
| **Rerun Rate** | Share of pipeline runs that were retries. | ratio | `testops_pipeline_metrics_daily` | `retry_count / pipelines_count` |
