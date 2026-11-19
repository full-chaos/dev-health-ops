# TestOps Architecture

## Overview
TestOps extends the Dev Health platform to provide deep visibility into CI/CD pipeline health, test reliability, and code coverage. It enables teams to identify bottlenecks in their delivery process, detect flaky tests before they impact productivity, and track coverage trends across services and repositories.

## Data Pipeline

### Ingestion Layer
The TestOps ingestion layer fetches raw execution data from CI/CD providers and parses test/coverage artifacts.

- **CI/CD Provider Adapters** (`src/dev_health_ops/providers/<provider>/`):
    - `providers/github/testops_pipeline.py`: Fetches workflow and job runs from GitHub Actions.
    - `providers/gitlab/testops_pipeline.py`: Fetches pipeline and job data from GitLab CI.
    - `providers/_base.py`: Defines the `BasePipelineAdapter` and `PipelineSyncBatch` contracts.
    - `connectors/testops/`: Legacy compatibility re-export shims only.
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
      A coverage day whose record is gone (the run of its newest coverage snapshot moved to another day) is retired on the next
      recompute of that day: the family writes a row with `retracted = 1` over the key, and readers take the newest row of a key
      first and leave the key out when that row is a retraction, so the day shows no point, never 0 (migration 161). Days that are
      not recomputed keep the old row until a redrive.

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
| **Success Rate** | Share of completed pipeline runs (succeeded or failed) that succeeded; cancelled, skipped and unfinished runs are not in the rate. | ratio | `testops_pipeline_metrics_daily` | `success_count / (success_count + failure_count)` |
| **Failure Rate** | Share of completed pipeline runs (succeeded or failed) that failed; with Success Rate it adds up to 100 %. | ratio | `testops_pipeline_metrics_daily` | `failure_count / (success_count + failure_count)` |
| **Median Duration** | Median time from pipeline start to finish. | seconds | `testops_pipeline_metrics_daily` | `quantile(0.5)(duration_seconds)` |
| **P95 Queue Time** | 95th percentile time spent waiting in queue. | seconds | `testops_pipeline_metrics_daily` | `quantile(0.95)(queue_seconds)` |
| **Flake Rate** | Share of test cases exhibiting inconsistent outcomes. | ratio | `testops_test_metrics_daily` | `flake_count / total_cases` |
| **Pass Rate** | Share of executed test cases that passed. | ratio | `testops_test_metrics_daily` | `passed_count / total_cases` |
| **Line Coverage** | Percentage of code lines covered by tests. | percent | `testops_coverage_metrics_daily` | `lines_covered / lines_total` |
| **Coverage Delta** | Change in coverage percentage from prior day. | percent | `testops_coverage_metrics_daily` | `current_pct - prior_pct` |
| **Rerun Rate** | Share of pipeline runs that were retries. | ratio | `testops_pipeline_metrics_daily` | `retry_count / pipelines_count` |
| **Run status counts** | Every counted run of the day is in exactly one bucket: success (`success`, `succeeded`, `passed`), failure (`failure`, `failed`, `error`, `errors`, `timeout`, `timed_out`), cancelled (`cancelled`, `canceled`, `cancel`), skipped (`skipped`), unfinished (`running`, `queued`), other (every other status: `stale`, `completed` with no conclusion, `canceling`, an unknown or empty status). A `deleted` run is in no bucket. A row written before these columns existed reads NULL, not 0. | count | `testops_pipeline_metrics_daily` | `pipelines_count = success_count + failure_count + cancelled_count + skipped_count + unfinished_count + other_count` |
| **Stale unfinished runs** | The unfinished runs whose `ci_pipeline_runs` row was last synced more than the open-run lookback (6 h, `testops.OpenRunRefreshLookback`) before `computed_at`: they may be finished already. A GitLab skipped pipeline is stored as skipped and counts in the skipped bucket, as a GitHub one does; GitLab rows stored as cancelled before that change stay cancelled until a re-sync of their day. | count | `testops_pipeline_metrics_daily` | `unfinished_stale_count` |
