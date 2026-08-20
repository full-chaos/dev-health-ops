# Runbook: TestOps Ingestion Failures

## Overview
This runbook provides diagnostic and recovery steps for failures in the TestOps data ingestion pipeline, including CI/CD events, test results, and coverage snapshots.

## Common Failure Modes

### 1. API Rate Limits
- **Symptoms**: Logs show 403 or 429 status codes from GitHub or GitLab.
- **Cause**: The ingestion process has exceeded the provider's API rate limit.
- **Resolution**: Wait for the rate limit to reset. Consider increasing the `per_page` setting or reducing sync frequency.

### 2. Authentication Failures
- **Symptoms**: `AuthenticationException` in logs; 401 status codes.
- **Cause**: Expired or invalid `GITHUB_TOKEN` or `GITLAB_TOKEN`.
- **Resolution**: Verify and update the relevant environment variables.

### 3. Malformed Artifacts
- **Symptoms**: Errors during JUnit XML or coverage report parsing.
- **Cause**: Invalid XML structure or unsupported report format.
- **Resolution**: Inspect the raw artifact from the CI provider. Ensure the test framework is configured to output standard JUnit XML.

### 4. Network Timeouts
- **Symptoms**: `ConnectTimeout` or `ReadTimeout` errors.
- **Cause**: Intermittent network issues or slow provider APIs.
- **Resolution**: The system will automatically retry. If persistent, check the provider's status page.

## Diagnostic Steps

### 1. Check Application Logs
Search for TestOps-specific errors:
```bash
grep -i "testops" /var/log/dev-health-ops.log
```

### 2. Verify ClickHouse Data
Check the latest synced records for a specific repository:
```sql
SELECT repo_id, max(last_synced) 
FROM ci_pipeline_runs 
WHERE repo_id = '...' 
GROUP BY repo_id;
```

For Cockpit/Govern risk surfaces, also confirm the derived analytics inputs are
present. Delivery Risk/TestOps requires all three TestOps daily rollups;
Compounding Risk requires review latency in `repo_metrics_daily` plus complexity
rows in `repo_complexity_daily`:

```sql
SELECT 'testops_pipeline_metrics_daily' AS table_name, count() AS rows FROM testops_pipeline_metrics_daily
UNION ALL
SELECT 'testops_test_metrics_daily', count() FROM testops_test_metrics_daily
UNION ALL
SELECT 'testops_coverage_metrics_daily', count() FROM testops_coverage_metrics_daily
UNION ALL
SELECT 'repo_metrics_daily.review_latency', count() FROM repo_metrics_daily WHERE pr_first_review_p90_hours IS NOT NULL
UNION ALL
SELECT 'repo_complexity_daily.complexity', count() FROM repo_complexity_daily WHERE cyclomatic_per_kloc IS NOT NULL;
```

### 3. Inspect Job Status
Check for failed jobs in the `ci_job_runs` table:
```sql
SELECT job_name, status, started_at 
FROM ci_job_runs 
WHERE status = 'failure' 
ORDER BY started_at DESC 
LIMIT 10;
```

## Recovery Procedures

### 1. Manual Sync Trigger
Trigger manual syncs for the affected provider. CI/CD pipeline runs/jobs and test
artifacts are separate sync targets; run both when Delivery Risk/TestOps surfaces
are missing pipeline, test, or coverage inputs:
```bash
dev-hops sync cicd --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner <org> \
  --repo <repo>

dev-hops sync tests --provider github \
  --auth "$GITHUB_TOKEN" \
  --owner <org> \
  --repo <repo>
```

### 2. Recompute Daily Metrics
After source rows are restored, recompute the daily TestOps rollups for the
affected window. Use `--repo-id` for a single repository or omit it for all repos:
```bash
dev-hops metrics daily \
  --since 2024-01-01 \
  --before 2024-01-08 \
  --repo-id <repo_id>
```

For Compounding Risk, daily metrics restore the review-latency input, but the
complexity input is produced by the complexity job. Rebuild complexity before
recomputing the composite score:

```bash
dev-hops metrics complexity \
  --since 2024-01-01 \
  --before 2024-01-08 \
  --repo-id <repo_id>

dev-hops metrics compounding-risk \
  --org <org_id> \
  --since 2024-01-01 \
  --before 2024-01-08
```

### 3. Historical Backfill
If the source sync configuration itself needs a historical replay, prefer the
admin backfill API described in `workers.md`. For inline recovery, use the
sync-configuration based command:

```bash
dev-hops backfill run \
  --config-id <sync_config_uuid> \
  --since 2024-01-01 \
  --before 2024-01-08
```

### 4. Clear Watermarks
If the sync is stuck due to a bad cursor, you may need to clear the watermark in the metadata store (PostgreSQL) to force a full re-sync of recent data.

## Alert Thresholds
- **Sync Latency**: Alert if `last_synced` for any active repo is > 4 hours old.
- **Failure Rate**: Alert if > 10% of ingestion attempts fail over a 1-hour window.
- **Auth Errors**: Alert immediately on any `AuthenticationException`.
