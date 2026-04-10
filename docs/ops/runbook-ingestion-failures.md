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
Trigger a manual sync for the affected provider:
```bash
dev-hops sync testops --provider github --owner <org> --repo <repo>
```

### 2. Data Backfill
If data is missing for a specific period, use the backfill command:
```bash
dev-hops backfill run --since 2024-01-01 --until 2024-01-07 --repo <repo_id>
```

### 3. Clear Watermarks
If the sync is stuck due to a bad cursor, you may need to clear the watermark in the metadata store (PostgreSQL) to force a full re-sync of recent data.

## Alert Thresholds
- **Sync Latency**: Alert if `last_synced` for any active repo is > 4 hours old.
- **Failure Rate**: Alert if > 10% of ingestion attempts fail over a 1-hour window.
- **Auth Errors**: Alert immediately on any `AuthenticationException`.
