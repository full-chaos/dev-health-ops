# Runbook: AI Report Generation Failures

## Overview
This runbook covers diagnostic and recovery steps for failures in the AI Generative Reports pipeline, from prompt parsing to report planning.

## Common Failure Modes

### 1. Invalid Prompt
- **Symptoms**: `PlanningResult` returns `ok=False` with validation errors.
- **Cause**: The prompt contains unsupported metrics, unknown entities, or an invalid time range.
- **Resolution**: Refine the prompt to use canonical metric names or recognized team/repo names.

### 2. Missing Metrics
- **Symptoms**: Report plan is created, but charts are empty or missing data.
- **Cause**: The requested metrics have not been computed for the specified time range or scope.
- **Resolution**: Ensure that the daily metrics rollups (`dev-hops metrics daily`) have completed successfully.

### 3. Empty Data Scope
- **Symptoms**: Valid plan, but no data found in ClickHouse.
- **Cause**: The selected teams or repos had no activity during the requested time range.
- **Resolution**: Expand the time range or check the scope of the report.

### 4. LLM Provider Failures
- **Symptoms**: Timeouts or errors during prompt parsing or insight generation.
- **Cause**: Issues with the underlying LLM provider (e.g., Anthropic, OpenAI).
- **Resolution**: Check the provider's status page. The system will automatically retry transient failures.

## Diagnostic Steps

### 1. Inspect Planning Validation
Check the `invalid_reasons` field in the `ParsedPrompt` or the `ValidationResult` in the `PlanningResult`.
- `invalid_time_range`: The start date is after the end date.
- `unresolved_metrics`: The prompt uses terms that don't map to the registry.
- `unresolved_entities`: The requested teams or repos were not found in the catalog.

### 2. Verify Metric Availability
Query ClickHouse to ensure data exists for the requested metric and scope:
```sql
SELECT count() 
FROM testops_pipeline_metrics_daily 
WHERE repo_id = '...' AND day >= '2024-01-01';
```

### 3. Check Report Plan Persistence
Verify that the `ReportPlan` was successfully persisted in the `report_plans` table:
```sql
SELECT * FROM report_plans WHERE plan_id = '...';
```

## Recovery Procedures

### 1. Adjust Prompt
If the failure is due to an invalid prompt, try a more explicit request:
- Use "past 7 days" instead of vague time references.
- Use full repository names (e.g., `org/repo`).
- Reference metrics from the canonical registry (e.g., "cycle time", "success rate").

### 2. Trigger Metrics Computation
If metrics are missing, manually trigger the daily rollup:
```bash
dev-hops metrics daily --day 2024-04-10
```

### 3. Re-run Report Request
Once the underlying data or prompt issues are resolved, re-submit the report request through the API or UI.

## Escalation Paths
- **Parsing Logic Issues**: Escalate to the Engineering team if the parser consistently fails to recognize valid intents.
- **Data Discrepancies**: Escalate to the Data Engineering team if persisted metrics do not match expectations.
- **LLM Reliability**: Escalate to the Platform team if provider timeouts exceed acceptable thresholds.
