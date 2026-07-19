---
page_id: admin-sync-status
summary: Verify that a configured source covers the selected scope and has completed recent synchronization.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - current provider connection and synchronization surfaces
applicability: current
lifecycle: active
---

# Check synchronization status and freshness

Use this procedure after a user has preserved the failing workspace, scope, period, and workflow.

## Check the administrative boundary

1. Confirm the expected source connection exists for the workspace.
2. Confirm its credential or installation is still valid without exposing the secret value.
3. Confirm the repository or team belongs to the configured source and scope.
4. Check the latest successful synchronization or ingestion time.
5. Check whether a backfill or processing job is still active, delayed, or failed.
6. Compare the source and processing timestamps with the product time window.

## Result

- If the source is healthy and current, return to the product workflow and reproduce with the same context.
- If data is still processing, communicate the visible state rather than representing it as zero.
- If ingestion or workers are failing, escalate to [Recover from ingestion failure](../../operate/runbooks/ingestion-failure.md) with identifiers and timestamps but no credentials.
