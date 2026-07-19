---
page_id: use-no-data
summary: Distinguish measured zero, unavailable, incomplete, stale, and delayed product results.
content_type: troubleshooting
owner: product-analytics
source_of_truth:
  - current product state behavior
  - src/dev_health_ops/api/services/investment.py
applicability: current
lifecycle: active
---

# Diagnose no or incomplete data

Use this page when a workflow is empty, partially populated, unexpectedly stale, or missing a source.

## 1. Preserve the failing context

Record the workspace, repository or team, time window, filters, workflow, visible state, and approximate time. Do not include credentials or customer-sensitive content.

## 2. Check the selected context

1. Confirm the intended workspace and scope.
2. Remove optional filters only to test whether they excluded all matches.
3. Use a wider period only as a coverage test; do not silently change the analytical question.
4. Confirm the feature is available to the workspace and your role.

## 3. Distinguish the state

| State | Meaning | Next step |
| --- | --- | --- |
| Measured zero | The supported calculation returned zero | Review the exact metric contract and evidence. |
| Unavailable | No supported value exists for the context | Check applicability, permissions, or prerequisites. |
| Incomplete | Required source or processed input is missing | Check source connection, scope coverage, and sync state. |
| Stale | A value exists but is older than the question requires | Check synchronization freshness. |
| Delayed | Ingestion, computation, or report processing has not completed | Check status and retry only through supported controls. |
| Empty response | The backend returned no rows or required storage is unavailable | Escalate with the preserved context. |

## 4. Escalate at the right boundary

- Workspace configuration, source connection, or coverage: [Check synchronization status and freshness](../../admin/sync-and-coverage/status-and-freshness.md).
- Platform ingestion or worker failure: [Recover from ingestion failure](../../operate/runbooks/ingestion-failure.md).

Do not expose credentials in screenshots, issue comments, logs, or support evidence.
