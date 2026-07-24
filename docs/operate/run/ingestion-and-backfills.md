---
page_id: op-ingestion
summary: Run bounded ingestion and backfills while protecting provider budgets, idempotency, and product freshness.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Ingestion and backfill operations

1. Define provider, organization, repositories or projects, record families, and time range.
2. Estimate units, provider budget, queue capacity, and completion window.
3. Start a bounded slice first.
4. Monitor dispatch, running, retrying, failed, and completed units.
5. Verify idempotent writes and watermarks.
6. Expand only after the bounded slice advances product coverage correctly.
7. Record any residual gap or replay requirement.

Avoid repeatedly starting overlapping backfills for the same scope and period.
