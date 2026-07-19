---
page_id: op-workers
summary: Monitor worker readiness, queue age, leases, retries, failures, and completion evidence.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/workers.md
  - docs/ops/investment-materialization.md
applicability: current
lifecycle: active
---

# Workers, jobs, retries, and schedules

1. Confirm each configured queue has the intended consumers.
2. Monitor queue depth and oldest age, not only worker process count.
3. Monitor job duration, lease expiry, retry reason, terminal failure, and completion rate.
4. Confirm scheduled jobs are not overlapping beyond safe capacity.
5. Verify downstream writes and product freshness after completion.
6. Use the supported retry or replay control only after identifying idempotency and provider-budget effects.

A busy worker is not proof of progress; a quiet queue is not proof that scheduled work was created.
