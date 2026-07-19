---
page_id: op-workers-config
summary: Configure worker queues, concurrency, leases, retries, schedules, and provider budgets as one system.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/workers.md
  - current worker and synchronization settings
applicability: current
lifecycle: active
---

# Workers, schedules, and queues

1. Identify every worker class and queue consumed by the deployment.
2. Match routing settings to workers that actually consume the named queues.
3. Configure concurrency from provider, queue, and store capacity.
4. Configure leases, stale detection, retry limits, and backoff deliberately.
5. Configure schedules so recurring work cannot overlap beyond safe capacity.
6. Verify one bounded job, retry, and failure path.

Do not enable a queue-routing option before the corresponding workers are deployed.
