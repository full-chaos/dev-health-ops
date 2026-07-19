---
page_id: op-rb-worker
summary: Recover when workers are unavailable, queues stop advancing, or retries repeat without progress.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Worker or queue failure

1. Identify queue, worker class, oldest age, affected operations, and last progress.
2. Confirm consumers are deployed with matching routing configuration.
3. Check store connectivity, leases, timeouts, retries, circuit breakers, and terminal failures.
4. Stop unsafe retry amplification before adding concurrency.
5. Recover one bounded job and verify downstream writes.
6. Restore normal concurrency only after queue age declines.

Escalate when job idempotency, tenant isolation, data corruption, or repeated worker loss is uncertain.
