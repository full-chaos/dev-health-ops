---
page_id: op-health
summary: Verify API, worker, queue, storage, migration, and first-ingestion health after installation.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Verify first health

1. Confirm the API health endpoint responds through the intended internal and external path.
2. Confirm required migrations completed exactly once.
3. Confirm workers are connected to the expected queues and schedules.
4. Confirm Redis or the supported queue/rate-limit store and primary data stores are healthy.
5. Confirm logs contain no repeated authentication, migration, or connection failures.
6. Run one bounded provider synchronization with a known repository or project.
7. Verify records advance into a product freshness or coverage surface.

Do not enable broad backfills until this bounded path succeeds.
