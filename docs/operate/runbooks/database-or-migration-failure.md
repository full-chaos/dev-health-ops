---
page_id: op-rb-db
summary: Contain and recover database connectivity, storage, schema, or migration failure.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Database, storage, or migration failure

1. Stop new high-volume work if continued writes can increase damage.
2. Identify store, schema revision, migration, affected services, and last successful operation.
3. Check connectivity, credentials, capacity, locks, replication, and storage health.
4. Do not rerun or reverse a migration until its idempotency and compatibility are verified.
5. Restore service or data from the approved recovery point when required.
6. Verify schema, API, workers, writes, reads, and product freshness.

Retain migration output and recovery evidence. Escalate suspected data loss or tenant-isolation impact immediately.
