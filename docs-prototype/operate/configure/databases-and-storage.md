---
page_id: op-db
summary: Configure supported data stores, connection pools, migrations, retention, and recovery boundaries.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/ops/database-connection-pooling.md
  - current migration and storage implementation
applicability: current
lifecycle: active
---

# Databases and storage

1. Use the supported data-store engines and connection URIs for the reviewed revision.
2. Configure least-privilege application and migration identities.
3. Size connection pools against service concurrency and store limits.
4. Apply migrations through one controlled release path.
5. Monitor storage growth, compaction, query latency, and failed writes.
6. Back up data and migration state before high-risk changes.
7. Test restore in an isolated environment.

Do not repair schema or delete data from a generic documentation command. Use the current migration or incident procedure and retain evidence.
