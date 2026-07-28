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

For Ask Dev persistence migration `0068`, keep the feature disabled while investigating.
Confirm all six tables exist together (`dev_conversations`, `dev_messages`,
`dev_runs`, `dev_tool_calls`, `dev_feedback`, and
`dev_conversation_tombstones`) and that the domain worker role has only
`SELECT, UPDATE, DELETE` on conversations plus `SELECT, INSERT` on tombstones.
The conversation `UPDATE` grant is required only for `FOR UPDATE` row locking;
the retention worker never updates conversation columns. Do not
manually delete child tables: conversation deletion owns the cascade. A
pre-release downgrade is supported only after all binaries that import the new
models are stopped; after release, restore forward from the migration and the
approved backup instead of using the destructive downgrade.
