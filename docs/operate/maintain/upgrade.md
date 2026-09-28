---
page_id: op-upgrade
summary: Upgrade an immutable reviewed revision with backups, migration control, health checks, and rollback criteria.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Upgrade Dev Health

1. Review release, configuration, schema, queue, and compatibility changes.
2. Back up required stores and capture current configuration references.
3. Define health, data-progress, and rollback criteria.
4. Pause or bound high-volume backfills if the release requires it.
5. Apply migrations through the supported release path.
6. Deploy the immutable revision.
7. Verify API, workers, queues, stores, product freshness, and one source path.
8. Roll back only according to schema compatibility and retained evidence.

Do not assume application rollback also reverses an irreversible data migration.

## Ask Dev sequential-tool-call wire contract compatibility

The OpenAI-compatible provider's readiness certification fingerprint folds in
a `READINESS_VERSION` constant. This deploy bumps it (v2 -> v3) because the
outbound wire contract changed: native tool requests now send
`parallel_tool_calls` (gated by model family). A v2 certification never
demonstrated that its endpoint accepts the new parameter.

**Expected state immediately after deploy**: every previously stored
readiness certification -- platform and BYO -- reads as not-current. Ask Dev
capability endpoints report degraded/not-ready readiness until preflight
re-runs and re-certifies each configured provider connection. This is
expected, one-time, self-healing behavior, not an incident: no data migration
or manual database change is required.

**Remediation**: run Ask Dev preflight/readiness certification for the
platform connection and every configured workspace BYO connection after this
deploy. Do not treat the transient not-ready state as a provider outage or
attempt to restore the prior fingerprint.
