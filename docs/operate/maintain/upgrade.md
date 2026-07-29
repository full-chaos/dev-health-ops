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

## Ask Dev Wave 1 scope-search compatibility

The Wave 1 authorized-scope delivery adds the `devScopeSearch` GraphQL field and
typed candidate/input enums. It does not add or migrate storage: resolution uses
the existing ClickHouse entity catalogs. Deploy the API schema before a web
consumer starts querying the field, and regenerate or verify downstream
GraphQL types against the deployed schema.

Application rollback removes the additive field without a data rollback. Keep
Ask Dev interaction surfaces disabled until their canonical contracts and
authenticated API path are deployed together. This backend-only delivery adds
no in-product copy or screenshots; user-visible scope and ambiguity states are
owned by the later shared Ask Dev interaction surface.

## Ask Dev Wave 1 metric compatibility

The Wave 1 metric delivery adds `devMetricCatalog` and `devMetric` without a
storage migration. Deploy the API schema only after the existing daily metric
tables, organization columns, and current readers are present. Regenerate
downstream GraphQL types before enabling a consumer.

The registry is intentionally closed at eight metrics. A rollback removes the
additive GraphQL fields and shared query service without reverting data. Do not
replace `deploy_metrics_daily` change-failure semantics with the older PR-revert
measure during rollback or compatibility work, and do not use the singular
`work_item_state_duration_daily` name: the supported table is
`work_item_state_durations_daily`.

## Ask Dev Wave 1 evidence and data-health compatibility

The evidence delivery adds `devEvidenceSearch` and `devDataHealth` as additive
typed GraphQL fields plus the shared `search_evidence.v1`, `get_evidence.v1`,
and `data_health.v1` application services. It adds no database migration,
index, embedding store, vector store, or MCP dependency. Deploy the Ops schema
before enabling consumers and regenerate downstream GraphQL types.

Before enabling either field for users, verify the organization has an allowed
decision for the canonical explicit-enable `ask_dev` feature. It is
default-denied for every tier; missing, invalid, and unreadable decisions fail
closed. Do not replace this decision with plan-name checks, and do not couple
independent platform/admin Context Fabric diagnostics to the user-facing
entitlement.

Evidence IDs are deterministic HMAC handles derived with `JWT_SECRET_KEY` and
the persisted canonical evidence descriptor. Rotation of that key invalidates
previous handles, so coordinate key rotation with retained-conversation policy
and the evidence-expansion API. The authenticated expansion route requires the
owning `answer_id`, returns `dev_evidence_expansion.v1`, and reauthorizes every
reference before reading source content. Rollback removes the additive fields,
services, and route without a storage rollback. Verify native evidence remains
available when ACR is absent or unavailable, and keep Ask Dev interaction
surfaces disabled until the matching web contract artifacts are deployed.
