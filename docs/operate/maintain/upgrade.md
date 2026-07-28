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
