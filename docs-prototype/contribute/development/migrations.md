---
page_id: con-migrations
summary: Design and test Postgres and ClickHouse migrations with compatibility, idempotency, rollback, and repair evidence.
content_type: task-guide
owner: engineering
applicability: current
lifecycle: active
---

# Create and validate database migrations

1. Identify the owning store and current schema source.
2. Define forward transformation, compatibility window, application sequencing, and rollback limits.
3. Make retries or repeated runs safe where the migration framework requires it.
4. Test empty, representative, large, partially migrated, and failure states.
5. Verify old and new application behavior for the intended compatibility window.
6. Add repair behavior only when the invariant and destructive scope are explicit.
7. Record operational checks and backup requirements.

Do not assume an application rollback reverses a data migration.
