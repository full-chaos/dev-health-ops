---
page_id: con-storage
summary: Preserve Postgres semantic state, ClickHouse analytics state, queue/outbox boundaries, migrations, and tenant isolation.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/architecture/database-architecture.md
  - docs/architecture/data-pipeline.md
  - docs/architecture/dispatch-outbox.md
  - current migrations and sink code
applicability: current
lifecycle: active
---

# Data and storage boundaries

- Postgres stores semantic and control-plane state such as organizations, users, settings, credentials, source registration, and durable ingestion state.
- ClickHouse stores high-volume source facts, analytics, and derived metric materializations.
- Redis/Valkey and worker queues coordinate asynchronous delivery, idempotency, rate, and retry behavior.
- Outbox or durable-pointer patterns separate committed state from asynchronous dispatch.

Every data path must preserve tenant scope before deduplication or aggregation. Migrations need explicit forward, compatibility, rollback, and repair behavior. A cache or queue is not the system of record unless the contract says so.
