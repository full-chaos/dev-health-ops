---
page_id: con-storage
summary: Preserve Postgres semantic authority, ClickHouse analytics, Celery coordination, River queue control, outbox delivery, migrations, and tenant isolation.
content_type: architecture
owner: engineering
source_of_truth:
  - docs/architecture/database-architecture.md
  - docs/architecture/data-pipeline.md
  - docs/architecture/dispatch-outbox.md
  - docs/ops/database-connection-pooling.md
  - current migrations and sink code
applicability: current
lifecycle: active
---

# Data and storage boundaries

Dev Health separates semantic authority, analytics, asynchronous coordination, and execution state. Contributors must preserve those boundaries when adding a provider, job, metric, webhook, or migration.
{: .fc-page-lede }

## Store ownership

- **PostgreSQL** stores organizations, users, settings, encrypted credentials, integration sources, webhook bindings, job/run control state, licensing decisions, operational authority, audit intents, and River execution state.
- **ClickHouse** stores high-volume provider facts, canonical operational events, work items, commits, analytics, and derived materializations.
- **Valkey/Redis** backs Celery queues/results, provider budget coordination, selected streams, and bounded claims.
- **River** is a PostgreSQL-backed execution queue for the additive Go foundation; it is not yet the production owner of current jobs.
- **Domain run tables** remain product-visible execution history. Bounded queue rows are not a replacement for durable domain evidence.

## Python and Go PostgreSQL access

The Python API and Celery runtime use semantic PostgreSQL access. Transaction-mode PgBouncer is supported when prepared-statement behavior is disabled through the configured engine path.

The Go coexistence foundation splits database responsibilities:

- `POSTGRES_URI` — semantic/domain access; transaction-mode PgBouncer is supported;
- `WORKER_DATABASE_URI` — direct PostgreSQL River queue control;
- `MIGRATION_DATABASE_URI` — direct elevated one-shot migration access.

The domain, queue, and migration identities must be distinct. Runtime usernames must match the declared role names. Long-running processes never receive the migration DSN.

## Role boundaries

The domain role can read and write semantic state but cannot administer River or Alembic metadata. The queue role can operate River and relay-owned outbox state but cannot access unrelated semantic tables. The migration role creates and upgrades schema and refreshes grants but is not used by a long-running process.

Readiness checks effective privileges, not merely successful login. A role that can inherit broader authority, create schema objects, or cross the domain/queue boundary fails closed.

## Durable outbox paths

A durable outbox separates a committed domain decision from asynchronous publication.

The generic `worker_job_outbox` path is route-safe:

1. the producer commits a job intent with its domain state;
2. the producer refuses to enqueue unless the checked-in migration route is executable;
3. the Go reconciler claims eligible rows;
4. the relay rechecks route ownership before inserting River work;
5. known Celery-routed rows remain untouched;
6. unknown or invalid kinds terminalize with bounded evidence rather than disappearing.

The domain role can insert and inspect producer-owned rows but cannot forge relay state. The queue role can claim and retire relay-owned state but cannot create producer intent.

## Canonical incident ordering

PagerDuty REST and webhook events, Customer Push, and future verified providers must use the shared canonical operational identity and ordering contract. A source-specific writer cannot create a parallel correctness protocol.

Webhook authority comes from the persisted binding. Durable deduplication uses bounded source/event identity and raw-body identity. Out-of-order events use the canonical ordering builder and current-row reader.

After the canonical incident contract cutover is admitted, production rollback cannot reintroduce a legacy writer or reader that does not understand the current ordering schema.

## ClickHouse writes

ClickHouse writes must preserve:

- organization and provider-instance scope;
- canonical external identity;
- source and observation timestamps;
- idempotent or replacement semantics appropriate to the table engine;
- raw provider context required for audit without leaking secrets;
- compatibility with current materializations and readers.

A missing provider transition or absent bounded-page result is unknown, not automatically a tombstone.

## Migration rules

Every migration needs:

- forward schema and data behavior;
- mixed-version compatibility where a rolling deployment requires it;
- an explicit writer/read barrier for incompatible cutovers;
- bounded backfill or copy behavior;
- resumable checkpoints and idempotency evidence;
- role/grant updates;
- health/readiness impact;
- rollback or an explicit no-downgrade decision.

Migrations run through one controlled process. Workers, APIs, and schedulers do not ambient-migrate.

## Tenant isolation

Every identity, dedupe key, outbox row, queue admission, query, and canonical write must preserve organization authority before aggregation. Provider payloads, URL parameters, or guessed namespace names cannot override server-owned organization and source bindings.

Use [Databases and storage](../../operate/configure/databases-and-storage.md) for operator configuration and [Platform architecture](platform.md) for the end-to-end execution path.
