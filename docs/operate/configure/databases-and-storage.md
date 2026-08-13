---
page_id: op-db
summary: Configure Postgres semantic state, direct River queue control, ClickHouse analytics, Valkey coordination, migrations, retention, and recovery boundaries.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - docs/architecture/data-pipeline.md
  - docs/architecture/dispatch-outbox.md
  - current migration and storage implementation
applicability: current
lifecycle: active
---

# Databases and storage

Dev Health uses different stores for different guarantees. PostgreSQL owns semantic and control-plane state, ClickHouse owns high-volume analytics and canonical source facts, and Valkey/Redis coordinates Celery delivery and distributed controls. The additive Go worker foundation also uses PostgreSQL-backed River queue state through a deliberately separate direct connection.
{: .fc-page-lede }

## Store responsibilities

| Store | Primary responsibility |
| --- | --- |
| PostgreSQL | Organizations, users, settings, encrypted provider credentials, source registration, job/run control state, webhook bindings, operational authority, and River job state |
| ClickHouse | Provider facts, work items, commits, incidents, analytics, derived metrics, and product materializations |
| Valkey or Redis | Celery broker and result backend, rate/budget coordination, stream delivery, and bounded ephemeral claims |

A queue, cache, or stream is not the system of record unless its contract explicitly says so. Product-visible history remains in domain tables even when execution records are retained for a shorter period.

## PostgreSQL connection model

### Python API and Celery domain access

Horizontal API and Celery fleets can multiply SQLAlchemy pools. Use transaction-mode PgBouncer where appropriate and set:

```dotenv
POSTGRES_URI="postgresql+asyncpg://...@pgbouncer:6432/devhealth"
PGBOUNCER_TRANSACTION_MODE=true
```

Run schema migrations against PostgreSQL directly, not through transaction-mode PgBouncer.

### Go worker coexistence

The Go foundation uses three distinct responsibilities:

| Purpose | Setting | Default maximum | Required endpoint |
| --- | --- | ---: | --- |
| Domain state | `POSTGRES_URI` | `WORKER_DOMAIN_DATABASE_MAX_CONNS=4` | Transaction-mode PgBouncer is supported |
| River queue control | `WORKER_DATABASE_URI` | `WORKER_DATABASE_MAX_CONNS=2` | Direct PostgreSQL |
| One-shot migrations | `MIGRATION_DATABASE_URI` | 2 migration connections | Direct PostgreSQL with the migration role |

`WORKER_DATABASE_MODE` defaults to `direct`. Transaction mode is rejected for River queue control because cancellation and listener behavior are not compatible with that pooling model. Session mode remains unsupported until it passes the same compatibility evidence.

Do not give long-running workers the migration DSN. Do not reuse the migration role for domain or queue-control access.

## Runtime roles

Provision distinct unprivileged login roles for:

- semantic/domain reads and writes;
- River queue control;
- one-shot migrations.

The domain role may access semantic tables but must not administer River or Alembic metadata. The queue role may operate River and the relay-owned outbox fields but must not gain general semantic-table authority. The migration role applies schema and grants but is not a runtime identity.

For an existing database, use the checked-in role-provisioning script as the database owner:

```bash
psql "$MIGRATION_DATABASE_URI" \
  --set=domain_role=devhealth_domain \
  --set=queue_role=devhealth_queue \
  --file=scripts/worker/provision_river_roles.sql
```

Then build `POSTGRES_URI` with the domain role and `WORKER_DATABASE_URI` with the queue role. Their usernames must match the declared role settings exactly.

## Migrations

Use one controlled migration process per release. The current deployment examples run application migrations and, where enabled, River migrations from a one-shot job. API and worker processes run with ambient migration disabled.

Before rollout:

1. back up semantic data and migration state;
2. verify the direct migration endpoint and role;
3. apply migrations once;
4. confirm PostgreSQL and ClickHouse status;
5. verify runtime role grants and River schema compatibility;
6. start or roll application processes only after migration succeeds.

The canonical incident cutover records no production downgrade to the legacy incident writer/reader after the new ordering contract is admitted. Rollback must use the compatible bridge and current schema rather than reintroducing an old binary that cannot understand the contract.

## Connection budgets

Size pools against the maximum deployment topology, not current replicas. Account for:

- SQLAlchemy pools across API and Celery processes;
- PgBouncer server pools per database/user pair;
- direct River queue-control connections;
- operator CLI invocations;
- migration and administrative reserve.

Monitor both client saturation and PostgreSQL server slots. A growing worker fleet can exhaust the database even when queue volume appears modest.

## Retention and recovery

River terminal execution rows have bounded retention independent of product history. Backups must cover:

- PostgreSQL semantic and control state;
- ClickHouse source facts and materializations;
- migration versions and role/grant configuration;
- required secret-manager data;
- deployment configuration needed to reconstruct queue and worker ownership.

Full dead worker-outbox rows also have bounded retention, but their minimal delivery-abandonment facts do not. Include `worker_job_delivery_abandonments` in PostgreSQL backup and restore validation. It contains no job arguments or detailed error text; its dedupe key, job kind, terminal timestamp, attempt count, and bounded error code are the durable evidence that prevents a retained scheduled-report run from receiving a fresh delivery budget after outbox cleanup.

Test restore in an isolated environment. Do not use ad hoc schema repair or data deletion from a generic documentation command; use the current migration or incident procedure and retain evidence.

Ask Dev conversation state lives in PostgreSQL. Its only supported retention
values are 0 and 30 days. The 30-day expiry is persisted on each conversation
and moves with its latest activity; the retention worker consumes that exact
timestamp and must not apply a second horizon. The zero-day path removes
content as soon as a request terminalizes. A daily bounded cleanup at 05:30 UTC
is staged to repair expired rows with `FOR UPDATE SKIP LOCKED` batches and is
replay-safe after the v3 producer compatibility cutover.

Migration `0069` adds the `(org_id, user_id, started_at)` and
`(org_id, started_at)` run indexes used by the serialized per-user and
per-organization admission checks. It is additive and contains no content
rewrite. Downgrading `0069` removes only those indexes; disabling Ask Dev
remains the normal rollback and preserves retained conversations.

Disabling the Ask Dev feature is the normal application rollback. The additive
tables remain dormant, existing LLM workflows are unchanged, and previously
saved conversations continue through their selected retention or explicit
delete path. Do not downgrade the database while a binary that imports the Ask
Dev models is deployed. The pre-release downgrade rehearsal for migration
`0068` drops only the six `dev_*` tables.
