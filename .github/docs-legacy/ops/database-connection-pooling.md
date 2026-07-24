# Database connection pooling (PgBouncer)

> **Why this exists (CHAOS-2065).** The semantic Postgres engine opens a
> SQLAlchemy/asyncpg connection pool **per worker process**. A large Celery
> fleet (or any horizontally-scaled API/worker tier) multiplies those pools and
> can exhaust Postgres `max_connections` (default ~100–500) long before the job
> volume itself is a problem. PgBouncer in **transaction mode** multiplexes many
> client connections onto a small server-connection set, decoupling fleet size
> from the Postgres connection ceiling.

## TL;DR

- Put a **transaction pooler** in front of Postgres. This can be a local
  PgBouncer service, a sidecar/shared PgBouncer tier, or a managed hosted
  pooler.
- Point the app's `DATABASE_URI` / `POSTGRES_URI` at that transaction-pooling
  endpoint and set **`PGBOUNCER_TRANSACTION_MODE=true`**.
- Run **schema migrations against Postgres directly** (bypass PgBouncer).

## Go worker dual-pool contract

The additive Go runtime uses two deliberately different PostgreSQL pools. The
domain and queue-control responsibilities must not be collapsed into one DSN:

| Purpose | Configuration | Default maximum | Required endpoint |
| --- | --- | ---: | --- |
| Domain state | `POSTGRES_URI` | `WORKER_DOMAIN_DATABASE_MAX_CONNS=4` | Existing transaction-mode PgBouncer endpoint is supported |
| River queue control | `WORKER_DATABASE_URI` | `WORKER_DATABASE_MAX_CONNS=2` | Direct PostgreSQL; a session-mode endpoint requires separate compatibility evidence |
| One-shot migrations | `MIGRATION_DATABASE_URI` | 2 migration connections | Direct PostgreSQL with the dedicated migration role |

Each URI also supports the shared `_FILE` secret form. Secret values are
mutually exclusive with their inline form. `WORKER_DATABASE_MODE` defaults to
`direct`; `transaction` is rejected because River cancellation and listener
semantics are incomplete under transaction-mode `PollOnly`, and `session`
remains rejected until the same compatibility matrix passes against a real
session-mode endpoint.

Long-running workers receive the domain and queue-control roles only. They do
not receive `MIGRATION_DATABASE_URI` and never apply River or application
migrations. The one-shot migration process receives a dedicated elevated DSN;
the non-secret `RIVER_DOMAIN_DATABASE_ROLE` and
`RIVER_QUEUE_DATABASE_ROLE` identifiers tell it which runtime grants to apply.
They default to `devhealth_domain` and `devhealth_queue` in the checked-in
deployment profiles, but production may override both names. Every worker and
the operator CLI bind the `POSTGRES_URI` and `WORKER_DATABASE_URI` usernames
to those declared names before opening pools; a mismatch is a fail-closed
configuration error, not a fallback to whatever login the DSN supplied.
The River schema defaults to `river`; migration grants its
queue role only the schema/table/sequence access needed by the runtime and does
not grant the domain role or `PUBLIC` access. Separately, the domain role gets
`USAGE` on `public`, DML on every semantic table, and sequence access. The
producer-owned `worker_job_outbox` is the domain-role exception: the domain
role receives
only `SELECT` and `INSERT`, so it can deduplicate and enqueue but cannot forge
relay state or delete committed dispatches. It gets no schema `CREATE`, no
River access, and no access to `alembic_version`; the queue role gets no general
semantic-table or sequence access. Its `public` allowlist is `SELECT`, `UPDATE`,
and `DELETE` on `worker_job_outbox`; `SELECT` and `UPDATE` on
`sync_dispatch_outbox`; and `SELECT` only on
`sync_dispatch_transport_routes`. PostgreSQL row-locking clauses require
mutation authority on the locked relation, so the reconciler locks only the
outbox row and rechecks the SELECT-only route generation in its terminal write.
A concurrent generation change therefore rolls back the claim, River
`InsertTx`, and terminal mark atomically when it commits before the terminal
write. A future route-mutation surface must separately serialize or quiesce the
post-terminal, pre-commit window. The queue role never receives outbox
`INSERT`, sync outbox `DELETE`, or route `INSERT`/`UPDATE`/`DELETE`. Readiness checks
effective privileges and all role memberships, including
`NOINHERIT` memberships that could be activated with `SET ROLE`. Runtime roles
must have no role memberships at all. Neither role can gain database or schema
`CREATE` or table-maintenance capabilities; the domain role cannot access
River, Alembic metadata, or public functions or mutate relay-owned outbox
state, and the queue role cannot access other semantic tables or public
functions or exceed that allowlist. These checks return stable health categories
only and never emit a DSN, grant detail, or database error.

### Provision the runtime roles first

The migration intentionally does not create production login roles or choose
their passwords. Before setting `MIGRATION_DATABASE_URI`, provision the two
distinct, unprivileged roles named by `RIVER_DOMAIN_DATABASE_ROLE` and
`RIVER_QUEUE_DATABASE_ROLE`. The migration command checks that both exist
and are unprivileged LOGIN roles before applying any River DDL. After Alembic
runs, the same one-shot command refreshes domain grants across the current
semantic tables and sequences, then grants River access only to the queue role.
This avoids broad default privileges and keeps migration metadata unavailable
to long-running workers.

For an existing database, run the idempotent provisioning script as the
database owner. It prompts for both passwords without echo; managed-database
installations may perform the equivalent operation through their provider or
secret-management workflow:

```bash
psql "$MIGRATION_DATABASE_URI" \
  --set=domain_role=devhealth_domain \
  --set=queue_role=devhealth_queue \
  --file=scripts/worker/provision_river_roles.sql
```

Then construct `POSTGRES_URI` with the domain role and
`WORKER_DATABASE_URI` with the queue role. The URI usernames must match the two
role-name settings exactly. Do not reuse the migration role in either runtime
DSN.

Worker readiness verifies effective authorization with read-only catalog
queries, not only connectivity. `domain_postgres` remains failed unless the
active role is an unprivileged LOGIN role with `USAGE` but no effective
`CREATE` on `public`, DML on every semantic table, and access to every semantic
sequence. A new Alembic table without a matching grant therefore fails closed
until the one-shot migration refreshes grants. A legacy cluster that still
grants `CREATE` on `public` to `PUBLIC` also fails this check; after assessing
other database users, remediate explicitly as the database owner:

```sql
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
```

The provisioning scripts do not make that cluster-wide change implicitly.

Fresh `compose.yml` volumes create the same local-only roles automatically.
Because `/docker-entrypoint-initdb.d` runs only on first initialization,
existing local volumes must run the script once before opting into the River
migration. The local upgrade form uses the checked-in development passwords:

```bash
docker compose exec -T postgres \
  psql -U postgres -d postgres \
  --set=domain_password=devhealth_domain \
  --set=queue_password=devhealth_queue \
  < scripts/worker/provision_river_roles.sql
```

The canonical Go worker deployment profile manifest at
`deploy/go-workers/profiles.json` calculates budgets from every profile's
`max_replicas`, even while all Phase 1 profiles remain disabled by default.
The current maximum topology uses:

- 22 direct River queue-control connections, including one bounded operator
  CLI invocation;
- 50 PgBouncer domain client connections, including that operator; and
- 87 PostgreSQL server slots after adding two 25-connection PgBouncer server
  pools (the existing app role and the Go domain role), 22 direct queue-control
  connections, and a 15-connection server reserve, below
  `max_connections=100`.

`worker-contractcheck validate` rejects registry/profile drift, runtime exposure
of the migration DSN, or any budget increase that crosses those ceilings.

River terminal-row cleanup is configured independently of product history:

- `RIVER_COMPLETED_JOB_RETENTION=168h`;
- `RIVER_CANCELLED_JOB_RETENTION=720h`;
- `RIVER_DISCARDED_JOB_RETENTION=720h`; and
- `RIVER_JOB_CLEANER_TIMEOUT=30s`.

Product-visible history remains in domain run tables; the River rows are
bounded execution records.

## The connection-budget math

```
peak_server_connections  ≈  worker_processes × sessions_per_job × hold_time_fraction
```

Without a pooler, each process holds up to `pool_size + max_overflow`
connections (defaults: `20 + 10 = 30`). The binding constraint is:

```
Σ(per-process SQLAlchemy pools)   <   Postgres max_connections
```

So ~3–4 fully-busy worker processes already approach a default
`max_connections=100`. With PgBouncer the budget becomes:

```
pgbouncer default_pool_size × number_of_(db,user)_pairs × pgbouncer_instances
        <   Postgres max_connections
```

Clients connect to PgBouncer (`max_client_conn`, e.g. 1000) and PgBouncer keeps
only `default_pool_size` (e.g. 25) **server** connections busy per pool. Tune so
the **server** side stays under `max_connections`; the **client** side can be
large and cheap.

| Knob | Where | Meaning |
| --- | --- | --- |
| `max_client_conn` | PgBouncer | how many app connections may attach |
| `default_pool_size` | PgBouncer | server connections kept open **per (db,user)** |
| `POSTGRES_POOL_SIZE` / `POSTGRES_MAX_OVERFLOW` | app env | per-process SQLAlchemy pool (used only on the **direct** path) |
| `max_connections` | Postgres | hard server ceiling — must exceed the sum of all pooler server pools |

## App configuration

`src/dev_health_ops/db.py` chooses the engine strategy from
`PGBOUNCER_TRANSACTION_MODE`:

- **`true`** (upstream endpoint behaves like transaction pooling): the async
  engine uses `NullPool` and disables asyncpg prepared-statement caching/naming via
  `connect_args={"timeout": 10.0, "statement_cache_size": 0, "prepared_statement_name_func": <uuid>}`.
  The sync engine uses `NullPool`.
- **unset / `false`** (direct connection): the async/sync engines use a
  SQLAlchemy `QueuePool` with `pool_pre_ping` and `POSTGRES_POOL_SIZE` /
  `POSTGRES_MAX_OVERFLOW` (defaults 20 / 10).

`POSTGRES_CONNECT_TIMEOUT_SECONDS` controls the asyncpg connection timeout
(default `10`). Invalid or non-positive values fall back to `10`.

### Why prepared statements must be disabled

Transaction pooling hands each client transaction a possibly-different server
connection. asyncpg (and the SQLAlchemy asyncpg dialect) use **server-side
prepared statements** by default, which live on one specific server connection —
so a follow-up that lands on a different connection fails with
`prepared statement "__asyncpg_stmt_*" does not exist` (or `already exists`).
Disabling the statement cache and using unique statement names removes the
cross-connection dependency.

> Requires **SQLAlchemy ≥ 2.0.18** (`prepared_statement_name_func`). The repo
> pins `sqlalchemy[asyncio]>=2.0.49`.

## Migrations bypass the pooler

Run `dev-hops migrate postgres` against Postgres **directly**, not through
transaction-mode PgBouncer. Migrations can rely on session-scoped behavior that
transaction pooling does not preserve. The dedicated
`MIGRATION_DATABASE_URI` activates both the existing Alembic upgrade and the
pinned River migration; it must use a role distinct from both runtime roles:

```bash
MIGRATION_DATABASE_URI="postgresql://devhealth_migrate:secret@postgres:5432/postgres" \
RIVER_DOMAIN_DATABASE_ROLE="devhealth_domain" \
RIVER_QUEUE_DATABASE_ROLE="devhealth_queue" \
  dev-hops migrate postgres
```

Existing Alembic-only installations may continue to supply a direct
`POSTGRES_URI`; without either form of `MIGRATION_DATABASE_URI`, the additive
River step is skipped. An explicitly configured but empty migration secret is
an error. Deployment templates therefore omit or unset empty placeholders
before invoking the compatibility path.

## Local development

`ops/compose.yml` ships a `pgbouncer` service (transaction mode, listening on
`6432`) in front of `postgres`. The `api`, `billing-edge`, and `worker` services
route `DATABASE_URI` / `POSTGRES_URI` through `pgbouncer:6432` and set
`PGBOUNCER_TRANSACTION_MODE=true`.

```bash
docker compose -f compose.yml up -d postgres pgbouncer
# inspect pools
psql -h localhost -p 6432 -U postgres -d pgbouncer -c "SHOW POOLS;"
```

The same pattern is mirrored in the platform stack (`dev-health/compose.yml`).

## Production topology

Production typically uses a **managed Postgres**. If the provider has a hosted
transaction pooler, point the app directly at that hosted endpoint. A local
`pgbouncer` compose service is not required in that topology.

For a hosted transaction pooler:

```env
POSTGRES_URI=postgresql+asyncpg://<user>:<pw>@<db-host>/devhealth?sslmode=require
DATABASE_URI=postgresql+asyncpg://<user>:<pw>@<db-host>/devhealth?sslmode=require
PGBOUNCER_TRANSACTION_MODE=true
POSTGRES_CONNECT_TIMEOUT_SECONDS=10
```

`PGBOUNCER_TRANSACTION_MODE=true` means the upstream endpoint behaves like
transaction pooling. It does not mean a local `pgbouncer` container must be
running.

If you operate PgBouncer yourself, deploy it either as a sidecar next to each
app/worker node or as a small shared tier, in transaction mode, in front of
managed Postgres. Required PgBouncer settings:

```ini
pool_mode = transaction
max_client_conn = 1000        ; size to fleet
default_pool_size = 25        ; size so Σ server pools < Postgres max_connections
auth_type = scram-sha-256
```

Set the app's `DATABASE_URI` / `POSTGRES_URI` to the transaction-pooling endpoint
and `PGBOUNCER_TRANSACTION_MODE=true` so asyncpg prepared statements are
disabled. Prefer `sslmode=require` in shared environment URLs: async app paths
normalize it to asyncpg's `ssl=require`, while sync worker/helper paths keep or
translate it to libpq-compatible `sslmode=require`.

Production Docker readiness should call `/ready`. `/health` remains a deep
dependency check for observability and may return 503 while Postgres,
ClickHouse, or Redis is temporarily unavailable.

## Validation

Smoke-validated (CHAOS-2065): 100 concurrent async sessions across two waves
through transaction-mode PgBouncer using the real `db.py` engine config —
`NullPool`, zero `prepared statement does not exist` errors, `SHOW POOLS`
reporting `transaction`. Engine-config invariants are guarded by
`tests/test_db_pgbouncer.py`.
