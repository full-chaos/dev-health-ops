# Database connection pooling (PgBouncer)

> **Why this exists (CHAOS-2065).** The semantic Postgres engine opens a
> SQLAlchemy/asyncpg connection pool **per worker process**. A large Celery
> fleet (or any horizontally-scaled API/worker tier) multiplies those pools and
> can exhaust Postgres `max_connections` (default ~100–500) long before the job
> volume itself is a problem. PgBouncer in **transaction mode** multiplexes many
> client connections onto a small server-connection set, decoupling fleet size
> from the Postgres connection ceiling.

## TL;DR

- Put **PgBouncer (transaction pooling)** in front of Postgres.
- Point the app's `DATABASE_URI` / `POSTGRES_URI` at PgBouncer and set
  **`PGBOUNCER_TRANSACTION_MODE=true`**.
- Run **schema migrations against Postgres directly** (bypass PgBouncer).

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

- **`true`** (behind PgBouncer transaction mode): the async engine uses
  `NullPool` (PgBouncer owns the pool) and disables asyncpg prepared-statement
  caching/naming via
  `connect_args={"statement_cache_size": 0, "prepared_statement_name_func": <uuid>}`.
  The sync engine uses `NullPool`.
- **unset / `false`** (direct connection): the async/sync engines use a
  SQLAlchemy `QueuePool` with `pool_pre_ping` and `POSTGRES_POOL_SIZE` /
  `POSTGRES_MAX_OVERFLOW` (defaults 20 / 10).

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

Run Alembic/`dev-hops migrate postgres` against Postgres **directly**, not
through transaction-mode PgBouncer. Migrations can rely on session-scoped
behavior that transaction pooling does not preserve. Point `POSTGRES_URI` at the
Postgres host (`postgres:5432`) for the migration step and leave
`PGBOUNCER_TRANSACTION_MODE` unset for that invocation, e.g.:

```bash
POSTGRES_URI="postgresql+asyncpg://postgres:postgres@postgres:5432/postgres" \
  dev-hops migrate postgres
```

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

Production typically uses a **managed Postgres**; deploy PgBouncer either as a
sidecar next to each app/worker node or as a small shared tier, in transaction
mode, in front of it. Required PgBouncer settings:

```ini
pool_mode = transaction
max_client_conn = 1000        ; size to fleet
default_pool_size = 25        ; size so Σ server pools < Postgres max_connections
auth_type = scram-sha-256
```

Set the app's `DATABASE_URI` / `POSTGRES_URI` to the PgBouncer endpoint and
`PGBOUNCER_TRANSACTION_MODE=true`. If the managed provider already offers a
transaction-mode pooler (e.g. RDS Proxy, Supabase pooler, Neon pooler), point at
that instead and still set `PGBOUNCER_TRANSACTION_MODE=true` so asyncpg prepared
statements are disabled.

## Validation

Smoke-validated (CHAOS-2065): 100 concurrent async sessions across two waves
through transaction-mode PgBouncer using the real `db.py` engine config —
`NullPool`, zero `prepared statement does not exist` errors, `SHOW POOLS`
reporting `transaction`. Engine-config invariants are guarded by
`tests/test_db_pgbouncer.py`.
