# v1 River compatibility spike

This artifact records the sanitized local Phase 0 results for CHAOS-3034. It is
compatibility evidence, not production performance evidence. Exact pins and
individual gate states are in
[`compatibility-matrix.json`](compatibility-matrix.json); the measured values
from the reproducible run are in
[`local-harness-results.json`](local-harness-results.json).

## Tested Python path

The local harness used:

- Python 3.13.14;
- `riverqueue` 0.7.0;
- SQLAlchemy 2.0.49;
- `asyncpg` 0.31.0;
- SQLAlchemy's ordinary async engine configuration for direct PostgreSQL;
- SQLAlchemy `NullPool`, disabled client/prepared-statement caches, and unique
  prepared-statement names for PgBouncer transaction-mode safety;
- both direct PostgreSQL and PgBouncer 1.25.2 transaction-mode connections;
- the River 0.40.0 PostgreSQL schema.

No DSN, credential, job payload, container ID, or raw database/log output is
stored in this repository.

## Verified results

For both direct PostgreSQL and transaction-mode PgBouncer:

- committing a domain transaction plus a standard Python insert produced
  exactly one River job;
- the committed row preserved queue, priority, maximum attempts, and scheduled
  state/availability;
- rolling the transaction back produced no River job.

This proves the base SQLAlchemy/asyncpg transaction boundary under the tested
connection settings. It does not make the client production-compatible.

## Blocking incompatibility

`riverqueue` 0.7.0 unique insertion fails against the River 0.40.0 schema with
`asyncpg.exceptions.InvalidColumnReferenceError`. The client emits
`ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL`, but River
migration 006 replaces and drops the matching `(kind, unique_key)` index in
favor of the newer `unique_states` design. PostgreSQL therefore cannot match
the client's conflict target to a qualifying unique constraint/index.

Primary source pointers:

- [`riverqueue` 0.7.0 unique insert SQL](https://github.com/riverqueue/riverqueue-python/blob/v0.7.0/src/riverqueue/driver/riversqlalchemy/dbsqlc/river_job.sql)
- [River 0.40.0 migration 006](https://github.com/riverqueue/river/blob/v0.40.0/riverdriver/riverpgxv5/migration/main/006_bulk_unique.up.sql)

Because uniqueness is a required job policy and the query depends on River's
internal schema, the Python client is a **NO-GO production dependency**. Raw
Python writes to River tables are rejected for the same schema-coupling reason.
The selected transition is the language-neutral `worker_job_outbox` with a Go
relay described in the [Phase 0 ADR](../../../../decisions/chaos-3034-river-compatibility.md).

## Go, rolling-version, load, and failure results

The checked-in harness ran with Go 1.25.9, River/`riverpgxv5` 0.40.0, River
N-1 0.39.0, a 250 ms fetch interval, and 20 execution samples per mode.

- Direct PostgreSQL passed execution, retry, scheduled-state, cross-client
  running cancellation, connection, and load gates. Queue-start p50/p95 was
  3.882/28.269 ms against the compatibility p95 limit of 100 ms.
- Transaction-mode PgBouncer with `PollOnly` passed execution, retry,
  scheduled-state, connection, and load gates. Queue-start p50/p95 was
  158.603/233.811 ms against the compatibility p95 limit of 600 ms.
- River 0.39 migrated a fresh database through schema 6 and worked a job;
  River 0.40 then applied schema 7; River 0.39 inserted on schema 7; and River
  0.40 consumed that job.
- A real `SIGKILL` of an executing worker was rescued as attempt 2 in both
  direct and PollOnly profiles, with one recorded first-attempt error.
- Go consumed the committed Python payload through both connection profiles;
  the exact fixture is also decoded by the Python and River 0.39 tests.

These values are local compatibility measurements, not production SLOs or
capacity evidence.

## PollOnly architecture blocker

With `riverpgxv5` 0.40.0 and `PollOnly=true`, neither a separate client nor the
worker's own client propagated `JobCancel` to the context of an already running
job. Each path was observed for approximately 750 ms, three fetch intervals.
The harness then used a conspicuously recorded test-only worker release so the
remaining matrix could run; that release is not counted as River cancellation.

The behavior matches the implementation boundary: PollOnly skips listener
startup, while the no-listener local-control helper returns early for a driver
that advertises listener support. River's 0.40 changelog also scopes its
poll-only cancellation fix to a single process. Primary pointers:

- [PollOnly listener startup and configuration](https://github.com/riverqueue/river/blob/v0.40.0/client.go#L914-L932)
- [`JobCancel` notification path](https://github.com/riverqueue/river/blob/v0.40.0/client.go#L1423-L1471)
- [no-listener local-control helper](https://github.com/riverqueue/river/blob/v0.40.0/client.go#L2848-L2868)
- [River 0.40 cancellation fix scope](https://github.com/riverqueue/river/blob/v0.40.0/CHANGELOG.md#fixed)

The architecture result is therefore:

- **GO** for River 0.40.0 with a direct PostgreSQL or session-capable
  queue-control endpoint;
- **NO-GO** for transaction-mode PgBouncer PollOnly as the sole production
  queue-control path while running-job cancellation is a runtime requirement;
- **NO-GO** for `riverqueue` 0.7.0 as a production dependency.

Phase 1 may proceed only with direct/session queue control as a hard deployment
prerequisite, or after a separately designed and verified cancellation control
plane closes this blocker. If neither is available, the broker decision must be
reopened. Missing production Celery baseline values continue to block every
production canary. Full generic-outbox claim/insert/mark crash-window proof is
a Phase 1 foundation gate because Phase 1 owns that relay implementation.
