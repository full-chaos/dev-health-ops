---
page_id: con-auth-cp-audit-outbox
summary: How the Auth Control Plane records security mutations - the single transaction that commits state, outbox event and audit row, the database commit guard, the outbox loop and reaper that run in Wave 1 - plus the metrics, the readiness check and a troubleshooting guide.
content_type: task-guide
owner: engineering
source_of_truth:
  - internal/audit/audit.go
  - internal/audit/outboxloop.go
  - internal/audit/publisher.go
  - internal/audit/reaper.go
  - internal/audit/allowlist.go
  - internal/storage/postgres/authschema/migrations/0005_audit_and_outbox.sql
  - internal/storage/postgres/authschema/migrations/0006_audit_commit_guard.sql
  - internal/auth/authruntime/runtime.go
applicability: current
lifecycle: active
---

# Auth Control Plane audit and outbox operations

A security mutation in the Auth Control Plane is three writes that must live or
die together: the state change, an **outbox event** that tells consumers about
it, and a **security audit row** that records it. They commit in one PostgreSQL
transaction or not at all (guardrail G-53). This page covers how that is
enforced, what runs in Wave 1, and how to operate and debug it.
{: .fc-page-lede }

## The write path

`audit.Commit(ctx, pool, schema, audit.Mutation{Apply, Audit, Events})` runs the
caller's `Apply` callback inside a transaction it owns, then writes the outbox
events and the audit row in the same transaction. The outbox and audit inserts
are unexported: there is no exported way to write one without the others, and a
mutation that changes no state rolls back with `ErrNoStateChange` (state = a
guarded row written, migration `0006`, or a new principal or user row, the
`0013` create guard). Any failure rolls back **everything**.

Each outbox row is a `provisioning-event.v1` event (the row is the envelope).
The caller gives the aggregate type, its wire id (`prn_`, `org_`, `idn_` ...),
the verb and the allowlisted `change` members; `Commit` refuses anything the
contract refuses, bumps the aggregate's revision in `aggregate_revisions`
(migration `0013`) inside the same transaction and writes the payload
`revision` and the key `aggregate_type:aggregate_id:revision`. The revision row
lock is held to commit, so revisions are contiguous per aggregate and a
rolled-back commit leaves no gap. A mutation that is not a provisioning event
(session lifecycle) sets `OffStream` instead of `Events` and writes its audit
row only.

The guarantee is held by PostgreSQL for code that does not set out to defeat
it. Migration `0006` puts a trigger on the audit and outbox tables and on the
security-state tables; `Commit` sets a transaction-local setting
(`dev_health.audit_txid`) to the current transaction id as its first statement,
and the trigger refuses a write made under any other id. So a second connection
from the pool, SQL assembled at run time or a write from another package is
refused, because none of them has armed its own transaction.

**The bound, stated as the code states it** (`audit.go` package comment):
the setting is not a secret. Code that already holds the runtime login's write
privilege and deliberately sets `dev_health.audit_txid` to its own transaction
id in its own transaction can write a guarded table with no outbox or audit
companion, and nothing in the database can tell that from the caller's own SQL.
What the database does guarantee is that no *other* role, `PUBLIC` included, can
write a guarded table (`authschema` test
`TestOnlyTheRuntimeAndMigrationRolesCanWriteGuardedTables`). Treat a handler that
sets that setting itself as a review defect. Tables classified unguarded
(sessions, refresh credentials, users, principals, read-only vocabulary) stay a
convention the caller keeps. The Go-side checks and the source scan in
`writer_guard_test.go` are early, legible refusals and a lint, not the control.

The audit row's attributes are an **allowlist** (`allowlist.go`, guardrail
G-65): a closed set of keys, each with a closed value rule (a vocabulary member,
an id of a fixed shape or a whole number). Free text has no slot, and a refused
value is never echoed. The runtime login can only insert and select audit rows;
it cannot update or delete them.

## What runs in Wave 1

`authruntime` builds one **outbox loop** with `audit.NewOutboxLoop(pool, nil,
config)`: a loop with **no sink**. No consumer of auth events exists yet, so:

- **Publishing is off.** A sink-less publisher would mark events delivered that
  nobody received. Events accumulate as pending until a sink is injected, which
  is a one-line change when the first consumer lands.
- **The reaper runs.** Once a second the loop deletes published events older
  than the retention (default 7 days), at most 1000 per sweep. The reaper never
  deletes an unpublished event at any age.
- With a sink, delivery is **at least once**: a process that dies after
  `Deliver` returns and before the row is marked published delivers again.
  Consumers must be idempotent on `idempotency_key` and apply per-aggregate
  revisions so a late or out-of-order event cannot revert newer state (G-54).
  Failures retry with backoff doubling from 2 s up to 10 min, a single delivery
  is bounded to 15 s, and `last_error` stores only a token from a closed set
  (`delivery_failed`, `delivery_timeout`, `consumer_unavailable`,
  `consumer_rejected`), never the error text, which can quote the payload.

Defaults are `audit.DefaultLoopConfig`: poll 1 s, publish limit 50, retention
7 days, reap limit 1000.

## Observe it

| Signal | Meaning |
| --- | --- |
| Readiness check `outbox_loop` (in `/readyz` `failed_checks`) | Required. Closes after **3 consecutive failed steps** and reopens on one success; a single transient failure does not flap readiness. Before the first successful step it is not ready. |
| `devhealth_auth_outbox_publisher_enabled` (gauge) | `1` when a sink is configured, `0` when publishing is disabled. In Wave 1 it is `0` by design; zero deliveries is not health. |
| `devhealth_auth_outbox_oldest_pending_age_seconds` (gauge) | Age of the oldest unpublished event, including one waiting out a retry backoff; `0` when none. Exported with or without a sink. In Wave 1 it grows with the backlog. |
| `devhealth_auth_outbox_delivered_total` (counter) | Events delivered and marked published (only with a sink). |
| `devhealth_auth_outbox_delivery_failures_total` (counter, by reason) | Failed delivery attempts, by the closed reason set above. |

## Troubleshooting

**`/readyz` fails with `outbox_loop`.** Three consecutive steps failed. The loop
logs `outbox step failed` with the consecutive-failure count and the error. Most often the database is
unreachable (then `postgres` fails too) or the runtime login lost a privilege on
`auth_outbox_events`; compare it with the manifest in
[database, roles and migrations](auth-control-plane-database.md) and re-run
`auth-migrate`, which re-grants exactly the manifest.

**Pending age keeps growing in Wave 1.** Expected: there is no sink. It is a
problem only if `publisher_enabled` is `1`. With a sink, find the stuck row (replace `auth` with the
configured `AUTH_DATABASE_SCHEMA` if it is not the default):

```sql
SELECT id, event_type, attempts, last_error, available_at, created_at
FROM auth.auth_outbox_events
WHERE published_at IS NULL
ORDER BY id LIMIT 20;
```

`attempts` rising with `last_error = 'consumer_unavailable'` is the consumer
being down; `delivery_timeout` is a sink slower than 15 s; `consumer_rejected`
needs a code fix on the consumer, not a retry.

**A mutation succeeded but has no audit row or event.** It should be
impossible for a guarded table: the trigger refuses the write. If it happened,
the table is classified unguarded (see `commit_guard_integration_test.go`, which
lists and classifies every table), or the writer is bypassing `Commit`.

**Duplicate key on commit.** `idempotency_key` is unique and is built from the
aggregate's revision, which only `Commit` advances. A duplicate means a row was
written outside `Commit` or a revision row was rewound; the whole transaction
fails on purpose.

**The table is large.** Published rows are reaped after the retention; an
unpublished backlog is never reaped. Do not delete from the table by hand.

## Verified against

- `internal/audit/audit.go` (`Commit`, `Mutation`, `OutboxEvent`, `insertOutboxEvents`) and `arm.go`; migrations `0006` and `0013`; `internal/auth/contracts/provisioning` (the vocabulary `Commit` checks).
- `internal/audit/outboxloop.go` (`DefaultLoopConfig`, `CheckOutboxLoop`, `degradeAfter`, both gauges), `publisher.go` (counters, `AllFailureReasons`, backoff and timeout defaults), `reaper.go` (`Reap`).
- `internal/auth/authruntime/runtime.go`: the sink-less `NewOutboxLoop` call and its comment.
- `internal/audit/allowlist.go`: the attribute allowlist.
