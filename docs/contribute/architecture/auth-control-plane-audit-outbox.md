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
  - internal/auth/authruntime/feed.go
  - internal/auth/revisionfeed/sink.go
  - internal/auth/revisionfeed/floors.go
  - internal/auth/revisionfeed/floorinput.go
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
guarded row written, migration `0006`, or a new principal, user or session
row, the `0013` create guard and its `0014` session trigger). Any failure rolls back **everything**.

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

`authruntime` builds one **outbox loop**. By default (`AUTH_REVISION_FEED` unset
or `off`) it is built with `audit.NewOutboxLoop(pool, nil, config)`: a loop with
**no sink**, exactly as before the revision feed existed. With
`AUTH_REVISION_FEED=on` it is built with the Valkey sink (see "The revision
feed" below). Without it:

- **Publishing is off.** A sink-less publisher would mark events delivered that
  nobody received. Events accumulate as pending until a sink is switched on.
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

## The revision feed

Shipped dark. `AUTH_REVISION_FEED=on` (default `off`; it needs `AUTH_VALKEY_URI`,
database 1) gives the outbox publisher a sink that raises a per-aggregate
high-water revision and publishes it, and subscribes every replica to the
channel.

- **The sink.** One Lua call (`EVAL`) per event: `HSET` the field
  `<aggregate_type>:<aggregate_id>` of the hash `dho:rl:auth_svc_rev_hwm` to the
  event's revision **only when it is higher than the stored one** (a delayed or
  redelivered lower revision never lowers it), then `PUBLISH`
  `<aggregate_type>:<aggregate_id>:<revision>` on the channel `auth_svc_rev`.
  `Deliver` returns nil only after the script succeeded. A Valkey that cannot be
  reached is the closed reason `consumer_unavailable` (the event stays pending
  and retries with backoff; the lag gauge rises); an error reply (an ACL
  refusal) is `consumer_rejected`. No error text is stored, and the URI is never
  logged. A row whose idempotency key disagrees with its revision is
  `consumer_rejected` and never reaches Valkey.
- **The consumer.** Every replica subscribes, and every (re)subscribe calls
  `InvalidateAll` on the consumer's invalidator (pub/sub has no replay) and then
  **catches up**: once the subscription is confirmed it reads the hash with
  `HGETALL` and gives each `<type>:<id>` floor to the consumer, so a revision
  delivered while the replica was away is reached with no further event. The order
  is subscribe, then read, then the marks move; a live event is revision-idempotent
  with the read (a floor at or below the mark is ignored). The read runs off the
  connection's read loop and retries with the backoff; `subscription_up` is 1 only
  after it succeeded. Per
  aggregate: the next revision applies (the aggregate is invalidated), a duplicate
  or older one is ignored, a revision that skips ahead is a gap (the aggregate is
  invalidated and the mark moves to the gap revision, so the gap does not repeat).
  The invalidation runs **before** the cursor and the mark move. An invalidation
  that returns an error leaves both where they were and is returned: a live
  event asks for a catch-up (the floor applies it again), a catch-up or a failed
  `InvalidateAll` of a (re)subscribe retries with the backoff, and
  `subscription_up` stays 0 until the `InvalidateAll` and the read succeeded.
  No decision cache exists yet (5.8), so the invalidator is a no-op and the
  consumer keeps its watermarks and metrics. Metrics:
  `devhealth_auth_revision_feed_events_total{disposition=apply|ignore|gap|dropped|invalidate_failed}` (`dropped`: a live message or a catch-up floor refused before the cursor, because it is malformed or its revision is outside 1..2^53-1; the cursor never moves on it; `invalidate_failed`: the invalidation returned an error, the event is not applied and is retried),
  `devhealth_auth_revision_feed_resync_total{cause=gap|resubscribe}` and
  `devhealth_auth_revision_feed_subscription_up` (1 while subscribed and caught up). No
  aggregate id is a label, and no log line carries an id or a payload.
- **ACL.** The auth Valkey login (`auth-service-ratelimit`) keeps its one key
  prefix and database; the feed needs one more channel pattern and more commands
  (`+hget +hset` run inside the script, `+publish`, `+subscribe`, and `+hgetall`
  for the catch-up read, granted by a **selector** that names the one hash, so
  `HGETALL` of any other `auth_svc_` key is refused; the selector needs Valkey 7 or
  later, the chart's Valkey is 9):
  `resetkeys ~dho:rl:auth_svc_* resetchannels &auth_svc_* -@all +hello +select +ping +get +eval +incr +pexpire +hget +hset +publish +subscribe +client|tracking (~dho:rl:auth_svc_rev_hwm +hgetall)`.
  The key `dho:rl:auth_svc_rev_hwm` and the channel `auth_svc_rev` sit under the
  existing `auth_svc_` prefixes; the ACL prefix is the separation, not the
  database number. Roll the ACL (chart `valkey.acl`, or `ACL SETUSER`) **before**
  turning the flag on, or the sink answers `consumer_rejected`.
- **The published floors (a stable interface).** `revisionfeed.PublishedFloors`
  returns the hash as `map[<type>:<id>]revision`, and `revisionfeed.ReadStatus`
  says whether the hash can be trusted, from one `INFO persistence memory` read (no
  `CONFIG GET`): usable only when `aof_enabled` is 1, `aof_last_write_status` and
  `aof_last_bgrewrite_status` are `ok`, **and** `maxmemory_policy` is `noeviction`
  or `volatile-*`, **and** the hash has no TTL (checked under both policies: an
  expiring hash gives a lower floor). A missing, unknown or failed field is
  not usable. The `appendfsync` policy is not in `INFO`; the chart sets none (the
  server default `everysec`) and a chart test pins that it is never `no`. The
  chart's default `--maxmemory-policy allkeys-lru` is **not** usable. A hash that
  was evicted or lost gives a lower floor, the unsafe direction, so it is an
  advisory second input, never the only source. `ReadStatus` needs `+info`, which
  the auth-service login does not have: the reader uses its own login.
- **A restore-fence floor input.** The restore fence's census requires every outbox
  consumer to hand its high-water marks to the fence, so the feed is one
  (`revisionfeed.NewFloorInput`): an advisory lower bound that the fence uses only
  while `ReadStatus` says usable. It fails closed: a store that is not usable
  (including the chart default `allkeys-lru`) or unreachable is reported as such,
  and an empty hash is `ErrNoFloors`, never an empty map or a zero floor (the fence
  would count an empty input as a usable source and refuse the blind margin). The
  registry is per process; the auth-service registers its input only when the feed
  is on, and the restore-fence CLI will register its own with its own Valkey login
  (CHAOS-11978, which blocks the production flag flip).
- **Restore.** Until the restore-fence leaf that reads these floors ships, a
  restore does not use the Valkey hash, and **the flag must stay off in
  production**.

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

- `internal/audit/audit.go` (`Commit`, `Mutation`, `OutboxEvent`, `insertOutboxEvents`) and `arm.go`; migrations `0006`, `0013` and `0014`; `internal/auth/contracts/provisioning` (the vocabulary `Commit` checks).
- `internal/audit/outboxloop.go` (`DefaultLoopConfig`, `CheckOutboxLoop`, `degradeAfter`, both gauges), `publisher.go` (counters, `AllFailureReasons`, backoff and timeout defaults), `reaper.go` (`Reap`).
- `internal/auth/authruntime/runtime.go`: the sink-less `NewOutboxLoop` call and its comment.
- `internal/audit/allowlist.go`: the attribute allowlist.
