# Durable Dispatch Outbox

The durable dispatch outbox guarantees that committed sync runs do not get stranded. If Celery publishes fail, workers die, brokers purge messages, or finalization is lost, the outbox and reconciler recover the run.

## Architectural Overview

The outbox pattern separates database transactions from message broker publishing. Producers write outbox entries within the same database transaction that updates the sync run state. A periodic reconciler relay then polls, claims, and publishes these entries to the Celery broker. Sync-config entrypoints first create the admin-visible `JobRun` activity row, then plan the execution-truth `SyncRun`, and link the two through `JobRun.result.sync_run_id`.

```mermaid
graph TD
    subgraph Producers [Producers (In-Transaction Write)]
        P1[Planner / plan_sync_run]
        P2[Sync Execution Triggers<br/>manual / scheduled / backfill]
        P3[run_sync_unit]
        P4[finalize_sync_run]
    end

    subgraph DB [Database Layer]
        Outbox[(sync_dispatch_outbox)]
        UnitGuard[(SyncRunUnit Claim/Lease CAS)]
    end

    subgraph Relay [Relay Layer]
        Reconciler[reconcile_sync_dispatch]
    end

    subgraph Broker [Celery Broker]
        Celery[Celery Broker / Redis]
    end

    subgraph Tasks [Celery Tasks]
        T1[dispatch_sync_run]
        T2[run_sync_unit]
        T3[finalize_sync_run]
        T4[post_sync fanout]
    end

    P1 -->|Write dispatch_sync_run| Outbox
    P2 -->|Write dispatch_sync_run| Outbox
    P3 -->|Write finalize_sync_run| Outbox
    P4 -->|Write post_sync| Outbox

    Reconciler -->|1. Claim due rows| Outbox
    Reconciler -->|2. Publish task| Celery
    Reconciler -->|3. Mark dispatched| Outbox

    Celery --> T1
    Celery --> T2
    Celery --> T3
    Celery --> T4

    T1 -->|Atomic claim/lease CAS| UnitGuard
    T2 -->|Atomic claim/lease CAS| UnitGuard
```

At-most-once provider execution is not enforced by the outbox. It is guaranteed by the separate unit claim and lease-token CAS guards. The atomic `DISPATCHING` to `RUNNING` claim and the lease checks prevent duplicate execution of provider units even if a task is published multiple times.

`JobRun` is not an execution lock. It is the activity/index row used by admin history surfaces. `SyncRun` and `SyncRunUnit` remain the execution source of truth, and the outbox remains the durable dispatch mechanism.

For how each `SyncRunUnit` is decomposed (`source × dataset × window`) and the reference-tier vs work-item-tier distinction, see [Sync Unit Model](sync-unit-model.md).

---

## Crash-Window Recovery Flow

The reconciler relay recovers from failures at any point in the sync lifecycle. The sequence below shows how the relay handles lost publishes, concurrency caps, and finalization failures.

```mermaid
sequenceDiagram
    autonumber
    participant DB as Database (Outbox / Units)
    participant Relay as Reconciler Relay
    participant Broker as Celery Broker
    participant Worker as Celery Worker

    Note over DB, Worker: Case 1: Committed-PLANNED dispatch never published (At-Least-Once)
    DB->>DB: SyncRun planned, outbox row created (dispatch_sync_run, pending)
    Note over DB: Celery publish fails or worker crashes
    Relay->>DB: Scan for pending outbox rows (available_at <= now)
    Relay->>DB: Claim row (claim_token, claim_expires_at)
    Relay->>Broker: Publish dispatch_sync_run task
    Broker->>Worker: Execute dispatch_sync_run
    Relay->>DB: Mark outbox row dispatched

    Note over DB, Worker: Case 2: Capped redispatch lost (At-Least-Once)
    Worker->>DB: Concurrency cap hit, overflow units left in PLANNED
    Worker->>DB: Rearm outbox row (dispatch_sync_run, pending, available_at = now + countdown)
    Note over DB: Redispatch publish lost
    Relay->>DB: Scan for pending outbox rows (available_at <= now)
    Relay->>DB: Claim row
    Relay->>Broker: Publish dispatch_sync_run task
    Relay->>DB: Mark outbox row dispatched

    Note over DB, Worker: Case 3: Finalize never published (At-Least-Once)
    Worker->>DB: run_sync_unit finishes, writes finalize_sync_run outbox row (pending)
    Note over DB: Finalize publish fails
    Relay->>DB: Scan for pending outbox rows (available_at <= now)
    Relay->>DB: Claim row
    Relay->>Broker: Publish finalize_sync_run task
    Relay->>DB: Mark outbox row dispatched

    Note over DB, Worker: Case 4: post_sync fanout lost (At-Most-Once)
    Worker->>DB: finalize_sync_run finishes, writes post_sync outbox row (pending)
    Relay->>DB: Scan for pending outbox rows (available_at <= now)
    Relay->>DB: Claim row
    Relay->>DB: Mark outbox row dispatched (BEFORE publishing)
    Note over Relay: If publish fails here, task is lost (At-Most-Once)
    Relay->>Broker: Publish post_sync fanout tasks

    Note over DB, Worker: Case 5: Permanent feature denial (Consumed, no publication)
    Worker->>DB: Terminalize run, units, discovery, and observers atomically
    Worker->>DB: Mark discovery/finalize outbox rows dispatched with feature_disabled
    Note over Relay: Denied rows are terminal and never claimable or re-armed

    Note over DB, Worker: Case 6: Eligible Linear backfill expired lease (Retry, not Fail) [CHAOS-2710]
    Worker->>DB: run_sync_unit holds lease; worker dies mid-chunk, lease expires
    Relay->>DB: Expired-lease loop finds RUNNING unit with dead lease
    Note over Relay: Eligible? provider=linear AND mode=backfill AND work-item family AND retry-SAFE surfaces AND count < max
    Relay->>DB: CAS RUNNING -> RETRYING, clear lease, expired_lease_retry_count++, available_at = now + backoff
    Note over DB: When available_at is reached, the existing dispatch path re-drives the unit
    Relay->>Broker: Publish dispatch_sync_run (redispatch the retrying unit)
    Broker->>Worker: Execute dispatch_sync_run -> run_sync_unit (fresh lease)
    Note over Relay: If count == max instead: unit -> FAILED (error_category=worker_lost_retry_exhausted)
```

---

## Task Hierarchy and Outbox Lifecycle

The sync execution pipeline is structured as a hierarchy of tasks. Outbox rows guard the transitions between these tasks.

```mermaid
graph TD
    Plan[plan_sync_run] -->|1. Guarded by dispatch_sync_run outbox| Dispatch[dispatch_sync_run]
    Dispatch -->|2. Fans out units| Group[group(run_sync_unit...)]
    Group -->|3. Chord callback guarded by finalize_sync_run outbox| Finalize[finalize_sync_run]
    Finalize -->|4. Guarded by post_sync outbox| Post[post_sync fanout]

    style Plan fill:#f9f,stroke:#333,stroke-width:2px
    style Dispatch fill:#bbf,stroke:#333,stroke-width:2px
    style Group fill:#dfd,stroke:#333,stroke-width:2px
    style Finalize fill:#fdd,stroke:#333,stroke-width:2px
    style Post fill:#ffb,stroke:#333,stroke-width:2px

    classDef note fill:#fff,stroke:#333,stroke-dasharray: 5 5;
    
    N1[dispatch_sync_run outbox row:<br/>- Created in-txn by planner/trigger<br/>- Marked dispatched by relay<br/>- Re-armed by dispatch_sync_run if capped]:::note
    N2[finalize_sync_run outbox row:<br/>- Created in-txn by run_sync_unit<br/>- Marked dispatched by relay]:::note
    N3[post_sync outbox row:<br/>- Created in-txn by finalize_sync_run<br/>- Marked dispatched by relay BEFORE publish]:::note

    Plan -.-> N1
    N1 -.-> Dispatch
    Group -.-> N2
    N2 -.-> Finalize
    Finalize -.-> N3
    N3 -.-> Post
```

---

## Delivery Semantics

The outbox kinds have different delivery guarantees depending on their idempotency characteristics.

| Outbox Kind | Delivery Guarantee | Idempotency Mechanism |
| :--- | :--- | :--- |
| `dispatch_sync_run` | At-Least-Once | Unit claim guards prevent duplicate execution. Capped units remain in `PLANNED` status. |
| `finalize_sync_run` | At-Least-Once | The `SyncRunPostDispatch` ledger enforces once-only finalization. |
| `post_sync` | At-Most-Once | The relay marks the row as dispatched before publishing. It never re-arms on publish failure. |

A permanent authorization denial consumes the transition using the existing
`dispatched` status and stores `feature_disabled` as the durable reason. These
rows are excluded from relay claims, and `upsert_outbox_wakeup` preserves that
terminal denial rather than re-arming it. A pending finalizer remains recoverable
after other terminal outcomes without reopening feature-denied work.

### At-Most-Once post_sync Semantics

Downstream metrics readers raw-aggregate `computed_at` generations. Duplicate post-sync fanouts would cause double-counting. To prevent this, the reconciler relay marks the `post_sync` outbox row as dispatched before publishing the tasks. If the publish fails, the row is not re-armed.

### Deferred: Durable Exactly-Once post_sync (CHAOS-2596)

Durable exactly-once delivery for `post_sync` is deferred. It will be implemented under issue CHAOS-2596.

---

## Reconciler Relay Details

The periodic `reconcile_sync_dispatch` task runs every 60 seconds. It performs the following operations:

1. **Lease Expiry**: Finds running units with expired leases. Most are marked terminal `FAILED` (`error_category = worker_lost`). **Exception (CHAOS-2710):** an eligible Linear backfill unit — `provider == linear`, `mode == backfill`, a work-item-family dataset, parent run non-terminal, all touched ClickHouse surfaces in the proven retry-SAFE set, and `expired_lease_retry_count < SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES` — is instead flipped `RUNNING -> RETRYING` (atomic CAS) with a cleared lease and an `available_at = now + SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS` backoff, so it is redispatched rather than failed. Exhausting the retry budget falls back to terminal `FAILED` (`error_category = worker_lost_retry_exhausted`). This is the **only** deviation from terminal-fail recovery; the dispatch, finalize, and post_sync semantics below are unchanged — a retried unit re-enters through the existing at-least-once `dispatch_sync_run` outbox path with no new outbox kind.
2. **Materialization**: Scans for runs that need dispatching, finalization, or post-sync processing, then creates outbox rows for them.
3. **Relay**: Claims pending outbox rows using a unique claim token and lease. It publishes the tasks to Celery and marks the rows as dispatched.

If the broker is down, trigger surfaces return a 202 status. The outbox row remains pending in the database, and the reconciler relay will retry the publish on its next pass.
