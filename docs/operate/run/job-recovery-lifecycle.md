---
page_id: op-job-recovery
summary: How a job recovers after its worker dies, why recovery is not immediate, and how to tell a working job from a dead one.
content_type: reference
owner: platform-operations
source_of_truth:
  - contracts/jobs/v1/ (kind timeouts and attempt limits)
  - cmd/dev-health-worker/river_process.go (River client configuration)
  - internal/jobs/metrics/daily/postgres.go (domain lease behaviour)
applicability: current
lifecycle: active
---

# Job recovery lifecycle

When a worker dies mid-job — a crash, an OOM kill, a container recreate during
a deploy — the job it was running does not fail. It stays marked `running`,
and recovery happens later, on a schedule that is longer than most operators
expect.

This page explains how long that takes, why, and how to tell the difference
between a job that is working and a job whose worker is already gone.

Use it when work looks stuck but nothing has reported an error. For the
recovery sequence itself, see
[Worker or queue failure](../runbooks/worker-or-queue-failure.md).

## Recovery is gated twice, not once

River rescues jobs stranded in `running` through its maintenance loop, which
runs on the elected leader. Two conditions must **both** be satisfied before a
stuck job is touched:

1. The job has been running longer than `RescueStuckJobsAfter`. This is not
   configured in `cmd/dev-health-worker/river_process.go`, so River's default
   of **one hour** applies.
2. The job has been running longer than **its own declared timeout**. River
   ignores a stuck job whose timeout has not yet elapsed, because a job that is
   legitimately still working must never be rescued out from under itself.

So the effective wait is:

```
rescue horizon = max(RescueStuckJobsAfter, kind timeout)
```

The second gate is the one that surprises people. A kind with a two-hour
timeout is not eligible for rescue after one hour — it is eligible after two.

### Current kind timeouts

From `contracts/jobs/v1/`. Check there rather than trusting this table if the
numbers matter to a decision.

| Kind | Timeout | Attempts | Effective rescue horizon |
| --- | --- | --- | --- |
| `investment.materialize` | 7200s | 3 | 2 hours |
| `metrics.daily_partition` | 7200s | 5 | 2 hours |
| `workgraph.build` | 3600s | 4 | 1 hour |
| `metrics.daily_finalize` | 900s | 4 | 1 hour |
| `metrics.daily_dispatch` | 300s | 3 | 1 hour |

A worker killed while running `metrics.daily_partition` leaves that job
untouched for two hours. During that window the job row looks busy, no error is
recorded, and no metric moves.

## The faster signal: domain leases

The job runtime is not the only thing tracking liveness, and it is the slower
of the two.

A worker holding durable work also holds a **lease** on the domain row, and
renews it every `lease/3` while it is alive. For daily metrics the lease is ten
minutes, so renewal happens about every **3 minutes 20 seconds**.

That gives a much sharper death signal:

> A domain row still marked `running` whose `lease_expires_at` is in the past
> means its holder stopped renewing. The holder is dead.

An expired lease is knowable within roughly ten minutes. River's rescue horizon
for the same job may be two hours. **The lease is up to twelve times faster than
the job runtime at telling you a worker died.**

This signal is durable and already in the database. It is not currently surfaced
as a metric or alert — see CHAOS-4025.

## Two lease layers, not one

Recovery involves two independent leases, and they answer different questions.

`worker_job_runs` is the semantic execution record for a logical job, separate
from both River and `worker_job_outbox`: queue rows say whether work was
*transported*, while a run row owns the durable idempotency claim, its bounded
lease, the terminal result, and the safe error category. **A retry may reclaim
only an expired lease; a completed or terminal key never invokes the handler
again.**

Beneath it sits the domain lease described above — the partition, finalization,
or request lease that the running work renews.

So a retry can be stopped at either layer, and the two failure modes look
identical from outside:

| Layer | Lease | If it is held when a retry arrives |
| --- | --- | --- |
| Idempotency | `worker_job_runs.lease_expires_at` | The retry is reported as a duplicate success and the handler never runs |
| Domain | `daily_metrics_*`, `work_graph_execution_requests`, `remaining_metric_*` | The retry parks until the lease expires, then reclaims it |

The domain-layer behaviour is current as of CHAOS-3991. The idempotency layer
still reports a live lease as duplicate success without running the handler, so
a worker killed hard enough to leave that lease behind can still lose its retry
— tracked as CHAOS-3998.

## The four liveness states

Work that has not completed is in one of four states. They look similar in a
job table and mean very different things.

| State | River job | Domain lease | Meaning | Action |
| --- | --- | --- | --- | --- |
| **Live** | `running` | not expired, renewing | Working normally | None — wait |
| **Stale** | `running` | expired | Worker is dead, rescue horizon not reached | None — River will rescue it |
| **Rescuable** | `running` past its horizon | expired | Eligible now; leader will retry or discard it | None — verify it advances |
| **Dead** | no live job | expired, or no lease at all | Nothing will ever run this | Needs a reclaim sweep |

**Stale and Dead are the pair that gets confused**, and confusing them is
expensive in both directions:

- Treating Stale as Dead and re-enqueuing it double-drives the work, because
  River is still going to rescue the original.
- Treating Dead as Stale means waiting forever for a rescue that will never
  come, because there is no job left to rescue.

The distinguishing question is not "is there a job row" but **"is there a job
that will still run"**.

A job can reach Dead by more than one route: its worker declared success while
another claimant's lease was live (fixed in CHAOS-3991), its head terminalized
as a non-success state, or it exhausted its attempts and was discarded. All
three end the same way — a non-terminal row with no live job.

## Diagnosing stuck work

Work in order. Each step narrows which of the four states you are in.

### 1. Is anything actually queued?

```sql
SELECT state, count(*), min(scheduled_at) AS oldest
FROM river.river_job
WHERE state IN ('available', 'running', 'scheduled', 'retryable')
GROUP BY state ORDER BY count(*) DESC;
```

An empty result is an idle system, not a stalled one. Confirm work is expected
before investigating further.

### 2. Is a leader elected?

```sql
SELECT leader_id, elected_at, expires_at FROM river.river_leader;
```

Maintenance — including rescue — only runs on the leader. If `expires_at` is
not advancing, no rescue will happen at any horizon.

### 3. How long has the job been running, against its own timeout?

```sql
SELECT id, kind, state, attempt, max_attempts, attempted_at,
       now() - attempted_at AS running_for
FROM river.river_job
WHERE state = 'running' ORDER BY attempted_at;
```

Compare `running_for` against the kind's timeout in the table above. Below the
horizon, the correct action is to wait.

### 4. Has the holder stopped renewing its lease?

This is the question that actually tells you whether the worker is alive.
Daily metrics, for example:

```sql
SELECT id, run_id, status, lease_expires_at,
       (lease_expires_at < now()) AS lease_expired, updated_at
FROM daily_metrics_partitions
WHERE status = 'running' ORDER BY updated_at;
```

The equivalent lease columns exist on `daily_metrics_runs`
(`finalization_lease_expires_at`), `work_graph_execution_requests`, and
`remaining_metric_partitions`.

An expired lease with a live job is **Stale**. An expired lease with no job is
**Dead**.

### 5. Did the job already end without finishing its work?

```sql
SELECT id, kind, state, attempt, max_attempts, finalized_at,
       left(errors::text, 500) AS errors
FROM river.river_job
WHERE state IN ('discarded', 'cancelled')
  AND finalized_at > now() - INTERVAL '2 hours'
ORDER BY finalized_at DESC;
```

A `discarded` job that exhausted its attempts leaves its domain row
non-terminal forever. Read the `errors` array: attempts that all failed against
an unreachable dependency mean the retry budget was spent on an outage rather
than on real failures.

### 6. Is a downstream chain gated behind it?

A handoff waits on a completion fence, which is written only when its
prerequisite **succeeds**:

```sql
SELECT job_kind, prerequisite_completion_key, status, attempt_count, created_at
FROM worker_job_outbox
WHERE status = 'pending' AND prerequisite_completion_key IS NOT NULL
ORDER BY created_at;
```

`attempt_count = 0` on a fenced row is **not** a staleness signal. Blocked
successors consume no retry attempts by design, so a row blocked for fourteen
hours is indistinguishable from one enqueued a second ago. Judge it by `created_at` and
by the state of the head named in `prerequisite_completion_key`.

## What recovery does not cover

Rescue re-runs a job that still exists. It cannot help when the job itself is
gone. If a domain row is non-terminal, its lease is expired, and no live job
references it, no amount of waiting will recover it — that work needs a reclaim
sweep (CHAOS-3997), not patience.

Equally, a fresh run does not supersede a stuck one. Completion fences are keyed
by run id, and run ids are derived from the organization, day, and generation.
A new run writes a fence that nothing is waiting on, so replaying work does not
release a chain blocked on an older run.

## Sources

- CHAOS-3991 — a claim conflict reported as success deleted the job that would
  have reclaimed the lease
- CHAOS-3997 — reclaim sweep for runs stranded with no live job
- CHAOS-4025 — expired leases on running rows are not surfaced as a signal
