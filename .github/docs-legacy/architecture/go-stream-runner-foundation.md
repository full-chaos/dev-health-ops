# Go stream-runner foundation (CHAOS-3042, CHAOS-3043)

The dormant `dev-health-stream-runner` profiles consume the existing Valkey
database-1 Streams through consumer groups. This document records the
repository-side transport invariant; it is not canary or soak evidence and it
does not authorize routing production traffic away from Celery.

## Delivery and acknowledgement

Each lifecycle-owned loop uses bounded `XREADGROUP` calls. A handler validates
the message and commits its idempotent PostgreSQL/ClickHouse write before the
runner calls `XACK`. If the process crashes after the durable write but before
the acknowledgement, the message remains pending and the handler must safely
deduplicate the replay. If the durable write fails transiently, the runner
leaves the entry in the pending-entry list.

Invalid messages use the existing quarantine key conventions without copying a
raw payload: `ingest:dlq:<entity>`, `product-telemetry:dlq`, or
`external-ingest:<org>:dlq`. The quarantine write precedes the source ACK. A
quarantine failure therefore leaves the original message pending.

## Reclaim and shutdown

Each window examines idle pending entries. Entries below the delivery limit are
claimed and reprocessed; entries at the limit are quarantined then ACKed.
External ingest retains the current conservative 15-minute idle threshold and
five-delivery limit. Its scaling policy remains a single replica and one
consumer identity; configuration with more than one external runner replica
is rejected by the deployment contract. The runner constructor also refuses a
singleton configuration whose resolved replica count is not exactly one.

On shutdown the runner closes readiness, cancels blocking reads, and waits only
for its bounded drain interval. It never ACKs an in-flight message merely to
finish shutdown. Any unfinished message remains in the PEL for a later
reclaimer.

## Telemetry and coexistence

The runner exports aggregate, payload-free metrics for stream lag, pending
count, oldest pending age, throughput, retries, reclaims, quarantines, failures
and the last successful window. Stream/tenant/message identifiers are excluded
from labels.

Celery consumer tasks, Beat entries, and their existing queues remain unchanged
while `deploy/go-workers/profiles.json` stays `coexistence_disabled` with zero
minimum replicas. Enabling Go routing requires separate canary and soak
evidence; none is represented by this implementation.

## Recompute flush

The external profile owns a Go debounce controller, but downstream metric
execution deliberately remains on the existing Python/Celery planner during
coexistence. Accepted batch scopes are atomically coalesced in Valkey at
`(org, source system, source instance)` grain. The first scope owns a
45-second due ticket; later scopes union repository, team, record-kind,
ingestion, and time-window bounds without extending that window.

A due scope moves atomically from pending to a leased inflight blob. Process
death after that drain does not lose the scope: the next controller reclaims
the inflight blob after its lease. Every ticket has a generation, so completing
an older inflight generation cannot delete a newer pending generation.

The compatibility dispatcher then writes one deterministic
`external_ingest_recompute_jobs` bridge identity and the identical allowlisted
scope onto every coalesced batch row in one PostgreSQL transaction. Replaying
the inflight claim cannot create a duplicate bridge or batch status row. A
dormant Python bridge poller accepts only
`external_ingest.recompute.compat.v1` /
`dev_health_ops.workers.tasks.dispatch_external_ingest_recompute_bridge`, then
invokes the current `plan_recompute` and `dispatch_and_persist_scope` behavior.
That preserves the existing 25-repository and 14-day caps, current Celery task
allowlist, and customer-facing persisted outcomes without introducing metric
River contracts owned by later migration phases.

Both stream profiles and the bridge remain disabled in
`deploy/go-workers/profiles.json`. The implementation is rollback-capable
foundation and crash-window evidence, not authorization to remove the current
Celery consumers or health task.
