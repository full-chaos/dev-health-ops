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

External recompute coalescing remains on the existing bounded control-job path
until its River job and persisted outcome writer have parity evidence. Its
critical crash-window invariant is unchanged: the pending scope is consumed
atomically with `GETDEL`, and an older flush must not delete a scope written by
a newer debounce owner. The stream runner does not delete the debounce guard.
