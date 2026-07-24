# CHAOS-2693 — External ingest durable stream and DLQ

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. Body-size cap: `EXTERNAL_INGEST_MAX_BODY_BYTES=10_000_000` (10 MB) — replaces this
>    brief's 8 MiB `EXTERNAL_INGEST_MAX_BATCH_BYTES` (name and value). Records cap 1000.
> 2. Producer function name: `enqueue_batch` (2691's seam, kept), same kwargs as this
>    brief's `enqueue_external_ingest_batch`; this issue drops the interim `payload_json`
>    kwarg and updates `router.py`'s call site in this PR (approved call-site change).
> 3. Batch row status at accept-time is `accepted` (2694's enum), NOT `received`.
> 4. `external_ingest_batch_payloads` DDL + declarative model are HOSTED in CHAOS-2694's
>    migration `0033` and `models/external_ingest.py` (single migration owner per wave).
>    This issue does NOT ship an Alembic migration; it owns `payload_store.py` (raw
>    `text()` SQL accessors) and the payload prune beat task.
> 5. This issue is the epic's SINGLE Celery-wiring owner (CC20): its `workers/config.py`
>    change also adds `dev_health_ops.workers.tasks.flush_external_ingest_recompute`
>    (CHAOS-2699's task, name pinned) to `late_ack_excluded_tasks` so 2699 never touches
>    config.py. D8 dedicated `worker-external-ingest` container is RATIFIED (Option A).
> 6. Orphaned-batch recovery: primary path is CHAOS-2695's RETRY idempotency outcome
>    (client re-POST re-enqueues the same ingestion_id); a background re-enqueue reconciler
>    is a NEW follow-up issue, not 2694/2699 scope. `reenqueue_batch()` stays as its seam.
> 7. Consumer give-up path (max_deliveries → DLQ) must also call
>    `processor.mark_batch_failed(ingestion_id, org_id, reason)` (pinned worker contract
>    CC23) so the batch row reaches `failed` and becomes resubmittable.
> 8. Landing wave: 3 (after 2694's tables exist). Per-org DLQ naming RATIFIED.
> 9. **POST-CRITIQUE (CC11): reclaim_idle_ms = 900_000 (15 min), NOT 60s** — the
>    in-process retry ladder alone sleeps 14s and a 1000-record batch can exceed 60s;
>    a 60s window invites duplicate concurrent processing on reclaim.
> 10. **POST-CRITIQUE (CC11): single-replica deployment invariant** — exactly ONE
>    `worker-external-ingest` replica at `--concurrency=1`; document it as a comment on
>    the compose service AND in this brief's ops notes; scaling replicas requires
>    revisiting reclaim semantics first (tests/test_compose_config.py should assert the
>    flag shape stays put).
> 11. **POST-CRITIQUE (CC11): terminal-status idempotent-skip guard** — before processing
>    ANY entry (fresh or reclaimed), the consumer loads the batch status; if terminal
>    (completed|partial|failed) → ACK + skip without reprocessing. Prevents double
>    processing when an entry is reclaimed after a slow-but-successful run.
> 12. **POST-CRITIQUE (CC22): payload_store's write primitive is `upsert_payload()`**
>    (SELECT-then-UPDATE-or-INSERT, same accept txn; no ON CONFLICT) — see the updated
>    sketch in §payload_store. RETRY accepts (incl. CC13's stale-accepted rule) reuse the
>    SAME ingestion_id, so the row may or may not exist.

Implementation brief. Repo: `dev-health-ops`, worktree
`/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`
(branch `chaos-2690-external-ingest`). Written as a read-only recon/planning
artifact — no repo files were modified to produce this brief.

Linear: https://linear.app/fullchaos/issue/CHAOS-2693/external-ingest-durable-stream-and-dlq
Parent epic: CHAOS-2690. Sibling issues referenced throughout: CHAOS-2691
(REST contract/router), CHAOS-2694 (status/rejection store), CHAOS-2695
(idempotency/ownership), CHAOS-2696 (source registration/token scopes),
CHAOS-2697 (worker normalization), CHAOS-2698 (sink writes), CHAOS-2699
(bounded recompute planner).

---

## Scope

This issue owns the **durable transport layer** between "a batch has been
accepted and durably persisted" and "a worker has picked it up for
processing" — plus its failure-handling primitives. Concretely:

1. **Producer helper** — `src/dev_health_ops/api/external_ingest/streams.py`.
   Enqueues a small pointer/metadata envelope (NOT the full payload — see
   Design Decision D2) onto a per-org Redis/Valkey Stream. Fails closed
   (raises, never accept-and-warn) so the caller (CHAOS-2691's router) can
   map failures to `503`.
2. **Consumer scaffold** — `src/dev_health_ops/api/external_ingest/consumer.py`
   (new file, not listed in either plan doc's module inventory — see Gap G1).
   Subclasses the shared `StreamConsumer` base
   (`src/dev_health_ops/api/_stream_consumer.py`), adds consumer-group
   creation, ack, claim/retry, and DLQ routing for external-ingest streams.
   Delegates actual record processing to a single injected callback owned by
   CHAOS-2697 (`external_ingest.processor.process_batch`) — this issue does
   **not** implement normalization/validation/sink-writing, only the
   transport envelope around calling it.
3. **Retry/claim primitive extension** — an additive, default-off extension
   to `src/dev_health_ops/api/_stream_consumer.py` (`StreamConsumer`) adding
   `XPENDING`/`XCLAIM`-based reclaim-with-delivery-count-limit, because the
   base class today has no true retry path (every failure is immediately
   DLQ'd and ACK'd — see Gap G2). This is shared machinery so
   ingest/product-telemetry consumers must be unaffected by default.
4. **Celery task wrapper + beat schedule + worker queue wiring** for the new
   consumer (`workers/system_ops.py`, `workers/config.py`, `compose.yml`) —
   unclaimed by any other sub-issue (see Gap G1).
5. **Payload-placement storage**: a new, narrowly-scoped Postgres table
   (`external_ingest_batch_payloads`) plus its Alembic migration, and the
   read/write helper functions the router and worker need to hand off raw
   batch JSON without ever putting it on the stream. Status/rejection
   tables (`external_ingest_batches`, `external_ingest_rejections`) remain
   CHAOS-2694's responsibility; this issue only adds the payload-blob table
   because it is inseparable from the stream's payload-placement decision
   (explicit FOCUS ask).
6. **Liveness/lag observability** — a beat-scheduled structured-logging task
   (`external_ingest_stream_health`) mirroring the existing
   `monitor_queue_depths` convention, reporting `XLEN`, `XPENDING` summary,
   and oldest-pending age per active org stream and the DLQ.
7. Unit tests for all of the above using `fakeredis.FakeValkey` (the
   established convention — see `tests/test_ingest_streams.py`), no live
   Redis/ClickHouse required.

## Out of scope

- REST router, Pydantic schemas, `/batches` `/validate` `/schemas` endpoints
  (CHAOS-2691).
- Record normalization, validation, provider-neutral dataclass mapping
  (CHAOS-2697).
- Sink writes to ClickHouse (CHAOS-2698).
- Status/rejected-record Postgres tables and their query API
  (`external_ingest_batches`, `external_ingest_rejections`) (CHAOS-2694).
- Idempotency-key conflict semantics (409 logic) and one-active-owner
  enforcement (CHAOS-2695).
- Source registration and token scopes (CHAOS-2696).
- Bounded metric recompute enqueue (CHAOS-2699).
- `dev-hops push` CLI (CHAOS-2700).
- DLQ **replay/reprocessing tooling** (admin UI or CLI to re-drive a DLQ
  entry) — DLQ in this design is a bounded operational trail, not a retry
  queue; replay is a follow-up (see Risks).
- Prometheus/metrics-exporter integration — no such exporter exists in this
  codebase today (verified: no `prometheus`/`statsd` hits anywhere under
  `src/dev_health_ops`); observability here is structured logging only,
  matching `queue_monitor.py`'s own documented scope ("writes NO ClickHouse
  rows... follow-up").
- FullChaos-hosted webhook ingestion (explicitly deferred past v1 per the
  webhooks addendum doc).

---

## Design decisions

**D1. Stream key naming: per-org streams, `external-ingest:<org_id>:batches`
+ `external-ingest:<org_id>:dlq` — resolves a plan-doc/issue-text
discrepancy.**

The core plan doc's response example shows `"stream": "external-ingest:<org_id>:events"`.
The CHAOS-2693 issue text itself specifies `external-ingest:<org_id>:batches`
and `external-ingest:<org_id>:dlq`. Adopt the issue text's naming as
canonical (`:batches` not `:events` — a stream entry corresponds 1:1 to an
accepted batch, not a generic event, so `:batches` is the more accurate
name) and flag to CHAOS-2691's implementer that the plan doc's response
example field must be updated to match. Per-org (not a single shared stream
with an `org_id` field) because: (a) it matches both existing precedents'
rationale — a single huge/bursty org's backlog must not delay another org's
consumer-group progress, since Redis Streams delivers entries in strict ID
order regardless of logical owner; (b) `StreamConsumer.discover_streams()`
already supports `SCAN ... TYPE stream` wildcard discovery
(`external-ingest:*:batches`), so per-org keys need no separate registry;
(c) it matches the issue's own explicit naming. Reject "single stream + org
field": would require either a fairness-scheduling scheme Valkey doesn't
provide, or manual per-org consumer partitioning — solving a problem the
per-org convention already avoids for free.

**D2. Payload placement: full batch JSON goes to Postgres, the stream
carries a metadata pointer only — not the payload.**

Batches can be multi-MB (up to whatever byte cap CHAOS-2691 enforces at the
API boundary, recommend 8 MiB — see D6). Putting full JSON on `XADD`:
(a) breaks the existing `maxlen=100000, approximate=True` backpressure
convention, which bounds by *entry count*, not bytes — 100k entries at
several MB each is gigabytes of Valkey memory with no byte-level cap
anywhere in the codebase today (confirmed: recon found zero payload-size
limits in either existing stream precedent); (b) makes DLQ entries
themselves multi-MB, defeating DLQ's purpose as a lightweight operational
trail; (c) means a stream trim (`maxlen` eviction) under load can silently
lose the *only* copy of a customer's data if the worker hasn't consumed it
yet. Instead: CHAOS-2691's router, in one Postgres transaction, inserts (a)
the `external_ingest_batches` status row (CHAOS-2694, status=`received`)
and (b) the `external_ingest_batch_payloads` row (this issue) holding the
raw batch JSON — commits — **then** calls this issue's
`enqueue_external_ingest_batch(...)`, which `XADD`s only
`{ingestion_id, org_id, source_system, source_instance, schema_version,
idempotency_key, record_count, window_started_at, window_ended_at,
enqueued_at}` (all short strings). The worker fetches the payload by
`ingestion_id` from Postgres. This also means the DLQ never needs the
payload — DLQ entries reference `ingestion_id`, and recovery/replay
(follow-up work) re-reads Postgres, not the DLQ stream.

Trade-off acknowledged: this makes the stream-write step *not* the sole
durability guarantee — if the Postgres commit succeeds but the subsequent
`XADD` fails, the client gets `503` per requirements, but a Postgres row
with `status='received'` now exists with nothing consuming it. This is a
genuine gap (see Gap G3) that needs a reconciler; flagged for CHAOS-2694/
CHAOS-2699 coordination, with a `reenqueue_batch(ingestion_id)` helper
exposed from this issue's `streams.py` for that future reconciler to call.

**D3. Fail-closed (503) semantics: `enqueue_external_ingest_batch` raises,
never returns a boolean/accept-and-warn sentinel.**

Mirrors product-telemetry's `raise ConnectionError(...)` (confirmed live in
code, `product_telemetry/streams.py:38-39`) and is stricter than
`/api/v1/ingest`'s `write_to_stream(...) -> bool` (silently drops on
`False`, confirmed in `api/ingest/router.py`, repeated across 5 handlers).
Define a dedicated exception so the router doesn't need to `except
ConnectionError` (too broad — also raised by unrelated Postgres/httpx code):

```python
class StreamUnavailableError(RuntimeError):
    """Durable enqueue failed; callers MUST map this to HTTP 503, never
    accept-and-warn. See CHAOS-2690 plan: "Do not silently accept customer
    data when the durable ingest path is unavailable."""
```

Also confirmed live (not just plan-doc claim) that product-telemetry's
*router* does its own accept-and-warn at the HTTP layer
(`product_telemetry/router.py:24-30`, catches broad `Exception`, logs a
warning, returns `stream="disabled"` with `202`) — this is the one thing
external-ingest must NOT copy from its own cited precedent. CHAOS-2691's
router must catch `StreamUnavailableError` specifically and return `503`,
not swallow it.

**D4. Consumer group: one group per stream, `id="0"`, reused
`StreamConsumer` base — plus an additive reclaim/retry extension.**

`consumer_group = "external-ingest-consumers"`. `XGROUP CREATE ... id="0"
mkstream=True` (replay-safe from stream start on first activation, same as
both precedents). New consumers must subclass `StreamConsumer`
(`api/_stream_consumer.py`) rather than hand-roll `XREADGROUP` — that base
class exists specifically because two independent consumers regressed the
same two production bugs (blocking-read socket-timeout race;
unguarded-loop crash). Do not reuse `get_redis_client()` (5s
`socket_timeout`, writer-side) for the consumer; use
`get_consumer_redis_client()` (`socket_timeout=None`) via the base class's
`get_client()`.

**D5. Ack/claim/retry policy — this is new, additive machinery on the
shared base class.**

Today's base `handle_entries()` has no real retry: every entry, success or
failure, is ACKed in the same pass (poison entries go to DLQ-and-ACK; there
is no PEL-based redelivery — confirmed by reading `_stream_consumer.py:194-218`
and `handle_entries` docstring). That is acceptable for `/api/v1/ingest`
(internal, best-effort, `accept-and-warn` already accepted upstream) but
**not** acceptable for external-ingest's durability requirement: a
transient ClickHouse/Postgres blip during sink-write must be retried, not
immediately treated as a customer-data-loss event.

Add an **opt-in, default-`False`** capability to `StreamConsumer` so
existing consumers (ingest, product-telemetry) are byte-for-byte unaffected:

```python
class StreamConsumer:
    ...
    enable_reclaim: bool = False       # opt-in; existing subclasses unaffected
    reclaim_idle_ms: int = 900_000     # 15 min (post-critique CC11; was 60s — the in-process
                                       # retry ladder alone sleeps 14s and a 1000-record batch
                                       # can exceed 60s, risking duplicate concurrent processing)
    max_deliveries: int = 5            # after this many attempts, give up -> DLQ

    def reclaim_stale(self, rc, stream_key: str) -> list[tuple[str, dict]]:
        """Reclaim entries idle > reclaim_idle_ms. Entries at/above
        max_deliveries are treated as poison: DLQ + ACK (give up), not
        reclaimed. No-op unless enable_reclaim is set."""
        if not self.enable_reclaim:
            return []
        pending = rc.xpending_range(
            stream_key, self.consumer_group,
            min="-", max="+", count=self.batch_size, idle=self.reclaim_idle_ms,
        )
        claim_ids = []
        for p in pending:
            if p["times_delivered"] >= self.max_deliveries:
                self.move_to_dlq(rc, stream_key, p["message_id"], "max_deliveries_exceeded")
                rc.xack(stream_key, self.consumer_group, p["message_id"])
            else:
                claim_ids.append(p["message_id"])
        if not claim_ids:
            return []
        return rc.xclaim(
            stream_key, self.consumer_group, self.consumer_name,
            min_idle_time=self.reclaim_idle_ms, message_ids=claim_ids,
        )
```

Wire `reclaim_stale()` into `consume()` immediately before each
`XREADGROUP(">")` call (one extra Redis round-trip per stream per poll,
acceptable at 30s beat cadence): reclaimed entries feed into the same
`handle_entries()` path as freshly-read entries so DLQ/ACK logic is
identical.

`ExternalIngestStreamConsumer` (in this issue's `consumer.py`) sets
`enable_reclaim = True`, `dlq_stream` computed **per-org** from the
`stream_key` being processed (the base class's `dlq_stream` class attribute
is a single fixed string — override `move_to_dlq` to derive
`external-ingest:{org_id}:dlq` from the entry's `org_id` field, since D1
chose per-org DLQ streams, not one flat DLQ like product-telemetry's
`product-telemetry:dlq`). Rationale for per-org DLQ (diverges from
product-telemetry's flat DLQ, matches the issue text's explicit naming): a
single bad-actor/misconfigured org producing a flood of poison batches
must not crowd out visibility into other orgs' DLQ entries, and per-org DLQ
keys are trivially discoverable via the same `SCAN` pattern already used
for the primary streams (`external-ingest:*:dlq`).

Distinguish permanent vs transient failure via two exception types the
worker's `processor.process_batch` (CHAOS-2697) must raise:

- `PermanentProcessingError` (e.g. unsupported `schema_version`, structurally
  invalid envelope that survived the API-layer validation) → immediate DLQ
  + ACK, no retry.
- Anything else (including unclassified exceptions, treated conservatively
  as transient — connection errors, timeouts) → NOT ACKed; left in the PEL
  for `reclaim_stale()` to retry on a later poll, up to `max_deliveries`.

This is a **new interface contract with CHAOS-2697**: document it in
`external_ingest/errors.py` (already in the plan's module list) so both
issues implement against the same exception hierarchy.

**D6. Body-size limit: 8 MiB per batch, enforced by the router
(CHAOS-2691), not by this issue — but the number is chosen here because it
gates the payload table's row size.**

No generic REST body-size-limit middleware exists in this codebase
(confirmed: only `graphql/security.py` has one, `DEFAULT_GRAPHQL_MAX_QUERY_BYTES
= 16 * 1024`, irrelevant scale). Recommend `EXTERNAL_INGEST_MAX_BATCH_BYTES`
env var, default `8 * 1024 * 1024` (8 MiB) — large enough for a few thousand
records, small enough to keep `external_ingest_batch_payloads` rows sane
without a TOAST/out-of-line-storage surprise budget blowing up. CHAOS-2691
should adapt the `_send_too_large`/`ASGI` pattern from
`graphql/security.py` for a generic request body-size guard returning `413`
before the body is fully buffered.

**D7. Payload retention: transient, actively cleaned up — not a permanent
audit log.**

`external_ingest_batch_payloads` is deleted by the worker
(CHAOS-2697/2698) immediately after a batch reaches a terminal status
(`processed`, `partial`, `failed`). A beat-scheduled prune task in this
issue additionally deletes any row older than
`EXTERNAL_INGEST_PAYLOAD_MAX_AGE_HOURS` (default 168h / 7 days) as a safety
net for stuck/orphaned rows (mirrors the `sync_reconciler.py`
beat-scheduled-prune pattern cited in `ProviderRateLimitObservation`'s own
docstring). The permanent audit trail is `external_ingest_batches`
(CHAOS-2694), which retains status/counts/timestamps without the raw
payload.

**D8. Dedicated Celery queue, not the shared `ingest` queue.**

`worker-ingest` today runs `-Q ingest --concurrency=1` shared by
`/api/v1/ingest` and product-telemetry consumers, deliberately isolated at
concurrency 1 to avoid starving other pools (`compose.yml:238-243`
comments). External-ingest is customer-facing (spiky, potentially larger
batches, and — per this issue's stricter durability bar — must not have its
processing throughput hostage to an unrelated internal consumer backlog,
nor vice versa: a slow external customer must not starve internal
ingest/telemetry). Add a new queue `external-ingest` and a new compose
worker `worker-external-ingest -Q external-ingest --concurrency=1` (matching
the existing single-concurrency-per-blocking-consumer convention). This is
a **decision, not a certainty** — flagged in `decisionsNeeded` for the epic
owner if infra cost of a 4th worker container is a concern; the fallback
(share `worker-ingest`) is documented as Option B below.

**D9. Observability: structured logs on a `monitoring`-queue beat task, no
new table.**

No metrics exporter (Prometheus/statsd) exists anywhere in this codebase.
`queue_monitor.py`'s own docstring explicitly scopes itself to "NO
ClickHouse rows... UI wiring is a follow-up" for the exact same class of
problem (Celery broker queue depth). Mirror that: a new beat task
`external_ingest_stream_health` (schedule 60s, `queue="monitoring"` — the
existing dedicated telemetry queue consumed by both `worker` and
`worker-heavy`, so no new compose wiring is needed for this task) that
`SCAN`s `external-ingest:*:batches` and `external-ingest:*:dlq`, and per
stream logs `XLEN`, `XPENDING` summary (count + oldest idle ms via
`XPENDING key group` summary form), and warns above thresholds analogous to
`QUEUE_DEPTH_WARNING_THRESHOLD`/`QUEUE_AGE_WARNING_SECONDS`. A future
ClickHouse/Data-Health-UI surface for this is explicitly out of scope here
(same posture as `queue_monitor.py`).

---

## API / DDL / schema sketches

### `streams.py` — producer + naming

```python
# src/dev_health_ops/api/external_ingest/streams.py
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "external-ingest-consumers"
STREAM_MAXLEN = 100_000  # matches ingest/product-telemetry convention


class StreamUnavailableError(RuntimeError):
    """Durable enqueue failed. Callers MUST map this to HTTP 503."""


def batches_stream_name(org_id: str) -> str:
    return f"external-ingest:{org_id}:batches"


def dlq_stream_name(org_id: str) -> str:
    return f"external-ingest:{org_id}:dlq"


def get_redis_client():
    """Writer-side client (finite socket_timeout; never used for blocking reads)."""
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import valkey as redis

        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Redis unavailable for external-ingest streams")
        return None


async def enqueue_external_ingest_batch(
    *,
    org_id: str,
    ingestion_id: str,
    source_system: str,
    source_instance: str,
    schema_version: str,
    idempotency_key: str,
    record_count: int,
    window_started_at: str,
    window_ended_at: str,
) -> str:
    """Durably enqueue a pointer to an already-persisted batch.

    Caller (CHAOS-2691 router) MUST have already committed the batch's
    status row (external_ingest_batches) and payload row
    (external_ingest_batch_payloads) to Postgres before calling this.
    Raises StreamUnavailableError on any failure -- never returns a
    boolean/accept-and-warn sentinel. Returns the stream key written to.
    """
    stream = batches_stream_name(org_id)
    rc = get_redis_client()
    if not rc:
        raise StreamUnavailableError("Redis unavailable for external-ingest streams")
    try:
        rc.xadd(
            stream,
            {
                "ingestion_id": ingestion_id,
                "org_id": org_id,
                "source_system": source_system,
                "source_instance": source_instance,
                "schema_version": schema_version,
                "idempotency_key": idempotency_key,
                "record_count": str(record_count),
                "window_started_at": window_started_at,
                "window_ended_at": window_ended_at,
                "enqueued_at": datetime.now(timezone.utc).isoformat(),
            },
            maxlen=STREAM_MAXLEN,
            approximate=True,
        )
    except Exception as exc:
        logger.exception("Failed to enqueue external-ingest batch %s", ingestion_id)
        raise StreamUnavailableError(str(exc)) from exc
    return stream


async def reenqueue_batch(
    *, org_id: str, ingestion_id: str, **kwargs
) -> str:
    """Re-drive a batch whose Postgres row exists but was never (successfully)
    enqueued (D2 orphan case) or whose processing needs a manual retry.
    Thin wrapper around enqueue_external_ingest_batch for a future
    reconciler (CHAOS-2694/2699) to call. Not scheduled by this issue.
    """
    return await enqueue_external_ingest_batch(org_id=org_id, ingestion_id=ingestion_id, **kwargs)
```

### `consumer.py` — consumer group + retry + DLQ

```python
# src/dev_health_ops/api/external_ingest/consumer.py
from __future__ import annotations

import logging

from .._stream_consumer import StreamConsumer
from .streams import CONSUMER_GROUP, dlq_stream_name

logger = logging.getLogger(__name__)

BATCH_SIZE = 50            # smaller than ingest's 100: each entry now triggers
                            # a Postgres payload fetch + full record processing
BLOCK_MS = 5000
RECLAIM_IDLE_MS = 900_000  # 15 min, post-critique CC11
MAX_DELIVERIES = 5


class PermanentProcessingError(Exception):
    """Non-retryable: bad schema version, envelope shape the API layer
    should have rejected but didn't. DLQ immediately, no reclaim."""


class ExternalIngestStreamConsumer(StreamConsumer):
    consumer_group = CONSUMER_GROUP
    consumer_name_prefix = "external-ingest-consumer"
    batch_size = BATCH_SIZE
    block_ms = BLOCK_MS
    enable_reclaim = True
    reclaim_idle_ms = RECLAIM_IDLE_MS
    max_deliveries = MAX_DELIVERIES
    reject_exceptions = (PermanentProcessingError,)

    def stream_patterns(self) -> list[str]:
        return ["external-ingest:*:batches"]

    def move_to_dlq(self, rc, stream_key: str, entry_id: str, reason: str) -> None:
        # Per-org DLQ: derive org_id from the stream key
        # ("external-ingest:<org_id>:batches").
        parts = stream_key.split(":")
        org_id = parts[1] if len(parts) >= 3 else "unknown"
        dlq = dlq_stream_name(org_id)
        try:
            rc.xadd(
                dlq,
                {
                    "original_stream": stream_key,
                    "entry_id": entry_id,
                    "reason": reason,
                    "moved_at": str(__import__("time").time()),
                },
                maxlen=100_000,
                approximate=True,
            )
        except Exception:
            logger.exception("Failed to move %s to DLQ", entry_id)

    def process_entry(self, stream_key: str, entry_id: str, data: dict[str, str]) -> int:
        from dev_health_ops.external_ingest.processor import process_batch

        return process_batch(
            ingestion_id=data["ingestion_id"],
            org_id=data["org_id"],
            source_system=data["source_system"],
            source_instance=data["source_instance"],
            schema_version=data["schema_version"],
        )


def consume_external_ingest_streams(
    max_iterations: int | None = None, consumer_name: str | None = None
) -> int:
    consumer = ExternalIngestStreamConsumer(consumer_name=consumer_name)
    return consumer.consume(max_iterations=max_iterations)
```

Interface contract with CHAOS-2697: `dev_health_ops.external_ingest.processor.process_batch(
ingestion_id, org_id, source_system, source_instance, schema_version) -> int`
must raise `dev_health_ops.external_ingest.errors.PermanentProcessingError`
for non-retryable failures (re-export or alias the type above — put the
canonical definition in `external_ingest/errors.py` per the plan's module
list, and have `consumer.py` import it from there rather than defining its
own) and any other exception for retryable/transient failures. It is
responsible for: fetching the payload row from
`external_ingest_batch_payloads`, validating/normalizing/writing through
sinks, updating `external_ingest_batches`/`external_ingest_rejections`
(CHAOS-2694), enqueuing bounded recompute (CHAOS-2699), and deleting the
payload row on terminal success (D7).

### Postgres DDL — `external_ingest_batch_payloads`

Follow the `provider_rate_limit_observations` migration precedent exactly
(guarded `create-if-missing`, plain SQLAlchemy `Text`/`UUID` columns, no
FK). **Verify the actual next revision id before implementing** — this
brief assumes `0032` chains off `0031`, but run
`dev-hops migrate postgres heads` first since other in-flight sub-issues
(e.g. CHAOS-2694) may also be adding migrations concurrently.

```python
# src/dev_health_ops/models/external_ingest.py
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, LargeBinary, Text
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class ExternalIngestBatchPayload(Base):
    """Transient store of raw accepted batch JSON, referenced by the durable
    stream entry via ingestion_id (CHAOS-2693 D2: streams carry a pointer,
    never the payload). Deleted by the worker on terminal batch status; also
    pruned by a beat-scheduled sweep after
    EXTERNAL_INGEST_PAYLOAD_MAX_AGE_HOURS as an orphan safety net (D7).

    No FK to external_ingest_batches (CHAOS-2694): this row must be
    independently deletable/prunable without touching the permanent audit
    row, mirroring ProviderRateLimitObservation's FK-less, independently-
    retained convention.
    """

    __tablename__ = "external_ingest_batch_payloads"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True)
    org_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
```

```python
# src/dev_health_ops/alembic/versions/0032_add_external_ingest_batch_payloads.py
"""Add external_ingest_batch_payloads table (CHAOS-2693).

Transient payload store the durable stream references by pointer instead of
carrying the raw batch JSON on XADD (see docs/architecture/ for the
external-ingest stream design). Rows are deleted by the worker on terminal
status and swept by a beat-scheduled prune job as an orphan safety net.

Revision ID: 0032
Revises: 0031
Create Date: 2026-07-XX 00:00:00
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0032"
down_revision: str | None = "0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE_NAME = "external_ingest_batch_payloads"
_ORG_INDEX_NAME = "ix_external_ingest_batch_payloads_org_id"


def upgrade() -> None:
    if not _table_exists(_TABLE_NAME):
        op.create_table(
            _TABLE_NAME,
            sa.Column("ingestion_id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("schema_version", sa.Text(), nullable=False),
            sa.Column("payload_json", sa.LargeBinary(), nullable=False),
            sa.Column("byte_size", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("ingestion_id"),
        )
    _create_index_if_missing(_ORG_INDEX_NAME, _TABLE_NAME, ["org_id"])


def downgrade() -> None:
    if _table_exists(_TABLE_NAME):
        op.drop_table(_TABLE_NAME)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns)
```

Direct-SQL access helpers (house rule: no ORM-only paths for API
persistence) — use `sqlalchemy.text()` raw parameterized SQL over the
Postgres session, matching the codebase's established pattern (e.g.
`api/admin/routers/sync.py` uses `text("SELECT pg_advisory_xact_lock(:lock_key)")`):

```python
# src/dev_health_ops/external_ingest/payload_store.py  (used by router + worker)
from datetime import datetime, timezone

from sqlalchemy import text

async def upsert_payload(session, *, ingestion_id, org_id, schema_version, payload_bytes) -> None:
    # POST-CRITIQUE (CC22): upsert, not plain INSERT — a RETRY accept (stream_unavailable,
    # failed, or stale-accepted; CC13) reuses the SAME ingestion_id, so the row may already
    # exist (stream_unavailable: worker never ran) or not (failed: worker deleted it).
    # SELECT-then-UPDATE-or-INSERT keeps sqlite portability (no ON CONFLICT/RETURNING,
    # CC19). Safe without row locks: the idempotency row's unique index — FIRST-written in
    # the same accept txn — is the serialization point; the residual concurrent same-key
    # race surfaces as 503 ingest_temporarily_unavailable upstream (2695).
    # created_at passed as a Python-side UTC timestamp bound parameter, not
    # SQL now() -- keeps this portable across the sqlite-in-memory engine
    # used by unit tests (matches tests/test_rate_limit_observations.py's
    # convention: Base.metadata.create_all(sqlite engine), no live Postgres
    # in the default unit tier) and real Postgres in prod.
    existing = (
        await session.execute(
            text(
                "SELECT 1 FROM external_ingest_batch_payloads "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            {"ingestion_id": ingestion_id, "org_id": org_id},
        )
    ).first()
    params = {
        "ingestion_id": ingestion_id,
        "org_id": org_id,
        "schema_version": schema_version,
        "payload_json": payload_bytes,
        "byte_size": len(payload_bytes),
        "created_at": datetime.now(timezone.utc),
    }
    if existing:
        await session.execute(
            text(
                "UPDATE external_ingest_batch_payloads "
                "SET schema_version = :schema_version, payload_json = :payload_json, "
                "byte_size = :byte_size, created_at = :created_at "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            params,
        )
    else:
        await session.execute(
            text(
                "INSERT INTO external_ingest_batch_payloads "
                "(ingestion_id, org_id, schema_version, payload_json, byte_size, created_at) "
                "VALUES (:ingestion_id, :org_id, :schema_version, :payload_json, :byte_size, :created_at)"
            ),
            params,
        )

async def fetch_payload(session, *, ingestion_id, org_id) -> bytes | None:
    # org_id in the predicate even though ingestion_id is already unique --
    # defense in depth against a leaked/guessed UUID crossing tenants.
    row = (
        await session.execute(
            text(
                "SELECT payload_json FROM external_ingest_batch_payloads "
                "WHERE ingestion_id = :ingestion_id AND org_id = :org_id"
            ),
            {"ingestion_id": ingestion_id, "org_id": org_id},
        )
    ).first()
    return row[0] if row else None

async def delete_payload(session, *, ingestion_id) -> None:
    await session.execute(
        text("DELETE FROM external_ingest_batch_payloads WHERE ingestion_id = :ingestion_id"),
        {"ingestion_id": ingestion_id},
    )
```

### Celery wiring

```python
# workers/system_ops.py — add
@celery_app.task(
    bind=True,
    queue="external-ingest",
    name="dev_health_ops.workers.tasks.run_external_ingest_consumer",
)
def run_external_ingest_consumer(self, max_iterations: int = 100):
    from dev_health_ops.api.external_ingest.consumer import (
        consume_external_ingest_streams,
    )

    processed = consume_external_ingest_streams(max_iterations=max_iterations)
    return {"processed": processed}


@celery_app.task(
    bind=True,
    queue="monitoring",
    name="dev_health_ops.workers.tasks.external_ingest_stream_health",
)
def external_ingest_stream_health(self) -> dict:
    from dev_health_ops.api.external_ingest.stream_health import report_stream_health

    return report_stream_health()
```

```python
# workers/config.py — add to task_queues
"external-ingest": {},

# add to late_ack_excluded_tasks tuple (Redis Streams' own PEL is the
# durability layer, not Celery's; a worker crash mid-poll must not
# redeliver the *Celery task*, only the *stream entries* via reclaim)
"dev_health_ops.workers.tasks.run_external_ingest_consumer",

# add to beat_schedule
"process-external-ingest-streams": {
    "task": "dev_health_ops.workers.tasks.run_external_ingest_consumer",
    "schedule": stream_consumer_schedule_seconds,  # 30.0, reuse existing constant
    "kwargs": {"max_iterations": stream_consumer_max_iterations},  # 5
    "options": {"queue": "external-ingest", "expires": stream_consumer_expires_seconds},
},
"external-ingest-stream-health": {
    "task": "dev_health_ops.workers.tasks.external_ingest_stream_health",
    "schedule": 60.0,
    "options": {"queue": "monitoring"},
},
```

```yaml
# compose.yml — new dedicated worker (D8), mirrors worker-ingest exactly
worker-external-ingest:
  <<: *worker-base
  container_name: worker-external-ingest
  command: -A dev_health_ops.workers.celery_app worker --loglevel=info --disable-prefetch -Q external-ingest --concurrency=1
```

Option B (fallback, if a 4th worker container is rejected on cost grounds):
add `external-ingest` to `worker-ingest`'s existing `-Q` list instead of a
new container. Document explicitly in the compose.yml comment block
(`compose.yml:238-243`) that this reintroduces the exact starvation risk
that block warns about, if chosen.

`tests/test_compose_config.py::test_compose_workers_cover_every_celery_queue`
will fail the build until whichever option is chosen is reflected in both
`task_queues` and some worker's `-Q` list — this is a hard CI gate, not
optional.

---

## Files to create/modify

Create:
- `src/dev_health_ops/api/external_ingest/__init__.py`
- `src/dev_health_ops/api/external_ingest/streams.py`
- `src/dev_health_ops/api/external_ingest/consumer.py`
- `src/dev_health_ops/api/external_ingest/stream_health.py` (D9 `report_stream_health()`)
- `src/dev_health_ops/external_ingest/errors.py` (`PermanentProcessingError` — canonical location per plan module list; `consumer.py` imports from here)
- `src/dev_health_ops/external_ingest/payload_store.py`
- `src/dev_health_ops/models/external_ingest.py` (`ExternalIngestBatchPayload`)
- `src/dev_health_ops/alembic/versions/00XX_add_external_ingest_batch_payloads.py` (confirm real next revision id first)
- `tests/api/test_external_ingest_streams.py`
- `tests/api/test_external_ingest_consumer.py`
- `tests/api/test_external_ingest_stream_health.py`
- `tests/test_external_ingest_payload_store.py`
- `docs/architecture/external-ingest-stream-design.md` (house rule: document decisions in the same changeset)

Modify:
- `src/dev_health_ops/api/_stream_consumer.py` — add `enable_reclaim`/`reclaim_idle_ms`/`max_deliveries`/`reclaim_stale()` (additive, default-off; must not change behavior for `IngestStreamConsumer`/`ProductTelemetryStreamConsumer` — add regression tests asserting their `consume()` output is unchanged with the new attributes at their defaults)
- `src/dev_health_ops/workers/system_ops.py` — add `run_external_ingest_consumer`, `external_ingest_stream_health` tasks
- `src/dev_health_ops/workers/config.py` — `task_queues["external-ingest"]`, `late_ack_excluded_tasks`, two new `beat_schedule` entries
- `compose.yml` — new `worker-external-ingest` service (or Option B)
- `pyproject.toml` — no new runtime deps expected (`valkey`, `fakeredis[valkey]` already present); do not add `respx`/similar here, this issue has no outbound HTTP client work

Not modified by this issue (owned elsewhere, listed for interface clarity):
- `src/dev_health_ops/api/external_ingest/router.py`, `schemas.py`, `status.py`, `auth.py` (CHAOS-2691/2694/2696)
- `src/dev_health_ops/external_ingest/normalize.py`, `validate.py`, `processor.py`, `mappings.py`, `idempotency.py` (CHAOS-2697/2695) — `consumer.py` only imports `processor.process_batch`, treating it as a stable interface boundary
- `src/dev_health_ops/api/main.py` (router registration is CHAOS-2691's concern; this issue has no router)

---

## Test plan

Unit (no live services; `fakeredis.FakeValkey(decode_responses=True)` per
the established `tests/test_ingest_streams.py` convention):

- `test_external_ingest_streams.py`
  - `enqueue_external_ingest_batch` writes exactly one XADD entry with all
    required fields as strings, to `external-ingest:{org_id}:batches`.
  - `enqueue_external_ingest_batch` raises `StreamUnavailableError` when
    `REDIS_URL` unset (client is `None`).
  - `enqueue_external_ingest_batch` raises `StreamUnavailableError` when
    the underlying `xadd` call raises (simulate via a fake client whose
    `xadd` raises `ConnectionError`) — assert it is NOT silently swallowed
    (this is the regression test for D3/fail-closed).
  - `maxlen=100000, approximate=True` is passed on every `xadd` call
    (assert via a spy/fake).
  - `batches_stream_name`/`dlq_stream_name` produce the exact canonical
    keys for a given org_id.
  - `reenqueue_batch` delegates to `enqueue_external_ingest_batch` with the
    same org_id/ingestion_id.

- `test_external_ingest_consumer.py`
  - Happy path: one entry on `external-ingest:{org}:batches`, `process_entry`
    succeeds, entry is ACKed, not present in `XPENDING` afterward.
  - Permanent failure: `process_entry` raises `PermanentProcessingError` →
    entry lands on `external-ingest:{org}:dlq` with `reason` populated, and
    is ACKed (not retried).
  - Transient failure: `process_entry` raises `RuntimeError` (or a stand-in
    transient exception) → entry is NOT acked; still present in `XPENDING`
    after `handle_entries`.
  - Reclaim under `max_deliveries`: an entry idle beyond `reclaim_idle_ms`
    with `times_delivered < max_deliveries` is returned by `reclaim_stale`
    and reprocessed.
  - Reclaim at `max_deliveries`: entry is routed to DLQ and ACKed instead
    of reclaimed — assert it is NOT retried a 6th time.
  - DLQ key is derived per-org from the stream key (two different orgs'
    poison entries land on two different DLQ streams).
  - `move_to_dlq` failure is swallowed (best-effort, matches base class
    convention) — assert no exception propagates from `handle_entries`.
  - `_stream_consumer.py` regression: `IngestStreamConsumer` and
    `ProductTelemetryStreamConsumer` behavior (existing test files
    `tests/test_ingest_streams.py`, `tests/test_ingest_consumer_backoff.py`,
    `tests/api/test_product_telemetry_persist.py`,
    `tests/api/test_stream_consumer.py`) must still pass unmodified — run
    them explicitly as part of this issue's PR to prove the additive
    change is truly additive.

- `test_external_ingest_stream_health.py`
  - Reports `XLEN` and pending-count per discovered `external-ingest:*:batches`
    stream; empty when no streams exist; warns (log assertion via
    `caplog`) above the depth/age thresholds.

- `test_external_ingest_payload_store.py` — **confirmed convention** (not
  a live-Postgres marker): `tests/test_rate_limit_observations.py` runs
  `ProviderRateLimitObservation` tests against
  `create_engine("sqlite:///:memory:")` + `Base.metadata.create_all(engine)`,
  unmarked, in the default unit tier — there is no live-Postgres pytest
  marker anywhere in this codebase (only `clickhouse`/`benchmark` are
  registered in `pytest.ini`). Follow the exact same pattern: sqlite
  in-memory engine, `ExternalIngestBatchPayload.__table__.create(engine)` or
  `Base.metadata.create_all`, plain `Session`. This is why `upsert_payload`
  passes `created_at` as a Python-side bound parameter rather than SQL
  `now()` — keeps the raw `text()` SQL portable across sqlite (tests) and
  Postgres (prod) without a dialect branch.
  - insert → fetch (round-trip byte equality) → delete → fetch returns
    `None`.
  - `fetch_payload` with mismatched `org_id` returns `None` (tenant
    isolation check — org_id in the predicate even though `ingestion_id`
    alone is already unique, per the house rule "org_id in every join
    predicate", extended here to every lookup predicate).

Live-Postgres (optional, migration-only): the sqlite-based unit tests above
do not exercise the real Alembic migration path (sqlite has no
`information_schema`/`pg_catalog`, so `_table_exists`/`_create_index_if_missing`
in the migration script need a real Postgres to validate against). Add one
manual/CI-optional check, not gated behind a new pytest marker: run
`dev-hops migrate postgres upgrade`/`downgrade` against a scratch Postgres
DB as part of the Gate commands below (mirrors `local_validate.sh`'s
scratch-ClickHouse-DB discipline, applied to Postgres). Do not invent a
`@pytest.mark.postgres_live` marker unless a second, genuinely-needs-a-real-
Postgres-engine test case emerges beyond this one migration check.

E2E (out of scope for this issue's own PR, but this issue's consumer is
exercised end-to-end by CHAOS-2702).

---

## Gate commands

ops (`/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`):

```bash
# scrub any inherited direnv/app-config env before running (see memory:
# direnv pollutes gate runs)
env -u POSTGRES_URI -u CLICKHOUSE_URI -u REDIS_URL bash -c '
  cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
  .venv/bin/ruff format --check .
  .venv/bin/ruff check .
  .venv/bin/mypy --install-types --non-interactive .
'

# Per-issue scratch DB name to avoid clobbering a concurrently-running
# sibling issue's local_validate run (all sub-issues of CHAOS-2690 will be
# implemented in parallel worktrees against the same dev containers):
SCRATCH_DB=ci_local_validate_2693 bash ci/local_validate.sh

# Run the new/modified stream tests explicitly and fast during iteration:
.venv/bin/pytest tests/api/test_external_ingest_streams.py \
  tests/api/test_external_ingest_consumer.py \
  tests/api/test_external_ingest_stream_health.py \
  tests/test_ingest_streams.py tests/test_ingest_consumer_backoff.py \
  tests/api/test_product_telemetry_persist.py tests/api/test_stream_consumer.py \
  -v

# Compose invariant (queue must appear in both task_queues and a worker -Q list):
.venv/bin/pytest tests/test_compose_config.py -v
```

Postgres live test for the payload table (adjust DSN to a scratch Postgres
DB, never `devhealth`):

```bash
POSTGRES_URI="postgresql+asyncpg://devhealth:devhealth@localhost:5432/ci_local_validate_2693_pg" \
  .venv/bin/dev-hops migrate postgres upgrade
POSTGRES_URI="postgresql+asyncpg://devhealth:devhealth@localhost:5432/ci_local_validate_2693_pg" \
  .venv/bin/pytest tests/test_external_ingest_payload_store.py -v
```

---

## Live verification procedure

Against the dev compose stack (do NOT run migrations against `default`/
`devhealth` databases per house rules — this section assumes a disposable
verification org/DB or that migrations were already applied by another
in-flight sub-issue's PR merge; this issue's own dev-loop should stay
inside the scratch-DB gate above). Once `worker-external-ingest` (or the
Option-B queue addition) is deployed to the running compose stack:

```bash
# 1. Confirm the queue is wired and the beat entry is registered.
docker exec dev-health-worker-1 celery -A dev_health_ops.workers.celery_app inspect registered | grep run_external_ingest_consumer

# 2. Manually enqueue a synthetic pointer entry (simulating what CHAOS-2691's
#    router would do) directly via valkey-cli, to prove the stream/consumer
#    wiring works even before CHAOS-2691 lands:
ORG_ID=<a real org uuid from postgres>
docker exec dev-health-valkey-1 valkey-cli -n 1 XADD "external-ingest:${ORG_ID}:batches" '*' \
  ingestion_id "$(uuidgen)" org_id "$ORG_ID" source_system github \
  source_instance github.com/acme schema_version external-ingest.v1 \
  idempotency_key smoke-test-1 record_count 0 \
  window_started_at 2026-07-01T00:00:00Z window_ended_at 2026-07-01T01:00:00Z \
  enqueued_at 2026-07-01T00:00:00Z

# 3. Confirm the entry is visible and pending before the worker picks it up:
docker exec dev-health-valkey-1 valkey-cli -n 1 XLEN "external-ingest:${ORG_ID}:batches"
docker exec dev-health-valkey-1 valkey-cli -n 1 XPENDING "external-ingest:${ORG_ID}:batches" external-ingest-consumers

# 4. Trigger a manual (out-of-beat) consume pass to avoid waiting up to 30s:
docker exec dev-health-worker-external-ingest-1 python -c \
  "from dev_health_ops.api.external_ingest.consumer import consume_external_ingest_streams; print(consume_external_ingest_streams(max_iterations=1))"
# Expect an exception here until CHAOS-2697's processor.process_batch and the
# payload row exist -- that's expected at this stage; confirm it becomes a
# PermanentProcessingError (or a transient one that leaves the entry
# reclaimable) rather than an unhandled crash that kills the process.

# 5. Confirm the poison/transient entry's fate:
docker exec dev-health-valkey-1 valkey-cli -n 1 XLEN "external-ingest:${ORG_ID}:dlq"      # after max_deliveries
docker exec dev-health-valkey-1 valkey-cli -n 1 XPENDING "external-ingest:${ORG_ID}:batches" external-ingest-consumers  # after a transient failure

# 6. Confirm 503 fail-closed behavior in isolation (stop valkey, call the
#    producer helper directly via a REPL, since the router doesn't exist
#    in this issue's scope yet):
docker stop dev-health-valkey-1
docker exec dev-health-worker-1 python -c \
  "import asyncio; from dev_health_ops.api.external_ingest.streams import enqueue_external_ingest_batch, StreamUnavailableError
try:
    asyncio.run(enqueue_external_ingest_batch(org_id='test', ingestion_id='x', source_system='github', source_instance='i', schema_version='external-ingest.v1', idempotency_key='k', record_count=0, window_started_at='2026-07-01T00:00:00Z', window_ended_at='2026-07-01T00:00:00Z'))
except StreamUnavailableError as e:
    print('OK: fail-closed ->', e)
"
docker start dev-health-valkey-1

# 7. Confirm liveness/lag observability logs:
docker exec dev-health-worker-1 celery -A dev_health_ops.workers.celery_app call \
  dev_health_ops.workers.tasks.external_ingest_stream_health
docker logs dev-health-worker-1 2>&1 | grep external_ingest_stream_health | tail -5
```

---

## Dependencies on other sub-issues

- **CHAOS-2691** (REST contract/router) — hard producer-side dependency:
  the router is the sole caller of `enqueue_external_ingest_batch` and must
  (a) persist the status row (2694) + payload row (this issue's table) in
  one Postgres transaction before calling it, (b) map
  `StreamUnavailableError` → `503`, (c) update its response envelope's
  `stream` field to `external-ingest:<org_id>:batches` (not `:events`, per
  D1) once implemented.
- **CHAOS-2694** (status/rejection store) — this issue's payload table is a
  sibling to, not a replacement for, `external_ingest_batches`/
  `external_ingest_rejections`; migration ordering must be coordinated
  (confirm actual next Alembic revision id at implementation time, not the
  `0032` placeholder above) to avoid a branch collision if both land
  migrations in the same review window.
- **CHAOS-2697** (worker normalization) — consumes this issue's
  `consumer.py` via the `processor.process_batch(...)` interface contract
  (D5); must raise `PermanentProcessingError` (from
  `external_ingest/errors.py`, this issue creates the file/exception,
  2697 raises it) vs any other exception per the retry policy.
- **CHAOS-2699** (bounded recompute planner) — a natural consumer of the
  orphan-batch reconciliation gap (Gap G3) if a dedicated reconciler beat
  task is added later; not blocking for this issue.
- **CHAOS-2695** (idempotency/ownership) — logically upstream of the
  stream write (dedup happens before enqueue), no code dependency from this
  issue, but the interface assumption ("caller already deduped") must hold.

---

## Risks

1. **Shared-file blast radius.** `_stream_consumer.py` is modified to add
   the reclaim/retry capability; a mistake here can regress
   `/api/v1/ingest` and product-telemetry consumption in production. Must
   ship with the explicit regression tests listed above (existing test
   files re-run unmodified) and keep `enable_reclaim` default `False`.
2. **Orphan Postgres rows on stream-enqueue failure (Gap G3 / D2
   trade-off).** A `503` response can still leave a `status='received'`
   row with no consumer ever notified. No reconciler is built in this
   issue; until CHAOS-2694/2699 (or a new follow-up) adds one, such batches
   are invisible until a human notices via the stream-health logs or a
   customer support ticket about a batch stuck at `received`.
3. **DLQ has no replay tooling.** Poison/give-up entries are discoverable
   (`XRANGE`/`valkey-cli`) but there's no admin UI or CLI to re-drive them
   in v1 — recommend filing a fast-follow issue once real DLQ volume is
   observed in production.
4. **`fakeredis` command coverage.** The reclaim design depends on
   `XPENDING` (extended/IDLE form) and `XCLAIM`; confirm the pinned
   `fakeredis[valkey]` version in `pyproject.toml` actually implements
   these before writing tests against it — if it doesn't, fall back to
   `MagicMock`-based tests for the reclaim path specifically (as
   `tests/test_ingest_consumer_backoff.py` already does for backoff), and
   pin/bump `fakeredis` in `pyproject.toml` (never via `uv.lock` per the
   dependency-model house rule) if a newer version is needed.
5. **Fourth worker container (D8).** Adds deploy/ops surface area
   (container count, `WORKER_CONCURRENCY`-style env knobs, another compose
   service to monitor). If rejected, Option B (share `worker-ingest`)
   reintroduces the exact concurrency-starvation risk the existing
   `compose.yml` comments warn about — flagged as `decisionsNeeded`.
6. **8 MiB batch cap (D6) is a guess, not derived from a product
   requirement.** No customer volume data exists yet to validate this
   number; likely needs revisiting once CHAOS-2701 (docs/examples) and
   real customer payload sizes are known.
7. **Plan-doc/issue-text stream-naming discrepancy (D1)** may already be
   baked into any customer-facing documentation drafted in parallel
   (CHAOS-2701) — coordinate so `:events` doesn't leak into a published
   doc before the naming is finalized as `:batches`.

---

## Gaps identified (for cross-referencing in structured output)

- **G1 (major):** Neither plan doc's module inventory, nor any sub-issue's
  explicit scope text, lists a stream-*consumer* module or Celery
  task/beat/compose wiring for external-ingest — only `streams.py`
  (producer) is named in CHAOS-2693's own Linear scope, and CHAOS-2697's
  scope lists only `processor.py`/`normalize.py`/`validate.py`/
  `mappings.py`. This brief resolves the gap by having CHAOS-2693 own the
  consumer/DLQ/Celery-wiring layer (mirrors the existing
  `streams.py`+`consumer.py` pairing convention in both `api/ingest/` and
  `api/product_telemetry/`), with CHAOS-2697 owning only the
  `processor.process_batch` callback it delegates to.
- **G2 (major):** The shared `StreamConsumer` base class has no true
  retry/redelivery mechanism today (every failure is immediately DLQ'd and
  ACKed) — acceptable for existing best-effort internal consumers, not
  acceptable for external-ingest's stated durability bar. Resolved via the
  additive `enable_reclaim` extension (D5).
- **G3 (major):** Payload-in-Postgres-then-pointer-on-stream (D2) means the
  Postgres commit and the stream `XADD` are two separate operations with no
  distributed transaction between them; a partial failure after Postgres
  commit but before/during `XADD` leaves an orphaned `received` row. No
  reconciler exists yet for this. Flagged as a risk and a dependency onto
  CHAOS-2694/2699, not solved in this issue.
- **G4 (minor):** Stream key naming conflict between the core plan doc
  (`:events`) and the CHAOS-2693 issue text (`:batches`) — resolved via D1,
  needs a doc fix (plan doc response example) once CHAOS-2691 lands.
- **G5 (minor, resolved):** Confirmed no live-Postgres pytest marker exists
  (only `clickhouse`/`benchmark` are registered in `pytest.ini`); confirmed
  `tests/test_rate_limit_observations.py` runs its Postgres-model tests
  against a `sqlite:///:memory:` engine, unmarked, in the default unit
  tier. This issue's payload-store tests follow that exact convention (see
  Test plan) — no new marker needed, and `upsert_payload`'s SQL was
  adjusted (Python-side `created_at` bound parameter instead of SQL
  `now()`) specifically so it works unmodified against both engines.
