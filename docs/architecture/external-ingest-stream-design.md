# External-ingest durable stream and DLQ (CHAOS-2693)

Part of the [customer-push ingestion epic](adr-003-external-ingest-rest-boundary.md)
(CHAOS-2690). This doc records the transport-layer decisions between "a batch
has been accepted and durably persisted" and "a worker has picked it up for
processing": stream naming, pointer-only payload placement, fail-closed
enqueue, and the retry/reclaim/DLQ machinery.

> **DEPLOYMENT BLOCKER (adversarial-review finding, critical): do not deploy
> this issue's PR standalone alongside CHAOS-2691's interim router without
> CHAOS-2695.** This issue removes the inline `payload` field from the
> stream `XADD` (D2, below) as ratified/pinned by master-spec CC9. CC22 pins
> the corresponding Postgres write (`payload_store.upsert_payload`, called
> from the accept transaction) as CHAOS-2695's wave-4 router-rewire
> responsibility -- until that PR lands, CHAOS-2691's interim `POST /batches`
> handler calls `enqueue_batch(...)` with the raw body as `payload_json`,
> which this issue's hardened `enqueue_batch()` silently accepts-but-discards
> (kept as a no-op kwarg only for call-site compatibility, since editing
> `router.py` is out of this issue's assigned scope -- see the "Scope
> boundaries" note in the PR). **Net effect: any environment that deploys
> this issue's PR without ALSO deploying CHAOS-2695 will accept customer
> batches (202) whose raw payload is persisted nowhere at all** -- not on
> the stream (removed here) and not in Postgres (not wired until 2695).
> This is a genuine regression from the wave-1-only state (payload at least
> sat inertly on the stream). The epic owner must sequence CHAOS-2693 and
> CHAOS-2695 into the same deployment, or hold CHAOS-2691/2693 out of any
> environment that accepts real traffic until CHAOS-2695 merges.
>
> **RESOLVED (CHAOS-2695):** the payload upsert was pulled forward into
> CHAOS-2693's own PR (authorized router amendment: `upsert_payload` +
> commit before enqueue), and CHAOS-2695's full CC22 rewrite has since
> landed the idempotency-row-first sequence around it -- see
> [external-ingest-idempotency-ownership.md](external-ingest-idempotency-ownership.md).
> Note: on a stream-unavailable 503 the payload row is now deliberately
> KEPT (the durable `stream_unavailable` status row references it and the
> same-key RETRY reuses it), superseding the interim orphan-delete this
> doc's D2 era assumed.

## Per-org streams, not one shared stream

`external-ingest:<org_id>:batches` + `external-ingest:<org_id>:dlq`, one
stream pair per org rather than a single stream with an `org_id` field.
Redis Streams delivers entries in strict ID order regardless of logical
owner, so a single huge/bursty org's backlog would delay every other org's
consumer-group progress on a shared stream. `StreamConsumer.discover_streams()`
(`api/_stream_consumer.py`) already supports `SCAN ... TYPE stream` wildcard
discovery (`external-ingest:*:batches`), so per-org keys need no separate
registry, and per-org DLQ keys are equally discoverable
(`external-ingest:*:dlq`) -- a single bad-actor/misconfigured org flooding
poison batches doesn't crowd out DLQ visibility for other orgs.

## Pointer-only transport, not payload-on-stream

The stream entry carries batch *metadata* only: `ingestion_id, org_id,
source_system, source_instance, schema_version, idempotency_key,
record_count, window_started_at, window_ended_at, enqueued_at` -- all short
strings. The full batch JSON lives in Postgres
(`external_ingest_batch_payloads`, DDL/model hosted by CHAOS-2694's
migration `0033`; this issue owns the `external_ingest/payload_store.py`
raw-SQL accessors: `upsert_payload`/`fetch_payload`/`delete_payload`). The
worker fetches the payload by `ingestion_id` (+`org_id` predicate).

Why: batches can be multi-MB. Putting full JSON on `XADD` would (a) break
the existing `maxlen=100_000, approximate=True` backpressure convention,
which bounds by *entry count* not bytes; (b) make DLQ entries themselves
multi-MB, defeating DLQ's purpose as a lightweight operational trail; (c)
mean a stream trim under load could silently lose the only copy of a
customer's data before a worker consumes it.

`upsert_payload()` is a SELECT-then-UPDATE-or-INSERT in the caller's own
transaction (no `ON CONFLICT`/`RETURNING` -- sqlite-portable, matching the
plan's dialect-portable-SQL mandate): a RETRY accept reuses the same
`ingestion_id`, so the row may already exist (`stream_unavailable` case:
worker never ran) or not (`failed` case: worker already deleted it on
terminal status). The idempotency row (`external_ingest_batches`, written
first under its unique index in the same accept transaction) is the
serialization point for concurrent same-key accepts; the residual sub-ms
concurrent-insert race surfaces as `503 ingest_temporarily_unavailable`
upstream (CHAOS-2695), not as a constraint violation here.

Trade-off: the Postgres commit and the stream `XADD` are two separate
operations with no distributed transaction between them. A `503` response
can still leave a `status='received'`-equivalent (`accepted`) row with no
consumer ever notified if the `XADD` fails or a stream trim races an unread
entry. `reenqueue_batch()` (`api/external_ingest/streams.py`) is exposed as
the seam a future reconciler (CHAOS-2769, and CHAOS-2695's RETRY-idempotency
path reusing the same `ingestion_id`) calls; no reconciler is scheduled by
this issue.

## Fail-closed, never accept-and-warn

`enqueue_batch()` raises `StreamUnavailableError` on any failure -- it never
returns a boolean/accept-and-warn sentinel. This mirrors
`product_telemetry/streams.py`'s `raise ConnectionError(...)` and is
stricter than legacy `/api/v1/ingest`'s silent-drop-on-`False`. The router
(CHAOS-2691) maps this to `503 stream_unavailable`.

## Consumer: shared `StreamConsumer` base + additive reclaim extension

`ExternalIngestStreamConsumer` (`api/external_ingest/consumer.py`) subclasses
the shared `StreamConsumer` base (`api/_stream_consumer.py`) rather than
hand-rolling `XREADGROUP` -- that base class exists specifically because two
independent consumers (ingest, product-telemetry) regressed the same two
production bugs (blocking-read socket-timeout race; unguarded-loop crash).

The base class's default `handle_entries()` has no true retry path: every
failure, permanent or transient, is immediately DLQ'd and ACKed in the same
pass. That's acceptable for the existing best-effort internal consumers but
not for external-ingest's stricter durability bar -- a transient
ClickHouse/Postgres blip during sink-write must be retried, not immediately
treated as a customer-data-loss event.

This issue adds an **opt-in, default-`False`** capability to the shared base
(`enable_reclaim`, `reclaim_idle_ms=900_000`, `max_deliveries=5`,
`reclaim_stale()`), wired into `consume()` immediately before each stream's
`XREADGROUP` call. Existing consumers (ingest, product-telemetry) are
byte-for-byte unaffected: `reclaim_stale()` returns `[]` without touching
Redis when `enable_reclaim` is `False` (see
`tests/api/test_stream_consumer.py`'s explicit regression coverage, plus the
unmodified re-run of the existing ingest/product-telemetry test files).

`reclaim_idle_ms=900_000` (15 minutes), not a naive 60s: the in-process
retry ladder alone can sleep ~14s, and a 1000-record batch can take well
over 60s to process -- a short reclaim window risks two workers concurrently
processing the same entry.

Retry policy (`external_ingest/errors.py::PermanentProcessingError`):

- `PermanentProcessingError` (unsupported schema version, a structurally
  invalid envelope that survived API-layer validation) -> immediate DLQ +
  ACK, no retry.
- Any other exception (including unclassified ones, treated conservatively
  as transient) -> **left un-ACKed**, staying in the consumer group's PEL
  for `reclaim_stale()` to retry on a later poll, up to `max_deliveries`.
  Once exhausted, `reclaim_stale()` itself calls `move_to_dlq()` (the
  give-up path) -- routed to the same DLQ logic as an explicit permanent
  failure, including the `mark_batch_failed` call below.

This is a new interface contract with CHAOS-2697's worker
(`external_ingest/processor.py::process_batch`), documented in
`external_ingest/errors.py` so both issues import the same canonical
`PermanentProcessingError` type.

## DLQ: best-effort XADD-and-ACK + mark_batch_failed

Giving up on an entry (permanent failure, or transient exhausted at
`max_deliveries`) does two things:

1. Best-effort `XADD` to the per-org DLQ stream (`external-ingest:<org_id>:dlq`)
   with `original_stream`, `entry_id`, `reason`, `ingestion_id`, `org_id`,
   `moved_at`. A DLQ write failure is logged and swallowed, matching the
   base class's existing DLQ philosophy -- it must never crash the consumer
   loop.
2. A best-effort call to `external_ingest.processor.mark_batch_failed(
   ingestion_id, org_id, reason)` (CHAOS-2697's pinned worker contract) so
   the batch row reaches `failed` and becomes resubmittable via the same
   idempotency key. Import-tolerant: CHAOS-2697 lands after this issue, so
   until it does, this logs a warning and returns rather than raising
   `ImportError` into the consumer loop.

Both the async permanent-failure call site (inside the batch's single
`run_async` event loop) and the sync give-up call site (the base's
`reclaim_stale()`, called outside any running loop) route through the same
XADD logic but call `mark_batch_failed` via their respective sync/async
paths -- see `consumer.py`'s `_dlq_entry_async` vs `move_to_dlq` docstrings.
Calling the sync path's `run_async()` from inside the async batch handler
would trip `run_async`'s own re-entrancy guard; this split exists
specifically to avoid that (caught by
`tests/api/test_external_ingest_consumer.py::TestPermanentFailure::test_calls_mark_batch_failed`
during implementation).

## Idempotent-skip guard

Before (re)processing ANY entry -- freshly read or reclaimed -- the consumer
loads the batch's current status (`api.external_ingest.status.get_batch`,
CHAOS-2694) and ACKs-and-skips without reprocessing if it is already
terminal (`completed`/`partial`/`failed`). This prevents double processing
when an entry is reclaimed after a slow-but-ultimately-successful run raced
the 15-minute reclaim window. A status-lookup failure (e.g. a transient
Postgres blip) fails open (treated as non-terminal, processing proceeds)
rather than silently stranding the batch.

## Deployment invariant: single replica, `--concurrency=1`

Exactly ONE `worker-external-ingest` replica must run, at Celery
`--concurrency=1`, across every deploy target (`compose.yml`,
`compose.production.yml`, `docker-swarm/stack.yml`,
`kubernetes/worker.yaml`, the Helm chart's `workerExternalIngest` pool).
The reclaim design assumes a single logical consumer identity draining the
PEL; scaling replicas without first revisiting reclaim semantics
reintroduces the double-processing window the 15-minute `reclaim_idle_ms`
and the idempotent-skip guard are built to close.
`tests/test_compose_config.py` enforces queue/worker topology coverage
across all deploy targets; the `--concurrency=1` flag itself is a comment
convention, not a machine-checked invariant, mirroring the existing
`worker-ingest` precedent.

## Dedicated Celery queue, not shared with `worker-ingest`

`external-ingest` is its own Celery queue, consumed only by the dedicated
`worker-external-ingest` container (`-Q external-ingest --concurrency=1`).
External-ingest is customer-facing (spiky, potentially larger batches) and
must not have its processing throughput hostage to an unrelated internal
consumer backlog (`worker-ingest`'s `/api/v1/ingest` + product-telemetry
traffic), nor vice versa.

## Observability: structured logs, no new table

`external_ingest_stream_health` (`api/external_ingest/stream_health.py`,
beat-scheduled every 60s on the `monitoring` queue) mirrors the existing
`workers/queue_monitor.py` convention: no Prometheus/statsd exporter exists
anywhere in this codebase today, so this logs depth (`XLEN`), pending count,
and the oldest pending entry's idle time (`XPENDING`) per discovered
`external-ingest:*:batches`/`external-ingest:*:dlq` stream, warning above
`STREAM_DEPTH_WARNING_THRESHOLD`/`STREAM_AGE_WARNING_MS`. A future
ClickHouse/Data-Health-UI surface for this is out of scope, same posture as
`queue_monitor.py`.

## Risks / follow-ups

1. **Shared-file blast radius.** `_stream_consumer.py`'s reclaim extension
   is additive and default-off, with explicit regression coverage (the
   unmodified existing ingest/product-telemetry test files, plus new tests
   asserting the disabled defaults) -- see the "Consumer" section above.
2. **Orphan Postgres rows on stream-enqueue failure.** No reconciler is
   built in this issue; `reenqueue_batch()` is the seam for a future one
   (CHAOS-2769 / a filed follow-up).
3. **DLQ has no replay tooling.** Poison/give-up entries are discoverable
   via `XRANGE`/`valkey-cli` but there is no admin UI or CLI to re-drive
   them in v1.
4. **`external_ingest.processor.mark_batch_failed` doesn't exist yet.**
   CHAOS-2697 lands after this issue; until it does, the give-up path logs
   a warning and the batch row is not marked `failed` (it remains in
   whatever status the worker itself left it in, or `accepted` if the
   worker crashed before reaching a status write). This is expected and
   resolves itself once CHAOS-2697 merges -- no action needed in this PR.
5. **`external_ingest.processor.process_batch` doesn't exist yet either --
   guarded at the consumer entry point, not just the give-up path.**
   `ExternalIngestStreamConsumer.consume()` checks module availability
   before claiming any entries and is a full no-op (no `XREADGROUP`, no
   reclaim) when the processor is missing, so beat-scheduling this consumer
   ahead of CHAOS-2697 leaves entries completely untouched rather than
   burning through `max_deliveries` reclaim cycles for every batch
   (adversarial-review finding, fixed in this PR).
6. **DEPLOYMENT BLOCKER: payload persists nowhere until CHAOS-2695 lands.**
   See the callout at the top of this document (adversarial-review finding,
   critical) -- do not deploy this PR standalone alongside CHAOS-2691's
   interim router without CHAOS-2695's payload-upsert wiring also present.
