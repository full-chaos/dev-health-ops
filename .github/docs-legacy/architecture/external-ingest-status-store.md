# External-ingest status store (CHAOS-2694)

Part of the [customer-push ingestion epic](adr-003-external-ingest-rest-boundary.md)
(CHAOS-2690). This doc records the durable status-store decisions for
answering "what happened to ingestion batch X" -- the DDL, the direct-SQL
access pattern, the state machine, bounded-rejection diagnostics, and the
retention policy.

## Postgres, not ClickHouse

`external_ingest_batches` / `external_ingest_rejections` /
`external_ingest_batch_payloads` are Postgres tables, not ClickHouse, for the
same reason as `provider_rate_limit_observations` (migration 0031) and
`external_ingest_sources` / `external_ingest_tokens` (migration 0032): this
is transactional state that must support a **strongly-consistent
read-after-write** status immediately after `202 Accepted` (`dev-hops push
batch --poll`, CHAOS-2700), and it joins the ingest-token/source-registration
model (CHAOS-2696) in the same database. ClickHouse's async merge semantics
on `ReplacingMergeTree` would make "poll immediately after accept" flaky, and
there is no transactional join between a separate analytics cluster and the
auth tables consulted on every request.

## Direct SQL, not ORM CRUD

The core implementation plan mandates direct SQL for API persistence/status
queries, which diverges from the house convention of using the ORM for
`session.add()`/`session.query()` elsewhere. This is resolved by keeping a
declarative `Base` model (`ExternalIngestBatch`, `ExternalIngestRejection`,
`ExternalIngestBatchPayload` in `src/dev_health_ops/models/external_ingest.py`)
purely as the schema-of-record for Alembic and for
`Base.metadata.create_all()` in sqlite-backed unit tests -- but all reads and
writes in `src/dev_health_ops/api/external_ingest/status.py` go through
`session.execute(text(...), params)`. This mirrors the established pattern in
`api/billing/reconciliation_service.py` and `api/billing/refund_service.py`.

Consequences of dialect-portable SQL (no `RETURNING`, no `ON CONFLICT`, no
Postgres-only functions -- there is no live-Postgres pytest tier, only
sqlite-in-memory unit tests plus a manual live-verification runbook, see
"Live verification" below):

- `ingestion_id`/rejection `id` values are generated in Python
  (`uuid.uuid4()`) and always bound as `str(...)`, never raw `uuid.UUID`
  objects, matching the `refund_service.py` precedent (`text()` binds get no
  column-type-aware processing, and asyncpg's UUID codec accepts plain
  strings).
- Idempotency-key collisions are detected by catching `IntegrityError` on the
  unique constraint (via a `session.begin_nested()` SAVEPOINT so the
  caller's outer transaction survives the violation), not `ON CONFLICT`.
- `created_at`/`updated_at`/`completed_at` are always passed as explicit
  Python `datetime` bind params -- **never** relied upon via the migration's
  `server_default=sa.text("now()")`, which is a safety net for manual `psql`
  inserts only, not something the status.py write paths use.
- `error_summary`/`record_counts` (JSON columns) are serialized with
  `json.dumps(...)` before binding and parsed with `json.loads(...)` on read
  -- neither SQLAlchemy's `text()` nor asyncpg apply JSON (de)serialization
  to untyped raw-SQL parameters.

## State machine

```
accepted -> (stream_unavailable) -> processing -> completed | partial | failed
```

- `accepted`: row committed (server default), enqueued or about to be.
- `stream_unavailable`: row committed but the stream enqueue failed (client
  got a `503`; retryable). `mark_stream_unavailable()` performs this
  transition; callers MUST commit this write **before** raising the 503
  ("commit-before-raise") so a resubmission has durable state to act on.
- `processing`: the worker has picked up the batch.
- Terminal: `completed` (0 rejected), `partial` (some rejected), `failed` (0
  accepted, given a non-empty batch). Derived by
  `terminal_status_for(items_received, items_accepted, items_rejected)`
  (`models/external_ingest.py`), a pure function with no DB dependency.

`mark_processing()`/`mark_stream_unavailable()` are idempotent no-op UPDATEs
guarded by a `WHERE status = '<expected-from-status>'` predicate (never a DB
`CHECK` constraint -- this matches the `SyncRunStatus`/
`SyncComputeCheckpointStatus` precedent of a Python-enum-validated `Text`
column). `complete_batch()` is stricter (adversarial-review finding): it only
accepts a batch currently in `processing` -- calling it against `accepted` or
`stream_unavailable` raises `ValueError` rather than silently completing an
unprocessed or never-enqueued batch, and the caller-supplied
`items_accepted`/`items_rejected` must be non-negative and sum to the batch's
recorded `items_received`, also raising otherwise. Once a batch is already
terminal, `complete_batch()` is a pure no-op: the (expected-identical)
inputs are discarded and the already-persisted row is returned unchanged --
this is what makes redelivered stream entries (at-least-once delivery) safe
to reprocess without regressing a terminal status or double counting rejection
rows -- see "Idempotency" below.

`attempts` (default 1) tracks accept-attempt count for CHAOS-2695's
same-key+hash resubmission policy (stale-accepted/`failed` -> fresh accept,
same `ingestion_id`, `attempts += 1`); this issue only hosts the column, the
increment logic belongs to CHAOS-2695's router rewire.

### Concurrency safety (adversarial-review finding)

`complete_batch()`'s status/counter write is an atomic compare-and-swap --
`UPDATE ... WHERE status = 'processing'` -- not a check-then-write. If two
callers race to complete the same batch, the row lock the UPDATE takes
serializes them: only the caller whose UPDATE actually matched a row (checked
via `rowcount`) proceeds to insert rejection rows; the loser re-reads and
returns whatever the winner persisted, discarding its own outcome entirely.
The documented deployment topology (single `worker-external-ingest` replica
at concurrency=1, plus the consumer's own idempotent-skip guard -- master-spec
CC11) shouldn't produce concurrent calls for the same `ingestion_id` in
practice, but this primitive's correctness doesn't rely on that discipline
holding -- the DB-level guarantee holds regardless. The `(ingestion_id,
record_index)` unique index on `external_ingest_rejections` is a second,
DB-enforced backstop against ever double-inserting a rejection row.

## Bounded rejection diagnostics

`MAX_STORED_REJECTIONS_PER_BATCH = 1000` (batch limits are capped at 1000
records -- master-spec CC3 -- so this stores *all* rejections in the worst
case; the cap is kept anyway as a safety bound). `complete_batch()`:

1. Stores up to the cap as individual `external_ingest_rejections` rows
   (`record_index`, `record_kind`, `external_id`, `code`, `message`, `path`).
2. Always records the **true** total on the batch row
   (`items_rejected`) and in `error_summary`, even when the stored rows are
   truncated:

   ```json
   {
     "total_rejected": 4213,
     "stored_rejections": 1000,
     "truncated": true,
     "top_codes": [{"code": "missing_external_id", "count": 3980}, ...]
   }
   ```

   The true magnitude of a failure is never hidden by the storage cap.
3. Is idempotent under worker redelivery: once a batch is already in a
   terminal status, a second `complete_batch()` call with the same inputs
   skips re-inserting rejection rows (no duplicates) but still re-runs the
   status/count UPDATE (a no-op given identical inputs).

## FK cascade divergence from `provider_rate_limit_observations`

`external_ingest_rejections.ingestion_id` **is** a foreign key to
`external_ingest_batches.ingestion_id` with `ON DELETE CASCADE` -- a
deliberate divergence from the FK-less `provider_rate_limit_observations`
precedent. Rejection rows have no independent existence or retention
requirement apart from their parent batch (unlike a rate-limit observation,
which must outlive a pruned `sync_run`); a single prune sweep on
`external_ingest_batches` is therefore sufficient to clean both tables.

`external_ingest_batch_payloads` (CHAOS-2693's transient raw-payload table,
hosted in this migration per the fixed migration chain, CC19) has **no** FK
to `external_ingest_batches`: it is written before the batch row exists in
CHAOS-2695's accept sequence and deleted independently by the worker on
terminal status (or swept as an orphan by CHAOS-2693's own prune task after
`EXTERNAL_INGEST_PAYLOAD_MAX_AGE_HOURS`), so its lifecycle is not tied to the
batch row's lifecycle.

## Retention

`EXTERNAL_INGEST_STATUS_RETENTION_DAYS = 90` (env-tunable), beat-scheduled at
`crontab(hour=5, minute=15)` on the existing `sync` queue (immediately after
`prune-rate-limit-observations` at 05:00; no new Celery queue).
`workers/external_ingest_reconciler.py::prune_external_ingest_batches`
deletes batches (cascading to rejections) whose `created_at` is older than
the window **and** whose `status` is one of `completed`/`partial`/`failed`,
in bounded chunks of 500 rows (each chunk committed independently, per an
adversarial-review finding -- a single unbounded `DELETE` against a large
backlog, e.g. the first run after months of accumulation, risks one
long-running transaction). It never deletes a batch still
`accepted`/`processing`/`stream_unavailable`, even past retention -- a stuck
batch past retention is a bug signal that should stay visible, not silently
disappear. This is a **deliberate, pinned design decision** (master-spec
CC13), not a gap: an adversarial review flagged "non-terminal rows accumulate
unbounded" as a concern, but the brief and master-spec explicitly scope a
defense-in-depth reconciler for never-resubmitted stale batches as a
**separate, filed follow-up issue**, out of v1 -- auto-expiring or archiving
stuck rows here would contradict the "stay visible as a bug signal" intent.
90 days (vs. the 14-day rate-limit-observation precedent) reflects that this
is customer-support/audit-facing operational history, closer in spirit to
audit logs than to transient rate-limit telemetry.

This task is retention-only. It never re-enqueues or resurrects stuck
batches.

## Endpoints

`src/dev_health_ops/api/external_ingest/status.py` defines its own
`APIRouter` (`status_router`), mounted directly in `api/main.py` -- it does
**not** append to CHAOS-2691's `router.py`/`schemas.py`, keeping wave-2 files
disjoint from CHAOS-2692 (schema registry) and CHAOS-2712 (real auth). Its
response Pydantic models (camelCase, matching the data-plane wire
convention) live locally in `status.py`.

- `GET /api/v1/external-ingest/batches` -- paginated/filterable list
  (`sourceSystem`, `sourceInstance`, `status`, `createdAfter`,
  `createdBefore`), `ingest:status` scope, `INGEST_READ_LIMIT` (120/min,
  token-keyed).
- `GET /api/v1/external-ingest/batches/{ingestion_id}` -- single-batch status
  + paginated rejected-record diagnostics (`errorLimit`/`errorOffset`), same
  scope/rate limit. Tenant isolation returns `404 not_found` for both
  "does not exist" and "exists but belongs to a different org" -- never a
  `403` (a token from one org must not be able to confirm the existence of
  another org's ingestion IDs).
- `/api/v1/admin/customer-push/sources/{id}/batches`,
  `/api/v1/admin/customer-push/batches/{ingestion_id}`,
  `/api/v1/admin/customer-push/schemas*` -- admin-plane read proxies over the
  same store (session-JWT + `require_admin`, house `HTTPException`/snake_case
  conventions -- deliberately a second, distinct surface convention from the
  data-plane's `ExternalIngestError`/camelCase envelope).

Neither surface exposes a `recompute_status` block in this issue's wave --
CHAOS-2699 (wave 3, migration 0034) extends `BatchStatusResponse`/
`AdminBatchResponse` with that block once the recompute-status columns land
(master-spec CC21); this is a deliberate, pinned cross-wave file touch.

## Live verification

There is no live-Postgres pytest tier in this repo (only sqlite-in-memory
unit tests and a `clickhouse` marker exist). Live-Postgres verification for
this store is a manual runbook step: apply migration 0033 to a scratch
Postgres database, confirm the three tables/indexes/FK via `psql \d`, and
exercise `status.py`'s functions directly via a one-off script before/after
CHAOS-2691's router and CHAOS-2696's auth land.
