# Implementation Brief: CHAOS-2694 — Ingest status and rejected-record diagnostics

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. Migration renumbered **0033** (`down_revision="0032"` — CHAOS-2696's auth tables are
>    0032; CHAOS-2699's recompute columns are 0034). Fixed chain, no runtime `heads` guessing.
> 2. 0033 ALSO creates **`external_ingest_batch_payloads`** (CHAOS-2693's transient payload
>    table — DDL/model per brief-2693 §DDL, hosted here so wave 3 has no migration; 2693
>    keeps the `payload_store.py` accessors and prune task).
> 3. `external_ingest_batches` gains three more columns: `attempts INTEGER NOT NULL
>    DEFAULT 1` (CC13 failed-batch resubmission), `record_counts JSON NULL` (per-kind counts
>    for Screen 6/detail), and the status enum adds **`stream_unavailable`** (non-terminal,
>    retryable — set when Postgres commit succeeded but XADD failed). Full enum:
>    `accepted | stream_unavailable | processing | completed | partial | failed`.
> 4. Max-records assumption corrected: **1000** per batch (not 5000) —
>    `MAX_STORED_REJECTIONS_PER_BATCH=1000` therefore stores ALL rejections in the worst
>    case; keep the cap as a safety bound anyway.
> 5. Scope += **admin proxies** in `api/admin/routers/customer_push.py` (created by 2696 in
>    wave 1; this issue appends): `GET .../sources/{id}/batches` (filters: status, producer,
>    from, to, limit/offset — NO record_kind filter in v1), `GET .../batches/{ingestion_id}`
>    (with rejected_records, record_counts, recompute_status), `GET .../schemas*`
>    passthrough to 2692's registry. Session-JWT + require_admin (CC25).
> 6. `status.py` defines its OWN `APIRouter` mounted directly in `main.py` (one line) and
>    defines its response Pydantic models locally — do NOT append to 2691's `schemas.py`
>    or edit 2691's `router.py` (keeps wave-2 files disjoint from 2692/2712).
> 7. GET data-plane errors use the `ExternalIngestError` envelope (CC16), code `not_found`
>    for cross-org/nonexistent (still a 404, same tenant-isolation semantics as D11).
> 8. Auth interface pinned: `IngestAuthContext(org_id: str, scopes: frozenset[str],
>    token_id: str | None, source: IngestSource | None)`; dependency
>    `require_ingest_scope("ingest:status")` from `api/external_ingest/auth.py` (interim
>    body exists from wave 1 — no local stub needed).
> 9. Reconciler task = retention prune ONLY; orphan re-enqueue is a new follow-up issue.
> 10. Status writes are upsert-by-ingestion_id (replay-safe, CC23).
> 11. **POST-CRITIQUE (CC21/CC25): GET batch detail (data-plane AND admin proxy) does
>     NOT surface recompute_status in this issue (wave 2)** — the columns land in 2699's
>     0034 (wave 3) and 2699 ALSO owns extending the response block in this issue's
>     status.py (deliberate cross-wave file touch). Do not reference recompute columns
>     anywhere in 0033 or the wave-2 response models.
> 12. **POST-CRITIQUE (CC15): GET /batches list + detail take
>     `INGEST_READ_LIMIT="120/minute"`** (token-keyed via get_ingest_token_key; constant
>     from 2691's rate_limit.py work).
> 13. **POST-CRITIQUE (CC13) heads-up**: 2695 (wave 4) treats same-key+hash resubmission
>     of a batch stuck in accepted/processing with `updated_at` older than
>     EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES=15 as RETRY. 0033 needs no extra columns
>     for this (updated_at + attempts already exist) — just don't add a partial index
>     that assumes accepted rows are always young.

Parent epic: CHAOS-2690 (External customer-push ingestion API)
Repo: `dev-health-ops`, worktree `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`
Plan refs:
- `docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md` (core plan — "Status store" / "Error store" sections)
- `docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md` ("Webhook status model", web "Screen 6: Ingest status")

Linear issue text (verbatim scope/AC) confirmed via `linear-cli issues get CHAOS-2694` — see body below; this brief supersedes nothing in it, only fills gaps.

---

## 1. Scope

Own everything needed to durably answer "what happened to ingestion batch X":

1. Postgres DDL (new Alembic migration `0032_*`) for two tables:
   - `external_ingest_batches` — one row per accepted batch, its status, and rollup counts.
   - `external_ingest_rejections` — bounded per-record rejection diagnostics for a batch.
2. `src/dev_health_ops/api/external_ingest/status.py` — the direct-SQL read/write module other sub-issues call into:
   - batch create / idempotency-key lookup (consumed by CHAOS-2691's `POST /batches`, coordinated with CHAOS-2695's conflict policy)
   - state transitions `accepted -> processing -> {completed, partial, failed}` (consumed by CHAOS-2697/2698's worker)
   - bounded rejection-row writes + aggregated `error_summary` (consumed by the worker)
   - paginated reads (consumed by this issue's own GET endpoints)
3. `GET /api/v1/external-ingest/batches/{ingestion_id}` — single-batch status + paginated rejected-record diagnostics, tenant-scoped, requires `ingest:status` scope.
4. **Gap-fill, recommended in-scope addition:** `GET /api/v1/external-ingest/batches` — paginated/filterable list endpoint. Neither the core plan nor the CHAOS-2694 issue text specifies this, but the webhook/setup addendum's "Screen 6: Ingest status" web mock (filters: source, status, time window, producer type; columns: ingestion id, source, window, status, items received/accepted/rejected, created at, completed at) has no other backend owner, and CHAOS-2714 (web setup screens) cannot be built without it. Since it reads the exact same table this issue owns, building it here is the only place that doesn't duplicate the status-store contract. See Gaps/decisionsNeeded.
5. Retention/pruning: a beat-scheduled Celery task that deletes batches (and cascade-deletes their rejections) past a retention window, following the `provider_rate_limit_observations` prune precedent.
6. Tests: sqlite-in-memory unit tests for status.py CRUD + state machine + migration idempotency; API tests via httpx ASGITransport for the GET endpoints.
7. Architecture doc: `ops/docs/architecture/external-ingest-status-store.md` recording the Postgres-vs-ClickHouse decision, the direct-SQL decision, and the retention policy (per house rule: document decisions in the same changeset).

## 2. Out of scope (owned elsewhere — do not implement)

- `POST /batches`, `POST /validate`, `GET /schemas`, `GET /schemas/{version}`, the Pydantic envelope/record schemas — CHAOS-2691.
- Ingest token model, scope enforcement machinery, source registration/one-active-owner enforcement — CHAOS-2696.
- Idempotency *policy* (same-key-same-hash-returns-200 vs different-hash-409, one-active-owner conflict logic) — CHAOS-2695. This issue only provides the DB primitives (unique constraint + `payload_hash` column + lookup function) that 2695's logic is built on.
- Redis/Valkey stream, DLQ, 503-on-unavailable — CHAOS-2693.
- Record normalization, sink writes — CHAOS-2697/CHAOS-2698.
- Bounded metric recomputation — CHAOS-2699. The GET response does **not** surface recompute status in v1 (the plan's example response has no such field either); leave a documented extension point but don't build it.
- `dev-hops push` CLI polling UX — CHAOS-2700 (consumes this GET endpoint; don't build the CLI here).
- Web "Screen 6/7" UI — CHAOS-2714 (consumes the list/get endpoints; don't build UI here).
- FullChaos-hosted webhook endpoint status wiring — CHAOS-2715 (webhook-assisted ingestion is deferred; this issue's status model must simply be reusable later, no special-casing needed now).

## 3. Design decisions

Each decision below resolves a gap the plan/issue left open. Rationale is one line; detail follows where useful.

1. **Table names**: `external_ingest_batches`, `external_ingest_rejections` — taken verbatim from the CHAOS-2694 issue body (only place these names are stated).
2. **`org_id` column type = `Text`, no FK, indexed** — matches `ProviderRateLimitObservation` (the recon-confirmed best in-repo precedent for an ingestion-adjacent, independently-retained, org-scoped table) and matches the runtime type of `get_current_org_id()`/`AuthenticatedUser`/token-context org id (`str`, not always a UUID-parseable value for pre-org-provisioning edge cases).
3. **Direct SQL, not ORM CRUD, but still a declarative model class for DDL** — the core plan explicitly says "Use direct SQL for API persistence/status queries... avoid adding ORM-only paths," which *contradicts* the observed house convention (ORM `Base` declarative classes for all other Postgres API tables). Resolve by keeping a declarative `Base` class (`ExternalIngestBatch`, `ExternalIngestRejection` in `src/dev_health_ops/models/external_ingest.py`) purely as the schema-of-record for Alembic + `Base.metadata` (needed so the existing sqlite-in-memory unit-test convention keeps working, e.g. `Base.metadata.create_all(engine)`), but **all reads/writes in `status.py` go through `session.execute(text(...), params)`** — no `session.add()`, no `session.query()`, no ORM relationship traversal. This exact `text()` pattern is precedented in `api/billing/reconciliation_service.py` and `api/billing/refund_service.py`.
4. **Dialect-portable SQL only (no `RETURNING`, no `ON CONFLICT`, no Postgres-only functions)** — because there is no live-Postgres pytest marker/tier in this repo (only `@pytest.mark.clickhouse` exists; Postgres-backed unit tests universally use sqlite-in-memory, per `tests/test_rate_limit_observations.py`). Writing Postgres-only SQL would make `status.py` untestable under the existing convention without introducing a brand-new CI tier. Consequences: generate `id`/`ingestion_id` UUIDs in Python (`uuid.uuid4()`) before building INSERT statements, not `gen_random_uuid()`; do INSERT then a follow-up SELECT instead of `RETURNING`; detect idempotency-key collisions via catching `IntegrityError` on the unique constraint, not `ON CONFLICT`; bind all UUID values as `str(...)` (matches `refund_service.py`'s `params["org_id"] = str(org_id)` precedent — safer for asyncpg than passing raw `uuid.UUID` through untyped `text()` binds); use plain `sa.JSON` (not `postgresql.JSONB`) for `error_summary` since it's never queried by contents.
5. **State machine, string-valued, not DB-enforced** — `BatchStatus(str, Enum)` = `accepted | processing | completed | partial | failed`, stored as `Text`, transitions validated in Python only (no `CHECK` constraint). Matches `SyncRunStatus`/`SyncComputeCheckpointStatus` precedent (plain `Text`, Python-enum-validated, no DB check constraint) rather than `ImpersonationSession`'s one-off `CheckConstraint` example (that constraint protects a security invariant, not a status enum — not the right precedent to copy here). Terminal-status derivation: `completed` iff `items_rejected == 0`; `failed` iff `items_accepted == 0` and `items_received > 0`; else `partial`. A batch with `items_received == 0` (empty records array — should be rejected by CHAOS-2691's schema validation before it ever reaches this table) is out of scope for the state machine; assert it can't happen.
6. **`payload_hash` column added to `external_ingest_batches`** (not listed in the plan's "Status store" column list, but required by CHAOS-2695's own acceptance criteria: "same idempotency key + same payload -> existing status; different payload -> 409"). This column, plus a `UNIQUE (org_id, source_system, source_instance, idempotency_key)` constraint, are the DB-level primitives 2695's conflict logic needs. Cross-cutting — see decisionsNeeded.
7. **`producer` / `producer_version` columns added** (present in the plan's request envelope example — `source.producer`, `source.producerVersion` — but missing from the plan's "Status store" column list). Needed for the list endpoint's "producer type" filter (web Screen 6) and for CLI/CI debugging ("which runner pushed this batch"). Stored as free-text `Text NULL`; no categorical enum is defined anywhere in the plan for "CLI/CI/relay/API" — that bucketing, if wanted, is a web-side derived-label concern, not a DB column (see Gaps).
8. **`external_ingest_rejections.ingestion_id` IS a foreign key** to `external_ingest_batches.ingestion_id` with `ON DELETE CASCADE` — this is a deliberate *divergence* from the FK-less `ProviderRateLimitObservation` precedent. Rationale: rejection rows have no independent existence or retention requirement apart from their parent batch (unlike a rate-limit observation, which must outlive a pruned `sync_run`); a single prune sweep on `external_ingest_batches` should be sufficient to clean both tables via cascade, avoiding a second prune task and avoiding orphaned rejection rows.
9. **Bounded rejection storage**: cap stored rejection rows at `MAX_STORED_REJECTIONS_PER_BATCH = 1000` per `ingestion_id`. `items_rejected` on the batch row always reflects the *true* total (even beyond 1000); `error_summary` JSON captures `{"total_rejected": N, "stored_rejections": min(N, 1000), "truncated": bool, "top_codes": [{"code": ..., "count": ...}, ...]}` so the true magnitude of a failure is never hidden even when diagnostics are truncated. 1000 is a judgment call (no number is given anywhere in the plan) chosen to keep a single batch's diagnostics comfortably under a few MB while still being far larger than any batch a human will page through in the UI.
10. **Working assumption: `max records per batch = 5000`** (CHAOS-2691 owns the actual enforcement/number; no number exists anywhere in either plan doc). This brief's rejection cap (1000) and pagination defaults are sized against that assumption. If CHAOS-2691 lands with a materially different cap, revisit `MAX_STORED_REJECTIONS_PER_BATCH`. Flagged in decisionsNeeded.
11. **Tenant isolation returns 404, not 403, on cross-org access** — `GET /batches/{ingestion_id}` filters by `WHERE org_id = :org_id AND ingestion_id = :id`; a row that exists for a different org is indistinguishable from a nonexistent row in the response (`404 {"detail": "ingestion batch not found"}`). Prevents a token from one org from confirming *existence* of another org's ingestion IDs (standard tenant-isolation practice; not discussed in the plan but directly implied by the acceptance criterion "Status endpoint is tenant-scoped" combined with "Cross-org source access is impossible" from sibling issue CHAOS-2696).
12. **Auth dependency interface is defined here, implemented by CHAOS-2696** — `status_router` depends on `IngestAuthContext` (dataclass: `org_id: str`, `token_id: uuid.UUID`, `scopes: frozenset[str]`) resolved via `Depends(require_ingest_scope(INGEST_SCOPE_STATUS))` from `dev_health_ops.api.external_ingest.auth` (CHAOS-2696's module). This does **not** reuse the legacy `api/ingest/auth.py` (env-var API key, no scopes, no org resolution) — that module is explicitly wrong for this purpose (see gotchas from the api-app/webhooks-auth recon). If CHAOS-2696 has not landed on the integration branch when this issue is implemented, add a minimal temporary stub (`# TODO(CHAOS-2696): replace with real ingest-token auth`) in the same file that requires `Authorization: Bearer <token>` + an explicit `X-Org-Id` header and performs no real scope check, so CHAOS-2694's own tests aren't blocked; delete the stub the moment 2696 lands.
13. **Retention**: default `EXTERNAL_INGEST_STATUS_RETENTION_DAYS = 90` (batches + cascade-deleted rejections), env-tunable, beat-scheduled at `crontab(hour=5, minute=15)` (immediately after `prune-rate-limit-observations` at 05:00, avoiding the 01:00/01:30/02:00/03:30 nightly cluster), on the existing `sync` queue (no new Celery queue — avoids the `test_compose_workers_cover_every_celery_queue` lockstep requirement entirely). 90 days (vs. rate-limit observations' 14) because this is customer-support/audit-facing operational history, not a transient signal — closer in spirit to audit logs than to rate-limit telemetry. No number is given in either plan doc; flagged as a judgment call, not a cross-cutting blocker.
14. **No ClickHouse involvement.** This table is Postgres-only, matching the `ProviderRateLimitObservation` justification pattern (transactional, no analytics fan-out, must be joinable to the ingest-token model in the same DB). Do not touch `src/dev_health_ops/migrations/clickhouse/`.

## 4. API / DDL / schema sketches

### 4.1 Alembic migration — `src/dev_health_ops/alembic/versions/0032_add_external_ingest_status_store.py`

**Before writing this file, re-run `dev-hops migrate postgres heads` (or `alembic heads`) against the integration branch** — multiple CHAOS-2690 sub-issues may land Postgres migrations in parallel on the same branch; `0032` is this brief's best-effort assumption as of this recon pass (confirmed clean at `0031` when read), not a guarantee. If a `0032` already exists, chain as `0033` with `down_revision` pointed at whatever the new head is.

```python
"""Add external_ingest_batches and external_ingest_rejections (CHAOS-2694).

Durable Postgres status store for customer-push ingestion batches (CHAOS-2690).
Postgres, not ClickHouse -- deliberately, mirroring the
provider_rate_limit_observations precedent (0031): transactional, joins the
ingest-token/source-registration model (CHAOS-2696) in the same database, and
must support a strongly-consistent read-after-write status for CLI polling
(dev-hops push batch --poll, CHAOS-2700) immediately after 202 Accepted.

Unlike provider_rate_limit_observations, external_ingest_rejections IS a
child of external_ingest_batches (FK, ON DELETE CASCADE): rejection rows have
no independent retention requirement, so a single prune sweep on the parent
table is sufficient.

Retry-safe / guarded per the 0025/0020/0031 create-if-missing convention.

Revision ID: 0032
Revises: 0031
Create Date: 2026-07-01 00:00:00
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

_BATCHES_TABLE = "external_ingest_batches"
_REJECTIONS_TABLE = "external_ingest_rejections"
_IDEM_INDEX = "uq_external_ingest_batches_idem"
_ORG_STATUS_INDEX = "ix_external_ingest_batches_org_status"
_ORG_CREATED_INDEX = "ix_external_ingest_batches_org_created"
_ORG_SOURCE_INDEX = "ix_external_ingest_batches_org_source"
_REJ_ORDER_INDEX = "ix_external_ingest_rejections_ingestion_order"
_REJ_ORG_INDEX = "ix_external_ingest_rejections_org_id"


def upgrade() -> None:
    if not _table_exists(_BATCHES_TABLE):
        op.create_table(
            _BATCHES_TABLE,
            sa.Column("ingestion_id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column("idempotency_key", sa.Text(), nullable=False),
            sa.Column("payload_hash", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("source_instance", sa.Text(), nullable=False),
            sa.Column("producer", sa.Text(), nullable=True),
            sa.Column("producer_version", sa.Text(), nullable=True),
            sa.Column("schema_version", sa.Text(), nullable=False),
            sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "status", sa.Text(), nullable=False, server_default="accepted"
            ),
            sa.Column(
                "items_received", sa.Integer(), nullable=False, server_default="0"
            ),
            sa.Column(
                "items_accepted", sa.Integer(), nullable=False, server_default="0"
            ),
            sa.Column(
                "items_rejected", sa.Integer(), nullable=False, server_default="0"
            ),
            sa.Column("error_summary", sa.JSON(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("ingestion_id"),
        )
    _create_index_if_missing(_ORG_STATUS_INDEX, _BATCHES_TABLE, ["org_id", "status"])
    _create_index_if_missing(
        _ORG_CREATED_INDEX, _BATCHES_TABLE, ["org_id", "created_at"]
    )
    _create_index_if_missing(
        _ORG_SOURCE_INDEX, _BATCHES_TABLE, ["org_id", "source_system", "source_instance"]
    )
    _create_unique_index_if_missing(
        _IDEM_INDEX,
        _BATCHES_TABLE,
        ["org_id", "source_system", "source_instance", "idempotency_key"],
    )

    if not _table_exists(_REJECTIONS_TABLE):
        op.create_table(
            _REJECTIONS_TABLE,
            sa.Column("id", UUID(as_uuid=True), nullable=False),
            sa.Column("org_id", sa.Text(), nullable=False),
            sa.Column(
                "ingestion_id",
                UUID(as_uuid=True),
                sa.ForeignKey(
                    f"{_BATCHES_TABLE}.ingestion_id",
                    ondelete="CASCADE",
                    name="fk_external_ingest_rejections_ingestion_id",
                ),
                nullable=False,
            ),
            sa.Column("record_index", sa.Integer(), nullable=False),
            sa.Column("record_kind", sa.Text(), nullable=False),
            sa.Column("external_id", sa.Text(), nullable=True),
            sa.Column("code", sa.Text(), nullable=False),
            sa.Column("message", sa.Text(), nullable=False),
            sa.Column("path", sa.Text(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.PrimaryKeyConstraint("id"),
        )
    _create_index_if_missing(
        _REJ_ORDER_INDEX, _REJECTIONS_TABLE, ["ingestion_id", "record_index"]
    )
    _create_index_if_missing(_REJ_ORG_INDEX, _REJECTIONS_TABLE, ["org_id"])


def downgrade() -> None:
    if _table_exists(_REJECTIONS_TABLE):
        op.drop_table(_REJECTIONS_TABLE)
    if _table_exists(_BATCHES_TABLE):
        op.drop_table(_BATCHES_TABLE)


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _create_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns)


def _create_unique_index_if_missing(
    index_name: str, table_name: str, columns: list[str]
) -> None:
    bind = op.get_bind()
    existing = {ix["name"] for ix in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns, unique=True)
```

### 4.2 Declarative model (DDL-only source of truth; NOT used for CRUD) — `src/dev_health_ops/models/external_ingest.py`

```python
"""External customer-push ingestion status store (CHAOS-2690 / CHAOS-2694).

These declarative classes exist ONLY as the schema-of-record for Alembic and
for Base.metadata.create_all() in sqlite-backed unit tests. All actual
reads/writes go through parameterized text() SQL in
dev_health_ops.api.external_ingest.status -- per the plan's "use direct SQL
for API persistence, avoid ORM-only paths" directive. Do not add
session.add()/session.query() call sites against these classes; extend
status.py instead.

Postgres, not ClickHouse: transactional, joins the ingest-token/source model
(CHAOS-2696) in the same database, and must support strongly-consistent
read-after-write for CLI polling (dev-hops push batch --poll) immediately
after 202 Accepted -- ClickHouse's async merge semantics on ReplacingMergeTree
would make "poll immediately after accept" flaky.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dev_health_ops.models.git import GUID, Base


class BatchStatus(str, Enum):
    ACCEPTED = "accepted"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


TERMINAL_STATUSES = frozenset(
    {BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED}
)

MAX_STORED_REJECTIONS_PER_BATCH = 1000


class ExternalIngestBatch(Base):
    __tablename__ = "external_ingest_batches"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        GUID, primary_key=True, default=uuid.uuid4
    )
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    source_instance: Mapped[str] = mapped_column(Text, nullable=False)
    producer: Mapped[str | None] = mapped_column(Text, nullable=True)
    producer_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    window_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    window_ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(
        Text, nullable=False, default=BatchStatus.ACCEPTED.value
    )
    items_received: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_accepted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_rejected: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_summary: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "source_system",
            "source_instance",
            "idempotency_key",
            name="uq_external_ingest_batches_idem",
        ),
        Index("ix_external_ingest_batches_org_status", "org_id", "status"),
        Index("ix_external_ingest_batches_org_created", "org_id", "created_at"),
        Index(
            "ix_external_ingest_batches_org_source",
            "org_id",
            "source_system",
            "source_instance",
        ),
    )


class ExternalIngestRejection(Base):
    __tablename__ = "external_ingest_rejections"

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        GUID,
        ForeignKey("external_ingest_batches.ingestion_id", ondelete="CASCADE"),
        nullable=False,
    )
    record_index: Mapped[int] = mapped_column(Integer, nullable=False)
    record_kind: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    path: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index(
            "ix_external_ingest_rejections_ingestion_order",
            "ingestion_id",
            "record_index",
        ),
        Index("ix_external_ingest_rejections_org_id", "org_id"),
    )
```

Register in `src/dev_health_ops/models/__init__.py` (alongside the existing `from .rate_limit_observations import ProviderRateLimitObservation` line):

```python
from .external_ingest import (
    MAX_STORED_REJECTIONS_PER_BATCH,
    BatchStatus,
    ExternalIngestBatch,
    ExternalIngestRejection,
    TERMINAL_STATUSES,
)
```

### 4.3 `src/dev_health_ops/api/external_ingest/status.py` (direct-SQL layer)

Key function signatures (bodies use `session.execute(text(...), params)` exclusively, per Design Decision #3/#4):

```python
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.external_ingest import (
    MAX_STORED_REJECTIONS_PER_BATCH,
    BatchStatus,
)

__all__ = [
    "BatchRow",
    "RejectedRecord",
    "RejectionRow",
    "DuplicateIdempotencyKeyError",
    "find_existing_batch",
    "create_batch",
    "mark_processing",
    "complete_batch",
    "get_batch",
    "list_batches",
    "list_rejections",
]


class DuplicateIdempotencyKeyError(Exception):
    """Unique-constraint collision on (org_id, source_system, source_instance,
    idempotency_key). Raised by create_batch() when a concurrent insert wins
    the race after the caller's own find_existing_batch() pre-check missed it.
    Callers (CHAOS-2691/2695) should catch this, re-run find_existing_batch(),
    and apply the same-hash-200 / different-hash-409 policy."""

    def __init__(self, org_id: str, source_system: str, source_instance: str, idempotency_key: str) -> None:
        self.org_id = org_id
        self.source_system = source_system
        self.source_instance = source_instance
        self.idempotency_key = idempotency_key


@dataclass(frozen=True)
class BatchRow:
    ingestion_id: uuid.UUID
    org_id: str
    idempotency_key: str
    payload_hash: str
    source_system: str
    source_instance: str
    producer: str | None
    producer_version: str | None
    schema_version: str
    window_started_at: datetime | None
    window_ended_at: datetime | None
    status: str
    items_received: int
    items_accepted: int
    items_rejected: int
    error_summary: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True)
class RejectedRecord:
    """Input shape the worker (CHAOS-2697/2698) passes to complete_batch()."""

    record_index: int
    record_kind: str
    external_id: str | None
    code: str
    message: str
    path: str | None


@dataclass(frozen=True)
class RejectionRow(RejectedRecord):
    id: uuid.UUID
    created_at: datetime


async def find_existing_batch(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str,
    source_instance: str,
    idempotency_key: str,
) -> BatchRow | None:
    """Idempotency-key lookup. Consumed by CHAOS-2695's conflict policy
    BEFORE calling create_batch() -- this is the primary dedupe path; the
    unique constraint + DuplicateIdempotencyKeyError is the race-safety
    backstop, not the primary mechanism."""
    ...


async def create_batch(
    session: AsyncSession,
    *,
    ingestion_id: uuid.UUID,
    org_id: str,
    idempotency_key: str,
    payload_hash: str,
    source_system: str,
    source_instance: str,
    producer: str | None,
    producer_version: str | None,
    schema_version: str,
    window_started_at: datetime | None,
    window_ended_at: datetime | None,
    items_received: int,
) -> BatchRow:
    """INSERT a new status='accepted' row. Caller (CHAOS-2691's POST /batches
    handler, after CHAOS-2693's stream enqueue has already SUCCEEDED -- never
    write a status row for a batch that failed to enqueue durably) must have
    already called find_existing_batch() for the idempotency pre-check.
    Raises DuplicateIdempotencyKeyError on a concurrent-insert race."""
    ...


async def mark_processing(session: AsyncSession, *, org_id: str, ingestion_id: uuid.UUID) -> None:
    """accepted -> processing. Idempotent: a no-op UPDATE (WHERE status =
    'accepted') if already processing/terminal, so redelivered stream entries
    (at-least-once) never regress a terminal status back to processing."""
    ...


async def complete_batch(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    items_accepted: int,
    items_rejected: int,
    rejections: list[RejectedRecord],
) -> BatchRow:
    """processing -> {completed, partial, failed} (derived from counts, see
    Design Decision #5) in the SAME transaction as writing up to
    MAX_STORED_REJECTIONS_PER_BATCH rejection rows and the aggregated
    error_summary -- callers must commit only after both succeed (never show
    a terminal status without its diagnostics already durable). Idempotent:
    if called twice for the same ingestion_id (worker redelivery), the second
    call is a no-op on rejections (re-derive via ON conflict-free delete+
    reinsert is NOT done -- see idempotency note in the docstring body) and
    only updates counts/status if they differ."""
    ...


async def get_batch(session: AsyncSession, *, org_id: str, ingestion_id: uuid.UUID) -> BatchRow | None:
    """Tenant-scoped single lookup. Returns None (never raises) for both
    'does not exist' and 'exists but belongs to a different org' -- callers
    MUST turn both into an identical 404, never a 403 (avoid leaking
    cross-org existence)."""
    ...


async def list_batches(
    session: AsyncSession,
    *,
    org_id: str,
    source_system: str | None = None,
    source_instance: str | None = None,
    status: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[BatchRow], int]:
    """Returns (page, total_count). Ordered by created_at DESC."""
    ...


async def list_rejections(
    session: AsyncSession,
    *,
    org_id: str,
    ingestion_id: uuid.UUID,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[RejectionRow], int]:
    """Returns (page, total_stored_count) ordered by record_index ASC.
    total_stored_count is capped at MAX_STORED_REJECTIONS_PER_BATCH -- for
    the TRUE total_rejected count (which may exceed what's stored), read
    BatchRow.items_rejected / error_summary['total_rejected'] instead."""
    ...
```

Terminal-status derivation helper (co-located, pure function, unit-testable without a DB):

```python
def _terminal_status_for(items_received: int, items_accepted: int, items_rejected: int) -> BatchStatus:
    assert items_received > 0, "empty batches must be rejected by CHAOS-2691 schema validation before reaching status store"
    if items_rejected == 0:
        return BatchStatus.COMPLETED
    if items_accepted == 0:
        return BatchStatus.FAILED
    return BatchStatus.PARTIAL
```

### 4.4 `error_summary` shape (written by `complete_batch`)

```json
{
  "total_rejected": 4213,
  "stored_rejections": 1000,
  "truncated": true,
  "top_codes": [
    {"code": "missing_external_id", "count": 3980},
    {"code": "unknown_record_kind", "count": 233}
  ]
}
```

### 4.5 Router — `status_router` in `status.py`, included by CHAOS-2691's `router.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Annotated

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.external_ingest.auth import (  # CHAOS-2696
    INGEST_SCOPE_STATUS,
    IngestAuthContext,
    require_ingest_scope,
)

status_router = APIRouter()


@status_router.get("/batches/{ingestion_id}")
async def get_batch_status(
    ingestion_id: uuid.UUID,
    auth: Annotated[IngestAuthContext, Depends(require_ingest_scope(INGEST_SCOPE_STATUS))],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    error_limit: Annotated[int, Query(ge=1, le=200)] = 50,
    error_offset: Annotated[int, Query(ge=0)] = 0,
) -> BatchStatusResponse:
    batch = await status.get_batch(session, org_id=auth.org_id, ingestion_id=ingestion_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="ingestion batch not found")
    errors, total_errors = await status.list_rejections(
        session, org_id=auth.org_id, ingestion_id=ingestion_id,
        limit=error_limit, offset=error_offset,
    )
    return BatchStatusResponse.from_row(batch, errors, total_errors, error_limit, error_offset)


@status_router.get("/batches")
async def list_batch_statuses(
    auth: Annotated[IngestAuthContext, Depends(require_ingest_scope(INGEST_SCOPE_STATUS))],
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    source_system: str | None = Query(None),
    source_instance: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    created_after: datetime | None = Query(None),
    created_before: datetime | None = Query(None),
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> BatchListResponse:
    ...
```

Registration in CHAOS-2691's `api/external_ingest/router.py` (a one-line edit CHAOS-2694 must also make if 2691 already landed):

```python
from dev_health_ops.api.external_ingest.status import status_router
router.include_router(status_router)
```

### 4.6 Response schemas (append to CHAOS-2691's `api/external_ingest/schemas.py`; create the file with just these if it doesn't exist yet)

Follow the house camelCase convention (`model_config = ConfigDict(populate_by_name=True)` + `Field(alias=...)`, per `api/product_telemetry/schemas.py`):

```python
class SourceRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    system: str
    instance: str


class WindowRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    started_at: datetime | None = Field(default=None, alias="startedAt")
    ended_at: datetime | None = Field(default=None, alias="endedAt")


class RejectedRecordResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    index: int
    kind: str
    external_id: str | None = Field(default=None, alias="externalId")
    code: str
    message: str
    path: str | None = None


class BatchStatusResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    ingestion_id: uuid.UUID = Field(alias="ingestionId")
    status: str
    items_received: int = Field(alias="itemsReceived")
    items_accepted: int = Field(alias="itemsAccepted")
    items_rejected: int = Field(alias="itemsRejected")
    source: SourceRef
    window: WindowRef
    producer: str | None = None
    producer_version: str | None = Field(default=None, alias="producerVersion")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    error_summary: dict[str, Any] | None = Field(default=None, alias="errorSummary")
    errors: list[RejectedRecordResponse]
    errors_total: int = Field(alias="errorsTotal")
    errors_limit: int = Field(alias="errorsLimit")
    errors_offset: int = Field(alias="errorsOffset")


class BatchListItemResponse(BaseModel):
    # subset of BatchStatusResponse's fields, no `errors` (list view, not drilldown)
    ...


class BatchListResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    items: list[BatchListItemResponse]
    total: int
    limit: int
    offset: int
```

### 4.7 Retention prune task — `src/dev_health_ops/workers/external_ingest_reconciler.py`

```python
"""Retention pruning for the external-ingest status store (CHAOS-2694).

Deletes batches (and cascade-deletes their rejections via
ON DELETE CASCADE) older than the retention window. Beat-scheduled,
env-tunable via EXTERNAL_INGEST_STATUS_RETENTION_DAYS (default 90).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

_DEFAULT_RETENTION_DAYS = 90


def _retention_days() -> int:
    raw = os.getenv("EXTERNAL_INGEST_STATUS_RETENTION_DAYS")
    return int(raw) if raw else _DEFAULT_RETENTION_DAYS


@celery_app.task(name="dev_health_ops.workers.tasks.prune_external_ingest_batches")
def prune_external_ingest_batches(retention_days: int | None = None) -> dict[str, Any]:
    days = retention_days if retention_days is not None else _retention_days()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(days)))
    with get_postgres_session_sync() as session:
        result = session.execute(
            text(
                "DELETE FROM external_ingest_batches WHERE created_at < :cutoff "
                "AND status IN ('completed', 'partial', 'failed')"
            ),
            {"cutoff": cutoff},
        )
        deleted = int(getattr(result, "rowcount", 0) or 0)
    logger.info(
        "prune_external_ingest_batches.completed",
        extra={"deleted": deleted, "retention_days": days},
    )
    return {"status": "completed", "deleted": deleted, "retention_days": days}
```

Note the `status IN ('completed','partial','failed')` guard: never prune a batch still `accepted`/`processing` even if it's old (a stuck batch past retention is a bug signal that should stay visible, not silently disappear).

Beat schedule addition to `src/dev_health_ops/workers/config.py` (append after `prune-rate-limit-observations`):

```python
    "prune-external-ingest-batches": {
        "task": "dev_health_ops.workers.tasks.prune_external_ingest_batches",
        "schedule": crontab(hour=5, minute=15),
        "options": {"queue": "sync"},
    },
```

No new Celery queue — reuses `sync`, so `tests/test_compose_config.py::test_compose_workers_cover_every_celery_queue` needs no compose.yml change.

### 4.8 Architecture doc — `ops/docs/architecture/external-ingest-status-store.md`

Must record, in the same changeset (house rule):
- Postgres-vs-ClickHouse decision + rationale (§4.2 docstring content, expanded)
- direct-SQL-not-ORM decision + rationale (Design Decision #3/#4)
- state machine diagram (`accepted -> processing -> {completed|partial|failed}`) + idempotent-transition guarantee
- bounded-rejections + `error_summary` truncation contract (so CHAOS-2697/2698 and CHAOS-2714 authors don't have to reverse-engineer it from code)
- retention policy + env var
- explicit note that `external_ingest_rejections` is FK-cascaded (divergent from `provider_rate_limit_observations`) with the one-line rationale

## 5. Files to create / modify

Create:
- `src/dev_health_ops/alembic/versions/0032_add_external_ingest_status_store.py` (confirm head first, see §4.1)
- `src/dev_health_ops/models/external_ingest.py`
- `src/dev_health_ops/api/external_ingest/__init__.py` (if not already created by CHAOS-2691; empty/`__all__` only)
- `src/dev_health_ops/api/external_ingest/status.py`
- `src/dev_health_ops/workers/external_ingest_reconciler.py`
- `tests/test_external_ingest_status.py` (status.py unit tests, sqlite-backed)
- `tests/test_external_ingest_status_migration.py` (migration idempotency test, mirrors `test_rate_limit_observations.py`'s `test_migration_0031_idempotent_upgrade`)
- `tests/test_external_ingest_status_api.py` (GET endpoint tests, httpx ASGITransport)
- `ops/docs/architecture/external-ingest-status-store.md`

Modify:
- `src/dev_health_ops/models/__init__.py` — export new model symbols
- `src/dev_health_ops/api/external_ingest/router.py` — add `router.include_router(status_router)` (created by CHAOS-2691; coordinate)
- `src/dev_health_ops/api/external_ingest/schemas.py` — append response models (created by CHAOS-2691; coordinate, or create with only these models if 2691 hasn't landed)
- `src/dev_health_ops/workers/config.py` — add `prune-external-ingest-batches` beat entry

## 6. Test plan

### Unit (sqlite-in-memory, no live DB, mirrors `tests/test_rate_limit_observations.py`)

`tests/test_external_ingest_status.py`:
- `create_batch` inserts a row with `status=accepted`, correct `items_received`, `payload_hash` stored verbatim.
- `find_existing_batch` returns `None` for a fresh key, returns the row for a known key.
- `create_batch` called twice with the same unique-constraint tuple raises `DuplicateIdempotencyKeyError` (sqlite enforces `UNIQUE` too, so this is testable without Postgres).
- `mark_processing` transitions `accepted -> processing`; calling it again when already `processing` (or already terminal) is a no-op (assert row unchanged, no exception).
- `complete_batch` with `items_rejected == 0` -> `completed`; `items_accepted == 0` -> `failed`; both > 0 -> `partial`.
- `complete_batch` writes exactly `min(len(rejections), MAX_STORED_REJECTIONS_PER_BATCH)` rejection rows, and `error_summary.total_rejected` reflects the true (possibly larger) `items_rejected` count; `error_summary.truncated is True` when `len(rejections) > 1000`.
- `complete_batch` is idempotent under redelivery: calling it twice with identical inputs does not duplicate rejection rows or double-count.
- `get_batch` returns `None` when `ingestion_id` exists but `org_id` doesn't match (cross-tenant isolation, the 404-not-403 contract).
- `list_batches` filters by `source_system`/`source_instance`/`status`/`created_after`/`created_before`; respects `limit`/`offset`; orders `created_at DESC`.
- `list_rejections` orders by `record_index ASC`; respects `limit`/`offset`; `total` reflects stored (capped) count, not `items_rejected`.
- `_terminal_status_for` pure-function table test (no DB) for all boundary combinations, including the `items_received == 0` assertion.

`tests/test_external_ingest_status_migration.py`:
- Mirror `test_migration_0031_idempotent_upgrade`: `upgrade()` creates both tables + all indexes (assert exact index name set for both tables); re-running `upgrade()` is a no-op; `downgrade()` drops rejections then batches (FK order); re-running `downgrade()` is a no-op; `upgrade()` again from clean slate works.
- Explicit test that the FK's `ondelete="CASCADE"` is present in the created table's FK metadata (sqlite records this; assert via `sa.inspect(conn).get_foreign_keys("external_ingest_rejections")`).

### API tests (httpx ASGITransport, mirrors `tests/test_ingest_api.py`)

`tests/test_external_ingest_status_api.py`:
- `GET /batches/{id}` with no token -> 401 (via the auth dependency, real or stubbed per Design Decision #12).
- `GET /batches/{id}` with a token lacking `ingest:status` scope -> 403.
- `GET /batches/{id}` for a nonexistent id -> 404.
- `GET /batches/{id}` for an id belonging to a different org (seed two orgs) -> 404 (never 403 — assert the response body doesn't differ from the nonexistent-id case, proving no existence leak).
- `GET /batches/{id}` happy path: seed a batch + rejections via `status.py` directly (not via HTTP), then GET and assert full response shape incl. camelCase field names.
- `GET /batches/{id}?errorLimit=1&errorOffset=1` pagination correctness against a batch with >=3 seeded rejections.
- `GET /batches` list filtering by `status`/`source_system`/`created_after` against several seeded batches.
- `GET /batches` pagination `total` correctness independent of `limit`.

### Live-DB

No `@pytest.mark.clickhouse` tests are needed for this issue (Postgres-only). No live-Postgres pytest tier exists in this repo (confirmed via `pytest.ini` — only the `clickhouse` marker is registered), so live-Postgres verification is manual (see §8) rather than an automated marked test tier. Do not invent a new pytest marker for this issue alone; if a `postgres`-marker tier gets introduced repo-wide by a different issue, migrate these tests to it then.

## 7. Gate commands

Ops (run from `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`):

```bash
# Full local-validate gate (ruff format/check, mypy, full unit tier, isolated live-CH stage).
# This issue touches no ClickHouse code, so SCRATCH_DB naming still matters only for
# collision-avoidance if other CHAOS-2690 sub-issues' agents run this concurrently on
# sibling worktrees against the same dockerized ClickHouse container.
SCRATCH_DB=ci_2694_status bash ci/local_validate.sh

# If ClickHouse isn't running / not relevant to this issue's own review pass:
SKIP_CLICKHOUSE=1 bash ci/local_validate.sh

# mypy alone (literal CI command, per CI-parity house rule):
.venv/bin/mypy --install-types --non-interactive .

# Targeted test run while iterating:
.venv/bin/pytest tests/test_external_ingest_status.py tests/test_external_ingest_status_migration.py tests/test_external_ingest_status_api.py -v
```

No web-side gate commands apply to this issue (CHAOS-2694 is ops-only; web consumption is CHAOS-2714's responsibility).

## 8. Live verification procedure (dev compose stack)

This issue is Postgres-only; ClickHouse is not touched. Verify against the real dev Postgres via the host `dev-hops` (per the "Live-validate via host dev-hops" house convention) rather than rebuilding containers:

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration

# 1. Apply the new migration to the real dev Postgres.
POSTGRES_URI=postgresql+asyncpg://devhealth:devhealth@localhost:5432/devhealth \
  .venv/bin/dev-hops migrate postgres upgrade

# 2. Confirm the tables + indexes + FK exist as designed.
docker exec -it dev-health-postgres-1 psql -U devhealth -d devhealth -c "\d external_ingest_batches"
docker exec -it dev-health-postgres-1 psql -U devhealth -d devhealth -c "\d external_ingest_rejections"

# 3. Exercise status.py directly via a one-off python -c (no HTTP layer needed yet
#    if CHAOS-2691's router hasn't landed): create a batch, mark_processing,
#    complete_batch with a mix of accepted/rejected records, then read it back.
POSTGRES_URI=postgresql+asyncpg://devhealth:devhealth@localhost:5432/devhealth \
  .venv/bin/python -c "
import asyncio, uuid
from datetime import datetime, timezone
from dev_health_ops.db import get_postgres_session
from dev_health_ops.api.external_ingest import status

async def main():
    async with get_postgres_session() as session:
        batch = await status.create_batch(
            session, ingestion_id=uuid.uuid4(), org_id='meridian-control-org',
            idempotency_key='live-verify-1', payload_hash='deadbeef',
            source_system='github', source_instance='github.com/acme',
            producer='dev-hops-cli', producer_version='0.1.0',
            schema_version='external-ingest.v1',
            window_started_at=datetime.now(timezone.utc), window_ended_at=None,
            items_received=3,
        )
        await status.mark_processing(session, org_id=batch.org_id, ingestion_id=batch.ingestion_id)
        result = await status.complete_batch(
            session, org_id=batch.org_id, ingestion_id=batch.ingestion_id,
            items_accepted=2, items_rejected=1,
            rejections=[status.RejectedRecord(0, 'pull_request', 'PR-1', 'missing_external_id', 'externalId required', 'externalId')],
        )
        print(result)
        fetched = await status.get_batch(session, org_id=batch.org_id, ingestion_id=batch.ingestion_id)
        print(fetched)
asyncio.run(main())
"

# 4. Once CHAOS-2691's router + CHAOS-2696's auth land, verify end-to-end via HTTP:
curl -s -H "Authorization: Bearer <ingest-token-with-status-scope>" \
  http://localhost:8000/api/v1/external-ingest/batches/<ingestion_id> | jq .

# 5. Confirm cross-org isolation manually: request the same ingestion_id with a
#    token minted for a DIFFERENT org and confirm identical 404 body.

# 6. Confirm the prune task is wired (dry run against a manually-backdated row):
docker exec -it dev-health-postgres-1 psql -U devhealth -d devhealth -c \
  "UPDATE external_ingest_batches SET created_at = now() - interval '100 days', status='completed' WHERE ingestion_id = '<id>';"
.venv/bin/python -c "
from dev_health_ops.workers.external_ingest_reconciler import prune_external_ingest_batches
print(getattr(prune_external_ingest_batches, 'run')())
"
```

Do NOT run this against the shared dev Postgres `devhealth` database's PRODUCTION-mirroring data without confirming with the team first if this is a shared instance holding real synced data — use a throwaway org_id (`meridian-control-org` or a fresh UUID) and clean up test rows afterward (`DELETE FROM external_ingest_batches WHERE org_id = '...'`).

## 9. Dependencies on other sub-issues

- **CHAOS-2691** (REST contract): owns `router.py`/`schemas.py` files this issue must edit (add `status_router` include + response models). Soft dependency — this issue can be built standalone against those files not existing yet (create them with minimal content), but final wiring requires coordination/rebase once 2691 lands.
- **CHAOS-2696** (source registration + token scopes): owns `IngestAuthContext`/`require_ingest_scope`/`INGEST_SCOPE_STATUS` that this issue's GET endpoints depend on. Hard dependency for the *real* auth; a temporary stub (Design Decision #12) unblocks this issue's own implementation/tests in the meantime.
- **CHAOS-2695** (idempotency + ownership policy): consumes this issue's `find_existing_batch`/`create_batch`/`payload_hash`/unique-constraint primitives to implement the same-hash-200/different-hash-409 policy. Soft dependency — this issue provides the DB primitives regardless of 2695's implementation order, but the exact conflict-detection call sequence should be confirmed with whoever implements 2695.
- **CHAOS-2693** (durable stream): the plan requires status rows to be written only *after* stream enqueue succeeds (never write "accepted" for a batch that wasn't durably enqueued). This issue's `create_batch` doesn't enforce that ordering itself (it just inserts what it's told) — the caller (2691's POST handler, informed by 2693's enqueue result) is responsible for correct ordering. Document this explicitly in the architecture doc so it isn't lost.
- **CHAOS-2697/CHAOS-2698** (worker normalization + sink writes): the worker is the caller of `mark_processing`/`complete_batch`, wrapped via `run_async()` (per `workers/async_runner.py` precedent) since Celery tasks are synchronous. Confirm with whoever implements 2697/2698 that they adopt `run_async()` rather than a bespoke sync DB path.
- **CHAOS-2699** (bounded recomputation planner): may eventually want to read batch scope (org/source/repo/team/window) from a completed batch to derive its recompute scope. This issue does not expose that as a separate query today; `get_batch`/the DB row already carries `source_system`/`source_instance`/`window_started_at`/`window_ended_at`, which should be sufficient — flag to 2699's implementer rather than pre-building an unused API.
- **CHAOS-2700** (dev-hops push CLI) and **CHAOS-2714** (web setup screens): both are pure consumers of the GET/list endpoints this issue builds; no action needed from this issue beyond keeping the response shape stable/documented.

## 10. Risks

1. **Alembic revision-number collision**: multiple CHAOS-2690 sub-issues may add Postgres migrations in parallel on the shared integration branch; whichever merges first claims `0032`, others must rebase to `0033+`. Mitigate by re-checking `alembic heads` immediately before writing the migration file, not trusting this brief's `0032` blindly (flagged in §4.1).
2. **Auth dependency not ready when this issue is implemented**: if CHAOS-2696 lags, the temporary stub auth (Design Decision #12) risks landing in a real PR and being forgotten. Mitigate with an explicit `# TODO(CHAOS-2696)` + a failing/skipped test asserting the stub is replaced (e.g., a test that asserts `require_ingest_scope` rejects a request with no `Authorization` header at all, which the stub should already satisfy, keeping the swap low-risk).
3. **`payload_hash`/unique-constraint ownership ambiguity with CHAOS-2695**: if 2695's implementer independently designs a different idempotency-storage shape (e.g., a separate `external_ingest_idempotency_keys` table) before seeing this brief, there will be a rework/merge conflict. Mitigate by surfacing this brief's Design Decision #6 explicitly before 2695 starts (see decisionsNeeded).
4. **"Max records per batch" assumption (5000) mismatch with CHAOS-2691's real number**: affects sizing/reasoning around `MAX_STORED_REJECTIONS_PER_BATCH` and pagination defaults, but not correctness — the cap is independent of whatever CHAOS-2691 chooses, just potentially mis-tuned. Low risk, easy to adjust.
5. **List endpoint scope creep**: adding `GET /batches` (not in the issue's literal text) could be seen as out-of-scope by a strict reviewer. Justified in §1/§3 by the CHAOS-2714 web dependency having no other owner; flagged in decisionsNeeded in case the team wants it split into its own follow-up issue instead.
6. **`server_default=sa.text("now()")` in the Alembic migration vs. Python-side `datetime.now(timezone.utc)` defaults in the declarative model**: these are two different default mechanisms (DB-side vs. ORM-side) that only matters if a row is ever inserted via the declarative model directly (which Design Decision #3 says shouldn't happen) — since `status.py`'s raw INSERT statements must set `created_at`/`updated_at` explicitly in Python (they will not get the DB `now()` default applied consistently across dialects the same way the ORM default would), this is a real footgun: the migration's `server_default` is a safety net for direct `psql` inserts (e.g. manual live-verification in §8), not something `status.py` should rely on — `status.py`'s INSERT SQL must always pass `created_at`/`updated_at` bind params explicitly.
