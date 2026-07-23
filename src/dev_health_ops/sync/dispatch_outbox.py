"""Durable sync dispatch outbox helpers.

This outbox provides AT-LEAST-ONCE delivery of sync-run continuation wakeups:
``dispatch_sync_run``, ``finalize_sync_run``, and ``post_sync``. A claimed row
may be published more than once if, for example, a worker stalls past its lease
and later resumes after another worker reclaimed and published the expired row.

AT-MOST-ONCE PROVIDER EXECUTION is not enforced here. It is guaranteed by the
unit claim and lease-token CAS guards in ``dev_health_ops/workers/sync_units.py``:
the atomic DISPATCHING -> RUNNING claim, and ``_claim_units`` never reclaiming
RUNNING units. A duplicate ``dispatch_sync_run`` publish cannot re-execute a
provider unit.

Consumers of claimed rows must be idempotent. ``dispatch_sync_run`` is
redispatch-idempotent through unit claims, ``finalize_sync_run`` is once-only via
the ``SyncRunPostDispatch`` ledger, and ``post_sync`` fanout must be idempotent by
org/window/target (tracked as gate G-IDEMP under CHAOS-2581).

Expired-lease reclaim is intentional dead-worker recovery. The mark helpers
require status=pending, a matching claim token, and a live claim lease so stale
workers cannot corrupt DB state. They intentionally cannot prevent an external
publish already issued before a stale worker resumes; duplicate publishes are
tolerated because consumers are idempotent by contract.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict

from sqlalchemy import and_, case, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    SyncDispatchOutbox,
    SyncRun,
)
from dev_health_ops.sync.dispatch_routes import (
    DispatchRouteContractError,
    TransportRoute,
    load_transport_routes,
)
from dev_health_ops.sync.error_sanitize import sanitize_error_text

OUTBOX_KIND_DISPATCH = "dispatch_sync_run"
OUTBOX_KIND_DISCOVERY = "reference_discovery"
OUTBOX_KIND_FINALIZE = "finalize_sync_run"
OUTBOX_KIND_POST_SYNC = "post_sync"

OUTBOX_STATUS_PENDING = "pending"
OUTBOX_STATUS_DISPATCHED = "dispatched"

_MAX_ERROR_LENGTH = 2000
_TERMINAL_DENIAL_ERROR = "feature_disabled"

SYNC_DISPATCH_PARITY_EVENT = "sync_dispatch_parity_observation"
SYNC_DISPATCH_PARITY_PREDICATE_VERSION = "sync_dispatch_due_v1"
SYNC_DISPATCH_PARITY_DIGEST_VERSION = "sync_dispatch_candidate_digest_v1"
_SYNC_DISPATCH_PARITY_MIN_LIMIT = 1
_SYNC_DISPATCH_PARITY_MAX_LIMIT = 100
_SYNC_DISPATCH_PARITY_DELIVERY = {
    OUTBOX_KIND_DISPATCH: "at_least_once",
    OUTBOX_KIND_FINALIZE: "at_least_once",
    OUTBOX_KIND_POST_SYNC: "at_most_once_mark_before",
    OUTBOX_KIND_DISCOVERY: "at_least_once",
}


@dataclass(frozen=True)
class ClaimedOutboxRow:
    id: uuid.UUID
    org_id: str
    sync_run_id: uuid.UUID
    kind: str
    attempts: int
    available_at: datetime
    claim_token: str


class SyncDispatchParityObservationUnavailable(RuntimeError):
    """A bounded reason why the optional read-only parity capture is unavailable."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _ParityKindRecord(TypedDict):
    kind: str
    route: str
    due_pending: int
    expired_claims: int


def backoff_seconds(attempts: int) -> int:
    return min(60 * 2 ** min(max(attempts, 1) - 1, 4), 900)


def upsert_outbox_wakeup(
    session: Session,
    *,
    sync_run_id: str | uuid.UUID,
    kind: str,
    available_at: datetime,
    now: datetime | None = None,
) -> None:
    """Atomically insert or re-arm a wakeup; earliest eligible time wins."""
    run_uuid = uuid.UUID(str(sync_run_id))
    run_org_id = session.query(SyncRun.org_id).filter(SyncRun.id == run_uuid).scalar()
    if run_org_id is None:
        raise ValueError(f"sync_run not found for outbox wakeup: {sync_run_id}")
    requested_available_at = _as_aware(available_at)
    upsert_now = _as_aware(now or _utcnow())
    dialect_name = session.get_bind().dialect.name
    insert_values = _insert_values(
        org_id=str(run_org_id),
        sync_run_id=run_uuid,
        kind=kind,
        available_at=requested_available_at,
        now=upsert_now,
    )
    if dialect_name not in {"postgresql", "sqlite"}:
        _upsert_outbox_wakeup_fallback(
            session,
            run_uuid=run_uuid,
            kind=kind,
            insert_values=insert_values,
            requested_available_at=requested_available_at,
            now=upsert_now,
        )
        return

    dialect_insert: Any
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as postgresql_insert

        dialect_insert = postgresql_insert
    else:
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        dialect_insert = sqlite_insert

    table: Any = SyncDispatchOutbox.__table__
    stmt = dialect_insert(table).values(**insert_values)
    live_claim = _live_claim_condition(table, upsert_now)
    terminal_denial = _terminal_denial_condition(table)
    stmt = stmt.on_conflict_do_update(
        index_elements=["sync_run_id", "kind"],
        set_={
            "available_at": case(
                (
                    terminal_denial,
                    table.c.available_at,
                ),
                (
                    stmt.excluded.available_at < table.c.available_at,
                    stmt.excluded.available_at,
                ),
                else_=table.c.available_at,
            ),
            "status": case(
                (
                    terminal_denial,
                    OUTBOX_STATUS_DISPATCHED,
                ),
                else_=OUTBOX_STATUS_PENDING,
            ),
            "dispatched_at": case(
                (
                    terminal_denial,
                    table.c.dispatched_at,
                ),
                else_=None,
            ),
            "claim_token": case(
                (
                    ~terminal_denial & live_claim,
                    table.c.claim_token,
                ),
                else_=None,
            ),
            "claim_expires_at": case(
                (
                    ~terminal_denial & live_claim,
                    table.c.claim_expires_at,
                ),
                else_=None,
            ),
            "updated_at": upsert_now,
        },
    )
    session.execute(stmt)
    session.flush()


def _upsert_outbox_wakeup_fallback(
    session: Session,
    *,
    run_uuid: uuid.UUID,
    kind: str,
    insert_values: dict[str, Any],
    requested_available_at: datetime,
    now: datetime,
) -> None:
    try:
        with session.begin_nested():
            row = (
                session.query(SyncDispatchOutbox.id)
                .filter_by(sync_run_id=run_uuid, kind=kind)
                .one_or_none()
            )
            if row is None:
                session.add(SyncDispatchOutbox(**insert_values))
            else:
                _execute_claim_aware_rearm_update(
                    session,
                    run_uuid=run_uuid,
                    kind=kind,
                    requested_available_at=requested_available_at,
                    now=now,
                )
        session.flush()
        return
    except IntegrityError:
        _execute_claim_aware_rearm_update(
            session,
            run_uuid=run_uuid,
            kind=kind,
            requested_available_at=requested_available_at,
            now=now,
        )
    session.flush()


def _execute_claim_aware_rearm_update(
    session: Session,
    *,
    run_uuid: uuid.UUID,
    kind: str,
    requested_available_at: datetime,
    now: datetime,
) -> None:
    table = SyncDispatchOutbox.__table__
    live_claim = _live_claim_condition(table, now)
    terminal_denial = _terminal_denial_condition(table)
    session.execute(
        update(SyncDispatchOutbox)
        .where(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.kind == kind,
            ~terminal_denial,
        )
        .values(
            available_at=case(
                (requested_available_at < table.c.available_at, requested_available_at),
                else_=table.c.available_at,
            ),
            status=OUTBOX_STATUS_PENDING,
            dispatched_at=None,
            claim_token=case((live_claim, table.c.claim_token), else_=None),
            claim_expires_at=case((live_claim, table.c.claim_expires_at), else_=None),
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )


def _insert_values(
    *,
    org_id: str,
    sync_run_id: uuid.UUID,
    kind: str,
    available_at: datetime,
    now: datetime,
) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "org_id": org_id,
        "sync_run_id": sync_run_id,
        "kind": kind,
        "status": OUTBOX_STATUS_PENDING,
        "available_at": available_at,
        "attempts": 0,
        "created_at": now,
        "updated_at": now,
    }


def _live_claim_condition(table: Any, now: datetime) -> Any:
    return and_(table.c.claim_expires_at.isnot(None), table.c.claim_expires_at > now)


def _terminal_denial_condition(table: Any) -> Any:
    return and_(
        table.c.status == OUTBOX_STATUS_DISPATCHED,
        table.c.last_error == _TERMINAL_DENIAL_ERROR,
    )


def observe_due_outbox_rows(
    session: Session,
    *,
    now: datetime,
    limit: int,
) -> dict[str, Any]:
    """Build one bounded, read-only parity record in Python claim order.

    The record intentionally contains aggregates and a digest only. It never
    exposes candidate identifiers, tenant data, claim tokens, or payloads.
    """
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not (
            _SYNC_DISPATCH_PARITY_MIN_LIMIT <= limit <= _SYNC_DISPATCH_PARITY_MAX_LIMIT
        )
    ):
        raise SyncDispatchParityObservationUnavailable("invalid_limit")

    observed_at = _as_aware(now)
    routes = _validated_parity_routes()
    try:
        rows = list(
            session.execute(
                select(
                    SyncDispatchOutbox.id,
                    SyncDispatchOutbox.kind,
                    SyncDispatchOutbox.claim_expires_at,
                )
                .where(
                    SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
                    SyncDispatchOutbox.available_at <= observed_at,
                    or_(
                        SyncDispatchOutbox.claim_expires_at.is_(None),
                        SyncDispatchOutbox.claim_expires_at <= observed_at,
                    ),
                )
                .order_by(SyncDispatchOutbox.available_at, SyncDispatchOutbox.id)
                .limit(limit + 1)
            ).all()
        )
    except Exception as error:
        raise SyncDispatchParityObservationUnavailable("query_unavailable") from error

    if len(rows) > limit + 1:
        raise SyncDispatchParityObservationUnavailable("query_unavailable")
    sampled_rows = rows[:limit]
    truncated = len(rows) > limit
    kinds: dict[str, _ParityKindRecord] = {
        kind: {
            "kind": kind,
            "route": route.route,
            "due_pending": 0,
            "expired_claims": 0,
        }
        for kind, route in routes.items()
    }
    hasher = hashlib.sha256()
    _write_parity_digest_field(
        hasher, "digest_version", SYNC_DISPATCH_PARITY_DIGEST_VERSION
    )
    _write_parity_digest_field(
        hasher, "predicate_version", SYNC_DISPATCH_PARITY_PREDICATE_VERSION
    )
    _write_parity_digest_field(
        hasher, "observed_at", _canonical_parity_observed_at(observed_at)
    )
    _write_parity_digest_field(hasher, "limit", str(limit))

    unknown_kind_count = 0
    celery_due_pending = 0
    river_due_pending = 0
    for row_id, kind, claim_expires_at in sampled_rows:
        candidate_id = _canonical_parity_candidate_id(row_id)
        if claim_expires_at is not None and _as_aware(claim_expires_at) > observed_at:
            raise SyncDispatchParityObservationUnavailable("query_unavailable")
        _write_parity_digest_field(hasher, "candidate_kind", kind)
        _write_parity_digest_field(hasher, "candidate_id", candidate_id)
        kind_record = kinds.get(kind)
        if kind_record is None:
            unknown_kind_count += 1
            continue
        kind_record["due_pending"] += 1
        if claim_expires_at is not None:
            kind_record["expired_claims"] += 1
        if kind_record["route"] == "celery":
            celery_due_pending += 1
        elif kind_record["route"] == "river":
            river_due_pending += 1
        else:
            raise SyncDispatchParityObservationUnavailable("route_unavailable")

    sampled_candidates = len(sampled_rows)
    if (
        celery_due_pending + river_due_pending + unknown_kind_count
        != sampled_candidates
    ):
        raise SyncDispatchParityObservationUnavailable("query_unavailable")
    return {
        "event": SYNC_DISPATCH_PARITY_EVENT,
        "runtime": "celery",
        "observed_at": _canonical_parity_observed_at(observed_at),
        "limit": limit,
        "predicate_version": SYNC_DISPATCH_PARITY_PREDICATE_VERSION,
        "digest_version": SYNC_DISPATCH_PARITY_DIGEST_VERSION,
        "candidate_digest": f"sha256:{hasher.hexdigest()}",
        "sampled_candidates": sampled_candidates,
        "truncated": truncated,
        "unknown_kind_count": unknown_kind_count,
        "celery_due_pending": celery_due_pending,
        "river_due_pending": river_due_pending,
        "kinds": [kinds[kind] for kind in sorted(kinds)],
    }


def _validated_parity_routes() -> dict[str, TransportRoute]:
    try:
        loaded = load_transport_routes()
        routes = {kind: loaded.by_kind(kind) for kind in _SYNC_DISPATCH_PARITY_DELIVERY}
    except DispatchRouteContractError as error:
        raise SyncDispatchParityObservationUnavailable("route_unavailable") from error
    for kind, expected_delivery in _SYNC_DISPATCH_PARITY_DELIVERY.items():
        route = routes.get(kind)
        if (
            route is None
            or route.delivery != expected_delivery
            or route.rollback_route != "celery"
            or route.route not in {"celery", "river"}
        ):
            raise SyncDispatchParityObservationUnavailable("route_unavailable")
    return routes


def _canonical_parity_candidate_id(value: object) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as error:
        raise SyncDispatchParityObservationUnavailable("query_unavailable") from error


def _canonical_parity_observed_at(value: datetime) -> str:
    value = _as_aware(value)
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{value.microsecond:06d}000Z"


def _write_parity_digest_field(hasher: Any, name: str, value: str) -> None:
    name_bytes = name.encode("utf-8")
    value_bytes = value.encode("utf-8")
    hasher.update(str(len(name_bytes)).encode("ascii"))
    hasher.update(b":")
    hasher.update(name_bytes)
    hasher.update(str(len(value_bytes)).encode("ascii"))
    hasher.update(b":")
    hasher.update(value_bytes)
    hasher.update(b"\n")


def claim_due_outbox_rows(
    session: Session,
    *,
    now: datetime,
    limit: int,
) -> list[ClaimedOutboxRow]:
    """Claim due wakeups for at-least-once publication by idempotent consumers."""
    claim_now = _as_aware(now)
    claim_limit = max(1, limit)
    candidate_ids = list(
        session.execute(
            select(SyncDispatchOutbox.id)
            .where(
                SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
                SyncDispatchOutbox.available_at <= claim_now,
                or_(
                    SyncDispatchOutbox.claim_expires_at.is_(None),
                    SyncDispatchOutbox.claim_expires_at <= claim_now,
                ),
            )
            .order_by(SyncDispatchOutbox.available_at, SyncDispatchOutbox.id)
            .limit(claim_limit)
        )
        .scalars()
        .all()
    )
    claimed: list[ClaimedOutboxRow] = []
    claim_expires_at = claim_now + timedelta(seconds=_claim_timeout_seconds())
    for candidate_id in candidate_ids:
        claim_token = str(uuid.uuid4())
        row = session.execute(
            update(SyncDispatchOutbox)
            .where(
                SyncDispatchOutbox.id == candidate_id,
                SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
                SyncDispatchOutbox.available_at <= claim_now,
                or_(
                    SyncDispatchOutbox.claim_expires_at.is_(None),
                    SyncDispatchOutbox.claim_expires_at <= claim_now,
                ),
            )
            .values(
                claim_token=claim_token,
                claim_expires_at=claim_expires_at,
                attempts=SyncDispatchOutbox.attempts + 1,
                updated_at=claim_now,
            )
            .returning(
                SyncDispatchOutbox.id,
                SyncDispatchOutbox.org_id,
                SyncDispatchOutbox.sync_run_id,
                SyncDispatchOutbox.kind,
                SyncDispatchOutbox.attempts,
                SyncDispatchOutbox.available_at,
            )
            .execution_options(synchronize_session=False)
        ).one_or_none()
        if row is None:
            continue
        claimed.append(
            ClaimedOutboxRow(
                id=row[0],
                org_id=row[1],
                sync_run_id=row[2],
                kind=row[3],
                attempts=row[4],
                available_at=_as_aware(row[5]),
                claim_token=claim_token,
            )
        )
    session.flush()
    return claimed


def lock_outbox_claim_for_publish(
    session: Session,
    row_id: str | uuid.UUID,
    claim_token: str,
) -> bool:
    publish_now = _utcnow()
    locked_id = session.scalar(
        select(SyncDispatchOutbox.id)
        .where(
            SyncDispatchOutbox.id == uuid.UUID(str(row_id)),
            SyncDispatchOutbox.claim_token == claim_token,
            SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
            SyncDispatchOutbox.claim_expires_at.is_not(None),
            SyncDispatchOutbox.claim_expires_at > publish_now,
        )
        .with_for_update()
    )
    return locked_id is not None


def mark_outbox_dispatched(
    session: Session,
    *,
    row_id: str | uuid.UUID,
    claim_token: str,
    now: datetime | None = None,
) -> bool:
    """Mark a row dispatched only while the caller still owns a live lease."""
    dispatch_now = _as_aware(now or _utcnow())
    result = session.execute(
        update(SyncDispatchOutbox)
        .where(
            SyncDispatchOutbox.id == uuid.UUID(str(row_id)),
            SyncDispatchOutbox.claim_token == claim_token,
            SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
            SyncDispatchOutbox.claim_expires_at.is_not(None),
            SyncDispatchOutbox.claim_expires_at > dispatch_now,
        )
        .values(
            status=OUTBOX_STATUS_DISPATCHED,
            dispatched_at=dispatch_now,
            claim_token=None,
            claim_expires_at=None,
            last_error=None,
            updated_at=dispatch_now,
        )
        .execution_options(synchronize_session=False)
    )
    session.flush()
    return _rowcount(result) == 1


def mark_outbox_publish_failed(
    session: Session,
    *,
    row_id: str | uuid.UUID,
    claim_token: str,
    error: BaseException | str,
    attempts: int,
    now: datetime | None = None,
) -> bool:
    """Re-arm a row after failure only while the caller owns a live lease."""
    failure_now = _as_aware(now or _utcnow())
    result = session.execute(
        update(SyncDispatchOutbox)
        .where(
            SyncDispatchOutbox.id == uuid.UUID(str(row_id)),
            SyncDispatchOutbox.claim_token == claim_token,
            SyncDispatchOutbox.status == OUTBOX_STATUS_PENDING,
            SyncDispatchOutbox.claim_expires_at.is_not(None),
            SyncDispatchOutbox.claim_expires_at > failure_now,
        )
        .values(
            status=OUTBOX_STATUS_PENDING,
            claim_token=None,
            claim_expires_at=None,
            available_at=failure_now + timedelta(seconds=backoff_seconds(attempts)),
            last_error=sanitize_error_text(error, max_length=_MAX_ERROR_LENGTH),
            updated_at=failure_now,
        )
        .execution_options(synchronize_session=False)
    )
    session.flush()
    return _rowcount(result) == 1


def _claim_timeout_seconds() -> int:
    try:
        return max(1, int(os.getenv("SYNC_OUTBOX_CLAIM_TIMEOUT_SECONDS", "300")))
    except ValueError:
        return 300


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _rowcount(result: object) -> int:
    return int(getattr(result, "rowcount", 0) or 0)


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
