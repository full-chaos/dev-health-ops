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

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, case, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dev_health_ops.models import SyncDispatchOutbox, SyncRun

OUTBOX_KIND_DISPATCH = "dispatch_sync_run"
OUTBOX_KIND_FINALIZE = "finalize_sync_run"
OUTBOX_KIND_POST_SYNC = "post_sync"

OUTBOX_STATUS_PENDING = "pending"
OUTBOX_STATUS_DISPATCHED = "dispatched"

_MAX_ERROR_LENGTH = 2000


@dataclass(frozen=True)
class ClaimedOutboxRow:
    id: uuid.UUID
    org_id: str
    sync_run_id: uuid.UUID
    kind: str
    attempts: int
    available_at: datetime
    claim_token: str


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
    stmt = stmt.on_conflict_do_update(
        index_elements=["sync_run_id", "kind"],
        set_={
            "available_at": case(
                (
                    stmt.excluded.available_at < table.c.available_at,
                    stmt.excluded.available_at,
                ),
                else_=table.c.available_at,
            ),
            "status": OUTBOX_STATUS_PENDING,
            "dispatched_at": None,
            "claim_token": case((live_claim, table.c.claim_token), else_=None),
            "claim_expires_at": case(
                (live_claim, table.c.claim_expires_at), else_=None
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
    session.execute(
        update(SyncDispatchOutbox)
        .where(
            SyncDispatchOutbox.sync_run_id == run_uuid,
            SyncDispatchOutbox.kind == kind,
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
    error: object,
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
            last_error=str(error)[:_MAX_ERROR_LENGTH],
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
