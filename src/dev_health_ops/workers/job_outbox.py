"""Strict transactional producer for the language-neutral worker job outbox."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, SessionTransactionOrigin

from dev_health_ops.models.worker_job_outbox import WorkerJobOutbox

from .job_contracts import (
    ContractDecodeError,
    ContractPayload,
    MigrationJob,
    Registry,
    build_envelope,
    encode_envelope,
    load_migration_jobs,
    load_registry,
)

_EXECUTABLE_MIGRATION_ROUTES = frozenset({"shadow", "river_canary", "river"})


class OutboxEnqueueError(ValueError):
    """Value-free producer rejection safe for logs and operator output."""


def enqueue_worker_job(
    session: Session,
    payload: ContractPayload,
    *,
    correlation_id: str,
    idempotency_key: str,
    domain_id: str,
    organization_id: str | None = None,
    scheduled_at: datetime | None = None,
    registry: Registry | None = None,
    now: datetime | None = None,
    allow_deferred_route: bool = False,
) -> WorkerJobOutbox:
    """Stage one dispatch in the caller's transaction without committing it.

    The idempotency key is also the globally unique outbox dedupe key.  The
    caller must already own an explicit transaction so the domain mutation and
    this row cannot be committed independently.
    """

    transaction = session.get_transaction()
    if transaction is None or transaction.origin is not SessionTransactionOrigin.BEGIN:
        raise OutboxEnqueueError("active producer transaction is required")
    current_time = _as_utc(now or datetime.now(UTC), "now")
    business_time = _as_utc(scheduled_at or current_time, "scheduled_at")
    try:
        contract_registry = registry or load_registry()
        contract = contract_registry.by_kind(payload.KIND)
        _require_migration_route(
            payload.KIND,
            load_migration_jobs(),
            allow_deferred_route=allow_deferred_route,
        )
        envelope = build_envelope(
            payload,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            domain_id=domain_id,
            organization_id=organization_id,
        )
        if envelope.contract_version not in contract.supported_versions:
            raise ContractDecodeError("unsupported producer contract version")
        encoded = encode_envelope(envelope)
    except (AttributeError, ContractDecodeError) as error:
        raise OutboxEnqueueError("worker job contract is invalid") from error

    payload_hash = "sha256:" + hashlib.sha256(encoded).hexdigest()
    args = json.loads(encoded)
    existing = _find_existing(session, idempotency_key)
    if existing is not None:
        return _verify_existing(
            existing, payload.KIND, envelope.contract_version, payload_hash
        )

    row = WorkerJobOutbox(
        dedupe_key=idempotency_key,
        job_kind=payload.KIND,
        contract_version=envelope.contract_version,
        args=args,
        payload_hash=payload_hash,
        queue=contract.queue,
        priority=contract.priority,
        max_attempts=contract.max_attempts,
        scheduled_at=business_time,
        next_attempt_at=current_time,
        created_at=current_time,
        updated_at=current_time,
    )
    try:
        if session.get_bind().dialect.name == "postgresql":
            with session.begin_nested():
                session.add(row)
                session.flush([row])
        else:
            # SQLite is a test-only semantic store. Its driver can commit a
            # first-write SAVEPOINT when the outer transaction has not yet
            # emitted DML, so keep rollback semantics faithful in unit tests.
            session.add(row)
            session.flush([row])
    except IntegrityError:
        existing = _find_existing(session, idempotency_key)
        if existing is None:
            raise OutboxEnqueueError("worker job enqueue conflict") from None
        return _verify_existing(
            existing, payload.KIND, envelope.contract_version, payload_hash
        )
    return row


def _require_migration_route(
    kind: str,
    jobs: tuple[MigrationJob, ...],
    *,
    allow_deferred_route: bool,
) -> None:
    matching_jobs = tuple(job for job in jobs if job.kind == kind)
    if len(matching_jobs) != 1:
        raise ContractDecodeError("migration state does not define job route")
    if matching_jobs[0].route == "celery" and allow_deferred_route:
        return
    if matching_jobs[0].route not in _EXECUTABLE_MIGRATION_ROUTES:
        raise ContractDecodeError("migration route is not executable")


def _find_existing(session: Session, dedupe_key: str) -> WorkerJobOutbox | None:
    return session.scalar(
        select(WorkerJobOutbox).where(WorkerJobOutbox.dedupe_key == dedupe_key)
    )


def _verify_existing(
    row: WorkerJobOutbox, kind: str, version: int, payload_hash: str
) -> WorkerJobOutbox:
    if (
        row.job_kind != kind
        or row.contract_version != version
        or row.payload_hash != payload_hash
    ):
        raise OutboxEnqueueError("dedupe key conflicts with an existing dispatch")
    return row


def _as_utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OutboxEnqueueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)
