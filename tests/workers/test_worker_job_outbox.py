from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from sqlalchemy import Table, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dev_health_ops.models import Base, WorkerJobOutbox
from dev_health_ops.workers.job_contracts import HeartbeatPayload
from dev_health_ops.workers.job_outbox import OutboxEnqueueError, enqueue_worker_job


@pytest.fixture
def engine():
    value = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(value, tables=[cast(Table, WorkerJobOutbox.__table__)])
    return value


def _enqueue(session: Session, **overrides):
    now = overrides.pop("now", datetime(2026, 7, 21, 12, 0, tzinfo=UTC))
    values = {
        "correlation_id": "heartbeat-producer-1",
        "idempotency_key": "heartbeat:2026-07-21T12:00:00Z",
        "domain_id": "00000000-0000-4000-8000-000000000001",
        "scheduled_at": now + timedelta(minutes=5),
        "now": now,
    }
    values.update(overrides)
    return enqueue_worker_job(
        session,
        HeartbeatPayload(scheduled_for="2026-07-21T12:00:00Z"),
        **values,
    )


def test_requires_caller_owned_transaction(engine):
    with Session(engine) as session:
        with pytest.raises(OutboxEnqueueError, match="active producer transaction"):
            _enqueue(session)


def test_rejects_implicit_autobegin_after_prior_read(engine):
    with Session(engine) as session:
        session.scalar(select(WorkerJobOutbox))
        with pytest.raises(OutboxEnqueueError, match="active producer transaction"):
            _enqueue(session)


def test_stages_canonical_registry_derived_dispatch_without_committing(engine):
    with Session(engine) as session:
        with session.begin():
            row = _enqueue(session)
            assert row.id is not None
            assert row.job_kind == "system.heartbeat"
            assert row.contract_version == 1
            assert row.queue == "heartbeat"
            assert row.priority == 2
            assert row.max_attempts == 1
            assert row.dedupe_key == row.args["idempotency_key"]
            assert row.payload_hash.startswith("sha256:")
            assert len(row.payload_hash) == 71
            assert "worker_outbox_id" not in row.args
            session.rollback()

    with Session(engine) as verifier:
        assert verifier.scalar(select(WorkerJobOutbox)) is None


def test_commit_and_same_content_reuse_return_one_logical_dispatch(engine):
    with Session(engine) as session, session.begin():
        first = _enqueue(session)
        second = _enqueue(session, scheduled_at=datetime(2026, 7, 22, tzinfo=UTC))
        assert second.id == first.id

    with Session(engine) as verifier:
        assert len(verifier.scalars(select(WorkerJobOutbox)).all()) == 1


def test_dedupe_key_reuse_with_different_content_fails_closed(engine):
    with Session(engine) as session, session.begin():
        _enqueue(session)

    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError, match="dedupe key conflicts"):
            enqueue_worker_job(
                session,
                HeartbeatPayload(scheduled_for="2026-07-21T13:00:00Z"),
                correlation_id="heartbeat-producer-2",
                idempotency_key="heartbeat:2026-07-21T12:00:00Z",
                domain_id="00000000-0000-4000-8000-000000000001",
            )


def test_invalid_envelope_is_value_free_and_writes_nothing(engine):
    secret = "postgres://worker:secret@example.invalid/app"
    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError) as raised:
            _enqueue(session, correlation_id=secret)
        assert secret not in str(raised.value)
        assert session.scalar(select(WorkerJobOutbox)) is None


def test_rejects_naive_schedule_timestamp(engine):
    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError, match="timezone-aware"):
            _enqueue(session, scheduled_at=datetime(2026, 7, 21, 12, 5))


def test_model_uses_uuid_primary_key(engine):
    with Session(engine) as session, session.begin():
        row = _enqueue(session)
        assert isinstance(row.id, uuid.UUID)


def test_database_rejects_oversized_stored_args(engine):
    with Session(engine) as session:
        with pytest.raises(IntegrityError):
            with session.begin():
                row = _enqueue(session)
                row.args = {"oversized": "x" * (16 * 1024)}
                session.flush()
