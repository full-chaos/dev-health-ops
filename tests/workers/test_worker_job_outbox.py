from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import patch

import pytest
from sqlalchemy import Table, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema, DropSchema

from dev_health_ops.models import Base, WorkerJobOutbox
from dev_health_ops.workers.job_contracts import HeartbeatPayload, MigrationJob
from dev_health_ops.workers.job_outbox import OutboxEnqueueError, enqueue_worker_job


@pytest.fixture
def engine():
    value = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(value, tables=[cast(Table, WorkerJobOutbox.__table__)])
    return value


@pytest.fixture
def postgres_engine():
    uri = os.getenv("WORKER_OUTBOX_POSTGRES_TEST_URI")
    if not uri:
        pytest.skip("WORKER_OUTBOX_POSTGRES_TEST_URI is not configured")
    schema = f"worker_outbox_test_{uuid.uuid4().hex}"
    admin = create_engine(uri)
    with admin.begin() as connection:
        connection.execute(CreateSchema(schema))
    value = create_engine(uri, connect_args={"options": f"-csearch_path={schema}"})
    worker_outbox_table = cast(Table, WorkerJobOutbox.__table__)
    worker_outbox_table.create(value)
    try:
        yield value
    finally:
        value.dispose()
        with admin.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        admin.dispose()


def _enqueue(session: Session, **overrides):
    migration_route = overrides.pop("migration_route", None)
    migration_jobs = overrides.pop("migration_jobs", None)
    now = overrides.pop("now", datetime(2026, 7, 21, 12, 0, tzinfo=UTC))
    values = {
        "correlation_id": "heartbeat-producer-1",
        "idempotency_key": "heartbeat:2026-07-21T12:00:00Z",
        "domain_id": "00000000-0000-4000-8000-000000000001",
        "scheduled_at": now + timedelta(minutes=5),
        "now": now,
    }
    values.update(overrides)
    if migration_route is not None:
        migration_jobs = _migration_job(migration_route)
    if migration_jobs is None:
        return enqueue_worker_job(
            session,
            HeartbeatPayload(scheduled_for="2026-07-21T12:00:00Z"),
            **values,
        )
    with patch(
        "dev_health_ops.workers.job_outbox.load_migration_jobs",
        return_value=migration_jobs,
    ):
        return enqueue_worker_job(
            session,
            HeartbeatPayload(scheduled_for="2026-07-21T12:00:00Z"),
            **values,
        )


def _migration_job(route: str) -> tuple[MigrationJob, ...]:
    return (
        MigrationJob(
            kind="system.heartbeat",
            producer_version=1,
            required_profiles=("ops",),
            route=route,
        ),
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


def test_rejects_celery_route_without_writing_or_weakening_transaction(engine):
    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError, match="worker job contract is invalid"):
            _enqueue(session)
        assert session.scalar(select(WorkerJobOutbox)) is None


def test_rejects_missing_migration_route_without_writing(engine):
    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError, match="worker job contract is invalid"):
            _enqueue(session, migration_jobs=())
        assert session.scalar(select(WorkerJobOutbox)) is None


@pytest.mark.parametrize("route", ("shadow", "river_canary", "river"))
def test_stages_executable_migration_route_without_committing(engine, route: str):
    with Session(engine) as session:
        with session.begin():
            row = _enqueue(session, migration_route=route)
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
        first = _enqueue(session, migration_route="shadow")
        second = _enqueue(
            session,
            scheduled_at=datetime(2026, 7, 22, tzinfo=UTC),
            migration_route="shadow",
        )
        assert second.id == first.id

    with Session(engine) as verifier:
        assert len(verifier.scalars(select(WorkerJobOutbox)).all()) == 1


def test_dedupe_key_reuse_with_different_content_fails_closed(engine):
    with Session(engine) as session, session.begin():
        _enqueue(session, migration_route="shadow")

    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError, match="dedupe key conflicts"):
            with patch(
                "dev_health_ops.workers.job_outbox.load_migration_jobs",
                return_value=_migration_job("shadow"),
            ):
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
            _enqueue(
                session,
                correlation_id=secret,
                migration_route="shadow",
            )
        assert secret not in str(raised.value)
        assert session.scalar(select(WorkerJobOutbox)) is None


def test_rejects_naive_schedule_timestamp(engine):
    with Session(engine) as session, session.begin():
        with pytest.raises(OutboxEnqueueError, match="timezone-aware"):
            _enqueue(
                session,
                scheduled_at=datetime(2026, 7, 21, 12, 5),
                migration_route="shadow",
            )


def test_model_uses_uuid_primary_key(engine):
    with Session(engine) as session, session.begin():
        row = _enqueue(session, migration_route="shadow")
        assert isinstance(row.id, uuid.UUID)


def test_database_rejects_oversized_stored_args(engine):
    with Session(engine) as session:
        with pytest.raises(IntegrityError):
            with session.begin():
                row = _enqueue(session, migration_route="shadow")
                row.args = {"oversized": "x" * (16 * 1024)}
                session.flush()


def test_postgres_savepoint_preserves_outer_rollback_and_dedupe(postgres_engine):
    with Session(postgres_engine) as session:
        with session.begin():
            _enqueue(session, migration_route="shadow")
            session.rollback()

    with Session(postgres_engine) as verifier:
        assert verifier.scalar(select(WorkerJobOutbox)) is None

    with Session(postgres_engine) as session, session.begin():
        first = _enqueue(session, migration_route="shadow")
        second = _enqueue(
            session,
            scheduled_at=datetime(2026, 7, 22, tzinfo=UTC),
            migration_route="shadow",
        )
        assert second.id == first.id

    with Session(postgres_engine) as verifier:
        assert len(verifier.scalars(select(WorkerJobOutbox)).all()) == 1
