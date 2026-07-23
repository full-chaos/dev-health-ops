from __future__ import annotations

import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event
from time import monotonic_ns
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    JobRun,
    ScheduledSyncOccurrence,
    SyncDispatchOutbox,
    SyncRun,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import ScheduledJob, SyncConfiguration
from dev_health_ops.sync.execution_trigger import (
    SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
    scheduled_sync_occurrence_identity,
)
from dev_health_ops.workers import sync_scheduler, sync_units
from tests.test_sync_scheduler_idempotency import (
    HOUR,
    _call,
    _fake_session_ctx,
    _hourly_croniter_module,
    _make_config,
    _make_job,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _patch_scheduler(monkeypatch, session) -> tuple[object, MagicMock]:
    """Wire the scheduler onto the fan-out planner dispatch path (CHAOS-2647).

    The legacy ``run_sync_config`` / ``dispatch_batch_sync`` worker tasks were
    removed; due, integration-linked configs now dispatch via
    ``dispatch_sync_run.apply_async``.
    """
    from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _fake_session_ctx(session),
    )
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_: True)
    dispatch_mock = MagicMock()
    monkeypatch.setattr(sync_units, "dispatch_sync_run", dispatch_mock)
    return dispatch_scheduled_syncs, dispatch_mock


def test_second_dispatcher_after_first_stamp_does_not_double_dispatch(
    monkeypatch, db_session
):
    now = datetime.now(timezone.utc)
    config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
    job = _make_job(config)
    db_session.add(job)
    db_session.flush()

    task, dispatch_mock = _patch_scheduler(monkeypatch, db_session)

    first = _call(task)
    second = _call(task)

    assert str(config.id) in first["dispatched"]
    assert second["dispatched"] == []
    assert first["errors"] == 0
    assert second["errors"] == 0
    dispatch_mock.apply_async.assert_not_called()
    assert db_session.query(SyncDispatchOutbox).count() == 1


@contextmanager
def _session_scope(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _ensure_postgres_sync_marker_constraint(engine) -> None:
    with engine.begin() as conn:
        constraints = inspect(conn).get_unique_constraints("scheduled_jobs")
        if any(
            constraint.get("name") == "uq_scheduled_job_org_sync_config_type"
            for constraint in constraints
        ):
            return
        conn.execute(
            text(
                """
                DELETE FROM scheduled_jobs
                WHERE id IN (
                    SELECT id FROM (
                        SELECT
                            id,
                            ROW_NUMBER() OVER (
                                PARTITION BY org_id, sync_config_id, job_type
                                ORDER BY updated_at DESC, created_at DESC
                            ) AS rn
                        FROM scheduled_jobs
                        WHERE sync_config_id IS NOT NULL
                    ) ranked
                    WHERE rn > 1
                )
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE scheduled_jobs
                ADD CONSTRAINT uq_scheduled_job_org_sync_config_type
                UNIQUE (org_id, sync_config_id, job_type)
                """
            )
        )


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_postgres_existing_job_marker_prevents_second_dispatcher(
    monkeypatch,
):
    pytest.importorskip("psycopg2")
    from sqlalchemy.orm import sessionmaker

    url = os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"]
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    _ensure_postgres_sync_marker_constraint(engine)
    session_factory = sessionmaker(bind=engine)
    org_id = f"scheduler-lock-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_: True)
    dispatch_mock = MagicMock()
    monkeypatch.setattr(sync_units, "dispatch_sync_run", dispatch_mock)

    with _session_scope(session_factory) as setup:
        config = _make_config(setup, last_sync_at=now - 2 * HOUR, org_id=org_id)
        job = _make_job(config)
        setup.add(job)
        setup.commit()
        config_id = config.id
        job_id = job.id

    try:
        session_one = session_factory()
        session_two = session_factory()
        try:
            config_one = session_one.get(SyncConfiguration, config_id)
            config_two = session_two.get(SyncConfiguration, config_id)
            assert config_one is not None
            assert config_two is not None

            assert sync_scheduler._maybe_dispatch_config(session_one, config_one, now)
            assert not sync_scheduler._maybe_dispatch_config(
                session_two, config_two, now
            )
            dispatch_mock.apply_async.assert_not_called()
            assert (
                session_one.query(SyncDispatchOutbox).filter_by(org_id=org_id).count()
                == 1
            )
            assert (
                session_one.query(ScheduledSyncOccurrence)
                .filter_by(org_id=org_id)
                .count()
                == 1
            )
            assert session_one.query(JobRun).filter_by(job_id=job_id).count() == 1
            assert session_one.query(SyncRun).filter_by(org_id=org_id).count() == 1
        finally:
            session_one.close()
            session_two.close()
    finally:
        with _session_scope(session_factory) as cleanup:
            job = cleanup.get(ScheduledJob, job_id)
            config = cleanup.get(SyncConfiguration, config_id)
            if job is not None:
                cleanup.delete(job)
            if config is not None:
                cleanup.delete(config)
            cleanup.commit()
        engine.dispose()


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_postgres_missing_job_row_race_dispatches_once(monkeypatch):
    pytest.importorskip("psycopg2")
    from sqlalchemy.orm import sessionmaker

    url = os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"]
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    _ensure_postgres_sync_marker_constraint(engine)
    session_factory = sessionmaker(bind=engine)
    org_id = f"scheduler-missing-row-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_: True)
    dispatch_mock = MagicMock()
    monkeypatch.setattr(sync_units, "dispatch_sync_run", dispatch_mock)

    with _session_scope(session_factory) as setup:
        config = _make_config(setup, last_sync_at=now - 2 * HOUR, org_id=org_id)
        setup.commit()
        config_id = config.id

    barrier = Barrier(2)

    def dispatch_attempt() -> bool:
        with _session_scope(session_factory) as session:
            config = session.get(SyncConfiguration, config_id)
            assert config is not None
            barrier.wait(timeout=10)
            return sync_scheduler._maybe_dispatch_config(session, config, now)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(dispatch_attempt) for _ in range(2)]
            results = [future.result(timeout=30) for future in futures]

        assert sorted(results) == [False, True]
        dispatch_mock.apply_async.assert_not_called()

        with _session_scope(session_factory) as verify:
            jobs = (
                verify.query(ScheduledJob)
                .filter(
                    ScheduledJob.org_id == org_id,
                    ScheduledJob.sync_config_id == config_id,
                    ScheduledJob.job_type == "sync",
                )
                .all()
            )
            assert len(jobs) == 1
            assert jobs[0].next_run_at is not None
            assert jobs[0].next_run_at > now
            assert (
                verify.query(SyncDispatchOutbox).filter_by(org_id=org_id).count() == 1
            )
            assert (
                verify.query(ScheduledSyncOccurrence).filter_by(org_id=org_id).count()
                == 1
            )
            assert verify.query(JobRun).filter_by(job_id=jobs[0].id).count() == 1
            assert verify.query(SyncRun).filter_by(org_id=org_id).count() == 1
    finally:
        with _session_scope(session_factory) as cleanup:
            for job in (
                cleanup.query(ScheduledJob)
                .filter(
                    ScheduledJob.org_id == org_id,
                    ScheduledJob.sync_config_id == config_id,
                    ScheduledJob.job_type == "sync",
                )
                .all()
            ):
                cleanup.delete(job)
            config = cleanup.get(SyncConfiguration, config_id)
            if config is not None:
                cleanup.delete(config)
            cleanup.commit()
        engine.dispose()


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_postgres_pending_occurrence_consumer_skips_locked_replica(monkeypatch):
    pytest.importorskip("psycopg2")
    from sqlalchemy.orm import sessionmaker

    from dev_health_ops.sync import execution_trigger

    url = os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"]
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    _ensure_postgres_sync_marker_constraint(engine)
    session_factory = sessionmaker(bind=engine)
    org_id = f"scheduler-pending-occurrence-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)
    planner_entered = Event()
    release_planner = Event()
    monkeypatch.setattr(
        "dev_health_ops.workers.org_guard.organization_exists_sync", lambda *_: True
    )
    real_create_trigger = execution_trigger.create_sync_execution_trigger

    def pause_planner(*args, **kwargs):
        planner_entered.set()
        assert release_planner.wait(timeout=10)
        return real_create_trigger(*args, **kwargs)

    monkeypatch.setattr(
        execution_trigger, "create_sync_execution_trigger", pause_planner
    )

    with _session_scope(session_factory) as setup:
        config = _make_config(setup, last_sync_at=now - 2 * HOUR, org_id=org_id)
        job = _make_job(config, next_run_at=now + timedelta(hours=1))
        setup.add(job)
        setup.flush()
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - HOUR,
        )
        setup.add(occurrence)
        setup.commit()
        config_id = config.id
        job_id = job.id
        occurrence_id = occurrence.occurrence_id

    def first_consumer() -> dict[str, int]:
        with _session_scope(session_factory) as session:
            result = sync_scheduler.reconcile_pending_scheduled_sync_occurrences(
                session, limit=1
            )
            session.commit()
            return result

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(first_consumer)
            assert planner_entered.wait(timeout=10)

            with _session_scope(session_factory) as second_session:
                second = sync_scheduler.reconcile_pending_scheduled_sync_occurrences(
                    second_session, limit=1
                )
                second_session.commit()

            release_planner.set()
            assert first.result(timeout=30) == {
                "scanned": 1,
                "completed": 1,
                "skipped": 0,
                "errors": 0,
            }

        assert second == {"scanned": 1, "completed": 0, "skipped": 1, "errors": 0}
        with _session_scope(session_factory) as verify:
            occurrence = verify.get(ScheduledSyncOccurrence, occurrence_id)
            assert occurrence is not None
            assert occurrence.job_run_id is not None
            assert occurrence.sync_run_id is not None
            assert verify.query(JobRun).filter_by(job_id=job_id).count() == 1
            assert verify.query(SyncRun).filter_by(org_id=org_id).count() == 1
            assert (
                verify.query(SyncDispatchOutbox).filter_by(org_id=org_id).count() == 1
            )
    finally:
        with _session_scope(session_factory) as cleanup:
            job = cleanup.get(ScheduledJob, job_id)
            config = cleanup.get(SyncConfiguration, config_id)
            if job is not None:
                cleanup.delete(job)
            if config is not None:
                cleanup.delete(config)
            cleanup.commit()
        engine.dispose()


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_postgres_config_lock_serializes_schedule_mutation(monkeypatch):
    pytest.importorskip("psycopg2")
    from sqlalchemy.orm import sessionmaker

    from dev_health_ops.sync import execution_trigger

    url = os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"]
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    _ensure_postgres_sync_marker_constraint(engine)
    session_factory = sessionmaker(bind=engine)
    org_id = f"scheduler-config-lock-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_: True)
    real_trigger = execution_trigger.create_sync_execution_trigger
    planner_entered = Event()
    release_planner = Event()
    timestamps: dict[str, int] = {}

    def pause_planner(*args, **kwargs):
        planner_entered.set()
        assert release_planner.wait(timeout=10)
        return real_trigger(*args, **kwargs)

    monkeypatch.setattr(
        execution_trigger, "create_sync_execution_trigger", pause_planner
    )

    with _session_scope(session_factory) as setup:
        config = _make_config(setup, last_sync_at=now - 2 * HOUR, org_id=org_id)
        job = _make_job(config)
        setup.add(job)
        setup.commit()
        config_id = config.id
        job_id = job.id

    def dispatch_schedule() -> bool:
        with _session_scope(session_factory) as session:
            config = session.get(SyncConfiguration, config_id)
            assert config is not None
            result = sync_scheduler._maybe_dispatch_config(session, config, now)
            timestamps["dispatch_commit"] = monotonic_ns()
            return result

    def pause_schedule() -> None:
        assert planner_entered.wait(timeout=10)
        with _session_scope(session_factory) as session:
            config = session.get(SyncConfiguration, config_id)
            assert config is not None
            config.is_active = False
            session.commit()
            timestamps["mutation_commit"] = monotonic_ns()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            dispatch_future = executor.submit(dispatch_schedule)
            mutation_future = executor.submit(pause_schedule)
            assert planner_entered.wait(timeout=10)
            release_planner.set()
            assert dispatch_future.result(timeout=30) is True
            mutation_future.result(timeout=30)

        assert timestamps["dispatch_commit"] < timestamps["mutation_commit"]
        with _session_scope(session_factory) as verify:
            assert (
                verify.query(ScheduledSyncOccurrence).filter_by(org_id=org_id).count()
                == 1
            )
    finally:
        with _session_scope(session_factory) as cleanup:
            job = cleanup.get(ScheduledJob, job_id)
            config = cleanup.get(SyncConfiguration, config_id)
            if job is not None:
                cleanup.delete(job)
            if config is not None:
                cleanup.delete(config)
            cleanup.commit()
        engine.dispose()
