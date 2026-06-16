from __future__ import annotations

import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import ScheduledJob, SyncConfiguration
from dev_health_ops.workers import sync_scheduler
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


def _patch_scheduler(monkeypatch, session):
    from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _fake_session_ctx(session),
    )
    run_sync_mock = MagicMock()
    batch_sync_mock = MagicMock()
    monkeypatch.setattr(sync_scheduler, "run_sync_config", run_sync_mock)
    monkeypatch.setattr(sync_scheduler, "dispatch_batch_sync", batch_sync_mock)
    return dispatch_scheduled_syncs, run_sync_mock, batch_sync_mock


def test_second_dispatcher_after_first_stamp_does_not_double_dispatch(
    monkeypatch, db_session
):
    now = datetime.now(timezone.utc)
    config = _make_config(last_sync_at=now - 2 * HOUR)
    job = _make_job(config)
    db_session.add_all([config, job])
    db_session.flush()

    task, run_sync_mock, batch_sync_mock = _patch_scheduler(monkeypatch, db_session)

    first = _call(task)
    second = _call(task)

    assert str(config.id) in first["dispatched"]
    assert second["dispatched"] == []
    assert first["errors"] == 0
    assert second["errors"] == 0
    run_sync_mock.apply_async.assert_called_once()
    batch_sync_mock.apply_async.assert_not_called()


@contextmanager
def _session_scope(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.mark.skipif(
    not os.environ.get("POSTGRES_SYNC_SCHEDULER_TEST_URL"),
    reason="POSTGRES_SYNC_SCHEDULER_TEST_URL is not set",
)
def test_postgres_skip_locked_skips_due_job_locked_by_competing_dispatcher(
    monkeypatch,
):
    pytest.importorskip("psycopg2")
    from sqlalchemy.orm import sessionmaker

    url = os.environ["POSTGRES_SYNC_SCHEDULER_TEST_URL"]
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    org_id = f"scheduler-lock-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_: True)
    run_sync_mock = MagicMock()
    batch_sync_mock = MagicMock()
    monkeypatch.setattr(sync_scheduler, "run_sync_config", run_sync_mock)
    monkeypatch.setattr(sync_scheduler, "dispatch_batch_sync", batch_sync_mock)

    with _session_scope(session_factory) as setup:
        config = _make_config(last_sync_at=now - 2 * HOUR)
        config.org_id = org_id
        job = _make_job(config)
        setup.add_all([config, job])
        setup.commit()
        config_id = config.id
        job_id = job.id

    try:
        session_one = session_factory()
        session_two = session_factory()
        trans_one = session_one.begin()
        trans_two = session_two.begin()
        try:
            config_one = session_one.get(SyncConfiguration, config_id)
            config_two = session_two.get(SyncConfiguration, config_id)
            assert config_one is not None
            assert config_two is not None

            assert sync_scheduler._maybe_dispatch_config(session_one, config_one, now)
            assert not sync_scheduler._maybe_dispatch_config(
                session_two, config_two, now
            )
            run_sync_mock.apply_async.assert_called_once()

            trans_two.rollback()
            trans_one.commit()
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
