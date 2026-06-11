"""Dispatch-time idempotency for the sync scheduler (CHAOS-2270).

`dispatch_scheduled_syncs` runs every beat tick, but `last_sync_at` only
advances when a run completes, so without a dispatch marker every due config
was re-enqueued on every tick and flooded the sync queue. These tests cover
the `ScheduledJob.next_run_at` dispatch marker, the `is_running` staleness
escape, and per-config error isolation.
"""

from __future__ import annotations

import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    SyncConfiguration,
)

HOUR = timedelta(hours=1)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    yield session


def _hourly_croniter_module() -> SimpleNamespace:
    """Fake croniter module simulating an hourly cron: next = base + 1h."""

    def _croniter(expr: str, base: datetime):
        if expr == "BAD":
            raise ValueError(f"malformed cron expression: {expr}")

        class _Iter:
            def get_next(self, _kind):
                return base + HOUR

        return _Iter()

    return SimpleNamespace(croniter=_croniter)


def _make_config(
    name: str = "test-config",
    last_sync_at: datetime | None = None,
    sync_options: dict | None = None,
    sync_targets: list | None = None,
    provider: str = "github",
) -> SyncConfiguration:
    config = SyncConfiguration(
        name=name,
        provider=provider,
        org_id="default",
        sync_targets=sync_targets or ["git", "prs"],
        # owner+repo set => not batch eligible; explicit schedule_cron so the
        # manual-only gate (CHAOS-2297) doesn't skip dispatch in these tests.
        sync_options=sync_options
        if sync_options is not None
        else {"owner": "org", "repo": "repo", "schedule_cron": "0 * * * *"},
        is_active=True,
    )
    if last_sync_at is not None:
        config.last_sync_at = last_sync_at
    return config


def _make_job(
    config: SyncConfiguration,
    schedule_cron: str = "0 * * * *",
    is_running: bool = False,
    last_run_at: datetime | None = None,
    next_run_at: datetime | None = None,
) -> ScheduledJob:
    job = ScheduledJob(
        name=f"sync-config-{config.id}",
        job_type="sync",
        schedule_cron=schedule_cron,
        org_id=config.org_id,
        provider=config.provider,
        sync_config_id=config.id,
    )
    job.is_running = is_running
    job.last_run_at = last_run_at
    job.next_run_at = next_run_at
    return job


def _run_dispatch(monkeypatch, db_session) -> tuple[Any, MagicMock, MagicMock]:
    """Wire mocks and return (task, run_sync_mock, batch_sync_mock)."""
    from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _fake_session_ctx(db_session),
    )
    run_sync_mock = MagicMock()
    batch_sync_mock = MagicMock()
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_scheduler.run_sync_config", run_sync_mock
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync", batch_sync_mock
    )
    return dispatch_scheduled_syncs, run_sync_mock, batch_sync_mock


def _call(task) -> dict:
    task.push_request(id=str(uuid.uuid4()))
    try:
        return task()
    finally:
        task.pop_request()


class TestDispatchIdempotency:
    def test_due_config_dispatched_once_not_redispatched_on_second_tick(
        self, monkeypatch, db_session
    ):
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        assert str(config.id) in first["dispatched"]
        assert run_sync_mock.apply_async.call_count == 1
        # The dispatch marker was stamped to the next cron occurrence.
        assert job.next_run_at is not None
        assert job.next_run_at > now

        second = _call(task)
        assert second["dispatched"] == []
        assert run_sync_mock.apply_async.call_count == 1

    def test_expired_dispatch_marker_allows_redispatch(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR)
        # Marker from a previous dispatch whose task was lost (queue purge,
        # worker crash) and whose cron occurrence has already passed.
        job = _make_job(config, next_run_at=now - timedelta(minutes=1))
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert str(config.id) in result["dispatched"]
        assert run_sync_mock.apply_async.call_count == 1
        assert job.next_run_at is not None
        assert job.next_run_at > now

    def test_fresh_is_running_marker_skips_dispatch(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR)
        job = _make_job(
            config, is_running=True, last_run_at=now - timedelta(minutes=10)
        )
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        run_sync_mock.apply_async.assert_not_called()
        assert job.next_run_at is None

    def test_stale_is_running_marker_allows_redispatch(self, monkeypatch, db_session):
        from dev_health_ops.workers.sync_scheduler import STALE_RUNNING_TTL_SECONDS

        now = datetime.now(timezone.utc)
        stale = now - timedelta(seconds=STALE_RUNNING_TTL_SECONDS + 60)
        config = _make_config(last_sync_at=now - 2 * HOUR)
        # Worker crashed mid-run: is_running was never cleared.
        job = _make_job(config, is_running=True, last_run_at=stale)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        assert str(config.id) in first["dispatched"]
        assert run_sync_mock.apply_async.call_count == 1

        # The re-dispatch stamps next_run_at, so even with the flag still
        # wedged the config is enqueued at most once per cron interval.
        second = _call(task)
        assert second["dispatched"] == []
        assert run_sync_mock.apply_async.call_count == 1

    def test_completed_run_resets_cycle_without_redispatch(
        self, monkeypatch, db_session
    ):
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        _call(task)
        assert run_sync_mock.apply_async.call_count == 1

        # Simulate the consumer terminal transition (sync_runtime): the run
        # completed, is_running cleared, last_sync_at advanced.
        job.is_running = False
        config.last_sync_at = now
        db_session.flush()

        # Still inside the same cron interval: nothing to dispatch.
        second = _call(task)
        assert second["dispatched"] == []
        assert run_sync_mock.apply_async.call_count == 1

        # Next cron interval reached (marker expired AND config due again).
        job.next_run_at = now - timedelta(seconds=1)
        config.last_sync_at = now - 2 * HOUR
        db_session.flush()

        third = _call(task)
        assert str(config.id) in third["dispatched"]
        assert run_sync_mock.apply_async.call_count == 2

    def test_creates_scheduled_job_row_when_missing(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            last_sync_at=now - 2 * HOUR,
            sync_options={"owner": "org", "repo": "repo", "schedule_cron": "0 * * * *"},
        )
        db_session.add(config)
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        assert str(config.id) in first["dispatched"]
        assert run_sync_mock.apply_async.call_count == 1

        job = (
            db_session.query(ScheduledJob)
            .filter(
                ScheduledJob.sync_config_id == config.id,
                ScheduledJob.job_type == "sync",
            )
            .one()
        )
        assert job.next_run_at is not None
        assert job.schedule_cron == "0 * * * *"
        assert job.is_running is False

        second = _call(task)
        assert second["dispatched"] == []
        assert run_sync_mock.apply_async.call_count == 1


class TestDispatchErrorIsolation:
    def test_bad_config_does_not_abort_remaining_configs(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        bad_config = _make_config(name="bad-config", last_sync_at=now - 2 * HOUR)
        bad_job = _make_job(bad_config, schedule_cron="BAD")
        good_config = _make_config(name="good-config", last_sync_at=now - 2 * HOUR)
        good_job = _make_job(good_config)
        db_session.add_all([bad_config, bad_job, good_config, good_job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["errors"] == 1
        assert str(good_config.id) in result["dispatched"]
        assert str(bad_config.id) not in result["dispatched"]
        assert run_sync_mock.apply_async.call_count == 1


class TestConsumerClearsRunningMarker:
    def test_terminal_failure_clears_is_running(self, monkeypatch, db_session):
        """run_sync_config must clear is_running on failure, or the scheduler
        would skip the config until the staleness TTL expires."""
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            name="no-owner-repo",
            # github + code targets but no owner/repo anywhere => terminal
            # ValueError AFTER the job is marked running.
            sync_options={},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()

        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _fake_session_ctx(db_session),
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_runtime._get_db_url",
            lambda: "sqlite:///:memory:",
        )

        task: Any = run_sync_config
        task.push_request(id=str(uuid.uuid4()), retries=0)
        try:
            with pytest.raises(ValueError, match="owner/repo"):
                task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        job = (
            db_session.query(ScheduledJob)
            .filter(
                ScheduledJob.sync_config_id == config.id,
                ScheduledJob.job_type == "sync",
            )
            .one()
        )
        assert job.is_running is False
        assert job.last_run_status == JobRunStatus.FAILED.value

    def test_pickup_reconciles_job_status_with_config(self, monkeypatch, db_session):
        """A job parked PAUSED (manual-only) must be reactivated when the
        config gained an explicit schedule out-of-band (CHAOS-2297)."""
        from dev_health_ops.workers.sync_runtime import run_sync_config

        config = _make_config(
            name="reactivate-me",
            # Explicit cron, but no owner/repo => terminal ValueError AFTER
            # the job row is resolved and reconciled.
            sync_options={"schedule_cron": "0 * * * *"},
            sync_targets=["git"],
        )
        db_session.add(config)
        db_session.flush()
        job = _make_job(config)
        job.status = JobStatus.PAUSED.value
        db_session.add(job)
        db_session.flush()

        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _fake_session_ctx(db_session),
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_runtime._get_db_url",
            lambda: "sqlite:///:memory:",
        )

        task: Any = run_sync_config
        task.push_request(id=str(uuid.uuid4()), retries=0)
        try:
            with pytest.raises(ValueError, match="owner/repo"):
                task(config_id=str(config.id), org_id="default")
        finally:
            task.pop_request()

        assert job.status == JobStatus.ACTIVE.value


class TestBatchRoutingStillStampsMarker:
    @patch("dev_health_ops.workers.sync_scheduler.dispatch_batch_sync")
    @patch("dev_health_ops.workers.sync_scheduler.run_sync_config")
    def test_batch_config_dispatched_once(
        self, run_sync_mock, batch_sync_mock, monkeypatch, db_session
    ):
        from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

        now = datetime.now(timezone.utc)
        config = _make_config(
            name="batch-config",
            last_sync_at=now - 2 * HOUR,
            sync_options={"search": "org/*", "schedule_cron": "0 * * * *"},
        )
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _fake_session_ctx(db_session),
        )

        task: Any = dispatch_scheduled_syncs
        first = _call(task)
        assert str(config.id) in first["dispatched"]
        assert batch_sync_mock.apply_async.call_count == 1
        run_sync_mock.apply_async.assert_not_called()

        # Batch configs never advance last_sync_at from the scheduler's
        # perspective; the marker alone must prevent the flood.
        second = _call(task)
        assert second["dispatched"] == []
        assert batch_sync_mock.apply_async.call_count == 1


class TestPerProviderQueueRouting:
    """Scheduled dispatch routes to per-provider sync queues (CHAOS-2299)."""

    @pytest.mark.parametrize(
        ("provider", "expected_queue"),
        [
            ("github", "sync.github"),
            ("gitlab", "sync.gitlab"),
            ("linear", "sync.linear"),
            ("jira", "sync.jira"),
            ("launchdarkly", "sync.launchdarkly"),
            ("mystery-provider", "sync"),
        ],
    )
    def test_run_sync_config_dispatched_to_provider_queue(
        self, monkeypatch, db_session, provider, expected_queue
    ):
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR, provider=provider)
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert str(config.id) in result["dispatched"]
        run_sync_mock.apply_async.assert_called_once()
        assert run_sync_mock.apply_async.call_args.kwargs["queue"] == expected_queue

    def test_batch_dispatch_routed_to_provider_queue(self, monkeypatch, db_session):
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        now = datetime.now(timezone.utc)
        config = _make_config(
            name="batch-queue-config",
            last_sync_at=now - 2 * HOUR,
            sync_options={"search": "org/*", "schedule_cron": "0 * * * *"},
        )
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, batch_sync_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert str(config.id) in result["dispatched"]
        batch_sync_mock.apply_async.assert_called_once()
        assert batch_sync_mock.apply_async.call_args.kwargs["queue"] == "sync.github"
        run_sync_mock.apply_async.assert_not_called()

    def test_flag_off_scheduled_dispatch_stays_on_shared_queue(
        self, monkeypatch, db_session
    ):
        """PROVIDER_SYNC_QUEUES_ENABLED unset (default): scheduled dispatch
        keeps using the legacy shared queue so old-`-Q` workers consume it."""
        monkeypatch.delenv("PROVIDER_SYNC_QUEUES_ENABLED", raising=False)
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR, provider="github")
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert str(config.id) in result["dispatched"]
        run_sync_mock.apply_async.assert_called_once()
        assert run_sync_mock.apply_async.call_args.kwargs["queue"] == "sync"


class TestManualOnlyConfigsNotDispatched:
    """Manual-only configs (no schedule_cron) must never auto-sync (CHAOS-2297)."""

    def test_config_without_schedule_cron_is_skipped(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            last_sync_at=now - 2 * HOUR,
            sync_options={"owner": "org", "repo": "repo"},
        )
        # Legacy rows: ACTIVE job carrying the default hourly placeholder cron.
        job = _make_job(config)
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, batch_sync_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        assert result["errors"] == 0
        run_sync_mock.apply_async.assert_not_called()
        batch_sync_mock.apply_async.assert_not_called()
        assert job.next_run_at is None

    def test_paused_job_is_skipped(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        job.status = JobStatus.PAUSED.value
        db_session.add_all([config, job])
        db_session.flush()

        task, run_sync_mock, _ = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        run_sync_mock.apply_async.assert_not_called()
        assert job.next_run_at is None
