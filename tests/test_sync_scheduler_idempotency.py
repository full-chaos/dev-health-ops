"""Dispatch-time idempotency for the sync scheduler (CHAOS-2270 / CHAOS-2647).

`dispatch_scheduled_syncs` runs every beat tick, but `last_sync_at` only
advances when a run completes, so without a dispatch marker every due config
was re-enqueued on every tick and flooded the sync queue. These tests cover
the `ScheduledJob.next_run_at` dispatch marker, the `is_running` staleness
escape, and per-config error isolation.

Post-CHAOS-2647 the scheduler routes integration-linked configs through the
fan-out planner (``plan_sync_run`` + ``dispatch_sync_run``); the legacy
``run_sync_config`` / ``dispatch_batch_sync`` worker path was removed. Configs
not linked to a migrated integration are skipped (logged + counted as skipped).
"""

from __future__ import annotations

import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
)
from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    ScheduledSyncOccurrence,
    SyncConfiguration,
)
from dev_health_ops.sync.execution_trigger import (
    SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
    scheduled_sync_occurrence_identity,
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
    session: Session,
    name: str = "test-config",
    last_sync_at: datetime | None = None,
    sync_options: dict | None = None,
    sync_targets: list | None = None,
    provider: str = "github",
    org_id: str = "default",
    *,
    migrated: bool = True,
) -> SyncConfiguration:
    """Create a SyncConfiguration, optionally linked to a migrated integration.

    A migrated config (the default) gets an Integration plus one enabled source
    and one enabled dataset so the scheduler can route it through the fan-out
    planner. Pass ``migrated=False`` to exercise the planner-only skip path.
    """
    integration_id: uuid.UUID | None = None
    if migrated:
        integration = Integration(
            org_id=org_id,
            provider=provider,
            name=f"integration-{name}-{uuid.uuid4()}",
            config={},
            is_active=True,
        )
        session.add(integration)
        session.flush()
        session.add_all(
            [
                IntegrationSource(
                    org_id=org_id,
                    integration_id=integration.id,
                    provider=provider,
                    source_type="repository",
                    external_id=f"{name}/repo",
                    name="repo",
                    full_name=f"{name}/repo",
                    metadata_={},
                    is_enabled=True,
                ),
                IntegrationDataset(
                    org_id=org_id,
                    integration_id=integration.id,
                    dataset_key="commits",
                    is_enabled=True,
                    options={},
                ),
            ]
        )
        session.flush()
        integration_id = integration.id

    config = SyncConfiguration(
        name=name,
        provider=provider,
        org_id=org_id,
        sync_targets=sync_targets or ["git", "prs"],
        # owner+repo set => explicit schedule_cron so the manual-only gate
        # (CHAOS-2297) doesn't skip dispatch in these tests.
        sync_options=sync_options
        if sync_options is not None
        else {"owner": "org", "repo": "repo", "schedule_cron": "0 * * * *"},
        is_active=True,
        integration_id=integration_id,
    )
    if last_sync_at is not None:
        config.last_sync_at = last_sync_at
    session.add(config)
    session.flush()
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


def _run_dispatch(monkeypatch, db_session) -> tuple[Any, MagicMock]:
    """Wire mocks and return (task, dispatch_mock).

    The scheduler routes due, integration-linked configs through
    ``plan_sync_run`` + ``dispatch_sync_run.apply_async``; the test patches the
    relocated ``dispatch_sync_run`` symbol so no real Celery enqueue happens.
    """
    from dev_health_ops.workers import sync_units as sync_units_mod
    from dev_health_ops.workers.sync_scheduler import dispatch_scheduled_syncs

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _fake_session_ctx(db_session),
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_scheduler.organization_exists_sync",
        lambda session, org_id_arg: True,
    )
    dispatch_mock = MagicMock()
    monkeypatch.setattr(sync_units_mod, "dispatch_sync_run", dispatch_mock)
    return dispatch_scheduled_syncs, dispatch_mock


def _call(task) -> dict:
    task.push_request(id=str(uuid.uuid4()))
    try:
        return task()
    finally:
        task.pop_request()


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


class TestDispatchIdempotency:
    def test_due_config_dispatched_once_not_redispatched_on_second_tick(
        self, monkeypatch, db_session
    ):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        assert str(config.id) in first["dispatched"]
        dispatch_mock.apply_async.assert_not_called()
        assert job.next_run_at is not None
        assert _aware(job.next_run_at) > now

        sync_run = db_session.query(SyncRun).one()
        runs = db_session.query(JobRun).filter(JobRun.job_id == job.id).all()
        assert sync_run is not None
        assert db_session.query(SyncDispatchOutbox).count() == 1
        assert len(runs) == 1
        assert runs[0].status == JobRunStatus.PENDING.value
        assert runs[0].triggered_by == "schedule"
        assert runs[0].result["sync_run_id"] == str(sync_run.id)

        second = _call(task)
        assert second["dispatched"] == []
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(JobRun).count() == 1
        assert db_session.query(SyncDispatchOutbox).count() == 1

    def test_expired_dispatch_marker_allows_redispatch(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        # Marker from a previous dispatch whose task was lost (queue purge,
        # worker crash) and whose cron occurrence has already passed.
        job = _make_job(config, next_run_at=now - timedelta(minutes=1))
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert str(config.id) in result["dispatched"]
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1
        assert job.next_run_at is not None
        assert _aware(job.next_run_at) > now

    def test_fresh_is_running_marker_skips_dispatch(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(
            config, is_running=True, last_run_at=now - timedelta(minutes=10)
        )
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        dispatch_mock.apply_async.assert_not_called()
        assert job.next_run_at is None

    def test_stale_is_running_marker_allows_redispatch(self, monkeypatch, db_session):
        from dev_health_ops.workers.sync_scheduler import STALE_RUNNING_TTL_SECONDS

        now = datetime.now(timezone.utc)
        stale = now - timedelta(seconds=STALE_RUNNING_TTL_SECONDS + 60)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        # Worker crashed mid-run: is_running was never cleared.
        job = _make_job(config, is_running=True, last_run_at=stale)
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        assert str(config.id) in first["dispatched"]
        dispatch_mock.apply_async.assert_not_called()

        # The re-dispatch stamps next_run_at, so even with the flag still
        # wedged the config is enqueued at most once per cron interval.
        second = _call(task)
        assert second["dispatched"] == []
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1

    def test_completed_run_resets_cycle_without_redispatch(
        self, monkeypatch, db_session
    ):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        _call(task)
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1

        # Simulate the consumer terminal transition: the run completed,
        # is_running cleared, last_sync_at advanced.
        job.is_running = False
        config.last_sync_at = now
        db_session.flush()

        # Still inside the same cron interval: nothing to dispatch.
        second = _call(task)
        assert second["dispatched"] == []
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1

        # Next cron interval reached (marker expired AND config due again).
        job.next_run_at = now - timedelta(seconds=1)
        config.last_sync_at = now - HOUR - timedelta(seconds=10)
        db_session.flush()

        third = _call(task)
        assert str(config.id) in third["dispatched"]
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 2

    def test_expired_marker_reuses_same_occurrence_without_duplicate_plan(
        self, monkeypatch, db_session
    ):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        first_occurrence = db_session.query(ScheduledSyncOccurrence).one()
        first_job_run_id = first_occurrence.job_run_id
        first_sync_run_id = first_occurrence.sync_run_id

        job.next_run_at = now - timedelta(seconds=1)
        db_session.flush()
        second = _call(task)

        assert str(config.id) in first["dispatched"]
        assert str(config.id) in second["dispatched"]
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(ScheduledSyncOccurrence).count() == 1
        assert db_session.query(JobRun).count() == 1
        assert db_session.query(SyncRun).count() == 1
        assert db_session.query(SyncDispatchOutbox).count() == 1
        persisted = db_session.query(ScheduledSyncOccurrence).one()
        assert persisted.job_run_id == first_job_run_id
        assert persisted.sync_run_id == first_sync_run_id

    def test_active_scheduler_completes_pending_go_occurrence_once(
        self, monkeypatch, db_session
    ):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        scheduled_for = now - HOUR
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(
                config.id,
                scheduled_for,
            ),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=scheduled_for,
        )
        db_session.add(occurrence)
        db_session.commit()
        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        db_session.refresh(occurrence)
        first_job_run_id = occurrence.job_run_id
        first_sync_run_id = occurrence.sync_run_id

        job.next_run_at = now - timedelta(seconds=1)
        db_session.commit()
        second = _call(task)

        assert str(config.id) in first["dispatched"]
        assert str(config.id) in second["dispatched"]
        assert first_job_run_id is not None
        assert first_sync_run_id is not None
        assert occurrence.job_run_id == first_job_run_id
        assert occurrence.sync_run_id == first_sync_run_id
        assert db_session.query(ScheduledSyncOccurrence).count() == 1
        assert db_session.query(JobRun).count() == 1
        assert db_session.query(SyncRun).count() == 1
        assert db_session.query(SyncDispatchOutbox).count() == 1
        dispatch_mock.apply_async.assert_not_called()

    def test_pending_occurrence_consumer_ignores_future_marker_and_replays_once(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        # The Go scheduler already advanced this marker.  The dormant Python
        # consumer must complete the durable occurrence without reconsidering
        # the next-run gate.
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        scheduled_for = now - HOUR
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(
                config.id,
                scheduled_for,
            ),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=scheduled_for,
        )
        db_session.add(occurrence)
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        first = reconcile_pending_scheduled_sync_occurrences(db_session)
        db_session.refresh(occurrence)
        first_job_run_id = occurrence.job_run_id
        first_sync_run_id = occurrence.sync_run_id

        second = reconcile_pending_scheduled_sync_occurrences(db_session)

        assert first == {
            "scanned": 1,
            "completed": 1,
            "retried": 0,
            "quarantined": 0,
            "already_completed": 0,
            "errors": 0,
        }
        assert second == {
            "scanned": 0,
            "completed": 0,
            "retried": 0,
            "quarantined": 0,
            "already_completed": 0,
            "errors": 0,
        }
        assert job.next_run_at is not None
        assert _aware(job.next_run_at) > now
        assert first_job_run_id is not None
        assert first_sync_run_id is not None
        assert occurrence.job_run_id == first_job_run_id
        assert occurrence.sync_run_id == first_sync_run_id
        assert db_session.query(ScheduledSyncOccurrence).count() == 1
        assert db_session.query(JobRun).count() == 1
        assert db_session.query(SyncRun).count() == 1
        assert db_session.query(SyncDispatchOutbox).count() == 1

    def test_pending_occurrence_reconciliation_is_deterministic_and_bounded(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 3 * HOUR)
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        later = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - HOUR,
        )
        earlier = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - 2 * HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - 2 * HOUR,
        )
        db_session.add_all([later, earlier])
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session, limit=1)

        db_session.refresh(earlier)
        db_session.refresh(later)
        assert result == {
            "scanned": 1,
            "completed": 1,
            "retried": 0,
            "quarantined": 0,
            "already_completed": 0,
            "errors": 0,
        }
        assert earlier.job_run_id is not None
        assert earlier.sync_run_id is not None
        assert later.job_run_id is None
        assert later.sync_run_id is None

    def test_pending_occurrence_reconciliation_caps_oversized_limit(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.workers import sync_scheduler

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 3 * HOUR)
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        for offset in range(101):
            scheduled_for = now - timedelta(hours=offset + 1)
            db_session.add(
                ScheduledSyncOccurrence(
                    occurrence_id=scheduled_sync_occurrence_identity(
                        config.id, scheduled_for
                    ),
                    identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
                    org_id=config.org_id,
                    sync_config_id=config.id,
                    scheduled_job_id=job.id,
                    scheduled_for=scheduled_for,
                )
            )
        db_session.commit()
        monkeypatch.setattr(
            sync_scheduler,
            "_complete_pending_scheduled_sync_occurrence",
            lambda *_args, **_kwargs: False,
        )

        result = sync_scheduler.reconcile_pending_scheduled_sync_occurrences(
            db_session,
            limit=10_000,
        )

        assert result == {
            "scanned": sync_scheduler.DEFAULT_PENDING_OCCURRENCE_RECONCILE_LIMIT + 1,
            "completed": 0,
            "retried": sync_scheduler.DEFAULT_PENDING_OCCURRENCE_RECONCILE_LIMIT + 1,
            "quarantined": 0,
            "already_completed": 0,
            "errors": 0,
        }

    def test_pending_occurrence_reconciliation_isolates_failed_rows(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.sync import execution_trigger
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        failing_config = _make_config(
            db_session, name="failing", last_sync_at=now - 2 * HOUR
        )
        succeeding_config = _make_config(
            db_session, name="succeeding", last_sync_at=now - 2 * HOUR
        )
        failing_job = _make_job(failing_config, next_run_at=now + HOUR)
        succeeding_job = _make_job(succeeding_config, next_run_at=now + HOUR)
        db_session.add_all([failing_job, succeeding_job])
        db_session.flush()
        failing_occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(
                failing_config.id, now - HOUR
            ),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=failing_config.org_id,
            sync_config_id=failing_config.id,
            scheduled_job_id=failing_job.id,
            scheduled_for=now - HOUR,
        )
        succeeding_occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(
                succeeding_config.id, now - HOUR
            ),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=succeeding_config.org_id,
            sync_config_id=succeeding_config.id,
            scheduled_job_id=succeeding_job.id,
            scheduled_for=now - HOUR,
        )
        db_session.add_all([failing_occurrence, succeeding_occurrence])
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )
        real_create_trigger = execution_trigger.create_sync_execution_trigger

        def fail_one_occurrence(session, config, org_id, **kwargs):
            if config.id == failing_config.id:
                raise RuntimeError("injected planner failure")
            return real_create_trigger(session, config, org_id, **kwargs)

        monkeypatch.setattr(
            execution_trigger, "create_sync_execution_trigger", fail_one_occurrence
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session)

        db_session.refresh(failing_occurrence)
        db_session.refresh(succeeding_occurrence)
        assert result == {
            "scanned": 2,
            "completed": 1,
            "retried": 1,
            "quarantined": 0,
            "already_completed": 0,
            "errors": 1,
        }
        assert failing_occurrence.job_run_id is None
        assert failing_occurrence.sync_run_id is None
        assert succeeding_occurrence.job_run_id is not None
        assert succeeding_occurrence.sync_run_id is not None

    def test_pending_occurrence_retry_exhaustion_is_terminal(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.sync import execution_trigger
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - HOUR,
            reconcile_attempt_count=4,
            reconcile_error_code="planner_error",
            reconcile_error_at=now - HOUR,
            reconcile_status="retry",
        )
        db_session.add(occurrence)
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        def fail_plan(*_args, **_kwargs):
            raise RuntimeError("injected planner failure")

        monkeypatch.setattr(
            execution_trigger, "create_sync_execution_trigger", fail_plan
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session, limit=1)

        db_session.refresh(occurrence)
        assert result == {
            "scanned": 1,
            "completed": 0,
            "retried": 0,
            "quarantined": 1,
            "already_completed": 0,
            "errors": 1,
        }
        assert occurrence.reconcile_attempt_count == 5
        assert occurrence.reconcile_status == "quarantined"
        assert occurrence.reconcile_error_code == "retry_exhausted"
        assert occurrence.reconcile_next_attempt_at is None
        assert occurrence.job_run_id is None
        assert occurrence.sync_run_id is None

    def test_pending_occurrence_identity_conflict_is_quarantined_without_planning(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.models.settings import (
            SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED,
        )
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        occurrence = ScheduledSyncOccurrence(
            occurrence_id="sha256:identity-conflict",
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - HOUR,
        )
        db_session.add(occurrence)
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session, limit=1)

        db_session.refresh(occurrence)
        assert result == {
            "scanned": 1,
            "completed": 0,
            "retried": 0,
            "quarantined": 1,
            "already_completed": 0,
            "errors": 0,
        }
        assert occurrence.reconcile_status == SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED
        assert occurrence.reconcile_error_code == "identity_conflict"
        assert occurrence.job_run_id is None
        assert occurrence.sync_run_id is None
        assert db_session.query(JobRun).count() == 0
        assert db_session.query(SyncRun).count() == 0
        assert db_session.query(SyncDispatchOutbox).count() == 0

    def test_pending_occurrence_org_mismatch_is_quarantined(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=f"{config.org_id}-mismatch",
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - HOUR,
        )
        db_session.add(occurrence)
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session, limit=1)

        db_session.refresh(occurrence)
        assert result == {
            "scanned": 1,
            "completed": 0,
            "retried": 0,
            "quarantined": 1,
            "already_completed": 0,
            "errors": 0,
        }
        assert occurrence.reconcile_status == "quarantined"
        assert occurrence.reconcile_error_code == "identity_conflict"
        assert occurrence.job_run_id is None
        assert occurrence.sync_run_id is None

    def test_quarantined_prefix_does_not_block_later_due_occurrence(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 3 * HOUR)
        job = _make_job(config, next_run_at=now + HOUR)
        db_session.add(job)
        db_session.flush()
        poisoned = ScheduledSyncOccurrence(
            occurrence_id="sha256:poisoned-prefix",
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - 2 * HOUR,
        )
        eligible = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=job.id,
            scheduled_for=now - HOUR,
        )
        db_session.add_all([poisoned, eligible])
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session, limit=1)

        db_session.refresh(poisoned)
        db_session.refresh(eligible)
        assert result == {
            "scanned": 2,
            "completed": 1,
            "retried": 0,
            "quarantined": 1,
            "already_completed": 0,
            "errors": 0,
        }
        assert poisoned.reconcile_status == "quarantined"
        assert eligible.job_run_id is not None
        assert eligible.sync_run_id is not None

    def test_mismatched_config_job_pair_is_quarantined(self, monkeypatch, db_session):
        from dev_health_ops.workers.sync_scheduler import (
            reconcile_pending_scheduled_sync_occurrences,
        )

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, name="config-a", last_sync_at=now - 2 * HOUR)
        other_config = _make_config(
            db_session, name="config-b", last_sync_at=now - 2 * HOUR
        )
        job = _make_job(config, next_run_at=now + HOUR)
        other_job = _make_job(other_config, next_run_at=now + HOUR)
        db_session.add_all([job, other_job])
        db_session.flush()
        occurrence = ScheduledSyncOccurrence(
            occurrence_id=scheduled_sync_occurrence_identity(config.id, now - HOUR),
            identity_version=SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION,
            org_id=config.org_id,
            sync_config_id=config.id,
            scheduled_job_id=other_job.id,
            scheduled_for=now - HOUR,
        )
        db_session.add(occurrence)
        db_session.commit()
        monkeypatch.setattr(
            "dev_health_ops.workers.org_guard.organization_exists_sync",
            lambda *_: True,
        )

        result = reconcile_pending_scheduled_sync_occurrences(db_session, limit=1)

        db_session.refresh(occurrence)
        assert result == {
            "scanned": 1,
            "completed": 0,
            "retried": 0,
            "quarantined": 1,
            "already_completed": 0,
            "errors": 0,
        }
        assert occurrence.reconcile_status == "quarantined"
        assert occurrence.job_run_id is None
        assert occurrence.sync_run_id is None

    def test_planner_failure_rolls_back_occurrence_plan_and_marker(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.sync import execution_trigger

        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add(job)
        db_session.commit()
        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        def fail_plan(*_args, **_kwargs):
            raise RuntimeError("injected planner failure")

        monkeypatch.setattr(execution_trigger, "plan_sync_run", fail_plan)
        result = _call(task)

        db_session.refresh(job)
        assert result["dispatched"] == []
        assert result["skipped"] == 1
        assert job.next_run_at is None
        assert db_session.query(ScheduledSyncOccurrence).count() == 0
        assert db_session.query(JobRun).count() == 0
        assert db_session.query(SyncRun).count() == 0
        assert db_session.query(SyncDispatchOutbox).count() == 0
        dispatch_mock.apply_async.assert_not_called()

    def test_deleting_authoritative_job_run_cascades_occurrence(
        self, monkeypatch, db_session
    ):
        db_session.execute(text("PRAGMA foreign_keys=ON"))
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        task, _ = _run_dispatch(monkeypatch, db_session)
        _call(task)

        occurrence = db_session.query(ScheduledSyncOccurrence).one()
        job_run = db_session.get(JobRun, occurrence.job_run_id)
        assert job_run is not None
        db_session.delete(job_run)
        db_session.commit()

        assert db_session.query(ScheduledSyncOccurrence).count() == 0

    def test_creates_scheduled_job_row_when_missing(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            sync_options={"owner": "org", "repo": "repo", "schedule_cron": "0 * * * *"},
        )
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        first = _call(task)
        assert str(config.id) in first["dispatched"]
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1

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
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1


class TestDispatchErrorIsolation:
    def test_bad_config_does_not_abort_remaining_configs(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        bad_config = _make_config(
            db_session, name="bad-config", last_sync_at=now - 2 * HOUR
        )
        bad_job = _make_job(bad_config, schedule_cron="BAD")
        good_config = _make_config(
            db_session, name="good-config", last_sync_at=now - 2 * HOUR
        )
        good_job = _make_job(good_config)
        db_session.add_all([bad_job, good_job])
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["errors"] == 1
        assert str(good_config.id) in result["dispatched"]
        assert str(bad_config.id) not in result["dispatched"]
        dispatch_mock.apply_async.assert_not_called()
        assert db_session.query(SyncDispatchOutbox).count() == 1


def test_scheduled_occurrence_identity_matches_go_golden() -> None:
    occurrence_id = scheduled_sync_occurrence_identity(
        "config-a",
        datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
    )
    assert SCHEDULED_SYNC_OCCURRENCE_IDENTITY_VERSION == "sync_scheduler_occurrence_v1"
    assert (
        occurrence_id
        == "sha256:27478ac7c7bbcfc33caa3922492910d97220984911632d754944fdeaf405f0f9"
    )


class TestUnmigratedConfigsSkipped:
    """Planner-only routing: a config not linked to a migrated integration is
    skipped by the scheduler (CHAOS-2647)."""

    def test_unmigrated_config_is_skipped(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR, migrated=False)
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        dispatch_mock.apply_async.assert_not_called()


class TestManualOnlyConfigsNotDispatched:
    """Manual-only configs (no schedule_cron) must never auto-sync (CHAOS-2297)."""

    def test_config_without_schedule_cron_is_skipped(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            sync_options={"owner": "org", "repo": "repo"},
        )
        # Legacy rows: ACTIVE job carrying the default hourly placeholder cron.
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        assert result["errors"] == 0
        dispatch_mock.apply_async.assert_not_called()
        assert job.next_run_at is None

    def test_paused_job_is_skipped(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(db_session, last_sync_at=now - 2 * HOUR)
        job = _make_job(config)
        job.status = JobStatus.PAUSED.value
        db_session.add(job)
        db_session.flush()

        task, dispatch_mock = _run_dispatch(monkeypatch, db_session)

        result = _call(task)
        assert result["dispatched"] == []
        dispatch_mock.apply_async.assert_not_called()
        assert job.next_run_at is None
