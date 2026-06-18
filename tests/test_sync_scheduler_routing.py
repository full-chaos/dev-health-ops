"""Migrated-trigger routing in the sync scheduler beat (CHAOS-2516).

Tests that `_maybe_dispatch_config` routes through the fan-out planner when:
  (a) the migrated-trigger-routing flag is ON and the config is migrated
      => plan_sync_run + dispatch_sync_run called; legacy tasks NOT called.
  (b) the flag is OFF => legacy path unchanged.
  (c) the flag is ON but the config is NOT migrated => legacy path unchanged.
"""

from __future__ import annotations

import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    JobStatus,
    ScheduledJob,
    Setting,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.sync.trigger_routing import MIGRATED_TRIGGER_ROUTING_SETTING_KEY

HOUR = timedelta(hours=1)
ORG_ID = "routing-test-org"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@contextmanager
def _fake_session_ctx(session):
    yield session


def _hourly_croniter_module() -> SimpleNamespace:
    """Fake croniter: next occurrence = base + 1 hour."""

    def _croniter(expr: str, base: datetime):
        class _Iter:
            def get_next(self, _kind):
                return base + HOUR

        return _Iter()

    return SimpleNamespace(croniter=_croniter)


def _make_config(
    session: Session,
    *,
    last_sync_at: datetime | None = None,
    migrated_integration_id: uuid.UUID | None = None,
) -> SyncConfiguration:
    config = SyncConfiguration(
        name="routing-test-config",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git", "prs"],
        sync_options={"owner": "org", "repo": "repo", "schedule_cron": "0 * * * *"},
        is_active=True,
    )
    if last_sync_at is not None:
        config.last_sync_at = last_sync_at
    if migrated_integration_id is not None:
        config.migrated_integration_id = migrated_integration_id
    session.add(config)
    session.flush()
    return config


def _make_job(config: SyncConfiguration) -> ScheduledJob:
    job = ScheduledJob(
        name=f"sync-config-{config.id}",
        job_type="sync",
        schedule_cron="0 * * * *",
        org_id=config.org_id,
        provider=config.provider,
        sync_config_id=config.id,
    )
    job.status = JobStatus.ACTIVE.value
    return job


def _enable_flag(session: Session) -> None:
    """Insert the migrated-trigger-routing feature flag for ORG_ID."""
    setting = Setting(
        key=MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
        category=SettingCategory.SYNC.value,
        value="true",
        org_id=ORG_ID,
    )
    session.add(setting)
    session.flush()


def _call_maybe_dispatch(
    monkeypatch, session: Session, config: SyncConfiguration, now: datetime
) -> bool:
    """Wire croniter stub and call _maybe_dispatch_config directly."""
    from dev_health_ops.workers.sync_scheduler import _maybe_dispatch_config

    monkeypatch.setitem(sys.modules, "croniter", _hourly_croniter_module())
    return _maybe_dispatch_config(session, config, now)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPlannerRouting:
    """Flag ON + migrated config => planner path."""

    def test_flag_on_migrated_config_uses_planner(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        integration_id = uuid.uuid4()
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            migrated_integration_id=integration_id,
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        _enable_flag(db_session)

        fake_plan = SimpleNamespace(
            sync_run_id=str(uuid.uuid4()), total_units=1, unit_ids=()
        )
        plan_sync_run_mock = MagicMock(return_value=fake_plan)
        dispatch_sync_run_mock = MagicMock()
        run_sync_config_mock = MagicMock()
        batch_sync_mock = MagicMock()

        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.run_sync_config",
            run_sync_config_mock,
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync",
            batch_sync_mock,
        )

        # plan_sync_run and dispatch_sync_run are imported lazily inside
        # _maybe_dispatch_config, so patch at their source modules.
        with (
            patch("dev_health_ops.sync.planner.plan_sync_run", plan_sync_run_mock),
            patch(
                "dev_health_ops.workers.sync_units.dispatch_sync_run",
                dispatch_sync_run_mock,
            ),
        ):
            result = _call_maybe_dispatch(monkeypatch, db_session, config, now)

        assert result is True
        plan_sync_run_mock.assert_called_once()
        dispatch_sync_run_mock.apply_async.assert_called_once_with(
            args=(fake_plan.sync_run_id,), queue="sync"
        )
        run_sync_config_mock.apply_async.assert_not_called()
        batch_sync_mock.apply_async.assert_not_called()

    def test_flag_on_migrated_config_stamps_idempotency_marker(
        self, monkeypatch, db_session
    ):
        """next_run_at must be stamped before the planner commit."""
        now = datetime.now(timezone.utc)
        integration_id = uuid.uuid4()
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            migrated_integration_id=integration_id,
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        _enable_flag(db_session)

        fake_plan = SimpleNamespace(
            sync_run_id=str(uuid.uuid4()), total_units=1, unit_ids=()
        )
        plan_sync_run_mock = MagicMock(return_value=fake_plan)
        dispatch_sync_run_mock = MagicMock()

        with (
            patch("dev_health_ops.sync.planner.plan_sync_run", plan_sync_run_mock),
            patch(
                "dev_health_ops.workers.sync_units.dispatch_sync_run",
                dispatch_sync_run_mock,
            ),
        ):
            _call_maybe_dispatch(monkeypatch, db_session, config, now)

        # Idempotency marker must have been stamped (next_run_at > now).
        assert job.next_run_at is not None
        marker = job.next_run_at
        if marker.tzinfo is None:
            marker = marker.replace(tzinfo=timezone.utc)
        assert marker > now


class TestLegacyPathFlagOff:
    """Flag OFF => legacy path regardless of migration status."""

    def test_flag_off_migrated_config_uses_legacy(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        integration_id = uuid.uuid4()
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            migrated_integration_id=integration_id,
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        # No flag row => disabled.

        plan_sync_run_mock = MagicMock()
        dispatch_sync_run_mock = MagicMock()
        run_sync_config_mock = MagicMock()
        batch_sync_mock = MagicMock()

        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.run_sync_config",
            run_sync_config_mock,
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync",
            batch_sync_mock,
        )

        with (
            patch("dev_health_ops.sync.planner.plan_sync_run", plan_sync_run_mock),
            patch(
                "dev_health_ops.workers.sync_units.dispatch_sync_run",
                dispatch_sync_run_mock,
            ),
        ):
            result = _call_maybe_dispatch(monkeypatch, db_session, config, now)

        assert result is True
        plan_sync_run_mock.assert_not_called()
        dispatch_sync_run_mock.apply_async.assert_not_called()
        # Legacy path: run_sync_config (not batch, because owner+repo set).
        run_sync_config_mock.apply_async.assert_called_once()

    def test_flag_off_unmigrated_config_uses_legacy(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            # No migrated_integration_id.
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()

        run_sync_config_mock = MagicMock()
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.run_sync_config",
            run_sync_config_mock,
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync",
            MagicMock(),
        )

        result = _call_maybe_dispatch(monkeypatch, db_session, config, now)

        assert result is True
        run_sync_config_mock.apply_async.assert_called_once()


class TestLegacyPathFlagOnUnmigrated:
    """Flag ON but config NOT migrated => legacy path (plan_request_for_config returns None)."""

    def test_flag_on_unmigrated_config_uses_legacy(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            # migrated_integration_id is None => not migrated.
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        _enable_flag(db_session)

        plan_sync_run_mock = MagicMock()
        dispatch_sync_run_mock = MagicMock()
        run_sync_config_mock = MagicMock()
        batch_sync_mock = MagicMock()

        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.run_sync_config",
            run_sync_config_mock,
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync",
            batch_sync_mock,
        )

        with (
            patch("dev_health_ops.sync.planner.plan_sync_run", plan_sync_run_mock),
            patch(
                "dev_health_ops.workers.sync_units.dispatch_sync_run",
                dispatch_sync_run_mock,
            ),
        ):
            result = _call_maybe_dispatch(monkeypatch, db_session, config, now)

        assert result is True
        # plan_request_for_config returns None for un-migrated config => planner skipped.
        plan_sync_run_mock.assert_not_called()
        dispatch_sync_run_mock.apply_async.assert_not_called()
        # Legacy path used.
        run_sync_config_mock.apply_async.assert_called_once()
        batch_sync_mock.apply_async.assert_not_called()


class TestPlannerFailureFallback:
    """Flag ON + migrated config, but the planner fails (e.g. a stale
    migrated_integration_id raising ValueError, or a transient DB/queue error).
    The scheduled sync must NOT go dark until the next cron occurrence: roll
    back the failed plan attempt and fall through to the legacy per-config
    dispatch so this tick still syncs exactly once."""

    def test_planner_error_falls_back_to_legacy(self, monkeypatch, db_session):
        now = datetime.now(timezone.utc)
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            migrated_integration_id=uuid.uuid4(),
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        _enable_flag(db_session)

        # Stale/invalid integration: plan_sync_run raises before committing a run.
        plan_sync_run_mock = MagicMock(
            side_effect=ValueError("Integration not found for org")
        )
        dispatch_sync_run_mock = MagicMock()
        run_sync_config_mock = MagicMock()
        batch_sync_mock = MagicMock()

        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.run_sync_config",
            run_sync_config_mock,
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync",
            batch_sync_mock,
        )

        with (
            patch("dev_health_ops.sync.planner.plan_sync_run", plan_sync_run_mock),
            patch(
                "dev_health_ops.workers.sync_units.dispatch_sync_run",
                dispatch_sync_run_mock,
            ),
        ):
            result = _call_maybe_dispatch(monkeypatch, db_session, config, now)

        # Planner attempted, failed, fell back to legacy -- sync still dispatched.
        assert result is True
        plan_sync_run_mock.assert_called_once()
        dispatch_sync_run_mock.apply_async.assert_not_called()
        run_sync_config_mock.apply_async.assert_called_once()


class TestDispatchEnqueueFailure:
    """Flag ON + migrated config, plan commits, but dispatch_sync_run.apply_async
    raises (e.g. broker down). The committed run must not be left silently
    PLANNED: it is marked FAILED and the tick falls through to legacy dispatch."""

    def test_enqueue_failure_marks_run_failed_and_falls_back(
        self, monkeypatch, db_session
    ):
        from dev_health_ops.models.integrations import (
            Base as IntegrationsBase,
        )
        from dev_health_ops.models.integrations import (
            Integration,
            SyncRun,
            SyncRunStatus,
        )

        # Ensure the integration/sync-run tables exist in this in-memory DB.
        IntegrationsBase.metadata.create_all(db_session.get_bind())

        now = datetime.now(timezone.utc)
        integration = Integration(
            org_id=ORG_ID,
            provider="github",
            name="acme",
        )
        db_session.add(integration)
        db_session.flush()
        config = _make_config(
            db_session,
            last_sync_at=now - 2 * HOUR,
            migrated_integration_id=integration.id,
        )
        job = _make_job(config)
        db_session.add(job)
        db_session.flush()
        _enable_flag(db_session)

        # Real plan that persists a SyncRun, so we can assert it gets marked FAILED.
        captured = {}

        def _real_plan(session, request):
            run = SyncRun(
                org_id=ORG_ID,
                integration_id=integration.id,
                triggered_by=request.triggered_by,
                mode=request.mode,
                status=SyncRunStatus.PLANNED.value,
                total_units=0,
            )
            session.add(run)
            session.flush()
            captured["run_id"] = str(run.id)
            return SimpleNamespace(sync_run_id=str(run.id), total_units=0, unit_ids=())

        plan_sync_run_mock = MagicMock(side_effect=_real_plan)
        dispatch_sync_run_mock = MagicMock()
        dispatch_sync_run_mock.apply_async.side_effect = RuntimeError("broker down")
        run_sync_config_mock = MagicMock()
        batch_sync_mock = MagicMock()

        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.run_sync_config",
            run_sync_config_mock,
        )
        monkeypatch.setattr(
            "dev_health_ops.workers.sync_scheduler.dispatch_batch_sync",
            batch_sync_mock,
        )

        with (
            patch("dev_health_ops.sync.planner.plan_sync_run", plan_sync_run_mock),
            patch(
                "dev_health_ops.workers.sync_units.dispatch_sync_run",
                dispatch_sync_run_mock,
            ),
        ):
            result = _call_maybe_dispatch(monkeypatch, db_session, config, now)

        # Enqueue failed => fell back to legacy, sync still attempted this tick.
        assert result is True
        dispatch_sync_run_mock.apply_async.assert_called_once()
        run_sync_config_mock.apply_async.assert_called_once()
        # The committed run must be marked FAILED, not left PLANNED.
        run = db_session.get(SyncRun, uuid.UUID(captured["run_id"]))
        assert run is not None
        assert run.status == SyncRunStatus.FAILED.value
