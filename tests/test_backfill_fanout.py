from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    ScheduledJob,
    Setting,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.sync.trigger_routing import MIGRATED_TRIGGER_ROUTING_SETTING_KEY
from dev_health_ops.sync.watermarks import get_watermark, set_watermark

ORG_ID = "backfill-fanout-org"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _session_ctx(session):
    yield session
    session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db
    from dev_health_ops.backfill import runner

    def session_factory():
        return _session_ctx(session)

    monkeypatch.setattr(db, "get_postgres_session_sync", session_factory)
    monkeypatch.setattr(runner, "get_postgres_session_sync", session_factory)


def _create_integration(session: Session, provider: str = "github") -> Integration:
    integration = Integration(
        org_id=ORG_ID,
        provider=provider,
        name=f"{provider} integration",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _create_source(
    session: Session, integration: Integration, external_id: str
) -> IntegrationSource:
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider=integration.provider,
        source_type="repo",
        external_id=external_id,
        name=external_id.rsplit("/", 1)[-1],
        full_name=external_id,
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    session.add(source)
    session.flush()
    return source


def _create_dataset(
    session: Session, integration: Integration, dataset_key: str
) -> IntegrationDataset:
    dataset = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=True,
        options={},
    )
    session.add(dataset)
    session.flush()
    return dataset


def _planned_units(session: Session, sync_run_id: str) -> list[SyncRunUnit]:
    return (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == uuid.UUID(sync_run_id))
        .order_by(SyncRunUnit.provider, SyncRunUnit.dataset_key, SyncRunUnit.source_id)
        .all()
    )


def _seed_single_unit_run(
    session: Session, *, mode: str
) -> tuple[SyncRun, SyncRunUnit]:
    integration = _create_integration(session)
    source = _create_source(session, integration, "full-chaos/dev-health")
    _create_dataset(session, integration, "commits")
    run = SyncRun(
        org_id=ORG_ID,
        integration_id=integration.id,
        triggered_by="backfill",
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add(run)
    session.flush()
    unit = SyncRunUnit(
        org_id=ORG_ID,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=mode,
        since_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        before_at=datetime(2026, 6, 7, 23, 59, tzinfo=timezone.utc),
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True},
    )
    session.add(unit)
    session.flush()
    return run, unit


def _patch_unit_runtime(monkeypatch):
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_bootstrap import ProviderRuntime

    class RuntimeCache:
        def get(self, context):
            return ProviderRuntime(extra={"unit_id": context.unit_id})

    monkeypatch.setattr(sync_units, "_runtime_cache", RuntimeCache())


def test_run_backfill_via_planner_creates_backfill_units_per_source_dataset_window(
    db_session, monkeypatch
):
    from dev_health_ops.backfill import runner

    integration = _create_integration(db_session)
    sources = [
        _create_source(db_session, integration, "full-chaos/dev-health"),
        _create_source(db_session, integration, "full-chaos/dev-health-web"),
    ]
    for dataset_key in ("commits", "prs"):
        _create_dataset(db_session, integration, dataset_key)
    _patch_db_session(monkeypatch, db_session)
    dispatched = []

    def _fake_dispatch(sync_run_id):
        dispatched.append(sync_run_id)
        return {"status": "dispatched", "queued_units": 8}

    monkeypatch.setattr(
        "dev_health_ops.workers.sync_units.dispatch_sync_run",
        _fake_dispatch,
    )

    result = runner.run_backfill_via_planner(
        str(integration.id),
        date(2026, 6, 1),
        date(2026, 6, 14),
        org_id=ORG_ID,
        triggered_by="manual",
    )

    sync_run = db_session.get(SyncRun, uuid.UUID(result["sync_run_id"]))
    units = _planned_units(db_session, result["sync_run_id"])
    assert result["unit_count"] == 8
    assert dispatched == [result["sync_run_id"]]
    assert sync_run is not None
    assert sync_run.mode == SyncRunMode.BACKFILL.value
    assert sync_run.total_units == 8
    assert len(units) == 8
    assert {unit.mode for unit in units} == {SyncRunMode.BACKFILL.value}
    assert {(str(unit.source_id), unit.dataset_key) for unit in units} == {
        (str(source.id), dataset_key)
        for source in sources
        for dataset_key in ("commits", "prs")
    }
    windows = set()
    for unit in units:
        assert unit.since_at is not None
        assert unit.before_at is not None
        windows.add((unit.since_at.date(), unit.before_at.date()))
    assert windows == {
        (date(2026, 6, 1), date(2026, 6, 7)),
        (date(2026, 6, 8), date(2026, 6, 14)),
    }


def test_backfill_unit_does_not_write_watermark(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_single_unit_run(db_session, mode=SyncRunMode.BACKFILL.value)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    db_session.flush()
    initial_watermark = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    set_watermark(
        db_session, ORG_ID, "full-chaos/dev-health", "commits", initial_watermark
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_unit_runtime(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "apply_async", lambda args=None, queue=None: None
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "success"
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    watermark = get_watermark(db_session, ORG_ID, "full-chaos/dev-health", "commits")
    assert watermark is not None
    assert watermark.replace(tzinfo=timezone.utc) == initial_watermark
    assert db_session.query(SyncWatermark).count() == 1
    assert run.mode == SyncRunMode.BACKFILL.value


def test_backfill_finalize_dispatches_post_sync_metrics_once(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_single_unit_run(db_session, mode=SyncRunMode.BACKFILL.value)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    monkeypatch.setattr(
        sync_units,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    first = sync_units.finalize_sync_run(str(run.id))
    second = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert first["status"] == "finalized"
    assert second["status"] == "already_dispatched"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert db_session.query(SyncRunPostDispatch).count() == 1
    assert len(dispatches) == 1
    assert dispatches[0]["sync_targets"] == ["git"]


def test_backfill_job_response_can_report_run_unit_progress(db_session):
    from dev_health_ops.api.admin.routers.sync import _backfill_job_response

    config = SyncConfiguration(
        name="fanout progress",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=True,
    )
    db_session.add(config)
    db_session.flush()
    job = BackfillJob(
        org_id=ORG_ID,
        sync_config_id=config.id,
        status="running",
        since_date=date(2026, 6, 1),
        before_date=date(2026, 6, 14),
        total_chunks=0,
        completed_chunks=0,
        failed_chunks=0,
    )
    db_session.add(job)
    db_session.flush()

    response = _backfill_job_response(
        job,
        {
            "status": "partial_failed",
            "total_chunks": 8,
            "completed_chunks": 6,
            "failed_chunks": 2,
            "error_message": None,
        },
    )

    assert response.total_chunks == 8
    assert response.completed_chunks == 6
    assert response.failed_chunks == 2
    assert response.progress_pct == 75.0
    assert response.status == "partial_failed"


def test_run_backfill_uses_legacy_path_when_feature_flag_off(db_session, monkeypatch):
    from dev_health_ops.backfill import runner
    from dev_health_ops.workers import sync_backfill, sync_runtime

    config = SyncConfiguration(
        name="legacy backfill",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=True,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.delenv("SYNC_FANOUT_BACKFILL", raising=False)
    monkeypatch.setattr(sync_backfill, "_get_db_url", lambda: "clickhouse://test")
    monkeypatch.setattr(
        sync_runtime, "_dispatch_post_sync_tasks", lambda **kwargs: None
    )
    legacy_calls = []

    def _fake_legacy(**kwargs):
        legacy_calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(
        runner,
        "run_backfill_for_config",
        _fake_legacy,
    )
    monkeypatch.setattr(
        runner,
        "run_backfill_via_planner",
        lambda *args, **kwargs: pytest.fail("fan-out path should be disabled"),
    )

    run_backfill_task = getattr(sync_backfill.run_backfill, "run")
    result = run_backfill_task(
        sync_config_id=str(config.id),
        since="2026-06-01",
        before="2026-06-07",
        org_id=ORG_ID,
    )

    assert result["status"] == "success"
    assert len(legacy_calls) == 1
    assert legacy_calls[0]["sync_config_id"] == str(config.id)
    assert legacy_calls[0]["since"] == date(2026, 6, 1)
    assert legacy_calls[0]["before"] == date(2026, 6, 7)


def test_run_backfill_paused_config_marks_backfill_and_job_run_cancelled(
    db_session, monkeypatch
):
    from dev_health_ops.backfill import runner
    from dev_health_ops.workers import sync_backfill

    config = SyncConfiguration(
        name="paused backfill",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=False,
    )
    db_session.add(config)
    db_session.flush()
    scheduled_job = ScheduledJob(
        name=f"sync-config-{config.id}",
        job_type="sync",
        schedule_cron="0 * * * *",
        org_id=ORG_ID,
        provider="github",
        job_config={},
        sync_config_id=config.id,
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled_job)
    db_session.flush()
    job_run = JobRun(
        job_id=scheduled_job.id,
        triggered_by="backfill",
        status=JobRunStatus.PENDING.value,
    )
    backfill_job = BackfillJob(
        org_id=ORG_ID,
        sync_config_id=config.id,
        status="queued",
        since_date=date(2026, 6, 1),
        before_date=date(2026, 6, 7),
        total_chunks=0,
        completed_chunks=0,
        failed_chunks=0,
    )
    db_session.add_all([job_run, backfill_job])
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    monkeypatch.setattr(
        runner,
        "run_backfill_for_config",
        lambda **kwargs: pytest.fail("paused config must not run legacy backfill"),
    )
    monkeypatch.setattr(
        runner,
        "run_backfill_via_planner",
        lambda *args, **kwargs: pytest.fail("paused config must not run planner"),
    )

    run_backfill_task = getattr(sync_backfill.run_backfill, "run")
    result = run_backfill_task(
        sync_config_id=str(config.id),
        since="2026-06-01",
        before="2026-06-07",
        org_id=ORG_ID,
        backfill_job_id=str(backfill_job.id),
        pending_run_id=str(job_run.id),
    )

    db_session.refresh(backfill_job)
    db_session.refresh(job_run)
    assert result == {"status": "skipped", "reason": "sync_config_inactive"}
    assert backfill_job.status == "cancelled"
    assert backfill_job.error_message == "Sync configuration is paused"
    assert backfill_job.completed_at is not None
    assert job_run.status == JobRunStatus.CANCELLED.value
    assert job_run.error == "Sync configuration is paused"
    assert job_run.completed_at is not None


def test_run_backfill_task_fanout_resolves_migrated_integration_id(
    db_session, monkeypatch
):
    """Fan-out backfill must plan against the migrated Integration id, not the
    SyncConfiguration id (regression: the worker used to fall back to
    sync_config_id and crash with 'Integration not found')."""
    from dev_health_ops.backfill import runner
    from dev_health_ops.workers import sync_backfill

    integration = _create_integration(db_session)
    _create_source(db_session, integration, "full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")
    config = SyncConfiguration(
        name="migrated parent backfill",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=True,
        migrated_integration_id=integration.id,
        planner_managed=True,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_FANOUT_BACKFILL", "1")

    captured: dict = {}

    def _fake_planner(
        integration_id,
        since,
        before,
        *,
        org_id,
        source_ids=None,
        dataset_keys=None,
        triggered_by,
    ):
        captured.update(
            integration_id=integration_id,
            since=since,
            before=before,
            org_id=org_id,
            source_ids=source_ids,
            dataset_keys=dataset_keys,
            triggered_by=triggered_by,
        )
        return {
            "status": "success",
            "sync_run_id": str(uuid.uuid4()),
            "unit_count": 0,
        }

    monkeypatch.setattr(runner, "run_backfill_via_planner", _fake_planner)
    monkeypatch.setattr(
        runner,
        "run_backfill_for_config",
        lambda **kwargs: pytest.fail(
            "legacy path should not run for a migrated config"
        ),
    )

    run_backfill_task = getattr(sync_backfill.run_backfill, "run")
    result = run_backfill_task(
        sync_config_id=str(config.id),
        since="2026-06-01",
        before="2026-06-07",
        org_id=ORG_ID,
    )

    assert result["status"] == "success"
    assert captured["integration_id"] == str(integration.id)
    assert captured["integration_id"] != str(config.id)
    assert captured["source_ids"] is None  # parent => whole integration
    assert captured["since"] == date(2026, 6, 1)
    assert captured["before"] == date(2026, 6, 7)
    assert captured["triggered_by"] == "backfill"


def test_run_backfill_task_fanout_child_config_scopes_source(db_session, monkeypatch):
    """A migrated child config must scope the planned run to its own source
    (and the datasets derived from its legacy targets), mirroring
    trigger_routing.plan_request_for_config."""
    from dev_health_ops.backfill import runner
    from dev_health_ops.sync.trigger_routing import _dataset_keys_for_config
    from dev_health_ops.workers import sync_backfill

    integration = _create_integration(db_session)
    source = _create_source(db_session, integration, "full-chaos/dev-health")
    _create_dataset(db_session, integration, "commits")
    config = SyncConfiguration(
        name="migrated child backfill",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=True,
        migrated_integration_id=integration.id,
        migrated_source_id=source.id,
        planner_managed=True,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_FANOUT_BACKFILL", "1")

    expected_dataset_keys = _dataset_keys_for_config(config) or None
    captured: dict = {}

    def _fake_planner(integration_id, since, before, **kwargs):
        captured.update(integration_id=integration_id, **kwargs)
        return {
            "status": "success",
            "sync_run_id": str(uuid.uuid4()),
            "unit_count": 0,
        }

    monkeypatch.setattr(runner, "run_backfill_via_planner", _fake_planner)

    run_backfill_task = getattr(sync_backfill.run_backfill, "run")
    result = run_backfill_task(
        sync_config_id=str(config.id),
        since="2026-06-01",
        before="2026-06-07",
        org_id=ORG_ID,
    )

    assert result["status"] == "success"
    assert captured["integration_id"] == str(integration.id)
    assert captured["source_ids"] == (str(source.id),)
    assert captured["dataset_keys"] == expected_dataset_keys


def test_run_backfill_task_unmigrated_config_falls_back_to_legacy(
    db_session, monkeypatch
):
    """Even with the global fan-out flag on, an un-migrated config (no
    migrated_integration_id) must use the legacy per-config path instead of
    crashing in the planner. This is the exact regression scenario."""
    from dev_health_ops.backfill import runner
    from dev_health_ops.workers import sync_backfill, sync_runtime

    config = SyncConfiguration(
        name="unmigrated backfill",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=True,
    )
    db_session.add(config)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_FANOUT_BACKFILL", "1")  # fan-out requested...
    monkeypatch.setattr(sync_backfill, "_get_db_url", lambda: "clickhouse://test")
    monkeypatch.setattr(
        sync_runtime, "_dispatch_post_sync_tasks", lambda **kwargs: None
    )

    legacy_calls: list = []

    def _fake_legacy(**kwargs):
        legacy_calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(runner, "run_backfill_for_config", _fake_legacy)
    monkeypatch.setattr(
        runner,
        "run_backfill_via_planner",
        lambda *args, **kwargs: pytest.fail(
            "planner must not run for an un-migrated config"
        ),
    )

    run_backfill_task = getattr(sync_backfill.run_backfill, "run")
    result = run_backfill_task(
        sync_config_id=str(config.id),
        since="2026-06-01",
        before="2026-06-07",
        org_id=ORG_ID,
    )

    assert result["status"] == "success"
    assert len(legacy_calls) == 1
    assert legacy_calls[0]["sync_config_id"] == str(config.id)


def test_run_backfill_task_migrated_non_planner_config_with_source_needs_flag(
    db_session, monkeypatch
):
    from dev_health_ops.backfill import runner
    from dev_health_ops.workers import sync_backfill, sync_runtime

    integration = _create_integration(db_session)
    _create_source(db_session, integration, "full-chaos/dev-health")
    config = SyncConfiguration(
        name="migrated legacy backfill",
        provider="github",
        org_id=ORG_ID,
        sync_targets=["git"],
        sync_options={},
        is_active=True,
        migrated_integration_id=integration.id,
    )
    db_session.add_all(
        [
            config,
            Setting(
                org_id=ORG_ID,
                category=SettingCategory.SYNC.value,
                key=MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
                value="false",
            ),
        ]
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(sync_backfill, "_get_db_url", lambda: "clickhouse://test")
    monkeypatch.setattr(
        sync_runtime, "_dispatch_post_sync_tasks", lambda **kwargs: None
    )

    legacy_calls: list = []

    def _fake_legacy(**kwargs):
        legacy_calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(runner, "run_backfill_for_config", _fake_legacy)
    monkeypatch.setattr(
        runner,
        "run_backfill_via_planner",
        lambda *args, **kwargs: pytest.fail(
            "planner must not run when planner_managed is false and flag is off"
        ),
    )

    run_backfill_task = getattr(sync_backfill.run_backfill, "run")
    result = run_backfill_task(
        sync_config_id=str(config.id),
        since="2026-06-01",
        before="2026-06-07",
        org_id=ORG_ID,
    )

    assert config.planner_managed is False
    assert result["status"] == "success"
    assert len(legacy_calls) == 1
    assert legacy_calls[0]["sync_config_id"] == str(config.id)
