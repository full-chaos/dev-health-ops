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
from dev_health_ops.models.settings import SyncConfiguration
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
