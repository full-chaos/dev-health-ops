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
    SyncConfiguration,
)
from dev_health_ops.sync.watermarks import get_watermark, set_watermark
from tests._helpers import seed_sync_dispatch_transport_routes

ORG_ID = "backfill-fanout-org"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_sync_dispatch_transport_routes(session)
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
    session: Session,
    *,
    mode: str,
    provider: str = "github",
    dataset_key: str = "commits",
    source_external_id: str = "full-chaos/dev-health",
    source_type: str = "repo",
) -> tuple[SyncRun, SyncRunUnit]:
    integration = _create_integration(session, provider=provider)
    source = _create_source(session, integration, source_external_id)
    source.source_type = source_type
    source.name = source_external_id
    source.full_name = source_external_id
    _create_dataset(session, integration, dataset_key)
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
        provider=provider,
        dataset_key=dataset_key,
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


def test_run_backfill_via_planner_returns_terminal_plan_without_dispatch(
    db_session, monkeypatch
):
    # Given: the planner returns a durable terminal PagerDuty repair run.
    from dev_health_ops.backfill import runner
    from dev_health_ops.sync.planner import SyncRunPlan

    _patch_db_session(monkeypatch, db_session)
    terminal_plan = SyncRunPlan(
        sync_run_id=str(uuid.uuid4()),
        total_units=0,
        unit_ids=(),
        dispatch_required=False,
        terminal_reason="PagerDuty target was disabled",
    )
    monkeypatch.setattr(
        "dev_health_ops.sync.planner.plan_sync_run", lambda *_args: terminal_plan
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_units.dispatch_sync_run",
        lambda *_args: pytest.fail("terminal plan must not dispatch"),
    )

    # When: the planner-backed backfill command runs.
    result = runner.run_backfill_via_planner(
        str(uuid.uuid4()),
        date(2026, 6, 1),
        date(2026, 6, 14),
        org_id=ORG_ID,
        triggered_by="manual",
    )

    # Then: it exposes the terminal result without executing work.
    assert result["status"] == "disabled"
    assert result["sync_run_id"] == terminal_plan.sync_run_id
    assert result["reason"] == terminal_plan.terminal_reason


def test_finalize_sync_run_terminalizes_backfill_job_and_job_run(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_single_unit_run(db_session, mode=SyncRunMode.BACKFILL.value)
    unit.status = SyncRunUnitStatus.FAILED.value
    unit.error = "bad credentials"
    unit.result = {"error_category": "auth"}
    backfill_job = BackfillJob(
        org_id=ORG_ID,
        sync_config_id=uuid.uuid4(),
        celery_task_id=f"worker|sync_run:{run.id}",
        status="running",
        since_date=date(2026, 6, 1),
        before_date=date(2026, 6, 7),
        total_chunks=1,
        completed_chunks=0,
        failed_chunks=0,
    )
    scheduled = ScheduledJob(
        org_id=ORG_ID,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="github",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=backfill_job.sync_config_id,
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    job_run = JobRun(
        job_id=scheduled.id,
        triggered_by="backfill",
        status=JobRunStatus.RUNNING.value,
    )
    job_run.result = {"sync_run_id": str(run.id)}
    db_session.add_all([backfill_job, job_run])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(backfill_job)
    db_session.refresh(job_run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.FAILED.value
    assert backfill_job.status == "failed"
    assert backfill_job.total_chunks == 1
    assert backfill_job.failed_chunks == 1
    assert backfill_job.completed_at is not None
    assert job_run.status == JobRunStatus.FAILED.value
    assert job_run.completed_at is not None
    job_run_result: dict[str, object] = dict(job_run.result or {})
    assert job_run_result.get("sync_run_status") == SyncRunStatus.FAILED.value


def test_reconciler_retry_exhaustion_terminalizes_backfill_job_and_job_run(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, unit = _seed_single_unit_run(
        db_session,
        mode=SyncRunMode.BACKFILL.value,
        provider="linear",
        dataset_key="work-items",
        source_external_id="ENG",
        source_type="team",
    )
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = "worker-dead"
    unit.lease_expires_at = now.replace(microsecond=0)
    unit.expired_lease_retry_count = 1
    backfill_job = BackfillJob(
        org_id=ORG_ID,
        sync_config_id=uuid.uuid4(),
        celery_task_id=f"worker|sync_run:{run.id}",
        status="running",
        since_date=date(2026, 6, 1),
        before_date=date(2026, 6, 7),
        total_chunks=1,
        completed_chunks=0,
        failed_chunks=0,
    )
    scheduled = ScheduledJob(
        org_id=ORG_ID,
        name=f"sync-config-{uuid.uuid4()}",
        job_type="sync",
        provider="linear",
        schedule_cron="0 * * * *",
        job_config={},
        sync_config_id=backfill_job.sync_config_id,
        tz="UTC",
        status=1,
    )
    db_session.add(scheduled)
    db_session.flush()
    job_run = JobRun(
        job_id=scheduled.id,
        triggered_by="backfill",
        status=JobRunStatus.RUNNING.value,
    )
    job_run.result = {"sync_run_id": str(run.id)}
    db_session.add_all([backfill_job, job_run])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "1")
    monkeypatch.setattr(
        sync_units,
        "_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES",
        sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES,
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )

    def finalize_inline(args=None, queue=None):
        assert args is not None
        sync_units.finalize_sync_run(str(args[0]))

    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        finalize_inline,
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(unit)
    db_session.refresh(run)
    db_session.refresh(backfill_job)
    db_session.refresh(job_run)
    assert result["expired_retry_exhausted_units"] == 1
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result is not None
    assert unit.result["error_category"] == "worker_lost_retry_exhausted"
    assert run.status == SyncRunStatus.FAILED.value
    assert backfill_job.status == "failed"
    assert backfill_job.failed_chunks == 1
    assert backfill_job.completed_at is not None
    assert job_run.status == JobRunStatus.FAILED.value
    assert job_run.completed_at is not None


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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

    run, unit = _seed_single_unit_run(db_session, mode=SyncRunMode.BACKFILL.value)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    monkeypatch.setattr(
        post_sync_dispatch,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: dispatches.append(kwargs),
    )

    first = sync_units.finalize_sync_run(str(run.id))
    second = sync_units.finalize_sync_run(str(run.id))
    relay_first = sync_reconciler.reconcile_sync_dispatch(limit=10)
    relay_second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(run)
    assert first["status"] == "finalized"
    assert second["status"] == "already_dispatched"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert db_session.query(SyncRunPostDispatch).count() == 1
    assert relay_first["relayed_post_sync"] == 1
    assert relay_second["relayed_post_sync"] == 0
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
