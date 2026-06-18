from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

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
    session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _seed_run(session, *, mode=SyncRunMode.INCREMENTAL.value):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key="commits",
        is_enabled=True,
        options={},
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, dataset, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=mode,
        since_at=None,
        before_at=datetime.now(timezone.utc),
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
        processor_flags={"sync_git": True},
    )
    session.add(unit)
    session.flush()
    return run, unit


def _patch_runtime(monkeypatch):
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_bootstrap import ProviderRuntime

    class RuntimeCache:
        def get(self, context):
            return ProviderRuntime(extra={"unit_id": context.unit_id})

    monkeypatch.setattr(sync_units, "_runtime_cache", RuntimeCache())


def _patch_finalize_apply(monkeypatch):
    from dev_health_ops.workers import sync_units

    calls = []
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: calls.append((args, queue)),
    )
    return calls


def test_run_sync_unit_success_persists_status_and_incremental_watermark(
    db_session, monkeypatch
):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.attempts == 1
    assert unit.result == {"ok": True}
    watermark = db_session.query(SyncWatermark).one()
    assert watermark.org_id == run.org_id
    assert watermark.source_id == "full-chaos/dev-health"
    assert watermark.dataset_key == "commits"
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_success_skips_watermark_for_backfill(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session, mode=SyncRunMode.BACKFILL.value)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        dataset_adapters, "run_dataset_unit", lambda ctx, runtime: {"ok": True}
    )

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "success"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert db_session.query(SyncWatermark).count() == 0
    assert finalize_calls == [((str(run.id),), "sync")]


def test_run_sync_unit_failure_persists_failed_and_error(db_session, monkeypatch):
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    finalize_calls = _patch_finalize_apply(monkeypatch)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def fail(ctx, runtime):
        raise RuntimeError("adapter failed")

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", fail)

    result = getattr(run_sync_unit, "run")(str(unit.id))
    assert result["status"] == "failed"
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.error == "adapter failed"
    assert finalize_calls == [((str(run.id),), "sync")]


def test_finalize_once_only_dispatches_metrics_once(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
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


def test_finalize_aggregates_partial_failed(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.SUCCESS.value
    failed = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=unit.integration_id,
        source_id=unit.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.FAILED.value,
        attempts=1,
    )
    run.total_units = 2
    db_session.add(failed)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(sync_units, "_dispatch_post_sync_tasks", lambda **kwargs: None)

    result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    assert result["status"] == "finalized"
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_units == 1
    assert run.failed_units == 1


def test_dispatch_sync_run_redispatches_only_planned_units(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, planned = _seed_run(db_session)
    recent_dispatching = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=planned.integration_id,
        source_id=planned.source_id,
        provider="github",
        dataset_key="prs",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
    )
    db_session.add(recent_dispatching)
    db_session.flush()
    recent_dispatching.updated_at = datetime.now(timezone.utc)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    queued = []

    class FakeSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id
            self.queue = None

        def set(self, *, queue):
            self.queue = queue
            queued.append(self)
            return self

    class FakeChord:
        def __init__(self, header, callback):
            self.header = header
            self.callback = callback

        def apply_async(self):
            return None

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: FakeSig(unit_id))
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "si", lambda run_id: FakeSig(run_id)
    )
    monkeypatch.setattr(sync_units, "group", lambda signatures: list(signatures))
    monkeypatch.setattr(
        sync_units, "chord", lambda header, callback: FakeChord(header, callback)
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(planned)
    db_session.refresh(recent_dispatching)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert [sig.unit_id for sig in queued if sig.unit_id != str(run.id)] == [
        str(planned.id)
    ]
    assert planned.status == SyncRunUnitStatus.DISPATCHING.value
    assert recent_dispatching.status == SyncRunUnitStatus.DISPATCHING.value


def test_dispatch_sync_run_redispatches_stale_dispatching_units(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    unit.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    queued = []

    class FakeSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            queued.append((self.unit_id, queue))
            return self

    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda unit_id: FakeSig(unit_id))
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "si", lambda run_id: FakeSig(run_id)
    )
    monkeypatch.setattr(sync_units, "group", lambda signatures: list(signatures))
    monkeypatch.setattr(
        sync_units,
        "chord",
        lambda header, callback: type("C", (), {"apply_async": lambda self: None})(),
    )

    result = sync_units.dispatch_sync_run(str(run.id))
    assert result["queued_units"] == 1
    assert queued[0][0] == str(unit.id)
