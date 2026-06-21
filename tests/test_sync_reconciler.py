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
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
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
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _seed_run(session, *, planned_units=0):
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
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=planned_units + 1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, run])
    session.flush()
    now = datetime.now(timezone.utc)
    running = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-dead",
        lease_expires_at=now - timedelta(seconds=1),
        last_heartbeat_at=now - timedelta(minutes=5),
    )
    session.add(running)
    planned = []
    for index in range(planned_units):
        unit = SyncRunUnit(
            org_id=org_id,
            sync_run_id=run.id,
            integration_id=integration.id,
            source_id=source.id,
            provider="github",
            dataset_key=f"prs-{index}",
            cost_class="medium",
            mode=SyncRunMode.INCREMENTAL.value,
            status=SyncRunUnitStatus.PLANNED.value,
            attempts=0,
        )
        session.add(unit)
        planned.append(unit)
    session.flush()
    return run, running, planned


def test_reconciler_expires_dead_running_and_dispatches_pending(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=1)
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalizers.append((args, queue)),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(running)
    db_session.refresh(planned[0])
    assert result["expired_units"] == 1
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert running.error == "sync unit lease expired"
    assert running.result is not None
    assert running.result["error_category"] == "worker_lost"
    assert running.lease_owner is None
    assert running.lease_expires_at is None
    assert planned[0].status == SyncRunUnitStatus.PLANNED.value
    assert dispatches == [((str(run.id),), "sync")]
    assert finalizers == []


def test_reconciler_finalizes_when_dead_running_makes_run_terminal(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=0)
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalizers.append((args, queue)),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(running)
    assert result["expired_units"] == 1
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert dispatches == []
    assert finalizers == [((str(run.id),), "sync")]


def test_reconciler_does_not_expire_live_lease(db_session, monkeypatch):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=0)
    running.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalizers.append((args, queue)),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(running)
    assert result["expired_units"] == 0
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert dispatches == []
    assert finalizers == []


def test_reconciler_retries_dispatchable_run_after_enqueue_failure(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=1)
    running.status = SyncRunUnitStatus.FAILED.value
    running.error = "sync unit lease expired"
    running.lease_owner = None
    running.lease_expires_at = None
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []

    def fail_dispatch(args=None, queue=None):
        raise RuntimeError("broker down")

    monkeypatch.setattr(sync_units.dispatch_sync_run, "apply_async", fail_dispatch)
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalizers.append((args, queue)),
    )

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(planned[0])
    assert first["expired_units"] == 0
    assert first["dispatches_enqueued"] == 0
    assert planned[0].status == SyncRunUnitStatus.PLANNED.value
    assert finalizers == []

    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )

    second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    assert second["dispatches_enqueued"] == 1
    assert dispatches == [((str(run.id),), "sync")]


def test_reconciler_retries_finalizable_run_after_enqueue_failure(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.FAILED.value
    running.error = "sync unit lease expired"
    running.lease_owner = None
    running.lease_expires_at = None
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )

    def fail_finalize(args=None, queue=None):
        raise RuntimeError("broker down")

    monkeypatch.setattr(sync_units.finalize_sync_run, "apply_async", fail_finalize)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    assert first["expired_units"] == 0
    assert first["finalizers_enqueued"] == 0
    assert dispatches == []

    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalizers.append((args, queue)),
    )

    second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    assert second["finalizers_enqueued"] == 1
    assert finalizers == [((str(run.id),), "sync")]


def test_reconciler_finalizable_scan_skips_older_nonfinalizable_runs(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    _, blocker_unit_one, _ = _seed_run(db_session, planned_units=0)
    _, blocker_unit_two, _ = _seed_run(db_session, planned_units=0)
    blocker_unit_one.lease_expires_at = datetime.now(timezone.utc) + timedelta(
        minutes=5
    )
    blocker_unit_two.lease_expires_at = datetime.now(timezone.utc) + timedelta(
        minutes=5
    )
    finalizable, final_unit, final_planned = _seed_run(db_session, planned_units=0)
    final_unit.status = SyncRunUnitStatus.FAILED.value
    final_unit.error = "sync unit lease expired"
    final_unit.lease_owner = None
    final_unit.lease_expires_at = None
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: finalizers.append((args, queue)),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=1)

    assert result["finalizers_enqueued"] == 1
    assert finalizers == [((str(finalizable.id),), "sync")]
    assert dispatches == []
