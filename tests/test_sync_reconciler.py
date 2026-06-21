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
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
    OUTBOX_KIND_POST_SYNC,
    OUTBOX_STATUS_DISPATCHED,
    OUTBOX_STATUS_PENDING,
    upsert_outbox_wakeup,
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


def _seed_zero_unit_run(session):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="demo-zero",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
    )
    session.add(run)
    session.flush()
    return run


def _outbox_row(session, run, kind):
    return (
        session.query(SyncDispatchOutbox).filter_by(sync_run_id=run.id, kind=kind).one()
    )


def _aware(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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
    dispatch_row = _outbox_row(db_session, run, OUTBOX_KIND_DISPATCH)
    assert result["expired_units"] == 1
    assert result["materialized_dispatch"] == 1
    assert result["relayed_dispatch"] == 1
    assert result["materialized_finalize"] == 0
    assert result["relayed_finalize"] == 0
    assert result["publish_failures"] == 0
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert running.error == "sync unit lease expired"
    assert running.result is not None
    assert running.result["error_category"] == "worker_lost"
    assert running.lease_owner is None
    assert running.lease_expires_at is None
    assert planned[0].status == SyncRunUnitStatus.PLANNED.value
    assert dispatches == [((str(run.id),), "sync")]
    assert finalizers == []
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert dispatch_row.claim_token is None


def test_reconciler_finalizes_when_dead_running_makes_run_terminal(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
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
    finalize_row = _outbox_row(db_session, run, OUTBOX_KIND_FINALIZE)
    assert result["expired_units"] == 1
    assert result["materialized_finalize"] == 1
    assert result["relayed_finalize"] == 1
    assert result["materialized_dispatch"] == 0
    assert result["relayed_dispatch"] == 0
    assert result["publish_failures"] == 0
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert dispatches == []
    assert finalizers == [((str(run.id),), "sync")]
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED
    assert finalize_row.claim_token is None


def test_reconciler_does_not_expire_live_lease(db_session, monkeypatch):
    from dev_health_ops.workers import sync_reconciler, sync_units

    _run, running, _planned = _seed_run(db_session, planned_units=0)
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
    assert result["materialized_dispatch"] == 0
    assert result["materialized_finalize"] == 0
    assert result["relayed_dispatch"] == 0
    assert result["relayed_finalize"] == 0
    assert running.status == SyncRunUnitStatus.RUNNING.value
    assert dispatches == []
    assert finalizers == []


def test_reconciler_rearms_dispatch_outbox_after_publish_failure(
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
    finalizers = []
    before = datetime.now(timezone.utc)

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
    dispatch_row = _outbox_row(db_session, run, OUTBOX_KIND_DISPATCH)
    assert first["expired_units"] == 0
    assert first["materialized_dispatch"] == 1
    assert first["relayed_dispatch"] == 0
    assert first["publish_failures"] == 1
    assert planned[0].status == SyncRunUnitStatus.PLANNED.value
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert finalizers == []
    assert dispatch_row.status == OUTBOX_STATUS_PENDING
    assert dispatch_row.claim_token is None
    assert dispatch_row.attempts == 1
    assert dispatch_row.last_error == "broker down"
    assert _aware(dispatch_row.available_at) > before


def test_reconciler_rearms_finalizer_outbox_after_publish_failure(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.FAILED.value
    running.error = "sync unit lease expired"
    running.lease_owner = None
    running.lease_expires_at = None
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    before = datetime.now(timezone.utc)
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )

    def fail_finalize(args=None, queue=None):
        raise RuntimeError("broker down")

    monkeypatch.setattr(sync_units.finalize_sync_run, "apply_async", fail_finalize)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    finalize_row = _outbox_row(db_session, run, OUTBOX_KIND_FINALIZE)
    assert first["expired_units"] == 0
    assert first["materialized_finalize"] == 1
    assert first["relayed_finalize"] == 0
    assert first["publish_failures"] == 1
    assert dispatches == []
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert finalize_row.status == OUTBOX_STATUS_PENDING
    assert finalize_row.claim_token is None
    assert finalize_row.attempts == 1
    assert finalize_row.last_error == "broker down"
    assert _aware(finalize_row.available_at) > before


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
    finalizable, final_unit, _final_planned = _seed_run(db_session, planned_units=0)
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

    finalize_row = _outbox_row(db_session, finalizable, OUTBOX_KIND_FINALIZE)
    assert result["materialized_finalize"] == 1
    assert result["relayed_finalize"] == 1
    assert finalizers == [((str(finalizable.id),), "sync")]
    assert dispatches == []
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED


def test_reconciler_materializes_and_relays_committed_planned_run(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=1)
    db_session.delete(running)
    run.total_units = 1
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

    db_session.refresh(planned[0])
    dispatch_row = _outbox_row(db_session, run, OUTBOX_KIND_DISPATCH)
    assert result["expired_units"] == 0
    assert result["materialized_dispatch"] == 1
    assert result["relayed_dispatch"] == 1
    assert result["publish_failures"] == 0
    assert planned[0].status == SyncRunUnitStatus.PLANNED.value
    assert dispatches == [((str(run.id),), "sync")]
    assert finalizers == []
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert dispatch_row.attempts == 1


def test_reconciler_zero_unit_run_relays_finalize_and_terminalizes(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run = _seed_zero_unit_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )

    def finalize_inline(args=None, queue=None):
        finalizers.append((args, queue))
        assert args is not None
        return sync_units.finalize_sync_run(args[0])

    monkeypatch.setattr(sync_units.finalize_sync_run, "apply_async", finalize_inline)

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(run)
    finalize_row = _outbox_row(db_session, run, OUTBOX_KIND_FINALIZE)
    post_sync_row = _outbox_row(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert result["expired_units"] == 0
    assert result["materialized_finalize"] == 1
    assert result["relayed_finalize"] == 1
    assert result["publish_failures"] == 0
    assert dispatches == []
    assert finalizers == [((str(run.id),), "sync")]
    assert run.status == SyncRunStatus.FAILED.value
    assert run.error == "No sync units planned"
    assert run.result == {
        "completed_units": 0,
        "failed_units": 0,
        "reason": "no_sync_units_planned",
    }
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.status == OUTBOX_STATUS_PENDING


def test_reconciler_relays_pending_post_sync_row_with_rebuilt_payload(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_runtime, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.SUCCESS.value
    running.lease_owner = None
    running.lease_expires_at = None
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_at = datetime.now(timezone.utc)
    running.since_at = datetime(2026, 6, 1, 10, 30, tzinfo=timezone.utc)
    running.before_at = datetime(2026, 6, 3, 22, 15, tzinfo=timezone.utc)
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_units = 1
    run.completed_at = datetime.now(timezone.utc)
    db_session.add(
        SyncRunPostDispatch(
            org_id=run.org_id,
            sync_run_id=run.id,
            kind=OUTBOX_KIND_POST_SYNC,
            dispatched_at=datetime.now(timezone.utc),
        )
    )
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_POST_SYNC,
        available_at=datetime.now(timezone.utc),
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    post_sync_dispatches = []
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
    monkeypatch.setattr(
        sync_runtime,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: post_sync_dispatches.append(kwargs),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    post_sync_row = _outbox_row(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert result["materialized_post_sync"] == 0
    assert result["relayed_post_sync"] == 1
    assert result["publish_failures"] == 0
    assert dispatches == []
    assert finalizers == []
    assert len(post_sync_dispatches) == 1
    assert post_sync_dispatches[0] == {
        "provider": "github",
        "sync_targets": ["git"],
        "org_id": run.org_id,
        "from_date": "2026-06-01",
        "to_date": "2026-06-03",
        "work_graph_from_date": "2026-06-01T00:00:00+00:00",
        "work_graph_to_date": "2026-06-04T00:00:00+00:00",
    }
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.claim_token is None


def test_reconciler_materializes_missing_post_sync_outbox_for_ledger(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_runtime, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.SUCCESS.value
    running.lease_owner = None
    running.lease_expires_at = None
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_units = 1
    run.completed_at = datetime.now(timezone.utc)
    db_session.add(
        SyncRunPostDispatch(
            org_id=run.org_id,
            sync_run_id=run.id,
            kind=OUTBOX_KIND_POST_SYNC,
            dispatched_at=datetime.now(timezone.utc),
        )
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    post_sync_dispatches = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )
    monkeypatch.setattr(
        sync_runtime,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: post_sync_dispatches.append(kwargs),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    post_sync_row = _outbox_row(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert result["materialized_post_sync"] == 1
    assert result["relayed_post_sync"] == 1
    assert len(post_sync_dispatches) == 1
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED


def test_reconciler_precondition_noop_marks_outbox_dispatched(db_session, monkeypatch):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.SUCCESS.value
    running.lease_owner = None
    running.lease_expires_at = None
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_at = datetime.now(timezone.utc)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=datetime.now(timezone.utc),
    )
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

    dispatch_row = _outbox_row(db_session, run, OUTBOX_KIND_DISPATCH)
    assert result["relayed_dispatch"] == 0
    assert result["publish_failures"] == 0
    assert dispatches == []
    assert finalizers == []
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert dispatch_row.claim_token is None


def test_reconciler_finalize_precondition_noop_marks_outbox_dispatched(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_FINALIZE,
        available_at=datetime.now(timezone.utc),
    )
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

    finalize_row = _outbox_row(db_session, run, OUTBOX_KIND_FINALIZE)
    assert result["relayed_finalize"] == 0
    assert result["publish_failures"] == 0
    assert dispatches == []
    assert finalizers == []
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED
    assert finalize_row.claim_token is None


def test_reconciler_post_sync_precondition_noop_marks_outbox_dispatched(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_runtime, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.SUCCESS.value
    running.lease_owner = None
    running.lease_expires_at = None
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_at = datetime.now(timezone.utc)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_POST_SYNC,
        available_at=datetime.now(timezone.utc),
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    post_sync_dispatches = []
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
    monkeypatch.setattr(
        sync_runtime,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: post_sync_dispatches.append(kwargs),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    post_sync_row = _outbox_row(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert result["relayed_post_sync"] == 0
    assert result["publish_failures"] == 0
    assert dispatches == []
    assert finalizers == []
    assert post_sync_dispatches == []
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.claim_token is None


def test_reconciler_unknown_outbox_kind_rearms_without_dispatching(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_runtime, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.SUCCESS.value
    running.lease_owner = None
    running.lease_expires_at = None
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_at = datetime.now(timezone.utc)
    unsupported_kind = "unsupported_kind"
    before = datetime.now(timezone.utc)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=unsupported_kind,
        available_at=before,
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    finalizers = []
    post_sync_dispatches = []
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
    monkeypatch.setattr(
        sync_runtime,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: post_sync_dispatches.append(kwargs),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    unsupported_row = _outbox_row(db_session, run, unsupported_kind)
    assert result["publish_failures"] == 1
    assert result["relayed_dispatch"] == 0
    assert result["relayed_finalize"] == 0
    assert result["relayed_post_sync"] == 0
    assert dispatches == []
    assert finalizers == []
    assert post_sync_dispatches == []
    assert unsupported_row.status == OUTBOX_STATUS_PENDING
    assert unsupported_row.claim_token is None
    assert unsupported_row.attempts == 1
    assert "unsupported sync dispatch outbox kind" in str(unsupported_row.last_error)
    assert _aware(unsupported_row.available_at) > before


def test_reconciler_two_passes_do_not_double_publish_post_sync(db_session, monkeypatch):
    from dev_health_ops.workers import sync_reconciler, sync_runtime, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.status = SyncRunUnitStatus.SUCCESS.value
    running.lease_owner = None
    running.lease_expires_at = None
    run.status = SyncRunStatus.SUCCESS.value
    run.completed_units = 1
    run.completed_at = datetime.now(timezone.utc)
    db_session.add(
        SyncRunPostDispatch(
            org_id=run.org_id,
            sync_run_id=run.id,
            kind=OUTBOX_KIND_POST_SYNC,
            dispatched_at=datetime.now(timezone.utc),
        )
    )
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_POST_SYNC,
        available_at=datetime.now(timezone.utc),
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    post_sync_dispatches = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )
    monkeypatch.setattr(
        sync_runtime,
        "_dispatch_post_sync_tasks",
        lambda **kwargs: post_sync_dispatches.append(kwargs),
    )

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)
    second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    post_sync_row = _outbox_row(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert first["relayed_post_sync"] == 1
    assert second["relayed_post_sync"] == 0
    assert len(post_sync_dispatches) == 1
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
