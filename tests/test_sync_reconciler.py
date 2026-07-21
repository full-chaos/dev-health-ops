from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    BackfillJob,
    Base,
    Integration,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunPostDispatch,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
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


def _seed_run(
    session,
    *,
    planned_units=0,
    provider="github",
    mode=SyncRunMode.INCREMENTAL.value,
    dataset_key="commits",
    source_type="repo",
    external_id="full-chaos/dev-health",
    name="dev-health",
    full_name="full-chaos/dev-health",
):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider=provider,
        name="demo",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider=provider,
        source_type=source_type,
        external_id=external_id,
        name=name,
        full_name=full_name,
        metadata_={},
        is_enabled=True,
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=planned_units + 1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, run])
    session.flush()
    session.add(
        SyncRunReferenceDiscovery(
            org_id=org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    session.flush()
    now = datetime.now(timezone.utc)
    running = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode=mode,
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
            provider=provider,
            dataset_key=f"prs-{index}",
            cost_class="medium",
            mode=mode,
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


def test_reconciler_retries_eligible_expired_linear_backfill_work_item_unit_once(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(
        db_session,
        provider="linear",
        mode=SyncRunMode.BACKFILL.value,
        dataset_key="work-items",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_RETRY_BACKOFF_SECONDS", "60")
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "2")
    monkeypatch.setattr(
        sync_units,
        "_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES",
        sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES,
    )
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

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)
    second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(running)
    dispatch_row = _outbox_row(db_session, run, OUTBOX_KIND_DISPATCH)
    assert first["expired_units"] == 1
    assert first["expired_retry_units"] == 1
    assert first["expired_retry_exhausted_units"] == 0
    assert first["relayed_dispatch"] == 0
    assert second["expired_units"] == 0
    assert running.status == SyncRunUnitStatus.RETRYING.value
    assert running.expired_lease_retry_count == 1
    assert running.last_retry_reason == "expired_lease"
    assert running.result is not None
    assert running.result["error_category"] == "worker_lost"
    assert running.result["retry_count"] == 1
    assert running.result["retry_reason"] == "expired_lease"
    assert running.result["retry_exhausted"] is False
    assert running.result["last_lease_expired_at"] is not None
    assert running.result["next_retry_at"] is not None
    assert running.lease_owner is None
    assert running.lease_expires_at is None
    assert dispatches == []
    assert finalizers == []
    assert dispatch_row.status == OUTBOX_STATUS_PENDING
    assert _aware(dispatch_row.available_at) > datetime.now(timezone.utc)


def test_reconciler_does_not_retry_linear_backfill_when_surface_is_unproven(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(
        db_session,
        provider="linear",
        mode=SyncRunMode.BACKFILL.value,
        dataset_key="work-items",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
    )
    proven_safe = set(sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES)
    proven_safe.remove("investment_classifications_daily")
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "2")
    monkeypatch.setattr(
        sync_units,
        "_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES",
        frozenset(proven_safe),
    )
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
    assert result["expired_retry_units"] == 0
    assert result["expired_retry_exhausted_units"] == 0
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert running.expired_lease_retry_count == 0
    assert running.retry_exhausted_at is None
    assert running.result is not None
    assert running.result["error_category"] == "worker_lost"
    assert running.result["retry_exhausted"] is False
    assert "investment_classifications_daily" in running.result["retry_surfaces"]
    assert dispatches == []
    assert finalizers == [((str(run.id),), "sync")]


def test_reconciler_exhausted_linear_backfill_retry_fails_worker_lost_exhausted(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(
        db_session,
        provider="linear",
        mode=SyncRunMode.BACKFILL.value,
        dataset_key="work-items",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
    )
    running.expired_lease_retry_count = 1
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_UNIT_EXPIRED_LEASE_MAX_RETRIES", "1")
    monkeypatch.setattr(
        sync_units,
        "_CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES",
        sync_units._LINEAR_BACKFILL_WORK_ITEM_IN_BAND_WRITE_SURFACES,
    )
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
    assert result["expired_retry_units"] == 0
    assert result["expired_retry_exhausted_units"] == 1
    assert result["relayed_finalize"] == 1
    assert running.status == SyncRunUnitStatus.FAILED.value
    assert running.expired_lease_retry_count == 1
    assert running.retry_exhausted_at is not None
    assert running.result is not None
    assert running.result["error_category"] == "worker_lost_retry_exhausted"
    assert running.result["retry_count"] == 1
    assert running.result["retry_exhausted"] is True
    assert dispatches == []
    assert finalizers == [((str(run.id),), "sync")]
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED


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
    assert dispatch_row.last_error == "RuntimeError: broker down"
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
    assert finalize_row.last_error == "RuntimeError: broker down"
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


def test_reconciler_rearms_expired_reference_discovery_lease(db_session, monkeypatch):
    from dev_health_ops.workers import reference_discovery, sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    db_session.delete(running)
    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    ledger.status = "running"
    ledger.lease_owner = "dead-discovery-worker"
    ledger.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    ledger.available_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    discovery_dispatches = []
    monkeypatch.setattr(
        reference_discovery.run_sync_reference_discovery,
        "apply_async",
        lambda args=None, queue=None: discovery_dispatches.append((args, queue)),
    )
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

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    discovery_row = _outbox_row(db_session, run, OUTBOX_KIND_DISCOVERY)
    assert result["materialized_discovery"] == 1
    assert discovery_dispatches == [((str(run.id),), "sync")]
    assert discovery_row.status == OUTBOX_STATUS_DISPATCHED


def test_reconciler_discovery_publish_failure_is_opaque(
    db_session,
    monkeypatch,
    caplog,
):
    from dev_health_ops.workers import reference_discovery, sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    db_session.delete(running)
    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    ledger.status = "planned"
    ledger.completed_at = None
    ledger.available_at = datetime.now(timezone.utc)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISCOVERY,
        available_at=datetime.now(timezone.utc),
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    secret = "ghp_" + "FAKE1234567890abcdefghijklmnopqrst"
    malicious_fragments = (
        "MaliciousDiscoveryPublishError",
        "internal-broker.example",
        "/srv/private/broker.json",
        "https://admin:password@internal.example/broker",
        secret,
        'payload={"access_token":"private"}',
    )

    class MaliciousDiscoveryPublishError(RuntimeError):
        pass

    monkeypatch.setattr(
        reference_discovery.run_sync_reference_discovery,
        "apply_async",
        lambda **_kwargs: (_ for _ in ()).throw(
            MaliciousDiscoveryPublishError(" | ".join(malicious_fragments[1:]))
        ),
    )
    monkeypatch.setattr(sync_units.dispatch_sync_run, "apply_async", lambda **_: None)
    monkeypatch.setattr(sync_units.finalize_sync_run, "apply_async", lambda **_: None)

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    discovery_row = _outbox_row(db_session, run, OUTBOX_KIND_DISCOVERY)
    assert result["publish_failures"] == 1
    assert discovery_row.status == OUTBOX_STATUS_PENDING
    assert discovery_row.last_error == "Reference discovery failed"
    captured = f"{caplog.text} " + " ".join(
        str(record.__dict__) for record in caplog.records
    )
    for fragment in malicious_fragments:
        assert fragment not in captured


def test_reconciler_rearms_discovery_when_dispatch_blocked_on_ledger(
    db_session, monkeypatch
):
    from dev_health_ops.workers import reference_discovery, sync_reconciler, sync_units

    run, running, planned = _seed_run(db_session, planned_units=1)
    db_session.delete(running)
    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    ledger.status = "planned"
    ledger.completed_at = None
    ledger.available_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=datetime.now(timezone.utc),
    )
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    dispatches = []
    discovery_dispatches = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatches.append((args, queue)),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )
    monkeypatch.setattr(
        reference_discovery.run_sync_reference_discovery,
        "apply_async",
        lambda args=None, queue=None: discovery_dispatches.append((args, queue)),
    )

    result = sync_reconciler.reconcile_sync_dispatch(limit=1)

    dispatch_row = _outbox_row(db_session, run, OUTBOX_KIND_DISPATCH)
    discovery_row = _outbox_row(db_session, run, OUTBOX_KIND_DISCOVERY)
    assert result["relayed_dispatch"] == 0
    assert dispatches == []
    assert discovery_dispatches == []
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert discovery_row.status == OUTBOX_STATUS_PENDING
    assert planned[0].status == SyncRunUnitStatus.PLANNED.value


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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

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
        post_sync_dispatch,
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
        "auto_import_teams": False,
        "sync_run_id": str(run.id),
    }
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.claim_token is None


def test_reconciler_materializes_missing_post_sync_outbox_for_ledger(
    db_session, monkeypatch
):
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

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
        post_sync_dispatch,
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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

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
        post_sync_dispatch,
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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

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
        post_sync_dispatch,
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
    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

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
        post_sync_dispatch,
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


def _seed_orphan_backfill_job(
    session,
    *,
    org_id=None,
    celery_task_id=None,
    status="pending",
    created_at=None,
):
    job = BackfillJob(
        org_id=org_id or str(uuid.uuid4()),
        sync_config_id=uuid.uuid4(),
        celery_task_id=celery_task_id,
        status=status,
        since_date=date(2026, 1, 1),
        before_date=date(2026, 1, 8),
        total_chunks=1,
    )
    if created_at is not None:
        job.created_at = created_at
    session.add(job)
    session.flush()
    return job


def test_reconciler_terminalizes_orphaned_backfill_job_without_marker_past_ttl(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    job = _seed_orphan_backfill_job(
        db_session,
        celery_task_id=None,
        status="pending",
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(job)
    assert result["orphaned_backfill_jobs"] == 1
    assert job.status == "failed"
    assert job.error_message == "backfill job orphaned: no linked sync run"
    assert job.completed_at is not None


def test_reconciler_terminalizes_orphaned_backfill_job_with_marker_to_deleted_run(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    deleted_run_id = uuid.uuid4()
    job = _seed_orphan_backfill_job(
        db_session,
        celery_task_id=f"worker|sync_run:{deleted_run_id}",
        status="running",
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(job)
    assert result["orphaned_backfill_jobs"] == 1
    assert job.status == "failed"
    assert job.error_message == "backfill job orphaned: no linked sync run"
    assert job.completed_at is not None


def test_reconciler_does_not_terminalize_young_orphaned_backfill_job(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    job = _seed_orphan_backfill_job(
        db_session,
        celery_task_id=None,
        status="pending",
        created_at=datetime.now(timezone.utc) - timedelta(minutes=5),
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(job)
    assert result["orphaned_backfill_jobs"] == 0
    assert job.status == "pending"
    assert job.error_message is None
    assert job.completed_at is None


def test_reconciler_does_not_terminalize_backfill_job_with_live_nonterminal_run(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    db_session.flush()
    job = _seed_orphan_backfill_job(
        db_session,
        org_id=run.org_id,
        celery_task_id=f"sync_run:{run.id}",
        status="pending",
        created_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")
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

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(run)
    db_session.refresh(job)
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert result["orphaned_backfill_jobs"] == 0
    assert job.status == "pending"
    assert job.error_message is None
    assert job.completed_at is None


def test_reconciler_still_repairs_pending_backfill_job_with_terminal_run_via_observer(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    run.status = SyncRunStatus.FAILED.value
    run.completed_at = None
    run.error = "provider auth failed"
    run.failed_units = 1
    job = _seed_orphan_backfill_job(
        db_session,
        org_id=run.org_id,
        celery_task_id=f"sync_run:{run.id}",
        status="pending",
    )
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")
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

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(job)
    assert result["observer_repairs"] == 1
    assert result["orphaned_backfill_jobs"] == 0
    assert job.status == "failed"
    assert job.error_message == "provider auth failed"
    assert job.completed_at is not None


def test_backfill_job_marker_sync_run_id_parses_and_rejects_marker_variants():
    from dev_health_ops.workers import sync_reconciler

    resolvable = BackfillJob(celery_task_id=f"worker|sync_run:{uuid.uuid4()}")
    missing_marker = BackfillJob(celery_task_id="worker-task-id")
    no_task_id = BackfillJob(celery_task_id=None)
    unparseable = BackfillJob(celery_task_id="worker|sync_run:not-a-uuid")

    assert sync_reconciler._backfill_job_marker_sync_run_id(resolvable) is not None
    assert sync_reconciler._backfill_job_marker_sync_run_id(missing_marker) is None
    assert sync_reconciler._backfill_job_marker_sync_run_id(no_task_id) is None
    assert sync_reconciler._backfill_job_marker_sync_run_id(unparseable) is None


def test_backfill_job_is_orphaned_predicate_against_existing_run_id_set():
    from dev_health_ops.workers import sync_reconciler

    live_id = uuid.uuid4()
    existing_run_ids = {live_id}
    missing_id = uuid.uuid4()

    assert sync_reconciler._backfill_job_is_orphaned(None, existing_run_ids) is True
    assert (
        sync_reconciler._backfill_job_is_orphaned(missing_id, existing_run_ids) is True
    )
    assert sync_reconciler._backfill_job_is_orphaned(live_id, existing_run_ids) is False


def test_terminalize_orphaned_backfill_jobs_boundary_created_at_equals_cutoff(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")
    fixed_now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    cutoff = fixed_now - timedelta(seconds=3600)
    job = _seed_orphan_backfill_job(
        db_session,
        celery_task_id=None,
        status="pending",
        created_at=cutoff,
    )

    terminalized = sync_reconciler._terminalize_orphaned_backfill_jobs(
        db_session, fixed_now, limit=10
    )
    db_session.flush()

    db_session.refresh(job)
    assert terminalized == 1
    assert job.status == "failed"
    assert job.error_message == "backfill job orphaned: no linked sync run"


def test_terminalize_orphaned_backfill_jobs_just_inside_ttl_is_untouched(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")
    fixed_now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    cutoff = fixed_now - timedelta(seconds=3600)
    job = _seed_orphan_backfill_job(
        db_session,
        celery_task_id=None,
        status="pending",
        created_at=cutoff + timedelta(seconds=1),
    )

    terminalized = sync_reconciler._terminalize_orphaned_backfill_jobs(
        db_session, fixed_now, limit=10
    )
    db_session.flush()

    db_session.refresh(job)
    assert terminalized == 0
    assert job.status == "pending"
    assert job.error_message is None


def test_terminalize_orphaned_backfill_jobs_unparseable_marker_is_orphaned(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")
    fixed_now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    job = _seed_orphan_backfill_job(
        db_session,
        celery_task_id="worker|sync_run:not-a-uuid",
        status="running",
        created_at=fixed_now - timedelta(hours=2),
    )

    terminalized = sync_reconciler._terminalize_orphaned_backfill_jobs(
        db_session, fixed_now, limit=10
    )
    db_session.flush()

    db_session.refresh(job)
    assert terminalized == 1
    assert job.status == "failed"
    assert job.error_message == "backfill job orphaned: no linked sync run"


def test_terminalize_orphaned_backfill_jobs_does_not_starve_orphan_behind_old_scan_window(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_reconciler

    monkeypatch.setenv("SYNC_BACKFILL_JOB_ORPHAN_TTL_SECONDS", "3600")
    fixed_now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    run, running, _planned = _seed_run(db_session, planned_units=0)
    running.lease_expires_at = fixed_now + timedelta(minutes=5)
    db_session.flush()

    repair_limit = 1
    old_scan_window = (
        repair_limit * sync_reconciler._BACKFILL_JOB_ORPHAN_SCAN_LIMIT_MULTIPLIER
    )
    head_non_orphans = [
        _seed_orphan_backfill_job(
            db_session,
            org_id=run.org_id,
            celery_task_id=f"sync_run:{run.id}",
            status="pending",
            created_at=fixed_now - timedelta(hours=3) + timedelta(seconds=index),
        )
        for index in range(old_scan_window + 1)
    ]

    orphan = _seed_orphan_backfill_job(
        db_session,
        celery_task_id=None,
        status="pending",
        created_at=fixed_now
        - timedelta(hours=3)
        + timedelta(seconds=old_scan_window + 1),
    )

    terminalized = sync_reconciler._terminalize_orphaned_backfill_jobs(
        db_session, fixed_now, limit=repair_limit
    )
    db_session.flush()

    for non_orphan in head_non_orphans:
        db_session.refresh(non_orphan)
    db_session.refresh(orphan)
    assert terminalized == 1
    assert len(head_non_orphans) > old_scan_window
    assert all(non_orphan.status == "pending" for non_orphan in head_non_orphans)
    assert all(non_orphan.error_message is None for non_orphan in head_non_orphans)
    assert orphan.status == "failed"
    assert orphan.error_message == "backfill job orphaned: no linked sync run"
