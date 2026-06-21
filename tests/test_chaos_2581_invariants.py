from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
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
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session: Session) -> Iterator[Session]:
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch: pytest.MonkeyPatch, session: Session) -> None:
    import dev_health_ops.db as db

    session.commit()
    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _seed_integration(
    session: Session,
    *,
    org_id: str | None = None,
    provider: str = "github",
    dataset_key: str = "commits",
    initial_sync_depth: int | None = None,
) -> tuple[Integration, IntegrationSource, IntegrationDataset]:
    config: dict[str, Any] = {}
    if initial_sync_depth is not None:
        config["initial_sync_depth"] = initial_sync_depth
    integration = Integration(
        org_id=org_id or str(uuid.uuid4()),
        provider=provider,
        name="demo",
        config=config,
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=integration.org_id,
        integration_id=integration.id,
        provider=provider,
        source_type="repo",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=integration.org_id,
        integration_id=integration.id,
        dataset_key=dataset_key,
        is_enabled=True,
        options={},
    )
    session.add_all([source, dataset])
    session.flush()
    return integration, source, dataset


def _seed_run(
    session: Session,
    *,
    unit_count: int = 1,
    status: str = SyncRunStatus.PLANNED.value,
    mode: str = SyncRunMode.INCREMENTAL.value,
    provider: str = "github",
    dataset_key: str = "commits",
) -> tuple[SyncRun, list[SyncRunUnit]]:
    integration, source, _dataset = _seed_integration(
        session, provider=provider, dataset_key=dataset_key
    )
    run = SyncRun(
        org_id=integration.org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=status,
        total_units=unit_count,
        completed_units=0,
        failed_units=0,
    )
    session.add(run)
    session.flush()
    units: list[SyncRunUnit] = []
    for index in range(unit_count):
        unit = SyncRunUnit(
            org_id=integration.org_id,
            sync_run_id=run.id,
            integration_id=integration.id,
            source_id=source.id,
            provider=provider,
            dataset_key=dataset_key,
            cost_class="medium",
            mode=mode,
            since_at=datetime(2026, 6, 1 + index, tzinfo=timezone.utc),
            before_at=datetime(2026, 6, 2 + index, tzinfo=timezone.utc),
            status=SyncRunUnitStatus.PLANNED.value,
            attempts=0,
            processor_flags={"sync_git": True},
        )
        session.add(unit)
        units.append(unit)
    session.flush()
    return run, units


def _outbox(session: Session, run: SyncRun, kind: str) -> SyncDispatchOutbox:
    return (
        session.query(SyncDispatchOutbox).filter_by(sync_run_id=run.id, kind=kind).one()
    )


def _patch_reconciler_enqueues(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dispatch_side_effect: Callable[..., Any] | None = None,
    finalize_side_effect: Callable[..., Any] | None = None,
    post_sync_side_effect: Callable[..., Any] | None = None,
) -> tuple[list[tuple[Any, Any]], list[tuple[Any, Any]], list[dict[str, Any]]]:
    from dev_health_ops.workers import sync_runtime, sync_units

    dispatches: list[tuple[Any, Any]] = []
    finalizers: list[tuple[Any, Any]] = []
    post_sync: list[dict[str, Any]] = []

    def dispatch_apply(args=None, queue=None):
        dispatches.append((args, queue))
        if dispatch_side_effect is not None:
            return dispatch_side_effect(args=args, queue=queue)
        return None

    def finalize_apply(args=None, queue=None):
        finalizers.append((args, queue))
        if finalize_side_effect is not None:
            return finalize_side_effect(args=args, queue=queue)
        return None

    def dispatch_post_sync(**kwargs):
        post_sync.append(kwargs)
        if post_sync_side_effect is not None:
            return post_sync_side_effect(**kwargs)
        return None

    monkeypatch.setattr(sync_units.dispatch_sync_run, "apply_async", dispatch_apply)
    monkeypatch.setattr(sync_units.finalize_sync_run, "apply_async", finalize_apply)
    monkeypatch.setattr(
        sync_runtime,
        "_dispatch_post_sync_tasks",
        dispatch_post_sync,
    )
    return dispatches, finalizers, post_sync


def _patch_chord_apply_async(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from dev_health_ops.workers import sync_units

    chord_calls: list[str] = []

    class FakeChord:
        def apply_async(self) -> None:
            chord_calls.append("apply_async")

    monkeypatch.setattr(sync_units, "chord", lambda *_args, **_kwargs: FakeChord())
    return chord_calls


def _mark_units_success(session: Session, run: SyncRun) -> None:
    for unit in session.query(SyncRunUnit).filter_by(sync_run_id=run.id).all():
        unit.status = SyncRunUnitStatus.SUCCESS.value
        unit.lease_owner = None
        unit.lease_expires_at = None
        unit.error = None
        unit.result = {"ok": True}
    session.flush()


def _refresh_all(session: Session, *objects: object) -> None:
    for obj in objects:
        session.refresh(obj)


def test_b1_a5_initial_dispatch_publish_loss_reconciler_recovers_and_run_terminalizes(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, units = _seed_run(db_session, unit_count=2, status=SyncRunStatus.PLANNED.value)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=datetime.now(timezone.utc),
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, finalizers, post_sync = _patch_reconciler_enqueues(monkeypatch)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    _refresh_all(db_session, run, *units)
    dispatch_row = _outbox(db_session, run, OUTBOX_KIND_DISPATCH)
    assert first["relayed_dispatch"] == 1
    assert dispatches == [((str(run.id),), "sync")]
    assert run.status != SyncRunStatus.FAILED.value
    assert [unit.status for unit in units] == [SyncRunUnitStatus.PLANNED.value] * 2
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert dispatch_row.attempts == 1
    assert dispatch_row.claim_token is None

    _mark_units_success(db_session, run)
    finalize_result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    post_sync_row = _outbox(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert finalize_result["status"] == "finalized"
    assert run.status == SyncRunStatus.SUCCESS.value
    assert run.completed_units == 2
    assert run.failed_units == 0
    assert post_sync_row.status == OUTBOX_STATUS_PENDING
    assert finalizers == []
    assert post_sync == []


def test_b2_redispatch_loss_after_cap_defer_reconciler_drains_overflow(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units = _seed_run(db_session, unit_count=3, status=SyncRunStatus.PLANNED.value)
    _patch_db_session(monkeypatch, db_session)
    _patch_reconciler_enqueues(monkeypatch)
    chord_calls: list[str] = []

    class FakeChord:
        def apply_async(self) -> None:
            chord_calls.append("apply_async")

    monkeypatch.setattr(sync_units, "chord", lambda *_args, **_kwargs: FakeChord())

    dispatch_result = sync_units.dispatch_sync_run(str(run.id))

    _refresh_all(db_session, run, *units)
    planned_after_cap = [
        u for u in units if u.status == SyncRunUnitStatus.PLANNED.value
    ]
    dispatch_row = _outbox(db_session, run, OUTBOX_KIND_DISPATCH)
    assert dispatch_result == {"status": "dispatched", "queued_units": 1}
    assert len(chord_calls) == 1
    assert len(planned_after_cap) == 2
    assert run.status == SyncRunStatus.DISPATCHING.value
    assert dispatch_row.status == OUTBOX_STATUS_PENDING
    assert _aware(dispatch_row.available_at) > datetime.now(timezone.utc)

    for unit in units:
        if unit.status == SyncRunUnitStatus.DISPATCHING.value:
            unit.status = SyncRunUnitStatus.SUCCESS.value
            unit.lease_owner = None
            unit.lease_expires_at = None
    dispatch_row.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.commit()
    relayed: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: relayed.append((args, queue)),
    )

    relay = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(dispatch_row)
    assert relay["relayed_dispatch"] == 1
    assert relayed == [((str(run.id),), "sync")]
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert (
        db_session.query(SyncRunUnit)
        .filter_by(sync_run_id=run.id, status=SyncRunUnitStatus.PLANNED.value)
        .count()
        == 2
    )
    assert run.status != SyncRunStatus.FAILED.value

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
    assert sync_units.dispatch_sync_run(str(run.id))["status"] == "dispatched"
    _refresh_all(db_session, *units)
    assert [unit.status for unit in units].count(
        SyncRunUnitStatus.DISPATCHING.value
    ) == 2
    _mark_units_success(db_session, run)
    assert sync_units.finalize_sync_run(str(run.id))["status"] == "finalized"
    db_session.refresh(run)
    assert run.status == SyncRunStatus.SUCCESS.value


def test_b3_stale_dispatching_recovery_respects_threshold_and_fresh_rows(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler

    monkeypatch.setenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "60")
    stale_run, stale_units = _seed_run(
        db_session, unit_count=1, status=SyncRunStatus.DISPATCHING.value
    )
    fresh_run, fresh_units = _seed_run(
        db_session, unit_count=1, status=SyncRunStatus.DISPATCHING.value
    )
    boundary_run, boundary_units = _seed_run(
        db_session, unit_count=1, status=SyncRunStatus.DISPATCHING.value
    )
    now = datetime.now(timezone.utc)
    stale_units[0].status = SyncRunUnitStatus.DISPATCHING.value
    stale_units[0].updated_at = now - timedelta(seconds=61)
    fresh_units[0].status = SyncRunUnitStatus.DISPATCHING.value
    fresh_units[0].updated_at = now - timedelta(seconds=10)
    boundary_units[0].status = SyncRunUnitStatus.DISPATCHING.value
    boundary_units[0].updated_at = now - timedelta(seconds=60)
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, finalizers, _post_sync = _patch_reconciler_enqueues(monkeypatch)

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    stale_row = _outbox(db_session, stale_run, OUTBOX_KIND_DISPATCH)
    boundary_row = _outbox(db_session, boundary_run, OUTBOX_KIND_DISPATCH)
    assert result["materialized_dispatch"] == 2
    assert result["relayed_dispatch"] == 2
    assert {call[0][0] for call in dispatches} == {
        str(stale_run.id),
        str(boundary_run.id),
    }
    assert finalizers == []
    assert stale_row.status == OUTBOX_STATUS_DISPATCHED
    assert boundary_row.status == OUTBOX_STATUS_DISPATCHED
    assert (
        db_session.query(SyncDispatchOutbox).filter_by(sync_run_id=fresh_run.id).count()
        == 0
    )
    assert fresh_units[0].status == SyncRunUnitStatus.DISPATCHING.value


def test_b4_post_sync_continuation_loss_relays_finalize_then_post_sync_once(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, units = _seed_run(
        db_session, unit_count=1, status=SyncRunStatus.DISPATCHING.value
    )
    _mark_units_success(db_session, run)
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, finalizers, post_sync = _patch_reconciler_enqueues(monkeypatch)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    finalize_row = _outbox(db_session, run, OUTBOX_KIND_FINALIZE)
    assert first["relayed_finalize"] == 1
    assert finalizers == [((str(run.id),), "sync")]
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED
    assert (
        db_session.query(SyncRunPostDispatch).filter_by(sync_run_id=run.id).count() == 0
    )
    assert dispatches == []

    assert sync_units.finalize_sync_run(str(run.id))["status"] == "finalized"
    db_session.refresh(run)
    assert run.status == SyncRunStatus.SUCCESS.value
    second = sync_reconciler.reconcile_sync_dispatch(limit=10)
    third = sync_reconciler.reconcile_sync_dispatch(limit=10)

    post_sync_row = _outbox(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert second["relayed_post_sync"] == 1
    assert third["relayed_post_sync"] == 0
    assert len(post_sync) == 1
    assert (
        db_session.query(SyncRunPostDispatch).filter_by(sync_run_id=run.id).count() == 1
    )
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.attempts == 1
    assert units[0].status == SyncRunUnitStatus.SUCCESS.value


def test_b4_post_sync_partial_fanout_failure_is_at_most_once_and_lossy(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Post-sync is at-most-once; publish failure is loss, not re-send."""
    from dev_health_ops.workers import sync_reconciler

    run, units = _seed_run(db_session, unit_count=1, status=SyncRunStatus.SUCCESS.value)
    units[0].status = SyncRunUnitStatus.SUCCESS.value
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
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    fanout_attempts = 0

    def partial_failure(**_kwargs):
        nonlocal fanout_attempts
        fanout_attempts += 1
        if fanout_attempts == 1:
            raise RuntimeError("partial fanout send failure")
        return None

    _dispatches, _finalizers, post_sync = _patch_reconciler_enqueues(
        monkeypatch, post_sync_side_effect=partial_failure
    )
    before = datetime.now(timezone.utc)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(run)
    post_sync_row = _outbox(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert first["relayed_post_sync"] == 0
    assert first["publish_failures"] == 0
    assert len(post_sync) == 1
    assert fanout_attempts == 1
    assert run.status == SyncRunStatus.SUCCESS.value
    assert run.completed_units == 1
    assert units[0].status == SyncRunUnitStatus.SUCCESS.value
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.attempts == 1
    assert post_sync_row.claim_token is None
    assert post_sync_row.dispatched_at is not None
    assert post_sync_row.last_error is None
    assert _aware(post_sync_row.available_at) <= before

    second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(post_sync_row)
    assert second["relayed_post_sync"] == 0
    assert second["publish_failures"] == 0
    assert len(post_sync) == 1
    assert fanout_attempts == 1
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.attempts == 1
    assert post_sync_row.claim_token is None
    assert post_sync_row.last_error is None


def test_b5_broker_failure_during_reconciler_enqueue_rearms_without_losing_work(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, units = _seed_run(db_session, unit_count=1, status=SyncRunStatus.PLANNED.value)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=datetime.now(timezone.utc),
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    before = datetime.now(timezone.utc)

    def fail_dispatch(args=None, queue=None):
        raise RuntimeError("broker down")

    _patch_reconciler_enqueues(monkeypatch, dispatch_side_effect=fail_dispatch)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(units[0])
    dispatch_row = _outbox(db_session, run, OUTBOX_KIND_DISPATCH)
    assert first["publish_failures"] == 1
    assert first["relayed_dispatch"] == 0
    assert units[0].status == SyncRunUnitStatus.PLANNED.value
    assert run.status == SyncRunStatus.PLANNED.value
    assert dispatch_row.status == OUTBOX_STATUS_PENDING
    assert dispatch_row.attempts == 1
    assert dispatch_row.claim_token is None
    assert dispatch_row.last_error == "broker down"
    assert _aware(dispatch_row.available_at) > before
    assert "reconcile_sync_dispatch.outbox_publish_failed" in caplog.text

    dispatch_row.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.commit()
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: calls.append((args, queue)),
    )

    second = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(dispatch_row)
    assert second["relayed_dispatch"] == 1
    assert second["publish_failures"] == 0
    assert calls == [((str(run.id),), "sync")]
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert dispatch_row.attempts == 2


def test_b6_idempotency_two_reconciler_passes_do_not_double_claim_or_post_sync(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    dispatch_run, dispatch_units = _seed_run(
        db_session, unit_count=1, status=SyncRunStatus.PLANNED.value
    )
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=dispatch_run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=datetime.now(timezone.utc),
    )
    post_run, post_units = _seed_run(
        db_session, unit_count=1, status=SyncRunStatus.SUCCESS.value
    )
    post_units[0].status = SyncRunUnitStatus.SUCCESS.value
    post_run.completed_units = 1
    post_run.completed_at = datetime.now(timezone.utc)
    db_session.add(
        SyncRunPostDispatch(
            org_id=post_run.org_id,
            sync_run_id=post_run.id,
            kind=OUTBOX_KIND_POST_SYNC,
            dispatched_at=datetime.now(timezone.utc),
        )
    )
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=post_run.id,
        kind=OUTBOX_KIND_POST_SYNC,
        available_at=datetime.now(timezone.utc),
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, _finalizers, post_sync = _patch_reconciler_enqueues(monkeypatch)
    chord_calls = _patch_chord_apply_async(monkeypatch)

    first = sync_reconciler.reconcile_sync_dispatch(limit=10)
    sync_units.dispatch_sync_run(str(dispatch_run.id))
    second = sync_reconciler.reconcile_sync_dispatch(limit=10)
    duplicate_dispatch = sync_units.dispatch_sync_run(str(dispatch_run.id))

    db_session.refresh(dispatch_units[0])
    dispatch_row = _outbox(db_session, dispatch_run, OUTBOX_KIND_DISPATCH)
    post_sync_row = _outbox(db_session, post_run, OUTBOX_KIND_POST_SYNC)
    assert first["relayed_dispatch"] == 1
    assert second["relayed_dispatch"] == 0
    assert dispatches == [((str(dispatch_run.id),), "sync")]
    assert chord_calls == ["apply_async"]
    assert dispatch_row.status == OUTBOX_STATUS_PENDING
    assert dispatch_row.attempts == 1
    assert dispatch_units[0].attempts == 0
    assert dispatch_units[0].status == SyncRunUnitStatus.DISPATCHING.value
    assert duplicate_dispatch["status"] == "noop"
    assert first["relayed_post_sync"] == 1
    assert second["relayed_post_sync"] == 0
    assert len(post_sync) == 1
    assert post_sync_row.status == OUTBOX_STATUS_DISPATCHED
    assert (
        db_session.query(SyncRunPostDispatch).filter_by(sync_run_id=post_run.id).count()
        == 1
    )


def test_b7_zero_unit_nonterminal_run_finalizes_failed_deterministically(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, _units = _seed_run(
        db_session, unit_count=0, status=SyncRunStatus.PLANNED.value
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, finalizers, _post_sync = _patch_reconciler_enqueues(monkeypatch)

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)
    finalize_result = sync_units.finalize_sync_run(str(run.id))

    db_session.refresh(run)
    finalize_row = _outbox(db_session, run, OUTBOX_KIND_FINALIZE)
    post_sync_row = _outbox(db_session, run, OUTBOX_KIND_POST_SYNC)
    assert result["materialized_finalize"] == 1
    assert result["relayed_finalize"] == 1
    assert finalizers == [((str(run.id),), "sync")]
    assert dispatches == []
    assert finalize_result["status"] == "finalized"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.error == "No sync units planned"
    assert run.result == {
        "completed_units": 0,
        "failed_units": 0,
        "reason": "no_sync_units_planned",
    }
    assert finalize_row.status == OUTBOX_STATUS_DISPATCHED
    assert post_sync_row.status == OUTBOX_STATUS_PENDING


def test_a1_first_sync_fresh_source_uses_configured_initial_depth(
    db_session: Session,
) -> None:
    depth_days = 14
    integration, _source, _dataset = _seed_integration(
        db_session, initial_sync_depth=depth_days
    )
    before_plan = datetime.now(timezone.utc)

    plan = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=integration.org_id,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    after_plan = datetime.now(timezone.utc)

    unit = db_session.query(SyncRunUnit).filter_by(id=uuid.UUID(plan.unit_ids[0])).one()
    assert plan.total_units == 1
    assert unit.since_at is not None
    assert before_plan - timedelta(days=depth_days, seconds=2) <= _aware(unit.since_at)
    assert _aware(unit.since_at) <= after_plan - timedelta(days=depth_days) + timedelta(
        seconds=2
    )
    assert unit.before_at is not None
    assert before_plan <= _aware(unit.before_at) <= after_plan + timedelta(seconds=2)
    planned_run = db_session.get(SyncRun, uuid.UUID(plan.sync_run_id))
    assert planned_run is not None
    assert (
        _outbox(db_session, planned_run, OUTBOX_KIND_DISPATCH).status
        == OUTBOX_STATUS_PENDING
    )


def test_a2_backfill_then_sync_now_has_no_date_gap_with_cold_start_depth(
    db_session: Session,
) -> None:
    depth_days = 10
    integration, _source, _dataset = _seed_integration(
        db_session, initial_sync_depth=depth_days
    )
    backfill_since = datetime.now(timezone.utc) - timedelta(days=20)
    backfill_before = datetime.now(timezone.utc) - timedelta(days=1)
    backfill = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=integration.org_id,
            mode=SyncRunMode.BACKFILL.value,
            triggered_by="manual",
            since=backfill_since,
            before=backfill_before,
        ),
    )
    incremental = plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=integration.org_id,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    backfill_units = (
        db_session.query(SyncRunUnit)
        .filter_by(sync_run_id=uuid.UUID(backfill.sync_run_id))
        .order_by(SyncRunUnit.since_at)
        .all()
    )
    incremental_unit = (
        db_session.query(SyncRunUnit)
        .filter_by(sync_run_id=uuid.UUID(incremental.sync_run_id))
        .one()
    )
    assert backfill_units[0].since_at is not None
    assert backfill_units[-1].before_at is not None
    assert _aware(backfill_units[-1].before_at) == backfill_before
    assert incremental_unit.since_at is not None
    assert _aware(incremental_unit.since_at) <= backfill_before
    assert incremental_unit.before_at is not None
    assert _aware(incremental_unit.before_at) >= backfill_before


def test_a3_over_cap_backfill_queues_overflow_rearm_and_reconciler_drains(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units = _seed_run(
        db_session,
        unit_count=2,
        status=SyncRunStatus.PLANNED.value,
        mode=SyncRunMode.BACKFILL.value,
    )
    _patch_db_session(monkeypatch, db_session)
    _patch_reconciler_enqueues(monkeypatch)

    class FakeChord:
        def apply_async(self) -> None:
            return None

    monkeypatch.setattr(sync_units, "chord", lambda *_args, **_kwargs: FakeChord())

    result = sync_units.dispatch_sync_run(str(run.id))

    _refresh_all(db_session, run, *units)
    dispatch_row = _outbox(db_session, run, OUTBOX_KIND_DISPATCH)
    assert result["status"] == "dispatched"
    assert [unit.status for unit in units].count(SyncRunUnitStatus.PLANNED.value) == 1
    assert dispatch_row.status == OUTBOX_STATUS_PENDING
    assert run.status != SyncRunStatus.FAILED.value

    for unit in units:
        if unit.status == SyncRunUnitStatus.DISPATCHING.value:
            unit.status = SyncRunUnitStatus.SUCCESS.value
    dispatch_row.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.commit()
    dispatch_calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None: dispatch_calls.append((args, queue)),
    )

    relayed = sync_reconciler.reconcile_sync_dispatch(limit=10)

    db_session.refresh(dispatch_row)
    assert relayed["relayed_dispatch"] == 1
    assert dispatch_calls == [((str(run.id),), "sync")]
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED
    assert run.status != SyncRunStatus.FAILED.value

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
    sync_units.dispatch_sync_run(str(run.id))
    _mark_units_success(db_session, run)
    sync_units.finalize_sync_run(str(run.id))
    db_session.refresh(run)
    assert run.status == SyncRunStatus.SUCCESS.value


def test_a4_worker_dies_after_running_bucket_frees_and_run_redrives_terminal(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_units

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units = _seed_run(
        db_session, unit_count=2, status=SyncRunStatus.DISPATCHING.value
    )
    now = datetime.now(timezone.utc)
    units[0].status = SyncRunUnitStatus.RUNNING.value
    units[0].lease_owner = "dead-worker"
    units[0].lease_expires_at = now - timedelta(seconds=1)
    units[0].last_heartbeat_at = now - timedelta(minutes=5)
    units[1].status = SyncRunUnitStatus.PLANNED.value
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, finalizers, _post_sync = _patch_reconciler_enqueues(monkeypatch)

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    _refresh_all(db_session, run, *units)
    dispatch_row = _outbox(db_session, run, OUTBOX_KIND_DISPATCH)
    assert result["expired_units"] == 1
    assert result["relayed_dispatch"] == 1
    assert units[0].status == SyncRunUnitStatus.FAILED.value
    assert units[0].result is not None
    assert units[0].result["error_category"] == "worker_lost"
    assert "lease_expired_at" in units[0].result
    assert units[0].lease_owner is None
    assert units[1].status == SyncRunUnitStatus.PLANNED.value
    assert dispatches == [((str(run.id),), "sync")]
    assert finalizers == []
    assert dispatch_row.status == OUTBOX_STATUS_DISPATCHED

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
    chord_calls = _patch_chord_apply_async(monkeypatch)
    sync_units.dispatch_sync_run(str(run.id))
    db_session.refresh(units[1])
    assert units[1].status == SyncRunUnitStatus.DISPATCHING.value
    assert chord_calls == ["apply_async"]
    units[1].status = SyncRunUnitStatus.SUCCESS.value
    units[1].lease_owner = None
    units[1].lease_expires_at = None
    db_session.commit()
    sync_units.finalize_sync_run(str(run.id))
    db_session.refresh(run)
    assert run.status == SyncRunStatus.PARTIAL_FAILED.value
    assert run.completed_units == 1
    assert run.failed_units == 1


def test_a6_post_sync_metrics_receive_exact_covered_window(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import sync_reconciler

    run, units = _seed_run(db_session, unit_count=2, status=SyncRunStatus.SUCCESS.value)
    units[0].status = SyncRunUnitStatus.SUCCESS.value
    units[0].since_at = datetime(2026, 5, 15, 13, 45, tzinfo=timezone.utc)
    units[0].before_at = datetime(2026, 5, 16, 8, 30, tzinfo=timezone.utc)
    units[1].status = SyncRunUnitStatus.SUCCESS.value
    units[1].since_at = datetime(2026, 5, 10, 9, 15, tzinfo=timezone.utc)
    units[1].before_at = datetime(2026, 5, 20, 21, 5, tzinfo=timezone.utc)
    run.completed_units = 2
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
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    dispatches, finalizers, post_sync = _patch_reconciler_enqueues(monkeypatch)

    result = sync_reconciler.reconcile_sync_dispatch(limit=10)

    assert result["relayed_post_sync"] == 1
    assert dispatches == []
    assert finalizers == []
    assert post_sync == [
        {
            "provider": "github",
            "sync_targets": ["git"],
            "org_id": run.org_id,
            "from_date": "2026-05-10",
            "to_date": "2026-05-20",
            "work_graph_from_date": "2026-05-10T00:00:00+00:00",
            "work_graph_to_date": "2026-05-21T00:00:00+00:00",
        }
    ]
    assert (
        _outbox(db_session, run, OUTBOX_KIND_POST_SYNC).status
        == OUTBOX_STATUS_DISPATCHED
    )
