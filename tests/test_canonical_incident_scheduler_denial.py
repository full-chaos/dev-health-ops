from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.models import (
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_KIND_FINALIZE,
    claim_due_outbox_rows,
    upsert_outbox_wakeup,
)
from tests.canonical_incident_orchestration_support import (
    CanonicalGraph,
    CanonicalState,
    canonical_state_context,
    create_canonical_graph,
    remove_feature_override,
)

NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def _seed_due_schedule(state: CanonicalState) -> CanonicalGraph:
    graph = create_canonical_graph(state, state.enabled_org_id, with_config=True)
    assert graph.config is not None
    state.session.add(
        ScheduledJob(
            name=f"sync-config-{graph.config.id}",
            job_type="sync",
            schedule_cron="* * * * *",
            org_id=str(state.enabled_org_id),
            provider="pagerduty",
            sync_config_id=graph.config.id,
            tz="UTC",
            status=JobStatus.ACTIVE.value,
        )
    )
    state.session.commit()
    return graph


def _install_plan_then_disable(
    state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[MagicMock, MagicMock]:
    from dev_health_ops.sync import execution_trigger
    from dev_health_ops.workers import sync_scheduler, sync_units

    dispatch = MagicMock()
    finalize = MagicMock()
    real_trigger = execution_trigger.create_sync_execution_trigger

    def create_then_disable(*args, **kwargs):
        trigger = real_trigger(*args, **kwargs)
        remove_feature_override(state, state.enabled_org_id, commit=False)
        return trigger

    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(
        execution_trigger,
        "create_sync_execution_trigger",
        create_then_disable,
    )
    monkeypatch.setattr(sync_units, "dispatch_sync_run", dispatch)
    monkeypatch.setattr(sync_units, "finalize_sync_run", finalize)
    return dispatch, finalize


def test_scheduler_terminalizes_plan_when_feature_flips_before_enqueue(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_scheduler

    state = canonical_state
    graph = _seed_due_schedule(state)
    assert graph.config is not None
    original_last_sync_at = graph.config.last_sync_at
    dispatch, finalize = _install_plan_then_disable(state, monkeypatch)

    result = sync_scheduler._maybe_dispatch_config(state.session, graph.config, NOW)

    run = state.session.query(SyncRun).one()
    unit = state.session.query(SyncRunUnit).one()
    discovery = state.session.query(SyncRunReferenceDiscovery).one()
    job_run = state.session.query(JobRun).one()
    outboxes = state.session.query(SyncDispatchOutbox).all()
    outbox_by_kind = {row.kind: row for row in outboxes}
    state.session.refresh(graph.config)

    assert result is False
    dispatch.apply_async.assert_not_called()
    finalize.apply_async.assert_not_called()
    assert run.status == SyncRunStatus.FAILED.value
    assert run.result == {"error_category": "feature_disabled"}
    assert run.completed_at is not None
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result == {"error_category": "feature_disabled"}
    assert unit.available_at is None
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert discovery.status == "failed"
    assert discovery.result == {"error_category": "feature_disabled"}
    assert discovery.completed_at is not None
    assert discovery.lease_owner is None
    assert discovery.lease_expires_at is None
    assert job_run.status == JobRunStatus.FAILED.value
    assert isinstance(job_run.result, dict)
    assert job_run.result["error_category"] == "feature_disabled"
    assert job_run.completed_at is not None
    assert set(outbox_by_kind) == {OUTBOX_KIND_DISCOVERY, OUTBOX_KIND_FINALIZE}
    assert outbox_by_kind[OUTBOX_KIND_DISCOVERY].status == "dispatched"
    assert outbox_by_kind[OUTBOX_KIND_FINALIZE].status == "dispatched"
    assert all(row.claim_token is None for row in outboxes)
    assert all(row.claim_expires_at is None for row in outboxes)
    assert graph.config.last_sync_at == original_last_sync_at


def test_scheduler_feature_denial_is_idempotent_and_not_reconciler_claimable(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_scheduler

    state = canonical_state
    graph = _seed_due_schedule(state)
    assert graph.config is not None
    dispatch, finalize = _install_plan_then_disable(state, monkeypatch)

    first = sync_scheduler._maybe_dispatch_config(state.session, graph.config, NOW)
    run = state.session.query(SyncRun).one()
    upsert_outbox_wakeup(
        state.session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISCOVERY,
        available_at=NOW,
        now=NOW,
    )
    upsert_outbox_wakeup(
        state.session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_FINALIZE,
        available_at=NOW,
        now=NOW,
    )
    second = sync_scheduler._maybe_dispatch_config(
        state.session,
        graph.config,
        NOW + timedelta(minutes=1),
    )
    claimed = claim_due_outbox_rows(
        state.session,
        now=NOW + timedelta(minutes=5),
        limit=10,
    )

    assert first is False
    assert second is False
    assert claimed == []
    assert {
        row.kind: row.status for row in state.session.query(SyncDispatchOutbox).all()
    } == {
        OUTBOX_KIND_DISCOVERY: "dispatched",
        OUTBOX_KIND_FINALIZE: "dispatched",
    }
    assert (
        state.session.query(SyncDispatchOutbox)
        .filter_by(kind=OUTBOX_KIND_FINALIZE)
        .count()
        == 1
    )
    dispatch.apply_async.assert_not_called()
    finalize.apply_async.assert_not_called()


def test_reconciler_can_recover_pending_finalizer_for_terminal_denial(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_reconciler, sync_scheduler

    state = canonical_state
    graph = _seed_due_schedule(state)
    assert graph.config is not None
    _install_plan_then_disable(state, monkeypatch)
    sync_scheduler._maybe_dispatch_config(state.session, graph.config, NOW)
    run = state.session.query(SyncRun).one()
    finalizer = (
        state.session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .one()
    )
    finalizer.status = "pending"
    state.session.commit()

    assert sync_reconciler._run_is_finalizable(state.session, run.id) is True


def test_scheduler_terminalization_db_error_rolls_back_and_is_raised(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_scheduler, sync_units

    state = canonical_state
    graph = _seed_due_schedule(state)
    assert graph.config is not None
    dispatch, finalize = _install_plan_then_disable(state, monkeypatch)

    def fail_terminalization(*_args, **_kwargs):
        raise SQLAlchemyError("terminalization failed")

    monkeypatch.setattr(
        sync_units,
        "terminalize_feature_disabled_plan",
        fail_terminalization,
        raising=False,
    )

    with pytest.raises(SQLAlchemyError, match="terminalization failed"):
        sync_scheduler._maybe_dispatch_config(state.session, graph.config, NOW)

    assert state.session.query(SyncRun).count() == 0
    assert state.session.query(SyncRunUnit).count() == 0
    assert state.session.query(SyncRunReferenceDiscovery).count() == 0
    assert state.session.query(JobRun).count() == 0
    assert state.session.query(SyncDispatchOutbox).count() == 0
    dispatch.apply_async.assert_not_called()
    finalize.apply_async.assert_not_called()
