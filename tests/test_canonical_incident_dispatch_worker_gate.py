from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.models import (
    SyncDispatchOutbox,
    SyncRun,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_FINALIZE
from tests.canonical_incident_dispatch_support import (
    patch_dispatch,
    plan_run,
    plan_zero_unit_run,
)
from tests.canonical_incident_orchestration_support import (
    CanonicalState,
    canonical_state_context,
    disable_feature_for_org,
)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def test_dispatch_terminalizes_run_when_feature_flips_off(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    disable_feature_for_org(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)
    finalize_calls: list[str] = []
    monkeypatch.setattr(
        sync_units,
        "_enqueue_denied_active_finalize",
        lambda run_id: finalize_calls.append(run_id),
    )

    # When
    result = sync_units.dispatch_sync_run(str(run.id))

    # Then
    state.session.refresh(run)
    state.session.refresh(unit)
    assert result["status"] == "feature_disabled"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.result == {"error_category": "feature_disabled"}
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result == {"error_category": "feature_disabled"}
    assert unit.available_at is None
    assert unit.attempts == 0
    assert state.session.query(SyncWatermark).count() == 0
    assert finalize_calls == []
    finalizer = (
        state.session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .one()
    )
    assert finalizer.status == "dispatched"


def test_repeated_dispatch_denial_is_idempotent_and_never_retries(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    disable_feature_for_org(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)
    finalize_calls: list[str] = []
    monkeypatch.setattr(
        sync_units,
        "_enqueue_denied_active_finalize",
        lambda run_id: finalize_calls.append(run_id),
    )
    first = sync_units.dispatch_sync_run(str(run.id))

    # When
    second = sync_units.dispatch_sync_run(str(run.id))

    # Then
    state.session.refresh(run)
    state.session.refresh(unit)
    assert first["status"] == second["status"] == "feature_disabled"
    assert run.failed_units == 1
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.available_at is None
    assert unit.attempts == 0
    assert finalize_calls == []
    assert (
        state.session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .count()
        == 1
    )


def test_zero_unit_denial_schedules_finalizer_once(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run = plan_zero_unit_run(state)
    disable_feature_for_org(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)
    finalize_calls: list[str] = []
    monkeypatch.setattr(
        sync_units,
        "_enqueue_denied_active_finalize",
        lambda run_id: finalize_calls.append(run_id),
    )

    # When
    result = sync_units.dispatch_sync_run(str(run.id))

    # Then
    state.session.refresh(run)
    assert result["status"] == "feature_disabled"
    assert run.status == SyncRunStatus.FAILED.value
    assert finalize_calls == []
    assert (
        state.session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .count()
        == 1
    )


def test_denial_never_publishes_terminal_finalizer(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    disable_feature_for_org(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)

    def reject_enqueue(_run_id: str) -> None:
        raise AssertionError("terminal finalizer must not be published")

    monkeypatch.setattr(
        sync_units,
        "_enqueue_denied_active_finalize",
        reject_enqueue,
    )

    # When
    result = sync_units.dispatch_sync_run(str(run.id))

    # Then
    state.session.expire_all()
    persisted_run = state.session.get(SyncRun, run.id)
    persisted_unit = state.session.get(SyncRunUnit, unit.id)
    assert persisted_run is not None
    assert persisted_run.status == SyncRunStatus.FAILED.value
    assert persisted_unit is not None
    assert persisted_unit.status == SyncRunUnitStatus.FAILED.value
    finalizer = (
        state.session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_FINALIZE)
        .one()
    )
    assert result["status"] == "feature_disabled"
    assert finalizer.status == "dispatched"


def test_dispatch_terminalizes_running_claim_when_feature_flips_off(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    now = datetime.now(timezone.utc)
    run.status = SyncRunStatus.RUNNING.value
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = "claimed-worker"
    unit.lease_expires_at = now + timedelta(minutes=5)
    unit.available_at = now
    state.session.commit()
    disable_feature_for_org(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)
    finalize_calls: list[str] = []
    monkeypatch.setattr(
        sync_units,
        "_enqueue_denied_active_finalize",
        lambda run_id: finalize_calls.append(run_id),
    )

    # When
    result = sync_units.dispatch_sync_run(str(run.id))

    # Then
    state.session.refresh(run)
    state.session.refresh(unit)
    assert result["status"] == "feature_disabled"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.result == {"error_category": "feature_disabled"}
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result == {"error_category": "feature_disabled"}
    assert unit.available_at is None
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert finalize_calls == []
