from __future__ import annotations

from collections.abc import Iterator

import pytest

from dev_health_ops.models import SyncRunStatus, SyncRunUnitStatus, SyncWatermark
from tests.canonical_incident_dispatch_support import patch_dispatch, plan_run
from tests.canonical_incident_orchestration_support import (
    CanonicalState,
    canonical_state_context,
    remove_feature_override,
)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def test_worker_terminalizes_claim_when_feature_flips_after_dispatch(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    patch_dispatch(monkeypatch, state.session)
    assert sync_units.dispatch_sync_run(str(run.id))["status"] == "dispatched"
    remove_feature_override(state, state.enabled_org_id)
    provider_calls: list[str] = []
    finalize_calls: list[str] = []
    monkeypatch.setattr(
        sync_units, "_start_unit_heartbeat", lambda *_args: (None, None)
    )
    monkeypatch.setattr(
        sync_units._runtime_cache,
        "get",
        lambda _ctx: provider_calls.append("runtime"),
    )
    monkeypatch.setattr(
        dataset_adapters,
        "run_dataset_unit",
        lambda _ctx, _runtime: provider_calls.append("provider"),
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda *, args, queue: finalize_calls.append(f"{queue}:{args[0]}"),
    )

    # When
    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))
    finalize_result = getattr(sync_units.finalize_sync_run, "run")(str(run.id))

    # Then
    state.session.refresh(run)
    state.session.refresh(unit)
    assert result["status"] == "failed"
    assert result["error_category"] == "feature_disabled"
    assert finalize_result["status"] == "finalized"
    assert run.status == SyncRunStatus.FAILED.value
    assert run.result == {
        "completed_units": 0,
        "failed_units": 1,
        "error_category": "feature_disabled",
    }
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result == {"error_category": "feature_disabled"}
    assert unit.attempts == 1
    assert unit.available_at is None
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
    assert provider_calls == []
    assert state.session.query(SyncWatermark).count() == 0
    assert finalize_calls == [f"sync:{run.id}"]


def test_repeated_worker_denial_is_terminal_and_does_not_finalize_twice(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    patch_dispatch(monkeypatch, state.session)
    sync_units.dispatch_sync_run(str(run.id))
    remove_feature_override(state, state.enabled_org_id)
    finalize_calls: list[str] = []
    monkeypatch.setattr(
        sync_units, "_start_unit_heartbeat", lambda *_args: (None, None)
    )
    monkeypatch.setattr(sync_units._runtime_cache, "get", lambda _ctx: None)
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda *, args, queue: finalize_calls.append(f"{queue}:{args[0]}"),
    )
    first = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    # When
    second = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    # Then
    state.session.refresh(unit)
    assert first["error_category"] == "feature_disabled"
    assert second == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "terminal",
    }
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.attempts == 1
    assert finalize_calls == [f"sync:{run.id}"]


def test_worker_rechecks_feature_after_lease_check_before_provider(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units

    # Given
    state = canonical_state
    run, unit = plan_run(state)
    patch_dispatch(monkeypatch, state.session)
    assert sync_units.dispatch_sync_run(str(run.id))["status"] == "dispatched"
    provider_calls: list[str] = []
    monkeypatch.setattr(
        sync_units, "_start_unit_heartbeat", lambda *_args: (None, None)
    )
    monkeypatch.setattr(sync_units._runtime_cache, "get", lambda _ctx: None)
    monkeypatch.setattr(
        dataset_adapters,
        "run_dataset_unit",
        lambda _ctx, _runtime: provider_calls.append("provider"),
    )

    def disable_after_lease_check(_unit_id: str, _lease_owner: str | None) -> bool:
        remove_feature_override(state, state.enabled_org_id)
        return True

    monkeypatch.setattr(
        sync_units,
        "_sync_unit_lease_is_owned_and_live",
        disable_after_lease_check,
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda **_kwargs: None,
    )

    # When
    result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

    # Then
    state.session.refresh(unit)
    assert result["status"] == "failed"
    assert result["error_category"] == "feature_disabled"
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert provider_calls == []
    assert state.session.query(SyncWatermark).count() == 0
