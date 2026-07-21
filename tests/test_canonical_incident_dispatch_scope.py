from __future__ import annotations

from collections.abc import Iterator

import pytest

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


def test_dispatch_gate_uses_persisted_run_scope_not_all_integration_datasets(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import sync_units

    state = canonical_state
    run, unit = plan_run(state)
    unit.provider = "github"
    unit.dataset_key = "commits"
    state.session.commit()
    remove_feature_override(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)

    result = sync_units.dispatch_sync_run(str(run.id))

    assert result["status"] == "dispatched"
