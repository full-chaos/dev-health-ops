from __future__ import annotations

from collections.abc import Iterator

import pytest

from dev_health_ops.models import IntegrationDataset
from tests.canonical_incident_dispatch_support import patch_dispatch, plan_run
from tests.canonical_incident_orchestration_support import (
    CanonicalState,
    canonical_state_context,
    disable_feature_for_org,
)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def test_plan_run_selects_incidents_unit_after_pagerduty_expansion(
    canonical_state: CanonicalState,
) -> None:
    state = canonical_state

    _run, unit = plan_run(state)

    datasets = (
        state.session.query(IntegrationDataset)
        .filter_by(integration_id=unit.integration_id)
        .all()
    )

    assert len(datasets) == 11
    assert unit.provider == "pagerduty"
    assert unit.dataset_key == "incidents"


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
    disable_feature_for_org(state, state.enabled_org_id)
    patch_dispatch(monkeypatch, state.session)

    result = sync_units.dispatch_sync_run(str(run.id))

    assert result["status"] == "dispatched"
