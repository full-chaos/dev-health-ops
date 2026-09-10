from __future__ import annotations

from collections.abc import Iterator

import pytest

from dev_health_ops.models import (
    IntegrationDataset,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunUnit,
)
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.sync import planner
from dev_health_ops.sync.canonical_incident_gate import (
    sync_dataset_requires_canonical_incident_feature,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from tests.canonical_incident_orchestration_support import (
    SYNC_FIXTURE_BEFORE,
    CanonicalState,
    canonical_state_context,
    create_canonical_graph,
)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def _request(
    state: CanonicalState, integration_id: str, org_id: str
) -> SyncPlanRequest:
    return SyncPlanRequest(
        integration_id=integration_id,
        org_id=org_id,
        mode=SyncRunMode.INCREMENTAL.value,
        triggered_by="test",
        before=SYNC_FIXTURE_BEFORE,
    )


def test_jira_incidents_use_the_existing_canonical_feature_gate() -> None:
    assert sync_dataset_requires_canonical_incident_feature("jira", "incidents") is True
    assert (
        sync_dataset_requires_canonical_incident_feature("jira", "work-items") is False
    )


def test_planner_creates_canonical_work_when_feature_enabled_by_default(
    canonical_state: CanonicalState,
) -> None:
    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.enabled_org_id)

    # When
    plan = plan_sync_run(
        state.session,
        _request(state, str(graph.integration.id), str(state.enabled_org_id)),
    )

    # Then
    assert plan.total_units == 11
    assert state.session.query(SyncRun).count() == 1


def test_planner_denies_canonical_work_before_persistence(
    canonical_state: CanonicalState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.disabled_org_id)
    monkeypatch.setattr(
        planner,
        "_resolve_credential_stamp",
        lambda *_args: pytest.fail("feature-denied plan hydrated credentials"),
    )

    # When
    with pytest.raises(RuntimeError, match="feature_disabled"):
        plan_sync_run(
            state.session,
            _request(state, str(graph.integration.id), str(state.disabled_org_id)),
        )

    # Then
    assert state.session.query(SyncRun).count() == 0
    assert state.session.query(SyncRunUnit).count() == 0
    assert state.session.query(SyncDispatchOutbox).count() == 0


def test_planner_denies_disabled_jira_incidents_in_a_mixed_plan(
    canonical_state: CanonicalState,
) -> None:
    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.disabled_org_id)
    credential = state.session.get(
        IntegrationCredential, graph.integration.credential_id
    )
    assert credential is not None
    credential.provider = "jira"
    graph.integration.provider = "jira"
    graph.source.provider = "jira"
    state.session.add(
        IntegrationDataset(
            org_id=str(state.disabled_org_id),
            integration_id=graph.integration.id,
            dataset_key="work-items",
            is_enabled=True,
            options={},
        )
    )
    state.session.commit()

    # When
    with pytest.raises(RuntimeError, match="feature_disabled"):
        plan_sync_run(
            state.session,
            _request(state, str(graph.integration.id), str(state.disabled_org_id)),
        )

    # Then
    assert state.session.query(SyncRun).count() == 0
    assert state.session.query(SyncRunUnit).count() == 0


def test_planner_keeps_jira_work_items_ungated_when_feature_is_off(
    canonical_state: CanonicalState,
) -> None:
    # Given
    state = canonical_state
    graph = create_canonical_graph(state, state.disabled_org_id)
    credential = state.session.get(
        IntegrationCredential, graph.integration.credential_id
    )
    assert credential is not None
    credential.provider = "jira"
    graph.integration.provider = "jira"
    graph.source.provider = "jira"
    graph.dataset.dataset_key = "work-items"
    state.session.commit()

    # When
    plan = plan_sync_run(
        state.session,
        _request(state, str(graph.integration.id), str(state.disabled_org_id)),
    )

    # Then
    assert plan.total_units == 1
    assert state.session.query(SyncRun).count() == 1


# CHAOS-3093 (2026-09-09, PR2a'): three tests tested workers/sync_scheduler.py's
# _maybe_dispatch_config directly, deleted outright along with the module --
# internal/scheduler/sync's coordinator/loop owns sync dispatch natively now.
#   * test_scheduler_skips_disabled_canonical_config_before_marker_or_work ->
#     internal/scheduler/sync/eligibility_gate_integration_test.go's
#     TestHandoffRefusesAConfigWhoseCanonicalIncidentFeatureIsDisabled.
#   * test_scheduler_rechecks_feature_immediately_before_enqueue -> the Go
#     architecture doesn't recheck-then-enqueue across two steps the way
#     Python did (an intentional structural improvement, not a gap): the
#     occurrence reconciler commits a decision and its materialization in
#     ONE transaction (occurrence_reconciler.go's reconcileOne), and a feature
#     disabled mid-flight terminalizes race-safely --
#     internal/syncdispatchruntime/feature_disabled_termination_integration_test.go's
#     TestTerminalizeFeatureDisabledRunBulkAndRaceSafeRunning.
#   * test_scheduler_skips_typed_pagerduty_disable_without_enqueuing -> the
#     Go materializer's own PagerDuty account-identity repair path
#     (materializer.go's preparePagerDutyRepair/disablePagerDutyConfigs)
#     disables without creating units, in the same domain transaction --
#     internal/scheduler/sync/materializer_integration_test.go's
#     TestNativeMaterializerPagerDutyRepairAndUnitsShareDomainTransaction.
