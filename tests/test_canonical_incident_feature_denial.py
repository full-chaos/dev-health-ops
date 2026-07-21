from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, update

from dev_health_ops.licensing import FeatureDecisionReason
from dev_health_ops.models import SyncRunStatus, SyncRunUnit, SyncRunUnitStatus
from dev_health_ops.sync.canonical_incident_gate import (
    CanonicalIncidentFeatureDisabledError,
)
from dev_health_ops.sync.feature_denial import terminalize_feature_disabled_run
from tests.canonical_incident_dispatch_support import plan_run
from tests.canonical_incident_orchestration_support import (
    CanonicalState,
    canonical_state_context,
)


@pytest.fixture
def canonical_state() -> Iterator[CanonicalState]:
    with canonical_state_context() as state:
        yield state


def test_running_denial_does_not_cancel_replacement_lease_owner(
    canonical_state: CanonicalState,
) -> None:
    # Given
    state = canonical_state
    run, unit = plan_run(state)
    now = datetime.now(timezone.utc)
    run.status = SyncRunStatus.RUNNING.value
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = "original-worker"
    unit.lease_expires_at = now + timedelta(minutes=5)
    state.session.commit()
    engine = state.session.get_bind()
    lease_replaced = False

    def replace_lease_before_denial(
        connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal lease_replaced
        if lease_replaced or not statement.lstrip().upper().startswith(
            "UPDATE SYNC_RUN_UNITS"
        ):
            return
        lease_replaced = True
        connection.execute(
            update(SyncRunUnit)
            .where(SyncRunUnit.id == unit.id)
            .values(lease_owner="replacement-worker")
        )

    event.listen(engine, "before_cursor_execute", replace_lease_before_denial)

    # When
    try:
        transition = terminalize_feature_disabled_run(
            state.session,
            run,
            CanonicalIncidentFeatureDisabledError(
                FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED
            ),
        )
    finally:
        event.remove(engine, "before_cursor_execute", replace_lease_before_denial)

    # Then
    state.session.expire_all()
    persisted_unit = state.session.get(SyncRunUnit, unit.id)
    assert lease_replaced is True
    assert transition.run_terminal is False
    assert persisted_unit is not None
    assert persisted_unit.status == SyncRunUnitStatus.RUNNING.value
    assert persisted_unit.lease_owner == "replacement-worker"


def test_running_denial_terminalizes_legacy_null_owner(
    canonical_state: CanonicalState,
) -> None:
    # Given
    state = canonical_state
    run, unit = plan_run(state)
    run.status = SyncRunStatus.RUNNING.value
    unit.status = SyncRunUnitStatus.RUNNING.value
    unit.lease_owner = None
    unit.lease_expires_at = None
    state.session.commit()

    # When
    transition = terminalize_feature_disabled_run(
        state.session,
        run,
        CanonicalIncidentFeatureDisabledError(
            FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED
        ),
    )

    # Then
    state.session.refresh(run)
    state.session.refresh(unit)
    assert transition.run_terminal is True
    assert run.status == SyncRunStatus.FAILED.value
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result == {"error_category": "feature_disabled"}
    assert unit.lease_owner is None
    assert unit.lease_expires_at is None
