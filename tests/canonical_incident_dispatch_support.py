from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    IntegrationDataset,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from tests.canonical_incident_orchestration_support import (
    CanonicalState,
    create_canonical_graph,
)


@contextmanager
def session_context(session: Session):
    yield session
    session.commit()


class Signature:
    def set(self, *, queue: str) -> Signature:
        return self


class Chord:
    def apply_async(self) -> None:
        return None


def plan_run(state: CanonicalState) -> tuple[SyncRun, SyncRunUnit]:
    graph = create_canonical_graph(state, state.enabled_org_id)
    plan = plan_sync_run(
        state.session,
        SyncPlanRequest(
            integration_id=str(graph.integration.id),
            org_id=str(state.enabled_org_id),
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="test",
            before=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
            dataset_keys=(graph.dataset.dataset_key,),
        ),
    )
    run = state.session.get(SyncRun, plan.sync_run_id)
    assert run is not None
    unit = (
        state.session.query(SyncRunUnit)
        .filter_by(
            sync_run_id=run.id,
            provider=graph.integration.provider,
            dataset_key=graph.dataset.dataset_key,
        )
        .one()
    )
    discovery = (
        state.session.query(SyncRunReferenceDiscovery)
        .filter_by(sync_run_id=run.id)
        .one()
    )
    discovery.status = "success"
    discovery.completed_at = datetime.now(timezone.utc)
    state.session.commit()
    return run, unit


def plan_zero_unit_run(state: CanonicalState) -> SyncRun:
    graph = create_canonical_graph(state, state.enabled_org_id)
    plan = plan_sync_run(
        state.session,
        SyncPlanRequest(
            integration_id=str(graph.integration.id),
            org_id=str(state.enabled_org_id),
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="test",
            before=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
            source_ids=(),
            dataset_keys=(graph.dataset.dataset_key,),
        ),
    )
    run = state.session.get(SyncRun, plan.sync_run_id)
    assert run is not None
    assert state.session.query(SyncRunUnit).filter_by(sync_run_id=run.id).count() == 0
    assert (
        state.session.query(IntegrationDataset)
        .filter_by(integration_id=graph.integration.id, dataset_key="incidents")
        .count()
        == 1
    )
    return run


def patch_dispatch(monkeypatch: pytest.MonkeyPatch, session: Session) -> None:
    from dev_health_ops.workers import sync_units

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: session_context(session),
    )
    monkeypatch.setattr(sync_units.run_sync_unit, "s", lambda _unit_id: Signature())
    monkeypatch.setattr(sync_units.finalize_sync_run, "si", lambda _run_id: Signature())
    monkeypatch.setattr(sync_units, "group", list)
    monkeypatch.setattr(sync_units, "chord", lambda *_args: Chord())
