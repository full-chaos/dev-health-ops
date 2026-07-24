from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from tests._helpers import seed_sync_dispatch_transport_routes


@contextmanager
def _session_context(session: Session):
    yield session
    session.commit()


class _Signature:
    def __init__(self, value: str, queued: list[str]) -> None:
        self.value = value
        self.queued = queued

    def set(self, *, queue: str) -> _Signature:
        self.queued.append(f"{queue}:{self.value}")
        return self

    def apply_async(self) -> None:
        return None


def test_plan_dispatch_worker_state_transitions_are_characterized(monkeypatch) -> None:
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_bootstrap import ProviderRuntime

    # Given
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    queued: list[str] = []
    with Session(engine) as session:
        seed_sync_dispatch_transport_routes(session)
        org_id = str(uuid.uuid4())
        integration = Integration(
            org_id=org_id,
            provider="github",
            name="baseline",
            config={},
            is_active=True,
        )
        session.add(integration)
        session.flush()
        source = IntegrationSource(
            org_id=org_id,
            integration_id=integration.id,
            provider="github",
            source_type="repo",
            external_id="full-chaos/dev-health",
            name="dev-health",
            full_name="full-chaos/dev-health",
            metadata_={},
            is_enabled=True,
        )
        dataset = IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key="commits",
            is_enabled=True,
            options={},
        )
        session.add_all([source, dataset])
        session.flush()

        # When
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                integration_id=str(integration.id),
                org_id=org_id,
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="baseline",
                before=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
            ),
        )

        # Then
        run = session.get(SyncRun, uuid.UUID(plan.sync_run_id))
        assert run is not None
        unit = session.query(SyncRunUnit).filter_by(sync_run_id=run.id).one()
        discovery = (
            session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
        )
        assert run.status == SyncRunStatus.PLANNED.value
        assert unit.status == SyncRunUnitStatus.PLANNED.value
        discovery.status = "success"
        discovery.completed_at = datetime.now(timezone.utc)
        session.commit()

        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session_sync",
            lambda: _session_context(session),
        )
        monkeypatch.setattr(
            sync_units.run_sync_unit,
            "s",
            lambda unit_id: _Signature(unit_id, queued),
        )
        # When
        dispatch_result = sync_units.dispatch_sync_run(plan.sync_run_id)

        # Then
        session.refresh(run)
        session.refresh(unit)
        assert dispatch_result == {"status": "dispatched", "queued_units": 1}
        assert run.status == SyncRunStatus.DISPATCHING.value
        assert unit.status == SyncRunUnitStatus.DISPATCHING.value
        assert queued[0] == f"sync:{unit.id}"

        monkeypatch.setattr(
            sync_units,
            "_start_unit_heartbeat",
            lambda *_args: (None, None),
        )
        monkeypatch.setattr(
            sync_units._runtime_cache,
            "get",
            lambda _ctx: ProviderRuntime(extra={}),
        )
        monkeypatch.setattr(
            dataset_adapters,
            "run_dataset_unit",
            lambda _ctx, _runtime: {"items_synced": 1},
        )
        monkeypatch.setattr(
            sync_units.finalize_sync_run,
            "apply_async",
            lambda *, args, queue: queued.append(f"{queue}:{args[0]}"),
        )

        # When
        worker_result = getattr(sync_units.run_sync_unit, "run")(str(unit.id))

        # Then
        session.refresh(unit)
        assert worker_result["status"] == "success"
        assert unit.status == SyncRunUnitStatus.SUCCESS.value
        assert unit.lease_owner is None
        assert unit.lease_expires_at is None
        assert session.query(SyncWatermark).count() == 1

    engine.dispose()
