from __future__ import annotations

import uuid
from collections.abc import Iterator
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
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    SyncWatermark,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_STATUS_PENDING,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from tests._helpers import (
    pin_provider_unit_routability,
    provider_unit_outbox_keys,
    seed_sync_dispatch_transport_routes,
)


@pytest.fixture(autouse=True)
def _routable_synthetic_pairs(monkeypatch):
    """Pin provider-unit routability for this module (CHAOS-4054 step 4).

    Every unit in this file uses a synthetic dataset key -- a bucket label,
    not a capability-matrix identity. Before step 4 those keys still reached a
    dispatcher, because a pair the matrix declined fell through to the Celery
    writer. Step 4 deleted that fallthrough: River is the only runtime, so an
    unrouted pair is now terminalized as ``feature_disabled`` and never
    dispatched at all.

    Without this pin every test below would observe a terminalized unit
    instead of the budget/guard/invariant behaviour it exists to assert. No
    test in this module is about routability; see
    ``tests/_helpers.pin_provider_unit_routability`` for why pinning beats
    renaming the keys to real datasets.
    """

    pin_provider_unit_routability(monkeypatch)


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_sync_dispatch_transport_routes(session)
        yield session
    engine.dispose()


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
    session.add(
        SyncRunReferenceDiscovery(
            org_id=integration.org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    session.flush()
    return run, units


def _outbox(session: Session, run: SyncRun, kind: str) -> SyncDispatchOutbox:
    return (
        session.query(SyncDispatchOutbox).filter_by(sync_run_id=run.id, kind=kind).one()
    )


def _dispatched_units(session: Session) -> set[str]:
    """Provider units staged for execution, by durable outbox dedupe key.

    Replaces a ``run_sync_unit.s(...).apply_async()`` spy. CHAOS-4054 step 4
    deleted that publish -- an admitted unit's dispatch IS its outbox row now,
    so "was this unit dispatched" is a database question. It is also a
    stronger one: the spy counted publishes without being able to say WHICH
    unit each belonged to.
    """

    return provider_unit_outbox_keys(session)


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
        _outbox(db_session, planned_run, OUTBOX_KIND_DISCOVERY).status
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
    last_backfill_before = _aware(backfill_units[-1].before_at)
    assert last_backfill_before == backfill_before
    assert incremental_unit.since_at is not None
    assert incremental_unit.before_at is not None
    incremental_window = (
        _aware(incremental_unit.since_at),
        _aware(incremental_unit.before_at),
    )
    assert incremental_window[0] <= last_backfill_before <= incremental_window[1]
    assert (
        db_session.query(SyncWatermark)
        .filter_by(org_id=integration.org_id, dataset_key="commits")
        .count()
        == 0
    )
