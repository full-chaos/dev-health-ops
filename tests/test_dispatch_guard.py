from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.guard import DispatchGuard


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _seed_run(session, *, unit_count=1, status=SyncRunUnitStatus.PLANNED.value):
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="demo",
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
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=unit_count,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, run])
    session.flush()
    units = []
    for index in range(unit_count):
        unit = SyncRunUnit(
            org_id=org_id,
            sync_run_id=run.id,
            integration_id=integration.id,
            source_id=source.id,
            provider="github",
            dataset_key=f"commits-{index}",
            cost_class="medium",
            mode=SyncRunMode.INCREMENTAL.value,
            status=status,
            attempts=0,
        )
        units.append(unit)
    session.add_all(units)
    session.flush()
    return run, units, integration, source


def test_dispatch_guard_allows_under_cap(db_session, monkeypatch):
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "3")
    run, _, _, _ = _seed_run(db_session, unit_count=2)

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is True
    assert decision.reason is None
    assert decision.capped_unit_ids == ()


def test_dispatch_guard_denies_over_total_unit_cap(db_session, monkeypatch):
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "1")
    run, units, _, _ = _seed_run(db_session, unit_count=2)

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is False
    assert "unit cap exceeded" in str(decision.reason)
    assert decision.capped_unit_ids == (str(units[1].id),)


def test_dispatch_guard_denies_over_inflight_concurrency(db_session, monkeypatch):
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units, integration, source = _seed_run(db_session, unit_count=1)
    active_run = SyncRun(
        org_id=run.org_id,
        integration_id=integration.id,
        triggered_by="schedule",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.RUNNING.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(active_run)
    db_session.flush()
    active_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=active_run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
    )
    db_session.add(active_unit)
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is False
    assert "concurrency cap exceeded" in str(decision.reason)
    assert decision.capped_unit_ids == (str(units[0].id),)
