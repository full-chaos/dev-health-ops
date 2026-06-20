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
    # exactly one unit is over the cap of 1; which specific id is an ordering
    # detail of the guard query, so assert count + membership, not order.
    assert len(decision.capped_unit_ids) == 1
    assert set(decision.capped_unit_ids) <= {str(unit.id) for unit in units}


def test_dispatch_guard_concurrency_cap_is_partial_allow_not_deny(
    db_session, monkeypatch
):
    """Concurrency cap returns allowed=True, concurrency_capped=True (CHAOS-2576).

    The old behaviour returned allowed=False, which caused the whole run to be
    marked FAILED.  The new shape is a DISTINCT partial-allow so the caller can
    leave capped units PLANNED and schedule a delayed redispatch.
    """
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


def test_concurrency_cap_defers_not_fails(db_session, monkeypatch):
    """Units over the concurrency cap stay PLANNED; run is NOT FAILED (CHAOS-2576).

    Scenario: concurrency cap = 2, 1 slot already consumed by another run.
    New run has 3 units in the same bucket: 1 is allowed, 2 are capped.
    Verifies: allowed unit dispatched, capped units remain PLANNED, run not FAILED,
    and a countdown redispatch is scheduled for the capped units.
    """
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
    # Seed a run with 3 units in the same bucket.
    run, units, integration, source = _seed_run(db_session, unit_count=3)
    # Simulate 1 active unit from another run consuming 1 of the 2 concurrency slots.
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
        dataset_key="commits-active",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
    )
    db_session.add(active_unit)
    db_session.flush()

    from contextlib import contextmanager

    import dev_health_ops.db as db

    @contextmanager
    def _fake_session_ctx(s):
        yield s
        s.commit()

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(db_session)
    )

    unit_queued = []
    redispatches = []

    class FakeUnitSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            unit_queued.append(self)
            return self

    class FakeFinalizeSig:
        def __init__(self, run_id):
            self.run_id = run_id

        def set(self, *, queue):
            return self

    class FakeChord:
        def __init__(self, header, callback):
            pass

        def apply_async(self):
            return None

    monkeypatch.setattr(
        sync_units.run_sync_unit, "s", lambda unit_id: FakeUnitSig(unit_id)
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run, "si", lambda run_id: FakeFinalizeSig(run_id)
    )
    monkeypatch.setattr(sync_units, "group", list)
    monkeypatch.setattr(
        sync_units, "chord", lambda header, callback: FakeChord(header, callback)
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, countdown=None: redispatches.append(
            (args, countdown)
        ),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    # Run must NOT be FAILED.
    assert run.status != "failed", f"run must not be FAILED, got {run.status}"
    # 1 allowed slot → 1 unit dispatched.
    assert len(unit_queued) == 1, f"expected 1 unit dispatched, got {len(unit_queued)}"
    # 2 capped units remain PLANNED.
    planned = [u for u in units if u.status == "planned"]
    assert len(planned) == 2, f"expected 2 units PLANNED, got {len(planned)}"
    # Dispatched path: no redispatch scheduled (chord handles finalize).
    assert result["status"] == "dispatched"
    assert len(redispatches) == 0


def test_total_unit_cap_still_hard_denies(db_session, monkeypatch):
    """Total-cap hard-deny is unchanged: allowed=False, concurrency_capped=False (CHAOS-2576)."""
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "1")
    run, units, _, _ = _seed_run(db_session, unit_count=2)

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is False
    assert decision.concurrency_capped is False
    assert "unit cap exceeded" in str(decision.reason)
    assert len(decision.capped_unit_ids) == 1
