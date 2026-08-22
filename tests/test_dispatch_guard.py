from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISPATCH,
    OUTBOX_STATUS_PENDING,
    upsert_outbox_wakeup,
)
from dev_health_ops.sync.guard import DispatchGuard
from tests._helpers import (
    pin_provider_unit_routability,
    provider_unit_outbox_keys,
    seed_sync_dispatch_transport_routes,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_sync_dispatch_transport_routes(session)
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
    session.add(
        SyncRunReferenceDiscovery(
            org_id=org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
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

    # These dataset keys are synthetic bucket labels, not matrix identities;
    # the subject here is the concurrency cap, not routability.
    pin_provider_unit_routability(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    # Run must NOT be FAILED.
    assert run.status != "failed", f"run must not be FAILED, got {run.status}"
    # 1 allowed slot → 1 unit dispatched.
    dispatched = provider_unit_outbox_keys(db_session)
    assert len(dispatched) == 1, f"expected 1 unit dispatched, got {len(dispatched)}"
    # 2 capped units remain PLANNED.
    planned = [u for u in units if u.status == "planned"]
    assert len(planned) == 2, f"expected 2 units PLANNED, got {len(planned)}"
    # Dispatched path WITH capped units: a countdown redispatch must also be
    # scheduled so the 2 PLANNED units eventually drain (Fix 1).
    assert result["status"] == "dispatched"
    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert outbox.status == OUTBOX_STATUS_PENDING


def test_total_unit_cap_still_hard_denies(db_session, monkeypatch):
    """Total-cap hard-deny is unchanged: allowed=False, concurrency_capped=False (CHAOS-2576)."""
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "1")
    run, units, _, _ = _seed_run(db_session, unit_count=2)

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is False
    assert decision.concurrency_capped is False
    assert "unit cap exceeded" in str(decision.reason)
    assert len(decision.capped_unit_ids) == 1


def test_partial_cap_dispatch_schedules_redispatch(db_session, monkeypatch):
    """Partial concurrency cap: allowed units dispatched AND countdown redispatch
    scheduled for capped units (Fix 1 regression).
    """
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
    # 2 units in the new run; 1 active unit from another run consumes 1 of 2 slots.
    # Result: 1 unit allowed, 1 unit capped → partial dispatch.
    run, units, integration, source = _seed_run(db_session, unit_count=2)
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

    # These dataset keys are synthetic bucket labels, not matrix identities;
    # the subject here is the concurrency cap, not routability.
    pin_provider_unit_routability(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    assert result["status"] == "dispatched"
    # 1 unit dispatched (the allowed slot).
    assert len(provider_unit_outbox_keys(db_session)) == 1
    # 1 unit remains PLANNED (the capped one).
    planned = [u for u in units if u.status == "planned"]
    assert len(planned) == 1
    # A countdown redispatch MUST be scheduled for the capped unit.
    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert outbox.status == OUTBOX_STATUS_PENDING


def test_zero_unit_dispatch_finalizes_not_loops(db_session, monkeypatch):
    """Zero-unit run: dispatch noop path calls finalize, not redispatch (Fix 2 regression)."""
    from dev_health_ops.workers import sync_units

    run = _seed_zero_unit_run(db_session)

    from contextlib import contextmanager

    import dev_health_ops.db as db

    @contextmanager
    def _fake_session_ctx(s):
        yield s
        s.commit()

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(db_session)
    )

    redispatches = []
    finalize_calls = []

    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, countdown=None: redispatches.append(args),
    )
    monkeypatch.setattr(
        sync_units,
        "finalize_sync_run",
        lambda run_id: finalize_calls.append(run_id),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    assert result["status"] == "noop"
    # Must call finalize, not schedule a redispatch loop.
    assert len(finalize_calls) == 1, "zero-unit run must call finalize"
    assert len(redispatches) == 0, "zero-unit run must not schedule redispatch"


def _seed_zero_unit_run(db_session):
    """Seed a run with no units (zero-unit run)."""
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="zero-unit-demo",
        config={},
        is_active=True,
    )
    db_session.add(integration)
    db_session.flush()
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.PLANNED.value,
        total_units=0,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(run)
    db_session.flush()
    db_session.add(
        SyncRunReferenceDiscovery(
            org_id=org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    db_session.flush()
    return run


def test_schedule_redispatch_rearms_outbox_and_never_raises(db_session, monkeypatch):
    from contextlib import contextmanager

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

    run, _, _, _ = _seed_run(db_session, unit_count=1)
    monkeypatch.setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "42")

    @contextmanager
    def _fake_session_ctx(s):
        yield s
        s.commit()

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(db_session)
    )
    existing_available_at = datetime.now(timezone.utc) - timedelta(hours=1)
    upsert_outbox_wakeup(
        db_session,
        sync_run_id=run.id,
        kind=OUTBOX_KIND_DISPATCH,
        available_at=existing_available_at,
        now=existing_available_at,
    )
    db_session.flush()

    before_call = datetime.now(timezone.utc)
    sync_units._schedule_redispatch(str(run.id))
    after_call = datetime.now(timezone.utc)
    row = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    row_available_at = row.available_at
    if row_available_at.tzinfo is None:
        row_available_at = row_available_at.replace(tzinfo=timezone.utc)
    assert row.status == OUTBOX_STATUS_PENDING
    assert row_available_at >= before_call + timedelta(seconds=42)
    assert row_available_at <= after_call + timedelta(seconds=42)

    def failing_session_ctx():
        raise RuntimeError("db down")

    monkeypatch.setattr(db, "get_postgres_session_sync", failing_session_ctx)
    sync_units._schedule_redispatch(str(run.id))


# ---------------------------------------------------------------------------
# Regression tests for Codex round-2 findings (F1-F5)
# ---------------------------------------------------------------------------


def test_same_run_running_units_count_against_cap(db_session, monkeypatch):
    """F1 regression (a): same-run RUNNING units reduce available slots.

    cap=2, 2 same-run units already RUNNING → 0 slots left → the 1 PLANNED
    unit in the same run must be capped, not dispatched.
    """
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
    run, units, integration, source = _seed_run(db_session, unit_count=3)
    # Mark 2 of the 3 units as RUNNING (same run, same bucket).
    units[0].status = SyncRunUnitStatus.RUNNING.value
    units[1].status = SyncRunUnitStatus.RUNNING.value
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    # cap=2, 2 same-run RUNNING → 0 slots → the 1 PLANNED unit must be capped.
    assert decision.allowed is True
    assert decision.concurrency_capped is True
    assert len(decision.capped_unit_ids) == 1
    assert str(units[2].id) in decision.capped_unit_ids


def test_null_lease_running_counts_as_live(db_session, monkeypatch):
    from datetime import timedelta

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    monkeypatch.setenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "60")

    run, units, integration, source = _seed_run(db_session, unit_count=1)

    other_run = SyncRun(
        org_id=run.org_id,
        integration_id=integration.id,
        triggered_by="schedule",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.RUNNING.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(other_run)
    db_session.flush()
    from datetime import datetime, timezone

    stale_time = datetime.now(timezone.utc) - timedelta(hours=2)
    other_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=other_run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits-stale",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        updated_at=stale_time,
    )
    db_session.add(other_unit)
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is True
    assert decision.concurrency_capped is True
    assert len(decision.capped_unit_ids) == 1


def test_expired_running_lease_does_not_consume_cap(db_session, monkeypatch):
    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units, integration, source = _seed_run(db_session, unit_count=1)
    other_run = SyncRun(
        org_id=run.org_id,
        integration_id=integration.id,
        triggered_by="schedule",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.RUNNING.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(other_run)
    db_session.flush()
    now = datetime.now(timezone.utc)
    other_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=other_run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits-expired",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-dead",
        lease_expires_at=now - timedelta(seconds=1),
        last_heartbeat_at=now - timedelta(minutes=2),
    )
    db_session.add(other_unit)
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is True
    assert decision.concurrency_capped is False
    assert decision.capped_unit_ids == ()


def test_live_running_lease_consumes_cap(db_session, monkeypatch):
    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units, integration, source = _seed_run(db_session, unit_count=1)
    other_run = SyncRun(
        org_id=run.org_id,
        integration_id=integration.id,
        triggered_by="schedule",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.RUNNING.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(other_run)
    db_session.flush()
    now = datetime.now(timezone.utc)
    other_unit = SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=other_run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider="github",
        dataset_key="commits-live",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RUNNING.value,
        attempts=1,
        lease_owner="worker-live",
        lease_expires_at=now + timedelta(minutes=10),
        last_heartbeat_at=now,
    )
    db_session.add(other_unit)
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed is True
    assert decision.concurrency_capped is True
    assert len(decision.capped_unit_ids) == 1


def test_finalize_passes_full_datetime_work_graph_window(db_session, monkeypatch):
    """F3 regression (c): finalize_sync_run passes full ISO datetimes for
    work_graph_from/to_date so the covered final day is not truncated to midnight.
    """
    from datetime import datetime, timedelta, timezone  # noqa: F401

    from dev_health_ops.workers import post_sync_dispatch, sync_reconciler, sync_units

    run, units, integration, source = _seed_run(db_session, unit_count=1)

    from contextlib import contextmanager

    import dev_health_ops.db as db

    @contextmanager
    def _fake_session_ctx(s):
        yield s
        s.commit()

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(db_session)
    )

    # Mark the unit SUCCESS with a known since_at / before_at window.
    since_dt = datetime(2024, 3, 1, 6, 0, 0, tzinfo=timezone.utc)
    before_dt = datetime(2024, 3, 15, 18, 30, 0, tzinfo=timezone.utc)
    unit = units[0]
    unit.status = SyncRunUnitStatus.SUCCESS.value
    unit.dataset_key = "commits"
    unit.since_at = since_dt
    unit.before_at = before_dt
    db_session.flush()

    # Mark the run DISPATCHING so finalize proceeds.
    run.status = SyncRunStatus.DISPATCHING.value
    db_session.flush()

    captured_kwargs: dict = {}

    def fake_dispatch_post_sync(**kwargs):
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        post_sync_dispatch, "_dispatch_post_sync_tasks", fake_dispatch_post_sync
    )
    sync_units.finalize_sync_run(str(run.id))
    relay_result = sync_reconciler.reconcile_sync_dispatch(limit=10)
    assert relay_result["relayed_post_sync"] == 1

    # from_date / to_date must be date-only strings.
    assert captured_kwargs.get("from_date") == since_dt.date().isoformat()
    assert captured_kwargs.get("to_date") == before_dt.date().isoformat()

    # work_graph_from_date must be a full ISO datetime (start of since day).
    wg_from = captured_kwargs.get("work_graph_from_date")
    assert wg_from is not None, "work_graph_from_date must be passed"
    parsed_from = datetime.fromisoformat(wg_from)
    assert parsed_from.date() == since_dt.date()
    assert parsed_from.hour == 0 and parsed_from.minute == 0

    # work_graph_to_date must be start of the day AFTER before_dt (covers full day).
    wg_to = captured_kwargs.get("work_graph_to_date")
    assert wg_to is not None, "work_graph_to_date must be passed"
    parsed_to = datetime.fromisoformat(wg_to)
    expected_to_date = before_dt.date() + timedelta(days=1)
    assert parsed_to.date() == expected_to_date, (
        f"work_graph_to_date must be start of day after before_dt, "
        f"got {parsed_to.date()}, expected {expected_to_date}"
    )
    assert parsed_to.hour == 0 and parsed_to.minute == 0


def _make_dispatch_harness(db_session, monkeypatch, *, unit_count, cap, active_slots):
    """Shared setup for F4/F5 redispatch-failure tests.

    Seeds a run with ``unit_count`` PLANNED units and ``active_slots`` RUNNING
    units from another run (to trigger partial-cap or all-capped scenarios).
    Returns (run, units, integration, source, redispatches, mark_failed_calls).
    """

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", str(cap))

    run, units, integration, source = _seed_run(db_session, unit_count=unit_count)

    for slot in range(active_slots):
        other_run = SyncRun(
            org_id=run.org_id,
            integration_id=integration.id,
            triggered_by="schedule",
            mode=SyncRunMode.INCREMENTAL.value,
            status=SyncRunStatus.RUNNING.value,
            total_units=1,
            completed_units=0,
            failed_units=0,
        )
        db_session.add(other_run)
        db_session.flush()
        other_unit = SyncRunUnit(
            org_id=run.org_id,
            sync_run_id=other_run.id,
            integration_id=integration.id,
            source_id=source.id,
            provider="github",
            dataset_key=f"commits-active-{slot}",
            cost_class="medium",
            mode=SyncRunMode.INCREMENTAL.value,
            status=SyncRunUnitStatus.RUNNING.value,
            attempts=1,
        )
        db_session.add(other_unit)
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

    return run, units, integration, source


def test_independent_unit_publish_capped_redispatch_failure_preserves_published_work(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, units, integration, source = _make_dispatch_harness(
        db_session, monkeypatch, unit_count=2, cap=2, active_slots=1
    )

    # Synthetic bucket labels again: the subject is that one admitted unit's
    # published work survives a capped sibling, not whether the matrix routes
    # "commits-0".
    pin_provider_unit_routability(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    assert run.status == SyncRunStatus.DISPATCHING.value
    assert run.completed_at is None
    dispatching = [
        unit for unit in units if unit.status == SyncRunUnitStatus.DISPATCHING.value
    ]
    assert sorted(unit.status for unit in units) == [
        SyncRunUnitStatus.DISPATCHING.value,
        SyncRunUnitStatus.PLANNED.value,
    ]
    assert result == {"status": "dispatched", "queued_units": 1}
    # The published work IS the durable outbox row, and it names the one unit
    # that was admitted -- stronger than the old publish-count spy, which
    # could not tell WHICH unit had been published.
    assert provider_unit_outbox_keys(db_session) == {
        f"sync.provider_unit:{dispatching[0].id}"
    }
    assert (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )


def test_noop_redispatch_failure_preserves_planned_for_reconciler(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units

    run, units, integration, source = _make_dispatch_harness(
        db_session, monkeypatch, unit_count=1, cap=1, active_slots=1
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    assert run.status == SyncRunStatus.PLANNED.value
    assert run.completed_at is None
    assert units[0].status == SyncRunUnitStatus.PLANNED.value
    assert result == {"status": "noop", "queued_units": 0}
    assert (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )


def test_fresh_same_run_dispatching_caps_planned_unit(db_session, monkeypatch):
    """F1 round-3 codex repro: cap=1, one fresh same-run DISPATCHING unit +
    one PLANNED unit in the same bucket → PLANNED must be capped.

    Fresh DISPATCHING is a capacity CONSUMER, not a reclaim candidate.
    The guard must NOT subtract it from active_count.
    """
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    # Use a long staleness window so the DISPATCHING unit is definitely fresh.
    monkeypatch.setenv("SYNC_UNIT_DISPATCH_STALE_SECONDS", "900")

    run, units, integration, source = _seed_run(db_session, unit_count=2)
    # Mark units[0] as fresh DISPATCHING (same run, same bucket).
    from datetime import datetime, timezone

    units[0].status = SyncRunUnitStatus.DISPATCHING.value
    units[0].updated_at = datetime.now(timezone.utc)  # fresh
    # units[1] remains PLANNED.
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    # cap=1, 1 fresh same-run DISPATCHING consumes the slot.
    # The 1 PLANNED unit must be capped — no cap overrun.
    assert decision.allowed is True
    assert decision.concurrency_capped is True, (
        "fresh same-run DISPATCHING must consume the slot; PLANNED must be capped"
    )
    assert len(decision.capped_unit_ids) == 1
    assert str(units[1].id) in decision.capped_unit_ids, (
        "the PLANNED unit must be in capped_unit_ids"
    )


def test_advisory_lock_key_deterministic_and_unique(db_session, monkeypatch):
    """F1 regression: advisory lock key is deterministic and bucket-unique.

    On SQLite (tests) the lock is a no-op, but the key derivation must be
    deterministic and produce distinct values for distinct buckets.
    The PostgreSQL-only guarantee is documented in guard.py.
    """
    from dev_health_ops.sync.guard import _bucket_advisory_lock_key

    key_a = _bucket_advisory_lock_key("org-1", "github", "medium")
    key_b = _bucket_advisory_lock_key("org-1", "github", "high")
    key_c = _bucket_advisory_lock_key("org-2", "github", "medium")
    key_same = _bucket_advisory_lock_key("org-1", "github", "medium")

    # Deterministic: same inputs → same key.
    assert key_a == key_same
    # Unique: different buckets → different keys.
    assert key_a != key_b
    assert key_a != key_c
    assert key_b != key_c
    # 63-bit range: fits in a PostgreSQL bigint (signed 64-bit).
    for key in (key_a, key_b, key_c):
        assert 0 <= key < (1 << 63)


def test_long_running_unit_counts_against_cap_and_is_not_re_enqueued(
    db_session, monkeypatch
):
    """F2 regression: a long-running RUNNING unit past the stale threshold
    still counts against the cap AND is not re-enqueued.

    cap=1, 1 RUNNING unit (updated_at past stale threshold) + 1 PLANNED unit.
    Expected: RUNNING consumes the slot → PLANNED is capped → queued_units == 0.
    RUNNING unit must remain RUNNING (not flipped to DISPATCHING).
    """
    from datetime import datetime, timedelta, timezone

    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    monkeypatch.setenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "60")

    run, units, integration, source = _seed_run(db_session, unit_count=2)
    # First unit: RUNNING, updated_at past the stale threshold.
    units[0].status = SyncRunUnitStatus.RUNNING.value
    units[0].updated_at = datetime.now(timezone.utc) - timedelta(hours=2)
    # Second unit: PLANNED (the one that should be capped).
    # units[1] remains PLANNED.
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

    class FakeUnitSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            return self

        def apply_async(self):
            return None

    monkeypatch.setattr(
        sync_units.run_sync_unit, "s", lambda unit_id: FakeUnitSig(unit_id)
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run, "apply_async", lambda *a, **k: None
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    # RUNNING consumes the slot → PLANNED is capped → nothing dispatched.
    assert result["queued_units"] == 0
    db_session.refresh(units[0])
    db_session.refresh(units[1])
    # RUNNING unit must NOT be re-enqueued.
    assert units[0].status == SyncRunUnitStatus.RUNNING.value, (
        "long-running RUNNING unit must not be reclaimed"
    )
    # PLANNED unit must remain PLANNED (deferred for redispatch).
    assert units[1].status == SyncRunUnitStatus.PLANNED.value, (
        "PLANNED unit must remain PLANNED when cap is consumed by RUNNING"
    )


# ---------------------------------------------------------------------------
# slot_headroom (CHAOS-3465): the only sanctioned way another admission path
# may add work this guard did not see.
# ---------------------------------------------------------------------------


def test_slot_headroom_reports_slots_left_after_this_pass_candidates(
    db_session, monkeypatch
):
    """Headroom is the cap minus consumers minus THIS pass's candidates.

    Deliberately conservative: every candidate is counted as if it will
    dispatch, even though some are about to be budget-deferred, so headroom
    can only ever under-admit. Cap 5, two PLANNED candidates, no consumers
    -> 3.
    """
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "5")
    run, units, _, _ = _seed_run(db_session, unit_count=2)

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    org_id = str(units[0].org_id)
    assert decision.allowed is True
    assert decision.slot_headroom == {(org_id, "github", "medium"): 3}


def test_slot_headroom_is_zero_when_candidates_fill_the_cap(db_session, monkeypatch):
    """The boundary that actually gates a surplus admission: a full bucket
    must report 0, never a negative number that would read as capacity."""
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, units, _, _ = _seed_run(db_session, unit_count=3)

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    org_id = str(units[0].org_id)
    assert decision.slot_headroom[(org_id, "github", "medium")] == 0


def test_slot_headroom_counts_live_consumers_not_just_candidates(
    db_session, monkeypatch
):
    """Work already running consumes the slots a surplus admission would
    otherwise be told are free."""
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "4")
    run, units, _, _ = _seed_run(db_session, unit_count=2)
    units[0].status = SyncRunUnitStatus.RUNNING.value
    units[0].lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    org_id = str(units[0].org_id)
    # 4 cap - 1 live RUNNING consumer - 1 remaining PLANNED candidate = 2.
    assert decision.slot_headroom[(org_id, "github", "medium")] == 2


def test_slot_headroom_covers_a_bucket_holding_only_not_yet_due_units(
    db_session, monkeypatch
):
    """CHAOS-3465: a bucket whose only unit is a not-yet-due RETRYING deferral
    has NO candidate this pass, so the candidate-driven loop never visited it
    and the map had no entry at all.

    An absent entry reads as "no slots" to the budget guard's surplus phase,
    which made the one unit alone in its bucket -- the case surplus most
    obviously exists to rescue -- the one case it could never reach.
    """
    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "4")
    run, units, _, _ = _seed_run(db_session, unit_count=2)
    deferred = units[1]
    deferred.cost_class = "heavy"  # a bucket of its own
    deferred.status = SyncRunUnitStatus.RETRYING.value
    deferred.available_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    db_session.flush()

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    org_id = str(units[0].org_id)
    assert (org_id, "github", "heavy") in decision.slot_headroom, (
        "the deferred-only bucket has no headroom entry, so surplus retry "
        "can never rescue a unit that is alone in its bucket"
    )
    # A future RETRYING unit is neither a consumer nor a candidate, so the
    # whole cap is free.
    assert decision.slot_headroom[(org_id, "github", "heavy")] == 4
    assert decision.slot_headroom[(org_id, "github", "medium")] == 3
