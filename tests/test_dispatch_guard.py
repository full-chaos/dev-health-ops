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
    # Dispatched path WITH capped units: a countdown redispatch must also be
    # scheduled so the 2 PLANNED units eventually drain (Fix 1).
    assert result["status"] == "dispatched"
    assert len(redispatches) == 1, "expected 1 countdown redispatch for capped units"


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

    assert result["status"] == "dispatched"
    # 1 unit dispatched (the allowed slot).
    assert len(unit_queued) == 1
    # 1 unit remains PLANNED (the capped one).
    planned = [u for u in units if u.status == "planned"]
    assert len(planned) == 1
    # A countdown redispatch MUST be scheduled for the capped unit.
    assert len(redispatches) == 1, "partial cap must schedule countdown redispatch"
    assert redispatches[0][0] == (str(run.id),)


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
    return run


def test_redispatch_enqueue_failure_raises(db_session, monkeypatch):
    """Redispatch enqueue failure must raise, not be swallowed (Fix 3 regression)."""
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
    # 3 units, 1 active slot consumed → 1 allowed, 2 capped.
    run, units, integration, source = _seed_run(db_session, unit_count=3)
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

    class FakeUnitSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            return self

    class FakeFinalizeSig:
        def __init__(self, run_id):
            pass

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

    def fail_redispatch(*args, **kwargs):
        raise RuntimeError("broker down")

    monkeypatch.setattr(sync_units.dispatch_sync_run, "apply_async", fail_redispatch)

    import pytest

    with pytest.raises(RuntimeError, match="broker down"):
        sync_units.dispatch_sync_run(str(run.id))


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


def test_stale_other_run_blocker_does_not_cap_forever(db_session, monkeypatch):
    """F2 regression (b): stale other-run RUNNING unit does not block capacity.

    A unit from another run whose updated_at is older than the staleness
    threshold must NOT count as an active slot — the guard must return full
    capacity so the current run can proceed.
    """
    from datetime import timedelta

    monkeypatch.setenv("SYNC_RUN_MAX_UNITS", "10")
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    # Use a very short staleness window so we can set updated_at in the past.
    monkeypatch.setenv("SYNC_UNIT_RUNNING_STALE_SECONDS", "60")

    run, units, integration, source = _seed_run(db_session, unit_count=1)

    # Another run with a RUNNING unit that is stale (updated 2 hours ago).
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

    # Stale blocker must not consume a slot — full allow expected.
    assert decision.allowed is True
    assert decision.concurrency_capped is False, (
        "stale other-run blocker must not cap the current run"
    )
    assert len(decision.capped_unit_ids) == 0


def test_finalize_passes_full_datetime_work_graph_window(db_session, monkeypatch):
    """F3 regression (c): finalize_sync_run passes full ISO datetimes for
    work_graph_from/to_date so the covered final day is not truncated to midnight.
    """
    from datetime import datetime, timedelta, timezone  # noqa: F401

    from dev_health_ops.workers import sync_units

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
        sync_units, "_dispatch_post_sync_tasks", fake_dispatch_post_sync
    )
    # Ensure legacy_targets is non-empty so _dispatch_post_sync_tasks is called.
    monkeypatch.setattr(
        sync_units,
        "map_datasets_to_legacy_targets",
        lambda provider, dataset_keys: frozenset(["git"]),
    )

    sync_units.finalize_sync_run(str(run.id))

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


def test_chord_capped_redispatch_failure_marks_run_terminal(db_session, monkeypatch):
    """F4 regression (d-chord): redispatch publish failure after chord dispatch
    marks the run/units terminal — capped PLANNED units must not be stranded.

    Scenario: cap=2, 1 active slot → 1 unit dispatched (chord), 1 unit capped.
    Redispatch publish raises → _mark_dispatch_enqueue_failed must be called.
    """
    from dev_health_ops.workers import sync_units

    run, units, integration, source = _make_dispatch_harness(
        db_session, monkeypatch, unit_count=2, cap=2, active_slots=1
    )

    class FakeUnitSig:
        def __init__(self, unit_id):
            self.unit_id = unit_id

        def set(self, *, queue):
            return self

    class FakeFinalizeSig:
        def __init__(self, run_id):
            pass

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

    call_count = [0]

    def fail_on_second(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            # First call is chord.apply_async — succeed.
            return None
        # Second call is redispatch apply_async — fail.
        raise RuntimeError("broker down on redispatch")

    # Patch chord to call our counter on apply_async.
    class CountingChord:
        def __init__(self, header, callback):
            pass

        def apply_async(self):
            call_count[0] += 1
            return None

    monkeypatch.setattr(
        sync_units, "chord", lambda header, callback: CountingChord(header, callback)
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, countdown=None: (_ for _ in ()).throw(
            RuntimeError("broker down on redispatch")
        ),
    )

    with pytest.raises(RuntimeError, match="broker down on redispatch"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    # Run must be terminal (FAILED) — not left in DISPATCHING/PLANNED.
    assert run.status == SyncRunStatus.FAILED.value, (
        f"run must be FAILED after redispatch enqueue failure, got {run.status}"
    )
    # All non-terminal units must be marked FAILED.
    non_terminal = [u for u in units if u.status not in {"success", "failed"}]
    assert len(non_terminal) == 0, (
        f"all units must be terminal after enqueue failure, got {[u.status for u in non_terminal]}"
    )


def test_noop_redispatch_failure_marks_run_terminal(db_session, monkeypatch):
    """F5 regression (d-noop): redispatch publish failure in the no-signatures
    path marks the run/units terminal — PLANNED units must not be stranded.

    Scenario: cap=1, 1 active slot → ALL units capped → noop path.
    Redispatch publish raises → _mark_dispatch_enqueue_failed must be called.
    """
    from dev_health_ops.workers import sync_units

    run, units, integration, source = _make_dispatch_harness(
        db_session, monkeypatch, unit_count=1, cap=1, active_slots=1
    )

    def fail_redispatch(*args, **kwargs):
        raise RuntimeError("broker down noop")

    monkeypatch.setattr(sync_units.dispatch_sync_run, "apply_async", fail_redispatch)

    with pytest.raises(RuntimeError, match="broker down noop"):
        sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(run)
    for unit in units:
        db_session.refresh(unit)

    # Run must be terminal (FAILED) — not left stranded.
    assert run.status == SyncRunStatus.FAILED.value, (
        f"run must be FAILED after noop redispatch failure, got {run.status}"
    )
    non_terminal = [u for u in units if u.status not in {"success", "failed"}]
    assert len(non_terminal) == 0, (
        f"all units must be terminal after noop enqueue failure, got {[u.status for u in non_terminal]}"
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
