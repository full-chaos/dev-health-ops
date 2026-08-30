"""CHAOS-4602: create_sync_execution_trigger's Go hand-off path.

Covers the rollout-flag-gated branch that mints a scheduled_sync_occurrences
row (+ its sync_manual_triggers payload) instead of calling plan_sync_run
in-process, and await_sync_execution_trigger_materialized's three typed
outcomes (materialized / pending-on-deadline / quarantined). All three are
modeled on await_reference_discovery_terminal (CHAOS-4498)'s shape, adapted
to be genuinely async (asyncio.sleep, not time.sleep) since this function is
called from the admin router's request/response cycle.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import Integration, SyncRun
from dev_health_ops.models.settings import (
    SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED,
    SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED,
    ScheduledSyncOccurrence,
    SyncConfiguration,
    SyncManualTrigger,
)
from dev_health_ops.sync.execution_trigger import (
    await_sync_execution_trigger_materialized,
    create_sync_execution_trigger,
)


class _FakeAsyncSession:
    """Minimal async-session shim over a sync SQLAlchemy Session -- the same
    shape tests/test_sync_manual_trigger_safety.py uses, since
    await_sync_execution_trigger_materialized's whole point is to run
    against the SAME AsyncSession.run_sync/commit surface the router uses.
    """

    def __init__(self, sync_session: Session):
        self._s = sync_session

    async def run_sync(self, fn, *args, **kwargs):
        return fn(self._s, *args, **kwargs)

    async def commit(self) -> None:
        self._s.commit()


def _seed_planner_managed_config(
    session: Session, org_id: str = "org-a"
) -> SyncConfiguration:
    integration = Integration(
        org_id=org_id,
        provider="jira",
        name=f"integration-{uuid.uuid4()}",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    config = SyncConfiguration(
        org_id=org_id,
        name="planner-managed-backfill",
        provider="jira",
        sync_targets=["work-items"],
        sync_options={},
        is_active=True,
        integration_id=integration.id,
        planner_managed=True,
    )
    session.add(config)
    session.flush()
    return config


@pytest.fixture
def sqlite_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            yield session
    finally:
        engine.dispose()


def test_go_handoff_mints_occurrence_and_trigger_row_when_flag_on(
    sqlite_session, monkeypatch
):
    """The rollout flag on + planner_managed=True routes to the Go hand-off:
    no plan_sync_run call, a pending occurrence + trigger row instead."""
    monkeypatch.setenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "true")
    config = _seed_planner_managed_config(sqlite_session)

    result = create_sync_execution_trigger(
        sqlite_session, config, "org-a", triggered_by="manual", mode="incremental"
    )
    sqlite_session.flush()

    assert result is not None
    assert result.occurrence_id is not None
    assert result.awaiting_materialization is True
    assert result.sync_run_id == ""
    assert result.job_run_id == ""

    occurrence = (
        sqlite_session.query(ScheduledSyncOccurrence)
        .filter(ScheduledSyncOccurrence.occurrence_id == result.occurrence_id)
        .one()
    )
    assert occurrence.reconcile_status == "pending"
    assert occurrence.job_run_id is None
    assert occurrence.sync_run_id is None

    trigger_row = (
        sqlite_session.query(SyncManualTrigger)
        .filter(SyncManualTrigger.occurrence_id == result.occurrence_id)
        .one()
    )
    assert trigger_row.mode == "incremental"
    assert trigger_row.triggered_by == "manual"


def test_go_handoff_disabled_by_default_even_for_planner_managed_config(
    sqlite_session, monkeypatch
):
    """Fork 1/rollout-flag default-off: a planner_managed config still goes
    through the pre-existing in-process path unless the flag is explicitly
    on -- this is what makes the rollout safe to ship dark."""
    monkeypatch.delenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", raising=False)
    config = _seed_planner_managed_config(sqlite_session)

    result = create_sync_execution_trigger(
        sqlite_session, config, "org-a", triggered_by="manual", mode="incremental"
    )

    assert result is not None
    assert result.occurrence_id is None
    assert result.awaiting_materialization is False
    assert sqlite_session.query(SyncManualTrigger).count() == 0, (
        "no sync_manual_triggers row should exist on the legacy in-process path"
    )


@pytest.mark.asyncio
async def test_await_materialized_returns_immediately_when_already_completed(
    sqlite_session,
):
    """If Go already reconciled the occurrence by the time the first poll
    runs, the outcome resolves on the first iteration -- no sleep needed."""
    config = _seed_planner_managed_config(sqlite_session)
    from dev_health_ops.sync.execution_trigger import (
        _create_go_manual_sync_execution_trigger,
    )
    from dev_health_ops.sync.planner import SyncPlanRequest

    request = SyncPlanRequest(
        integration_id=str(config.integration_id),
        org_id="org-a",
        mode="incremental",
        triggered_by="manual",
    )
    minted = _create_go_manual_sync_execution_trigger(
        sqlite_session, config, "org-a", request
    )
    assert minted.occurrence_id is not None
    sqlite_session.commit()

    job_run_id = uuid.uuid4()
    sync_run_id = uuid.uuid4()
    sqlite_session.add(
        SyncRun(
            id=sync_run_id,
            org_id="org-a",
            integration_id=config.integration_id,
            triggered_by="manual",
            mode="incremental",
            status="planned",
            total_units=4,
            completed_units=0,
            failed_units=0,
            created_at=datetime.now(timezone.utc),
        )
    )
    occurrence = (
        sqlite_session.query(ScheduledSyncOccurrence)
        .filter(ScheduledSyncOccurrence.occurrence_id == minted.occurrence_id)
        .one()
    )
    occurrence.job_run_id = job_run_id
    occurrence.sync_run_id = sync_run_id
    occurrence.reconcile_status = SCHEDULED_OCCURRENCE_RECONCILE_COMPLETED
    sqlite_session.commit()

    outcome = await await_sync_execution_trigger_materialized(
        cast(Any, _FakeAsyncSession(sqlite_session)),
        minted.occurrence_id,
        poll_interval=0.01,
    )

    assert outcome.awaiting_materialization is False
    assert outcome.quarantined is False
    assert outcome.sync_run_id == str(sync_run_id)
    assert outcome.job_run_id == str(job_run_id)
    assert outcome.total_units == 4
    assert outcome.dispatch_required is True


@pytest.mark.asyncio
async def test_await_materialized_reports_quarantined_as_client_visible_failure(
    sqlite_session,
):
    """Go quarantining the occurrence (identity conflict or retry exhaustion)
    must surface as a client-visible failure -- never a silent 'pending'."""
    config = _seed_planner_managed_config(sqlite_session)
    from dev_health_ops.sync.execution_trigger import (
        _create_go_manual_sync_execution_trigger,
    )
    from dev_health_ops.sync.planner import SyncPlanRequest

    request = SyncPlanRequest(
        integration_id=str(config.integration_id),
        org_id="org-a",
        mode="incremental",
        triggered_by="manual",
    )
    minted = _create_go_manual_sync_execution_trigger(
        sqlite_session, config, "org-a", request
    )
    assert minted.occurrence_id is not None
    sqlite_session.commit()

    occurrence = (
        sqlite_session.query(ScheduledSyncOccurrence)
        .filter(ScheduledSyncOccurrence.occurrence_id == minted.occurrence_id)
        .one()
    )
    occurrence.reconcile_status = SCHEDULED_OCCURRENCE_RECONCILE_QUARANTINED
    occurrence.reconcile_error_code = "planner_error"
    occurrence.reconcile_error_at = datetime.now(timezone.utc)
    sqlite_session.commit()

    outcome = await await_sync_execution_trigger_materialized(
        cast(Any, _FakeAsyncSession(sqlite_session)),
        minted.occurrence_id,
        poll_interval=0.01,
    )

    assert outcome.quarantined is True
    assert outcome.awaiting_materialization is False
    assert "planner_error" in outcome.terminal_reason
    assert outcome.dispatch_required is False


@pytest.mark.asyncio
async def test_await_materialized_returns_pending_on_deadline_never_an_error(
    sqlite_session, monkeypatch
):
    """An occurrence that never leaves 'pending' within the bounded window
    resolves to awaiting_materialization=True -- never raises, never blocks
    past the configured deadline."""
    monkeypatch.setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", "0.05")
    config = _seed_planner_managed_config(sqlite_session)
    from dev_health_ops.sync.execution_trigger import (
        _create_go_manual_sync_execution_trigger,
    )
    from dev_health_ops.sync.planner import SyncPlanRequest

    request = SyncPlanRequest(
        integration_id=str(config.integration_id),
        org_id="org-a",
        mode="incremental",
        triggered_by="manual",
    )
    minted = _create_go_manual_sync_execution_trigger(
        sqlite_session, config, "org-a", request
    )
    assert minted.occurrence_id is not None
    sqlite_session.commit()

    outcome = await await_sync_execution_trigger_materialized(
        cast(Any, _FakeAsyncSession(sqlite_session)),
        minted.occurrence_id,
        poll_interval=0.01,
    )

    assert outcome.awaiting_materialization is True
    assert outcome.quarantined is False
    assert outcome.occurrence_id == minted.occurrence_id


@pytest.mark.asyncio
async def test_await_materialized_never_blocks_the_event_loop(
    sqlite_session, monkeypatch
):
    """CHAOS-4602 fork 2 ruling: the poll must be genuinely async
    (asyncio.sleep), never a blocking time.sleep -- this function runs
    inside the admin router's real request/response cycle, where a
    blocking sleep would stall EVERY concurrent request for the whole
    await window, not just this one.

    Proof, by WALL-CLOCK time, not raw tick count: asyncio timers are never
    dropped, only delayed, so a concurrent ticker task eventually completes
    its fixed number of ticks regardless of whether the other task blocked
    the loop in between -- a tick-count assertion alone cannot tell a
    genuinely concurrent run from a blocking one that merely finishes
    later. What DOES tell them apart is total elapsed time: run the bounded
    await (a never-completing occurrence, so it always runs its full
    deadline) CONCURRENTLY with an independent ticker task on a shorter,
    unrelated interval. A genuinely async poll overlaps the two, so total
    elapsed is close to max(poll deadline, ticker duration). A blocking
    time.sleep serializes them instead (each of the poll's blocking slices
    delays the ticker's queued wakeups), so elapsed drifts toward roughly
    their SUM. Empirically measured: ~0.28s genuinely async, ~0.49s with a
    known-bad time.sleep() substituted in -- asserting well under the
    sum (which would be ~0.45s here) is a tight, real discriminator.
    """
    monkeypatch.setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", "0.2")
    config = _seed_planner_managed_config(sqlite_session)
    from dev_health_ops.sync.execution_trigger import (
        _create_go_manual_sync_execution_trigger,
    )
    from dev_health_ops.sync.planner import SyncPlanRequest

    request = SyncPlanRequest(
        integration_id=str(config.integration_id),
        org_id="org-a",
        mode="incremental",
        triggered_by="manual",
    )
    minted = _create_go_manual_sync_execution_trigger(
        sqlite_session, config, "org-a", request
    )
    assert minted.occurrence_id is not None
    sqlite_session.commit()

    ticks = 0

    async def _ticker() -> None:
        nonlocal ticks
        for _ in range(50):
            await asyncio.sleep(0.005)
            ticks += 1

    started = asyncio.get_event_loop().time()
    outcome, _ = await asyncio.gather(
        await_sync_execution_trigger_materialized(
            cast(Any, _FakeAsyncSession(sqlite_session)),
            minted.occurrence_id,
            poll_interval=0.02,
        ),
        _ticker(),
    )
    elapsed = asyncio.get_event_loop().time() - started

    assert outcome.awaiting_materialization is True
    assert ticks == 50, f"ticker only completed {ticks}/50 ticks"
    # ticker alone needs ~0.25s (50 * 0.005s); the poll's deadline is 0.2s.
    # Concurrent: elapsed ~= max(0.25, 0.2) ~= 0.25-0.3s. Serialized behind
    # a blocking sleep: elapsed drifts toward the SUM, ~0.45-0.5s.
    assert elapsed < 0.4, (
        f"elapsed={elapsed:.3f}s is close to the serialized sum (~0.45s), "
        "not the concurrent max (~0.25-0.3s) -- the event loop was blocked"
    )
