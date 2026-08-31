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


def _seed_child_config(
    session: Session, org_id: str = "org-a", *, with_explicit_source: bool
) -> SyncConfiguration:
    """CHAOS-4604: a non-planner-managed CHILD config, optionally pinned to
    one explicit IntegrationSource -- the shape the routing gate's widening
    targets. Deliberately does not create an actual IntegrationSource row:
    the routing decision reads only config.source_id's presence, and sqlite
    (this fixture's engine) does not enforce the FK by default."""
    integration = Integration(
        org_id=org_id,
        provider="github",
        name=f"integration-{uuid.uuid4()}",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    config = SyncConfiguration(
        org_id=org_id,
        name="child-config",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        is_active=True,
        integration_id=integration.id,
        planner_managed=False,
        source_id=uuid.uuid4() if with_explicit_source else None,
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


def test_go_handoff_enabled_by_default_for_planner_managed_config(
    sqlite_session, monkeypatch
):
    """CHAOS-4629 (chris ruling, 2026-08-31 06:20 PT): the rollout flag's
    default flipped PERMANENTLY ON now that this parity ticket has landed --
    a planner_managed config routes to the Go hand-off with NO env var set
    at all, pinning the new default so a future edit that silently reverts
    it fails this test instead of shipping unnoticed."""
    monkeypatch.delenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", raising=False)
    config = _seed_planner_managed_config(sqlite_session)

    result = create_sync_execution_trigger(
        sqlite_session, config, "org-a", triggered_by="manual", mode="incremental"
    )
    sqlite_session.flush()

    assert result is not None
    assert result.occurrence_id is not None
    assert result.awaiting_materialization is True
    assert sqlite_session.query(SyncManualTrigger).count() == 1, (
        "default-true means the Go hand-off path mints a sync_manual_triggers "
        "row with no env var set at all"
    )


def test_go_handoff_can_still_be_explicitly_disabled(sqlite_session, monkeypatch):
    """The escape hatch survives the default flip: an operator/ops override
    explicitly setting the flag to a falsy value still routes to the legacy
    in-process path, exactly like the old default-off behavior did."""
    monkeypatch.setenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "false")
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


def test_go_handoff_never_intercepts_an_ordinary_scheduled_cron_tick(
    sqlite_session, monkeypatch
):
    """Codex review (gate round 9, P1): create_sync_execution_trigger is
    also the ordinary scheduled-cron path's call target --
    create_scheduled_sync_execution_trigger (sync_scheduler.py) passes
    triggered_by="schedule" straight through. Before this fix, the Go
    hand-off branch checked only config.planner_managed and the rollout
    flag, with no check on triggered_by at all -- flipping the flag on for
    an org would ALSO route every regular scheduled cron tick for a
    planner-managed config into the Go hand-off, which writes
    triggered_by verbatim into sync_manual_triggers.triggered_by, a value
    the CHECK constraint (settings.py, 'manual'/'backfill' only) rejects
    outright. This must take the legacy in-process path instead, exactly
    like the flag-off case, regardless of the flag."""
    monkeypatch.setenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "true")
    config = _seed_planner_managed_config(sqlite_session)

    result = create_sync_execution_trigger(
        sqlite_session, config, "org-a", triggered_by="schedule", mode="incremental"
    )

    assert result is not None
    assert result.occurrence_id is None
    assert result.awaiting_materialization is False
    assert sqlite_session.query(SyncManualTrigger).count() == 0, (
        "an ordinary scheduled cron tick must never mint a sync_manual_triggers "
        "row, even with the rollout flag on -- it would violate "
        "ck_sync_manual_triggers_triggered_by ('manual'/'backfill' only)"
    )
    assert sqlite_session.query(ScheduledSyncOccurrence).count() == 0, (
        "the Go hand-off's own occurrence row must not be minted either"
    )


def test_go_handoff_routes_non_planner_managed_child_config_with_explicit_source(
    sqlite_session, monkeypatch
):
    """CHAOS-4604: a non-planner-managed CHILD config pinned to one explicit
    source_id routes to the Go hand-off exactly like a planner-managed
    parent does -- the ticket's own target shape, and the Go-side
    Materialize gate's mirror-image widening (materializer.go)."""
    monkeypatch.setenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "true")
    config = _seed_child_config(sqlite_session, with_explicit_source=True)

    result = create_sync_execution_trigger(
        sqlite_session, config, "org-a", triggered_by="manual", mode="incremental"
    )
    sqlite_session.flush()

    assert result is not None
    assert result.occurrence_id is not None
    assert result.awaiting_materialization is True

    trigger_row = (
        sqlite_session.query(SyncManualTrigger)
        .filter(SyncManualTrigger.occurrence_id == result.occurrence_id)
        .one()
    )
    assert trigger_row.triggered_by == "manual"


def test_go_handoff_never_routes_non_planner_managed_config_without_source_id(
    sqlite_session, monkeypatch
):
    """Regression guard for the structural property CHAOS-4604 calls out: a
    legacy, fully-unscoped non-planner-managed config (source_id NULL) is
    NOT one of the two shapes the routing gate admits, even with the flag
    on and triggered_by='manual' -- it must keep falling through to the
    pre-existing in-process plan_sync_run path, exactly as before this
    ticket. A bug that widened this gate by accident would otherwise send an
    unscoped config's occurrence into the Go materializer, which itself
    still refuses it (ErrOccurrenceIneligible) -- but this test pins the
    ROUTING decision, not just the Go-side backstop."""
    monkeypatch.setenv("SYNC_GO_MANUAL_BACKFILL_PLANNER_ENABLED", "true")
    config = _seed_child_config(sqlite_session, with_explicit_source=False)

    result = create_sync_execution_trigger(
        sqlite_session, config, "org-a", triggered_by="manual", mode="incremental"
    )

    assert result is not None
    assert result.occurrence_id is None
    assert result.awaiting_materialization is False
    assert sqlite_session.query(SyncManualTrigger).count() == 0, (
        "an unscoped, non-planner-managed config (source_id NULL) is not one "
        "of the two shapes the routing gate admits, even with the flag on -- "
        "it must fall through to the legacy in-process plan_sync_run path"
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
async def test_await_materialized_reports_invalid_plan_quarantine_as_client_visible_failure(
    sqlite_session,
):
    """`invalid_plan` (occurrence_reconciler.go's fast-quarantine code for
    ErrInvalidPlan, added by migrations 0120/0121, CHAOS-4602 gate round 6)
    must be representable in a SQLite fixture built from this model's own
    __table_args__, not just in a real Postgres DB migrated through
    alembic. Codex review (gate round 13, P3): the ORM-declared
    ck_scheduled_sync_occurrence_reconcile_error_code had drifted from what
    those migrations installed -- EXECUTED repro: inserting this exact row
    against the pre-fix constraint raised `IntegrityError: CHECK constraint
    failed: ck_scheduled_sync_occurrence_reconcile_error_code`. Sibling of
    test_await_materialized_reports_quarantined_as_client_visible_failure
    above, same shape, different reconcile_error_code."""
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
    occurrence.reconcile_error_code = "invalid_plan"
    occurrence.reconcile_error_at = datetime.now(timezone.utc)
    sqlite_session.commit()

    outcome = await await_sync_execution_trigger_materialized(
        cast(Any, _FakeAsyncSession(sqlite_session)),
        minted.occurrence_id,
        poll_interval=0.01,
    )

    assert outcome.quarantined is True
    assert outcome.awaiting_materialization is False
    assert "invalid_plan" in outcome.terminal_reason
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


def test_manual_trigger_await_seconds_rejects_non_finite_values(monkeypatch):
    """Codex review (gate round 9, P2): float("inf")/"nan" both parse
    successfully via float(raw), and "inf" > 0 is True, so the old check
    (value > 0) alone accepted them -- an infinite deadline breaks the
    bounded-await contract fork 2 requires: the poll loop's own
    `time.monotonic() >= deadline` check can never become true, so it
    would hang the admin request's coroutine/connection indefinitely on an
    occurrence that never completes."""
    from dev_health_ops.sync.execution_trigger import (
        _DEFAULT_MANUAL_TRIGGER_AWAIT_SECONDS,
        _manual_trigger_await_seconds,
    )

    for non_finite in ("inf", "-inf", "Infinity", "nan"):
        monkeypatch.setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", non_finite)
        assert (
            _manual_trigger_await_seconds() == _DEFAULT_MANUAL_TRIGGER_AWAIT_SECONDS
        ), (
            f"SYNC_MANUAL_TRIGGER_AWAIT_SECONDS={non_finite!r} must fall back "
            "to the default, not produce a deadline that never elapses"
        )


@pytest.mark.asyncio
async def test_await_materialized_respects_a_configured_bound_shorter_than_the_poll_interval(
    sqlite_session, monkeypatch
):
    """Codex review (gate round 9, P2): the poll loop slept the full
    poll_interval unconditionally, so a configured deadline shorter than
    it overshot by up to one whole poll cycle (measured: 0.05s configured,
    ~0.252s actual with the default 0.25s poll_interval -- 5x over the
    promised bound). The sleep must be capped at whatever time actually
    remains until the deadline."""
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

    started = asyncio.get_event_loop().time()
    outcome = await await_sync_execution_trigger_materialized(
        cast(Any, _FakeAsyncSession(sqlite_session)),
        minted.occurrence_id,
        # Deliberately the DEFAULT poll_interval (0.25s), not a small
        # override -- this is exactly the shape that overshot before the
        # fix (a caller's configured deadline is shorter than the fixed
        # poll granularity).
    )
    elapsed = asyncio.get_event_loop().time() - started

    assert outcome.awaiting_materialization is True
    assert elapsed < 0.15, (
        f"elapsed={elapsed:.3f}s, want close to the configured 0.05s bound, "
        "not the ~0.25s+ a full unconditional poll_interval sleep would produce"
    )


@pytest.mark.asyncio
async def test_await_materialized_records_outcome_and_latency_telemetry(
    sqlite_session, monkeypatch
):
    """CHAOS-4602 fork 2: 'telemetry: counter + histogram on await
    outcome/latency' -- a codex review finding caught that all three
    terminal branches returned without ever recording either."""
    from dev_health_ops.metrics.prometheus import (
        SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS,
        SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL,
    )
    from dev_health_ops.sync.execution_trigger import (
        _create_go_manual_sync_execution_trigger,
    )
    from dev_health_ops.sync.planner import SyncPlanRequest

    monkeypatch.setenv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS", "0.05")
    config = _seed_planner_managed_config(sqlite_session)
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

    before_count = SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL.labels(
        outcome="pending"
    )._value.get()
    before_observations = SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS.labels(
        outcome="pending"
    )._sum.get()

    outcome = await await_sync_execution_trigger_materialized(
        cast(Any, _FakeAsyncSession(sqlite_session)),
        minted.occurrence_id,
        poll_interval=0.01,
    )
    assert outcome.awaiting_materialization is True

    after_count = SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL.labels(
        outcome="pending"
    )._value.get()
    after_observations = SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS.labels(
        outcome="pending"
    )._sum.get()
    assert after_count == before_count + 1, (
        f"pending outcome counter did not increment: before={before_count} after={after_count}"
    )
    assert after_observations > before_observations, (
        "pending latency histogram recorded no observation"
    )
