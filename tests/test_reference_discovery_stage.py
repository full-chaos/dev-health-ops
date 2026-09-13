from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

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
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_KIND_DISPATCH,
    OUTBOX_STATUS_PENDING,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch: pytest.MonkeyPatch, session: Session) -> None:
    import dev_health_ops.db as db

    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _seed_unitized_run(
    session: Session,
    *,
    provider: str = "linear",
    mode: str = SyncRunMode.INCREMENTAL.value,
    dataset_key: str = "work-items",
    external_id: str = "ENG",
) -> tuple[SyncRun, SyncRunUnit]:
    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider=provider,
        name=f"{provider} integration",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider=provider,
        source_type="team" if provider == "linear" else "repo",
        external_id=external_id,
        name=external_id,
        full_name=external_id,
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    run = SyncRun(
        org_id=org_id,
        integration_id=integration.id,
        triggered_by="manual",
        mode=mode,
        status=SyncRunStatus.PLANNED.value,
        total_units=1,
        completed_units=0,
        failed_units=0,
    )
    session.add_all([source, run])
    session.flush()
    unit = SyncRunUnit(
        org_id=org_id,
        sync_run_id=run.id,
        integration_id=integration.id,
        source_id=source.id,
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode=mode,
        status=SyncRunUnitStatus.PLANNED.value,
        attempts=0,
    )
    session.add(unit)
    session.flush()
    return run, unit


def _add_discovery(
    session: Session,
    run: SyncRun,
    *,
    status: str = "planned",
    attempts: int = 0,
    available_at: datetime | None = None,
) -> SyncRunReferenceDiscovery:
    ledger = SyncRunReferenceDiscovery(
        org_id=str(run.org_id),
        sync_run_id=run.id,
        status=status,
        attempts=attempts,
        available_at=available_at or datetime.now(timezone.utc),
    )
    session.add(ledger)
    session.flush()
    return ledger


def _outbox_rows(session: Session, run: SyncRun, kind: str) -> list[SyncDispatchOutbox]:
    return (
        session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=kind)
        .order_by(SyncDispatchOutbox.id)
        .all()
    )


def test_dispatch_sync_run_pre_discovery_blocks_and_claims_no_units(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers.sync_units import dispatch_sync_run

    run, unit = _seed_unitized_run(db_session)
    _patch_db_session(monkeypatch, db_session)

    result = dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    discovery_outbox = _outbox_rows(db_session, run, OUTBOX_KIND_DISCOVERY)
    assert result["status"] == "blocked_on_reference_discovery"
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert ledger.status == "planned"
    assert len(discovery_outbox) == 1
    assert discovery_outbox[0].status == OUTBOX_STATUS_PENDING
    assert _outbox_rows(db_session, run, OUTBOX_KIND_DISPATCH) == []


def test_sync_task_bootstrap_marks_linear_provider_name_source_as_org_wide(
    db_session: Session,
) -> None:
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

    run, unit = _seed_unitized_run(db_session, external_id="linear")
    integration = db_session.query(Integration).filter_by(id=run.integration_id).one()
    integration.config = {"auto_import_teams": False}
    source = db_session.query(IntegrationSource).filter_by(id=unit.source_id).one()
    source.source_type = "project"
    source.metadata_ = {"planner_managed_sync_config_id": str(uuid.uuid4())}
    db_session.flush()

    context = SyncTaskBootstrap.load(db_session, str(unit.id))

    assert context.source_external_id == "linear"
    assert context.source_is_org_wide_placeholder is True


def test_sync_task_bootstrap_keeps_explicit_provider_name_source_scoped(
    db_session: Session,
) -> None:
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

    run, unit = _seed_unitized_run(db_session, external_id="linear")
    integration = db_session.query(Integration).filter_by(id=run.integration_id).one()
    integration.config = {"team_id": "linear"}
    source = db_session.query(IntegrationSource).filter_by(id=unit.source_id).one()
    source.source_type = "project"
    source.metadata_ = {"planner_managed_sync_config_id": str(uuid.uuid4())}
    db_session.flush()

    context = SyncTaskBootstrap.load(db_session, str(unit.id))

    assert context.source_external_id == "linear"
    assert context.source_is_org_wide_placeholder is False


def test_backfill_runner_dispatch_path_blocks_until_discovery(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.backfill import runner

    org_id = str(uuid.uuid4())
    integration = Integration(
        org_id=org_id,
        provider="linear",
        name="Linear integration",
        config={},
        is_active=True,
    )
    db_session.add(integration)
    db_session.flush()
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="linear",
        source_type="team",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key="work-items",
        is_enabled=True,
        options={},
    )
    db_session.add_all([source, dataset])
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(
        runner, "get_postgres_session_sync", lambda: _fake_session_ctx(db_session)
    )

    result = runner.run_backfill_via_planner(
        str(integration.id),
        date(2026, 6, 1),
        date(2026, 6, 2),
        org_id=org_id,
        source_ids=(str(source.id),),
        dataset_keys=("work-items",),
        triggered_by="test",
    )

    assert result["dispatch"]["status"] == "blocked_on_reference_discovery"
    run = db_session.get(SyncRun, uuid.UUID(result["sync_run_id"]))
    assert run is not None
    assert _outbox_rows(db_session, run, OUTBOX_KIND_DISCOVERY)
    assert _outbox_rows(db_session, run, OUTBOX_KIND_DISPATCH) == []


# CHAOS-5351: test_seed_reference_discovery_run_arms_ledger_and_outbox_like_plan_sync_run
# (a dedicated unit test of sync.planner.seed_reference_discovery_run)
# deleted along with the function itself -- its only caller was the legacy
# backfill path (run_backfill_for_config /
# _run_strict_reference_discovery_for_backfill), already deleted earlier in
# this ticket; the backfill tool now dispatches through plan_sync_run like
# every other sync run, which arms the SAME
# SyncRunReferenceDiscovery/OUTBOX_KIND_DISCOVERY seeding via
# seed_reference_discovery_ledger directly -- see
# test_backfill_runner_dispatch_path_blocks_until_discovery above for the
# surviving executed proof of that path.


def test_await_reference_discovery_terminal_returns_success_with_result(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    ledger = _add_discovery(db_session, run)
    _patch_db_session(monkeypatch, db_session)

    ledger.status = "success"
    ledger.result = {"status": "success", "teams_imported": 1}
    db_session.flush()

    outcome = reference_discovery.await_reference_discovery_terminal(
        str(run.id), poll_interval=0.01
    )

    assert outcome == {
        "outcome": "success",
        "sync_run_id": str(run.id),
        "result": {"status": "success", "teams_imported": 1},
    }


def test_await_reference_discovery_terminal_returns_failed_with_reason(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    ledger = _add_discovery(db_session, run)
    _patch_db_session(monkeypatch, db_session)

    ledger.status = "failed"
    ledger.error = "Reference discovery failed"
    db_session.flush()

    outcome = reference_discovery.await_reference_discovery_terminal(
        str(run.id), poll_interval=0.01
    )

    assert outcome == {
        "outcome": "failed",
        "sync_run_id": str(run.id),
        "reason": "Reference discovery failed",
    }


def test_await_reference_discovery_terminal_reports_not_claimed_past_lease_bound(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CHAOS-4498: a row stuck PLANNED past one full lease window (no
    worker ever claimed it) reports not_claimed -- fails closed, never
    falls back to calling the Python populator directly. The bound is the
    Go service's own SYNC_REFERENCE_DISCOVERY_LEASE_SECONDS constant
    (_discovery_lease_seconds), shrunk here so the test runs in
    milliseconds, not an invented separate constant."""
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    _add_discovery(db_session, run)  # stays "planned" -- never claimed
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(reference_discovery, "_discovery_lease_seconds", lambda: 0.05)

    outcome = reference_discovery.await_reference_discovery_terminal(
        str(run.id), poll_interval=0.01
    )

    assert outcome == {"outcome": "not_claimed", "sync_run_id": str(run.id)}


def test_await_reference_discovery_terminal_reports_timeout_running_past_lifetime_bound(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CHAOS-4498: a row claimed (RUNNING) but never reaching terminal
    within the Go service's own max-lifetime bound
    (_max_discovery_lifetime_seconds) reports timeout_running, not a silent
    fallback."""
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    ledger = _add_discovery(db_session, run)
    ledger.status = "running"
    ledger.lease_owner = str(uuid.uuid4())
    ledger.lease_expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setattr(
        reference_discovery, "_max_discovery_lifetime_seconds", lambda: 0.05
    )

    outcome = reference_discovery.await_reference_discovery_terminal(
        str(run.id), poll_interval=0.01
    )

    assert outcome == {"outcome": "timeout_running", "sync_run_id": str(run.id)}
