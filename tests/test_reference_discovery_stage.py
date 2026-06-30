from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
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
)
from dev_health_ops.sync.dispatch_outbox import (
    OUTBOX_KIND_DISCOVERY,
    OUTBOX_KIND_DISPATCH,
    OUTBOX_KIND_FINALIZE,
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


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def test_reference_discovery_success_stamps_ledger_and_arms_dispatch(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    _add_discovery(db_session, run)
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example/test")
    calls: list[dict[str, Any]] = []

    def strict_import(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "teams_imported": 1,
            "sprints_imported": 1,
            "reference_team_keys": ["ENG"],
            "reference_sprint_ids": ["linear:cycle:1"],
        }

    monkeypatch.setattr(
        reference_discovery, "run_team_autoimport_strict", strict_import
    )
    monkeypatch.setattr(
        reference_discovery, "_verify_reference_readback", lambda **_: None
    )

    result = reference_discovery.run_sync_reference_discovery(str(run.id))

    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    dispatch_outbox = _outbox_rows(db_session, run, OUTBOX_KIND_DISPATCH)
    assert result["status"] == "success"
    assert ledger.status == "success"
    assert ledger.completed_at is not None
    assert ledger.lease_owner is None
    assert len(dispatch_outbox) == 1
    assert dispatch_outbox[0].status == OUTBOX_STATUS_PENDING
    assert calls[0]["provider"] == "linear"
    assert calls[0]["scope"]["source_external_ids"] == ["ENG"]


def test_reference_discovery_fails_when_unit_source_inventory_is_incomplete(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, unit = _seed_unitized_run(db_session)
    unit.source_id = uuid.uuid4()
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)

    with pytest.raises(ValueError, match="source inventory incomplete"):
        reference_discovery._load_discovery_context(run.id)


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


@pytest.mark.parametrize(
    ("module_name", "message"),
    [
        ("team_autoimport_linear", "missing Linear credentials"),
        ("team_autoimport_jira", "missing Jira credentials"),
        ("team_autoimport_github", "missing GitHub credentials"),
        ("team_autoimport_gitlab", "missing GitLab credentials"),
    ],
)
def test_strict_reference_discovery_missing_credentials_raise(
    module_name: str, message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = __import__(f"dev_health_ops.workers.{module_name}", fromlist=["populate"])
    if module_name in {"team_autoimport_github", "team_autoimport_gitlab"}:
        monkeypatch.setattr(module, "_provider_capable", lambda: True)

    with pytest.raises(ValueError, match=message):
        module.populate(
            org_id="org-1",
            credentials={},
            scope={"strict_reference_discovery": True},
        )


def test_reference_discovery_readback_verifies_exact_team_and_sprint_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import reference_discovery

    queries: list[tuple[str, dict[str, Any]]] = []

    class FakeSink:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

        def query_dicts(
            self, query: str, parameters: dict[str, Any]
        ) -> list[dict[str, Any]]:
            queries.append((query, parameters))
            if "FROM teams" in query:
                return [{"native_team_key": key} for key in parameters["keys"]]
            if "FROM sprints" in query:
                return [{"sprint_id": sprint_id} for sprint_id in parameters["ids"]]
            raise AssertionError(f"unexpected readback query: {query}")

        def close(self) -> None:
            return None

    monkeypatch.setattr(reference_discovery, "ClickHouseMetricsSink", FakeSink)

    reference_discovery._verify_reference_readback(
        org_id="org-1",
        provider="linear",
        analytics_db_url="clickhouse://example/test",
        summary={
            "reference_team_keys": ["ENG", "OPS"],
            "reference_sprint_ids": ["linear:cycle:1"],
        },
    )

    assert len(queries) == 2
    assert "FROM teams" in queries[0][0]
    assert "FINAL" not in queries[0][0]
    assert "GROUP BY org_id, provider, native_team_key" in queries[0][0]
    assert queries[0][1] == {
        "org_id": "org-1",
        "provider": "linear",
        "keys": ["ENG", "OPS"],
    }
    assert "FROM sprints" in queries[1][0]
    assert "FINAL" not in queries[1][0]
    assert "argMax(native_team_key, last_synced) AS native_team_key" in queries[1][0]
    assert "GROUP BY org_id, provider, sprint_id" in queries[1][0]
    assert queries[1][1] == {
        "org_id": "org-1",
        "provider": "linear",
        "ids": ["linear:cycle:1"],
    }


def test_concurrent_reference_discovery_only_one_claims(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    _add_discovery(db_session, run)
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example/test")
    monkeypatch.setattr(
        reference_discovery,
        "run_team_autoimport_strict",
        lambda **_: {"reference_team_keys": [], "reference_sprint_ids": []},
    )
    monkeypatch.setattr(
        reference_discovery, "_verify_reference_readback", lambda **_: None
    )

    first = reference_discovery.run_sync_reference_discovery(str(run.id))
    second = reference_discovery.run_sync_reference_discovery(str(run.id))

    assert first["status"] == "success"
    assert second == {
        "status": "skipped",
        "sync_run_id": str(run.id),
        "reason": "not_claimed",
    }
    assert len(_outbox_rows(db_session, run, OUTBOX_KIND_DISPATCH)) == 1


def test_expired_running_discovery_reruns_and_dispatches_once(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, _unit = _seed_unitized_run(db_session)
    ledger = _add_discovery(db_session, run, status="running", attempts=1)
    ledger.lease_owner = "dead-worker"
    ledger.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.flush()
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example/test")
    calls = 0

    def strict_import(**_: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"reference_team_keys": [], "reference_sprint_ids": []}

    monkeypatch.setattr(
        reference_discovery, "run_team_autoimport_strict", strict_import
    )
    monkeypatch.setattr(
        reference_discovery, "_verify_reference_readback", lambda **_: None
    )

    first = reference_discovery.run_sync_reference_discovery(str(run.id))
    second = reference_discovery.run_sync_reference_discovery(str(run.id))

    db_session.refresh(ledger)
    assert first["status"] == "success"
    assert second["status"] == "skipped"
    assert ledger.attempts == 2
    assert calls == 1
    assert len(_outbox_rows(db_session, run, OUTBOX_KIND_DISPATCH)) == 1


def test_reference_discovery_failure_exhaustion_fails_units_and_run(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery
    from dev_health_ops.workers.sync_units import finalize_sync_run

    run, unit = _seed_unitized_run(db_session)
    _add_discovery(db_session, run)
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example/test")
    monkeypatch.setenv("SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS", "1")
    monkeypatch.setattr(
        reference_discovery,
        "run_team_autoimport_strict",
        lambda **_: (_ for _ in ()).throw(ValueError("provider discovery failed")),
    )

    result = reference_discovery.run_sync_reference_discovery(str(run.id))
    finalized = finalize_sync_run(str(run.id))

    db_session.refresh(run)
    db_session.refresh(unit)
    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    assert result["status"] == "failed"
    assert finalized["status"] == "finalized"
    assert ledger.status == "failed"
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result == {"error_category": "reference_discovery_failed"}
    assert run.status == SyncRunStatus.FAILED.value
    assert run.failed_units == 1
    assert _outbox_rows(db_session, run, OUTBOX_KIND_FINALIZE)


def test_reference_discovery_transient_failure_retries(
    db_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.workers import reference_discovery

    run, unit = _seed_unitized_run(db_session)
    _add_discovery(db_session, run)
    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example/test")
    monkeypatch.setenv("SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS", "5")
    monkeypatch.setattr(reference_discovery.random, "randint", lambda _a, _b: 0)
    monkeypatch.setattr(
        reference_discovery,
        "run_team_autoimport_strict",
        lambda **_: (_ for _ in ()).throw(TimeoutError("provider timeout")),
    )

    result = reference_discovery.run_sync_reference_discovery(str(run.id))

    db_session.refresh(unit)
    ledger = (
        db_session.query(SyncRunReferenceDiscovery).filter_by(sync_run_id=run.id).one()
    )
    assert result["status"] == "retrying"
    assert ledger.status == "retrying"
    assert ledger.attempts == 1
    assert ledger.lease_owner is None
    assert _aware(ledger.available_at) > datetime.now(timezone.utc)
    assert unit.status == SyncRunUnitStatus.PLANNED.value
    assert len(_outbox_rows(db_session, run, OUTBOX_KIND_DISCOVERY)) == 1


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
