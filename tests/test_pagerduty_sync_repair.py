from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationCredential,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
)
from dev_health_ops.models.settings import JobRun, JobRunStatus, SyncConfiguration
from dev_health_ops.sync import planner
from dev_health_ops.sync.execution_trigger import (
    create_sync_execution_trigger,
)
from dev_health_ops.sync.pagerduty_repair import (
    pagerduty_provider_instance_id,
    repair_pagerduty_operational_integration,
)
from dev_health_ops.sync.planner import SyncPlanRequest

_ORG_ID = "pagerduty-repair-org"
_OPERATIONAL_DATASETS = {
    "incidents",
    "services",
    "business-services",
    "escalation-policies",
    "schedules",
    "on-calls",
    "users",
    "teams",
    "incident-alerts",
    "incident-log-entries",
    "incident-notes",
}


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _integration_with_verified_account(session: Session) -> Integration:
    credential = IntegrationCredential(
        org_id=_ORG_ID,
        provider="pagerduty",
        name="arbitrary-config-name",
        config={"account_id": "Acme", "subdomain": "acme"},
        is_active=True,
    )
    session.add(credential)
    session.flush()
    integration = Integration(
        org_id=_ORG_ID,
        provider="pagerduty",
        credential_id=credential.id,
        name="operator selected name",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _legacy_source(session: Session, integration: Integration) -> IntegrationSource:
    source = IntegrationSource(
        org_id=_ORG_ID,
        integration_id=integration.id,
        provider="pagerduty",
        source_type="source",
        external_id="operator selected name",
        name="operator selected name",
        full_name="operator selected name",
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    session.add(source)
    session.add_all(
        [
            IntegrationDataset(
                org_id=_ORG_ID,
                integration_id=integration.id,
                dataset_key="services",
                is_enabled=False,
                options={},
            ),
            IntegrationDataset(
                org_id=_ORG_ID,
                integration_id=integration.id,
                dataset_key="not-pagerduty",
                is_enabled=True,
                options={},
            ),
        ]
    )
    session.flush()
    return source


def _operational_config(
    session: Session, integration: Integration
) -> SyncConfiguration:
    config = SyncConfiguration(
        org_id=_ORG_ID,
        name="PagerDuty operational",
        provider="pagerduty",
        sync_targets=["operational"],
        sync_options={},
        is_active=True,
        integration_id=integration.id,
    )
    session.add(config)
    session.flush()
    return config


def test_repair_uses_verified_subdomain_and_restores_operational_dataset_set(
    db_session: Session,
) -> None:
    # Given: a legacy target named from arbitrary operator configuration.
    integration = _integration_with_verified_account(db_session)
    legacy_source = _legacy_source(db_session, integration)
    _operational_config(db_session, integration)

    # When: PagerDuty persistence is repaired before planning.
    repair_pagerduty_operational_integration(db_session, integration)

    # Then: the canonical account identity is enabled and malformed rows are safe.
    sources = (
        db_session.query(IntegrationSource)
        .order_by(IntegrationSource.external_id)
        .all()
    )
    assert [(source.external_id, source.is_enabled) for source in sources] == [
        ("Acme", True),
        ("operator selected name", False),
    ]
    canonical_source = sources[0]
    assert canonical_source.source_type == "account"
    assert canonical_source.name == "Acme"
    assert canonical_source.full_name == "Acme"
    assert legacy_source.is_enabled is False
    datasets = db_session.query(IntegrationDataset).all()
    enabled_keys = {dataset.dataset_key for dataset in datasets if dataset.is_enabled}
    assert enabled_keys == _OPERATIONAL_DATASETS
    assert (
        len(
            [
                dataset
                for dataset in datasets
                if dataset.dataset_key in _OPERATIONAL_DATASETS
            ]
        )
        == 11
    )
    assert (
        next(
            dataset for dataset in datasets if dataset.dataset_key == "not-pagerduty"
        ).is_enabled
        is False
    )

    # When: the repair is retried after a resumable interruption.
    repair_pagerduty_operational_integration(db_session, integration)

    # Then: it is idempotent and never duplicates the canonical rows.
    assert db_session.query(IntegrationSource).count() == 2
    assert db_session.query(IntegrationDataset).count() == 12


def test_repair_disables_malformed_pagerduty_config_before_executable_plan(
    db_session: Session,
) -> None:
    # Given: a legacy PagerDuty config that targets only child incidents.
    integration = _integration_with_verified_account(db_session)
    config = SyncConfiguration(
        org_id=_ORG_ID,
        name="legacy incidents only",
        provider="pagerduty",
        sync_targets=["incidents"],
        sync_options={},
        is_active=True,
        integration_id=integration.id,
    )
    db_session.add(config)
    db_session.flush()

    # When: the planner prepares this integration.
    outcome = repair_pagerduty_operational_integration(db_session, integration)
    db_session.commit()

    # Then: it cannot be executed as a misleading partial PagerDuty sync.
    assert outcome is not None
    assert (
        outcome
        == "PagerDuty sync target must be operational; malformed configs were disabled"
    )
    assert config.is_active is False
    assert config.last_sync_success is False


def test_planner_repairs_existing_operational_target_missing_parent_incidents(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: an operational target persisted before the full PagerDuty dataset set.
    integration = _integration_with_verified_account(db_session)
    _legacy_source(db_session, integration)
    _operational_config(db_session, integration)
    monkeypatch.setattr(
        planner,
        "require_canonical_incident_feature_sync",
        lambda _session, _org_id: None,
    )
    monkeypatch.setattr(
        planner,
        "_resolve_credential_stamp",
        lambda _session, linked_integration: (
            linked_integration.credential_id,
            "credential-fingerprint",
            "integration_credential",
        ),
    )

    # When: a planner request turns the persisted target into executable work.
    plan = planner.plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=_ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )
    # Then: incidents and every operational child/reference dataset are planned.
    assert plan.total_units == 11
    assert {
        unit.dataset_key
        for unit in db_session.query(planner.SyncRunUnit)
        .filter_by(sync_run_id=plan.sync_run_id)
        .all()
    } == _OPERATIONAL_DATASETS


def test_planner_persists_terminal_pagerduty_disable_without_outbox(
    db_session: Session,
) -> None:
    # Given: a malformed PagerDuty target tied to an executable integration.
    integration = _integration_with_verified_account(db_session)
    config = SyncConfiguration(
        org_id=_ORG_ID,
        name="legacy incidents only",
        provider="pagerduty",
        sync_targets=["incidents"],
        sync_options={},
        is_active=True,
        integration_id=integration.id,
    )
    db_session.add(config)
    db_session.flush()

    # When: the public planner API receives the malformed integration.
    plan = planner.plan_sync_run(
        db_session,
        SyncPlanRequest(
            integration_id=str(integration.id),
            org_id=_ORG_ID,
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="manual",
        ),
    )

    # Then: repair evidence is terminal and requires no dispatch.
    sync_run = db_session.get(SyncRun, plan.sync_run_id)
    assert sync_run is not None
    assert plan.dispatch_required is False
    assert plan.total_units == 0
    assert plan.unit_ids == ()
    assert plan.terminal_reason == (
        "PagerDuty sync target must be operational; malformed configs were disabled"
    )
    assert sync_run.status == SyncRunStatus.FAILED.value
    assert sync_run.result == {"error_category": "pagerduty_sync_disabled"}
    assert sync_run.error == plan.terminal_reason
    assert sync_run.completed_at is not None
    assert db_session.query(SyncDispatchOutbox).count() == 0


def test_repaired_source_identity_matches_rest_and_webhook_paths(
    db_session: Session,
) -> None:
    # Given: a source repaired from the credential's account identity.
    integration = _integration_with_verified_account(db_session)
    _operational_config(db_session, integration)
    repair_pagerduty_operational_integration(db_session, integration)
    source = db_session.query(IntegrationSource).one()
    credential_config = {"account_id": "Acme", "subdomain": "acme"}

    # When: REST reads the source identity and webhook trust validation resolves it.
    rest_provider_instance_id = source.external_id
    webhook_provider_instance_id = pagerduty_provider_instance_id(credential_config)

    # Then: both ingestion paths write under the same canonical provider instance.
    assert rest_provider_instance_id == "Acme"
    assert webhook_provider_instance_id == rest_provider_instance_id


def test_verified_account_id_is_not_replaced_by_distinct_subdomain() -> None:
    assert (
        pagerduty_provider_instance_id(
            {"account_id": "account-123", "subdomain": "acme"}
        )
        == "account-123"
    )


def test_execution_trigger_commits_terminal_disabled_outcome(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dev_health_ops.sync.execution_trigger.require_canonical_incident_feature_sync",
        lambda _session, _org_id: None,
    )
    integration = _integration_with_verified_account(db_session)
    config = SyncConfiguration(
        org_id=_ORG_ID,
        name="legacy incidents only",
        provider="pagerduty",
        sync_targets=["incidents"],
        sync_options={},
        is_active=True,
        integration_id=integration.id,
    )
    db_session.add(config)
    db_session.flush()

    outcome = create_sync_execution_trigger(
        db_session,
        config,
        _ORG_ID,
        triggered_by="manual",
        mode="incremental",
    )
    db_session.commit()

    assert outcome is not None
    assert outcome.dispatch_required is False
    db_session.refresh(config)
    assert config.is_active is False
    job_run = db_session.get(JobRun, outcome.job_run_id)
    assert job_run is not None
    assert job_run.completed_at is not None
    sync_run = db_session.get(SyncRun, outcome.sync_run_id)
    assert sync_run is not None
    assert sync_run.total_units == 0
    assert sync_run.result == {"error_category": "pagerduty_sync_disabled"}
    assert sync_run.error == outcome.terminal_reason
    assert sync_run.completed_at is not None
    assert db_session.query(SyncDispatchOutbox).count() == 0


def test_scheduler_commits_pagerduty_disabled_evidence_without_enqueue(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.sync import execution_trigger
    from dev_health_ops.workers import sync_scheduler

    # Given: a due scheduled legacy PagerDuty configuration with a malformed target.
    integration = _integration_with_verified_account(db_session)
    config = SyncConfiguration(
        org_id=_ORG_ID,
        name="legacy incidents only",
        provider="pagerduty",
        sync_targets=["incidents"],
        sync_options={"schedule_cron": "* * * * *"},
        is_active=True,
        integration_id=integration.id,
    )
    db_session.add(config)
    config.last_sync_at = datetime(2026, 7, 20, 11, 58, tzinfo=timezone.utc)
    db_session.commit()
    dispatch = MagicMock()
    monkeypatch.setattr(sync_scheduler, "organization_exists_sync", lambda *_args: True)
    monkeypatch.setattr(
        sync_scheduler,
        "is_canonical_incident_feature_enabled_sync",
        lambda *_args: True,
    )
    monkeypatch.setattr(
        execution_trigger,
        "require_canonical_incident_feature_sync",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_units.dispatch_sync_run",
        dispatch,
    )

    # When: the scheduler evaluates the due configuration.
    dispatched = sync_scheduler._maybe_dispatch_config(
        db_session,
        config,
        datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    )

    # Then: terminal disabled evidence persists and no executable work is enqueued.
    assert dispatched is False
    db_session.refresh(config)
    assert config.is_active is False
    job_run = db_session.query(JobRun).one()
    assert job_run.status == JobRunStatus.FAILED.value
    assert isinstance(job_run.result, dict)
    assert job_run.result == {
        "sync_run_id": job_run.result["sync_run_id"],
        "terminal_status": "pagerduty_sync_disabled",
        "reason": "PagerDuty sync target must be operational; malformed configs were disabled",
        "total_units": 0,
    }
    sync_run = db_session.get(SyncRun, job_run.result["sync_run_id"])
    assert sync_run is not None
    assert sync_run.status == "failed"
    assert sync_run.total_units == 0
    assert sync_run.result == {"error_category": "pagerduty_sync_disabled"}
    assert sync_run.completed_at is not None
    assert db_session.query(SyncDispatchOutbox).count() == 0
    dispatch.apply_async.assert_not_called()


def test_repair_leaves_other_provider_rows_untouched(db_session: Session) -> None:
    # Given: an unrelated provider with a source and a disabled dataset.
    integration = Integration(
        org_id=_ORG_ID,
        provider="github",
        name="GitHub",
        config={},
        is_active=True,
    )
    db_session.add(integration)
    db_session.flush()
    source = IntegrationSource(
        org_id=_ORG_ID,
        integration_id=integration.id,
        provider="github",
        source_type="repository",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
    )
    dataset = IntegrationDataset(
        org_id=_ORG_ID,
        integration_id=integration.id,
        dataset_key="incidents",
        is_enabled=False,
        options={},
    )
    db_session.add_all([source, dataset])
    db_session.flush()

    # When: the PagerDuty repair is reached by a generic planner invocation.
    repair_pagerduty_operational_integration(db_session, integration)

    # Then: other providers retain their independently configured dataset state.
    assert source.is_enabled is True
    assert dataset.is_enabled is False
