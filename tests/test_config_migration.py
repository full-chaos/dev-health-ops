from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    Setting,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.sync.config_migration import (
    MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
    migrate_configs_to_integrations,
)

ORG_ID = "migration-org"


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _parent(session: Session, provider: str = "github") -> SyncConfiguration:
    config = SyncConfiguration(
        org_id=ORG_ID,
        name=f"{provider.title()} parent",
        provider=provider,
        sync_targets=["git"],
        sync_options={"schedule_cron": "0 * * * *", "timezone": "America/New_York"},
    )
    session.add(config)
    session.flush()
    return config


def _child(
    session: Session,
    parent: SyncConfiguration,
    *,
    name: str,
    sync_options: dict,
    sync_targets: list[str] | None = None,
) -> SyncConfiguration:
    config = SyncConfiguration(
        org_id=parent.org_id,
        name=name,
        provider=parent.provider,
        parent_id=parent.id,
        sync_targets=sync_targets or ["prs"],
        sync_options=sync_options,
    )
    session.add(config)
    session.flush()
    return config


def test_parent_child_and_targets_migrate_to_integration_source_datasets(db_session):
    parent = _parent(db_session)
    synced_at = datetime(2026, 6, 17, 12, 0, tzinfo=timezone.utc)
    child = _child(
        db_session,
        parent,
        name="full-chaos/dev-health",
        sync_options={"owner": "full-chaos", "repo": "dev-health"},
        sync_targets=["prs"],
    )
    child.last_sync_at = synced_at
    child.last_sync_success = False
    child.last_sync_error = "rate limited"

    report = migrate_configs_to_integrations(db_session)
    db_session.commit()

    integration = db_session.query(Integration).one()
    source = db_session.query(IntegrationSource).one()
    dataset_keys = {
        dataset.dataset_key for dataset in db_session.query(IntegrationDataset).all()
    }
    setting = (
        db_session.query(Setting)
        .filter(Setting.key == MIGRATED_TRIGGER_ROUTING_SETTING_KEY)
        .one()
    )

    assert report.integrations_created == 1
    assert report.sources_created == 1
    assert parent.migrated_integration_id == integration.id
    assert child.migrated_integration_id == integration.id
    assert child.migrated_source_id == source.id
    assert integration.provider == "github"
    assert integration.schedule_cron == "0 * * * *"
    assert integration.timezone == "America/New_York"
    assert source.external_id == "full-chaos/dev-health"
    assert source.source_type == "repository"
    assert source.last_sync_at is not None
    assert source.last_sync_at.replace(tzinfo=timezone.utc) == synced_at
    assert source.last_sync_success is False
    assert source.last_sync_error == "rate limited"
    assert {
        "repo-metadata",
        "commits",
        "commit-stats",
        "files",
        "prs",
        "pr-reviews",
        "pr-comments",
    }.issubset(dataset_keys)
    assert db_session.query(SyncConfiguration).count() == 2
    assert setting.category == SettingCategory.SYNC.value
    assert setting.value == "false"


def test_migration_is_idempotent(db_session):
    parent = _parent(db_session)
    _child(
        db_session,
        parent,
        name="full-chaos/dev-health",
        sync_options={"owner": "full-chaos", "repo": "dev-health"},
    )

    first = migrate_configs_to_integrations(db_session)
    second = migrate_configs_to_integrations(db_session)

    assert first.integrations_created == 1
    assert first.sources_created == 1
    assert second.integrations_created == 0
    assert second.sources_created == 0
    assert second.datasets_created == 0
    assert db_session.query(Integration).count() == 1
    assert db_session.query(IntegrationSource).count() == 1
    assert db_session.query(IntegrationDataset).count() == 7


def test_gitlab_project_id_is_numeric_and_repo_only_child_is_repaired_or_skipped(
    db_session,
):
    parent = _parent(db_session, provider="gitlab")
    repaired = _child(
        db_session,
        parent,
        name="gitlab numeric child",
        sync_options={"repo": "123"},
    )
    skipped = _child(
        db_session,
        parent,
        name="gitlab broken child",
        sync_options={"repo": "group/project"},
    )

    report = migrate_configs_to_integrations(db_session)

    source = db_session.query(IntegrationSource).one()
    reasons = {issue.reason: issue for issue in report.issues}
    assert source.provider == "gitlab"
    assert source.source_type == "project"
    assert source.external_id == "123"
    assert repaired.sync_options["project_id"] == 123
    assert repaired.migrated_source_id == source.id
    assert skipped.migrated_source_id is None
    assert reasons["gitlab_child_project_id_repaired_from_repo"].repaired is True
    assert "gitlab_child_repo_without_numeric_project_id" in reasons


def test_existing_source_and_dataset_are_reused(db_session):
    parent = _parent(db_session)
    child = _child(
        db_session,
        parent,
        name="full-chaos/dev-health",
        sync_options={"owner": "full-chaos", "repo": "dev-health"},
        sync_targets=["prs"],
    )
    integration = Integration(
        org_id=ORG_ID,
        provider="github",
        name="Existing",
        config={},
        is_active=True,
    )
    db_session.add(integration)
    db_session.flush()
    parent.migrated_integration_id = integration.id
    source = IntegrationSource(
        org_id=ORG_ID,
        integration_id=integration.id,
        provider="github",
        source_type="repository",
        external_id="full-chaos/dev-health",
        name="dev-health",
        full_name="full-chaos/dev-health",
        metadata_={},
        is_enabled=True,
        discovered_at=datetime.now(timezone.utc),
        last_seen_at=datetime.now(timezone.utc),
    )
    dataset = IntegrationDataset(
        org_id=ORG_ID,
        integration_id=integration.id,
        dataset_key="prs",
        is_enabled=True,
        options={},
    )
    db_session.add_all([source, dataset])
    db_session.flush()

    report = migrate_configs_to_integrations(db_session)

    assert report.integrations_created == 0
    assert report.sources_created == 0
    assert report.sources_linked == 1
    assert child.migrated_source_id == source.id
    assert db_session.query(Integration).count() == 1
    assert db_session.query(IntegrationSource).count() == 1
    assert (
        db_session.query(IntegrationDataset).filter_by(dataset_key="prs").count() == 1
    )
