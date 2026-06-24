from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.sync.planner_config_audit import (
    audit_active_planner_managed_configs,
)
from tests._helpers import tables_of

_TABLES = tables_of(SyncConfiguration, Integration, IntegrationSource)


@pytest.fixture()
def session(tmp_path: Path) -> Iterator[Session]:
    engine = create_engine(f"sqlite:///{tmp_path / 'planner-config-audit.db'}")
    Base.metadata.create_all(engine, tables=_TABLES)
    maker = sessionmaker(engine, expire_on_commit=False)
    try:
        with maker() as db_session:
            yield db_session
    finally:
        engine.dispose()


def _integration(session: Session, org_id: str, name: str) -> Integration:
    integration = Integration(
        org_id=org_id,
        provider="github",
        name=name,
        config={"owner": "acme"},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _planner_config(
    session: Session,
    org_id: str,
    name: str,
    integration_id: uuid.UUID | None,
) -> SyncConfiguration:
    config = SyncConfiguration(
        name=name,
        provider="github",
        org_id=org_id,
        sync_targets=["git"],
        migrated_integration_id=integration_id,
        planner_managed=True,
        is_active=True,
    )
    session.add(config)
    session.flush()
    return config


def _tagged_source(
    session: Session,
    org_id: str,
    integration_id: uuid.UUID,
    config_id: uuid.UUID,
    name: str,
) -> IntegrationSource:
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration_id,
        provider="github",
        source_type="repository",
        external_id=f"acme/{name}",
        name=name,
        full_name=f"acme/{name}",
        metadata_={"planner_managed_sync_config_id": str(config_id), "owner": "acme"},
        is_enabled=True,
    )
    session.add(source)
    session.flush()
    return source


def test_audit_active_planner_configs_lists_offenders(session: Session):
    org_id = "org-test"
    no_tag_integration = _integration(session, org_id, "no-tag-integration")
    zero_tagged = _planner_config(session, org_id, "zero-tagged", no_tag_integration.id)
    missing_integration = _planner_config(session, org_id, "missing-integration", None)
    session.add(
        IntegrationSource(
            org_id=org_id,
            integration_id=no_tag_integration.id,
            provider="github",
            source_type="repository",
            external_id="acme/untagged",
            name="untagged",
            full_name="acme/untagged",
            metadata_={"owner": "acme"},
            is_enabled=True,
        )
    )
    session.commit()

    findings = audit_active_planner_managed_configs(session, org_id=org_id)

    assert {(finding.config_id, finding.reason) for finding in findings} == {
        (str(zero_tagged.id), "zero_tagged_enabled_sources"),
        (str(missing_integration.id), "missing_migrated_integration_id"),
    }


def test_audit_active_planner_configs_is_clean_for_good_fixture(session: Session):
    org_id = "org-test"
    integration = _integration(session, org_id, "good-integration")
    config = _planner_config(session, org_id, "good-config", integration.id)
    _tagged_source(session, org_id, integration.id, config.id, "api")
    session.commit()

    assert audit_active_planner_managed_configs(session, org_id=org_id) == []


def test_audit_without_org_scans_all_orgs(session: Session):
    # Regression: the audit must default to ALL orgs (org_id=None), not a single
    # org, so offenders in any org are surfaced.
    integration_a = _integration(session, "org-a", "int-a")
    zero_tagged_a = _planner_config(session, "org-a", "cfg-a", integration_a.id)
    missing_b = _planner_config(session, "org-b", "cfg-b", None)
    session.commit()

    findings = audit_active_planner_managed_configs(session)

    assert {(finding.config_id, finding.reason) for finding in findings} == {
        (str(zero_tagged_a.id), "zero_tagged_enabled_sources"),
        (str(missing_b.id), "missing_migrated_integration_id"),
    }
