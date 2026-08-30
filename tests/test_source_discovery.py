"""Tests for dev_health_ops.sync.discovery.

Uses SQLite in-memory (aiosqlite pattern adapted to sync SQLAlchemy) so no
real database is required.  ``discover_repos_for_config`` is mocked to avoid
real provider network calls.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.settings import IntegrationCredential, SyncConfiguration
from tests._helpers import tables_of

# Tables needed for these tests. SyncConfiguration is included so we can
# assert it stays empty after discovery.
_TABLES = tables_of(
    IntegrationCredential,
    SyncConfiguration,
    Integration,
    IntegrationSource,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine(tmp_path: Path):
    db_path = tmp_path / "source-discovery.db"
    eng = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(eng, tables=_TABLES)
    yield eng
    eng.dispose()


@pytest.fixture()
def session(engine) -> Iterator[Session]:
    maker = sessionmaker(engine, expire_on_commit=False)
    with maker() as s:
        yield s


@pytest.fixture()
def github_integration(session: Session) -> Integration:
    integration = Integration(
        id=uuid.uuid4(),
        org_id="org-test",
        provider="github",
        name="Test GitHub",
        config={"owner": "acme", "search": "acme/*", "all_repos": True},
        is_active=True,
    )
    session.add(integration)
    session.commit()
    return integration


@pytest.fixture()
def gitlab_integration(session: Session) -> Integration:
    integration = Integration(
        id=uuid.uuid4(),
        org_id="org-test",
        provider="gitlab",
        name="Test GitLab",
        config={"group": "acme", "all_repos": True},
        is_active=True,
    )
    session.add(integration)
    session.commit()
    return integration


@pytest.fixture()
def jira_integration(session: Session) -> Integration:
    integration = Integration(
        id=uuid.uuid4(),
        org_id="org-test",
        provider="jira",
        name="Test Jira",
        config={"auto_import_projects": True},
        is_active=True,
    )
    session.add(integration)
    session.commit()
    return integration


@pytest.fixture()
def jira_planner_config(
    session: Session, jira_integration: Integration
) -> SyncConfiguration:
    """The planner-managed parent ``SyncConfiguration`` every jira
    integration created via ``_create_planner_managed_config`` gets --
    freshly discovered jira sources must be tagged with its id (CHAOS-4584)
    so ``sync/trigger_routing.py::_planner_scoped_source_ids`` picks them up."""
    config = SyncConfiguration(
        name="Test Jira",
        provider="jira",
        org_id="org-test",
        sync_targets=["work-items"],
        sync_options={},
        is_active=True,
        integration_id=jira_integration.id,
        planner_managed=True,
    )
    session.add(config)
    session.commit()
    return config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JIRA_TUPLES: list[tuple[str, ...]] = [
    ("SUP", "Support Desk", "service_desk"),
    ("OPS", "Platform Ops", "software"),
]

_GITHUB_TUPLES: list[tuple[str, ...]] = [
    ("acme", "api"),
    ("acme", "frontend"),
]

_GITLAB_TUPLES: list[tuple[str, ...]] = [
    ("42", "acme/api"),
    ("99", "acme/frontend"),
]

DISCOVERY_PATH = "dev_health_ops.sync.discovery.discover_repos_for_config"


# ---------------------------------------------------------------------------
# Test 1: GitHub discovery upserts source rows
# ---------------------------------------------------------------------------


def test_github_discovery_upserts_sources(
    session: Session, github_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        sources = discover_sources_for_integration(session, github_integration.id)

    assert len(sources) == 2

    full_names = {s.full_name for s in sources}
    assert full_names == {"acme/api", "acme/frontend"}

    for source in sources:
        assert source.provider == "github"
        assert source.source_type == "repository"
        assert source.external_id == source.full_name
        assert source.org_id == "org-test"
        assert source.integration_id == github_integration.id
        assert source.is_enabled is True
        assert source.discovered_at is not None
        assert source.last_seen_at is not None


# ---------------------------------------------------------------------------
# Test 2: GitLab discovery upserts source rows with project_id external_id
# ---------------------------------------------------------------------------


def test_gitlab_discovery_upserts_sources_with_project_id(
    session: Session, gitlab_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_GITLAB_TUPLES):
        sources = discover_sources_for_integration(session, gitlab_integration.id)

    assert len(sources) == 2

    by_external = {s.external_id: s for s in sources}
    assert set(by_external.keys()) == {"42", "99"}

    s42 = by_external["42"]
    assert s42.provider == "gitlab"
    assert s42.source_type == "project"
    assert s42.full_name == "acme/api"
    assert s42.name == "api"
    assert s42.external_id == "42"  # project_id, NOT "acme/api"

    s99 = by_external["99"]
    assert s99.full_name == "acme/frontend"
    assert s99.name == "frontend"
    assert s99.external_id == "99"


# ---------------------------------------------------------------------------
# Test 3: Discovery creates ZERO SyncConfiguration rows
# ---------------------------------------------------------------------------


def test_discovery_creates_zero_sync_configuration_rows(
    session: Session, github_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        discover_sources_for_integration(session, github_integration.id)

    # SyncConfiguration is in a different table; we verify it was never touched
    # by checking the models.settings module directly.
    from dev_health_ops.models.settings import SyncConfiguration

    count = session.query(SyncConfiguration).count()
    assert count == 0, (
        "discover_sources_for_integration must not create SyncConfiguration rows"
    )


# ---------------------------------------------------------------------------
# Test 4: Re-discovery updates last_seen_at and does not duplicate
# ---------------------------------------------------------------------------


def test_rediscovery_updates_last_seen_at_no_duplicates(
    session: Session, github_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    t_before = datetime(2020, 1, 1, tzinfo=timezone.utc)

    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        first_run = discover_sources_for_integration(session, github_integration.id)

    # Manually backdate last_seen_at to simulate an earlier run.
    for source in first_run:
        source.last_seen_at = t_before
    session.commit()

    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        second_run = discover_sources_for_integration(session, github_integration.id)

    # Row count must not grow.
    total = (
        session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == github_integration.id)
        .count()
    )
    assert total == 2, "Re-discovery must not duplicate rows"

    # last_seen_at must have been updated.
    for source in second_run:
        assert source.last_seen_at > t_before, (
            f"last_seen_at not updated for {source.full_name}"
        )

    # discovered_at must NOT have changed (still the original value from first run).
    first_discovered = {s.external_id: s.discovered_at for s in first_run}
    for source in second_run:
        assert source.discovered_at == first_discovered[source.external_id], (
            f"discovered_at changed on re-discovery for {source.full_name}"
        )


def test_github_rediscovery_preserves_planner_tag_and_updates_fields(
    session: Session, github_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    config_id = uuid.uuid4()
    t_before = datetime(2020, 1, 1, tzinfo=timezone.utc)
    source = IntegrationSource(
        org_id="org-test",
        integration_id=github_integration.id,
        provider="github",
        source_type="repository",
        external_id="acme/api",
        name="old-name",
        full_name="old/full-name",
        metadata_={
            "owner": "old-owner",
            "planner_managed_sync_config_id": str(config_id),
        },
        is_enabled=True,
        discovered_at=t_before,
        last_seen_at=t_before,
    )
    session.add(source)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=[("acme", "api")]):
        updated = discover_sources_for_integration(session, github_integration.id)

    assert len(updated) == 1
    assert updated[0].name == "api"
    assert updated[0].full_name == "acme/api"
    assert updated[0].last_seen_at > t_before
    assert updated[0].metadata_ == {
        "owner": "acme",
        "planner_managed_sync_config_id": str(config_id),
    }


def test_gitlab_rediscovery_preserves_planner_tag_and_updates_fields(
    session: Session, gitlab_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    config_id = uuid.uuid4()
    t_before = datetime(2020, 1, 1, tzinfo=timezone.utc)
    source = IntegrationSource(
        org_id="org-test",
        integration_id=gitlab_integration.id,
        provider="gitlab",
        source_type="project",
        external_id="42",
        name="old-name",
        full_name="old/full-name",
        metadata_={
            "path_with_namespace": "old/full-name",
            "planner_managed_sync_config_id": str(config_id),
        },
        is_enabled=True,
        discovered_at=t_before,
        last_seen_at=t_before,
    )
    session.add(source)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=[("42", "acme/api")]):
        updated = discover_sources_for_integration(session, gitlab_integration.id)

    assert len(updated) == 1
    assert updated[0].name == "api"
    assert updated[0].full_name == "acme/api"
    assert updated[0].last_seen_at > t_before
    assert updated[0].metadata_ == {
        "path_with_namespace": "acme/api",
        "planner_managed_sync_config_id": str(config_id),
    }


def test_new_github_discovery_row_has_only_fresh_metadata(
    session: Session, github_integration: Integration
):
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=[("acme", "api")]):
        sources = discover_sources_for_integration(session, github_integration.id)

    assert sources[0].metadata_ == {"owner": "acme"}


# ---------------------------------------------------------------------------
# Test 5: Disabled source stays disabled after re-discovery
# ---------------------------------------------------------------------------


def test_disabled_source_stays_disabled_after_rediscovery(
    session: Session, github_integration: Integration
):
    from dev_health_ops.sync.discovery import (
        discover_sources_for_integration,
        set_source_enabled,
    )

    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        first_run = discover_sources_for_integration(session, github_integration.id)

    # Disable one source.
    target = next(s for s in first_run if s.full_name == "acme/api")
    set_source_enabled(session, target.id, enabled=False)
    session.commit()

    # Re-discover — the disabled source must remain disabled.
    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        second_run = discover_sources_for_integration(session, github_integration.id)

    by_full_name = {s.full_name: s for s in second_run}
    assert by_full_name["acme/api"].is_enabled is False, (
        "Disabled source must remain disabled after re-discovery"
    )
    assert by_full_name["acme/frontend"].is_enabled is True


# ---------------------------------------------------------------------------
# Bonus: list_sources respects enabled_only flag
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Jira per-project auto-discovery (CHAOS-4584)
# ---------------------------------------------------------------------------


def test_jira_discovery_upserts_sources_with_planner_tag(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """A jira config with no explicit project scope used to materialize
    ZERO sources forever (CHAOS-4582/CHAOS-4584's headline gap). Real
    per-project discovery must now upsert one row per project, shaped like
    the pre-existing hand-inserted SUP/OPS proof rows: source_type='project',
    external_id=project key, is_enabled=True, and
    metadata.planner_managed_sync_config_id set to the owning planner-managed
    config so the planner actually schedules units for it."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        sources = discover_sources_for_integration(session, jira_integration.id)

    assert len(sources) == 2
    by_external = {s.external_id: s for s in sources}
    assert set(by_external.keys()) == {"SUP", "OPS"}

    sup = by_external["SUP"]
    assert sup.provider == "jira"
    assert sup.source_type == "project"
    assert sup.name == "Support Desk"
    assert sup.full_name == "SUP"
    assert sup.is_enabled is True
    assert sup.metadata_ == {
        "project_type_key": "service_desk",
        "planner_managed_sync_config_id": str(jira_planner_config.id),
    }

    ops = by_external["OPS"]
    assert ops.metadata_["project_type_key"] == "software"
    assert ops.metadata_["planner_managed_sync_config_id"] == str(
        jira_planner_config.id
    )


def test_jira_discovery_without_planner_parent_does_not_stamp_tag(
    session: Session, jira_integration: Integration
):
    """No planner-managed parent config exists yet for this integration (a
    discovery run before config creation finishes, or a config that never
    got one) -- discovery must still materialize the source rows, just
    without a tag the planner cannot resolve to anything."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        sources = discover_sources_for_integration(session, jira_integration.id)

    assert len(sources) == 2
    for source in sources:
        assert "planner_managed_sync_config_id" not in source.metadata_


def test_jira_rediscovery_no_duplicates_and_preserves_disabled(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Idempotent on re-run (chris's ruling): no duplicate rows for existing
    project keys, and a disabled row -- e.g. the hand-inserted SUP/OPS proof
    rows chris asked NOT be auto-enabled -- stays disabled."""
    from dev_health_ops.sync.discovery import (
        discover_sources_for_integration,
        set_source_enabled,
    )

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        first_run = discover_sources_for_integration(session, jira_integration.id)

    sup = next(s for s in first_run if s.external_id == "SUP")
    set_source_enabled(session, sup.id, enabled=False)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        second_run = discover_sources_for_integration(session, jira_integration.id)

    total = (
        session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .count()
    )
    assert total == 2, "Re-discovery must not duplicate rows for existing project keys"

    by_external = {s.external_id: s for s in second_run}
    assert by_external["SUP"].is_enabled is False
    assert by_external["OPS"].is_enabled is True
    # Re-run still stamps/refreshes the planner tag on the untouched row.
    assert by_external["SUP"].metadata_["planner_managed_sync_config_id"] == str(
        jira_planner_config.id
    )


def test_jira_rediscovery_preserves_existing_disabled_hand_inserted_row(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Mirrors the live org 70d529e0 state (lane-jira handoff 2026-08-30):
    a hand-inserted, disabled SUP row that predates this fix. Discovery
    finding the same project key must merge in project_type_key/refresh
    name+full_name, but never flip is_enabled -- discovery must never
    enable/disable an operator-managed row on its own."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    existing = IntegrationSource(
        org_id="org-test",
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="SUP",
        name="SUP",
        full_name="SUP",
        metadata_={"chaos": "4582", "proof_only": True},
        is_enabled=False,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add(existing)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        sources = discover_sources_for_integration(session, jira_integration.id)

    by_external = {s.external_id: s for s in sources}
    sup = by_external["SUP"]
    assert sup.is_enabled is False, "discovery must never auto-enable an existing row"
    assert sup.name == "Support Desk"
    assert sup.metadata_ == {
        "chaos": "4582",
        "proof_only": True,
        "project_type_key": "service_desk",
        "planner_managed_sync_config_id": str(jira_planner_config.id),
    }
    assert by_external["OPS"].is_enabled is True


def test_list_sources_enabled_only(session: Session, github_integration: Integration):
    from dev_health_ops.sync.discovery import (
        discover_sources_for_integration,
        list_sources,
        set_source_enabled,
    )

    with patch(DISCOVERY_PATH, return_value=_GITHUB_TUPLES):
        all_sources = discover_sources_for_integration(session, github_integration.id)

    target = next(s for s in all_sources if s.full_name == "acme/api")
    set_source_enabled(session, target.id, enabled=False)
    session.commit()

    enabled = list_sources(session, github_integration.id, enabled_only=True)
    assert len(enabled) == 1
    assert enabled[0].full_name == "acme/frontend"

    all_listed = list_sources(session, github_integration.id, enabled_only=False)
    assert len(all_listed) == 2
