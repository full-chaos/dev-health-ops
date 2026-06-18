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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
