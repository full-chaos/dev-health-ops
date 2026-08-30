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
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

# Tables needed for these tests. SyncConfiguration is included so we can
# assert it stays empty after discovery. Organization/OrgLicense: jira
# discovery calls TierLimitService (CHAOS-4584 round 2 P1/P2 repo-limit
# capping) which queries both, even when no row exists for the test org --
# an absent table (not just an absent row) raises OperationalError.
_TABLES = tables_of(
    IntegrationCredential,
    SyncConfiguration,
    Integration,
    IntegrationSource,
    Organization,
    OrgLicense,
    SyncWatermark,
)

# Jira discovery exercises TierLimitService, which requires a real UUID
# org_id (unlike the plain "org-test" string placeholder github/gitlab
# fixtures below use, since their discovery path never touches licensing).
_JIRA_ORG_ID = "22222222-2222-2222-2222-222222222222"

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
        org_id=_JIRA_ORG_ID,
        provider="jira",
        name="Test Jira",
        config={},
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
        org_id=_JIRA_ORG_ID,
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
        "discovered_project": True,
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


def test_jira_discovery_passes_org_id_to_discover_repos_for_config(
    session: Session, jira_integration: Integration
):
    """Codex review (CHAOS-4584 round 1, P2): the config-shim discovery
    builds from an Integration must carry org_id so the jira branch of
    discover_repos_for_config can scope its rate-limit gate per org instead
    of collapsing every org on the same Jira host onto one shared key."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES) as mock_discover:
        discover_sources_for_integration(session, jira_integration.id)

    passed_config = mock_discover.call_args.args[0]
    assert passed_config.org_id == _JIRA_ORG_ID


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
        org_id=_JIRA_ORG_ID,
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
        "discovered_project": True,
    }
    assert by_external["OPS"].is_enabled is True


def test_jira_rediscovery_reports_existing_outcome_telemetry(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Team-lead's collision rule: a project key discovery finds ALREADY has
    a source (the disabled SUP/OPS proof rows, or any prior run) is counted
    as ``existing``, distinct from ``created`` -- so the outcome is visible
    in the counter/log without reading the DB."""
    from dev_health_ops.metrics.prometheus import JIRA_PROJECT_DISCOVERY_TOTAL
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        discover_sources_for_integration(
            session, jira_integration.id
        )  # first run: 2 created

    before = JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="existing")._value.get()

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        discover_sources_for_integration(
            session, jira_integration.id
        )  # second run: 2 existing

    after = JIRA_PROJECT_DISCOVERY_TOTAL.labels(outcome="existing")._value.get()
    assert after - before == 2


def test_jira_rediscovery_matches_existing_source_case_insensitively(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 2, P1): an explicitly-scoped config's
    source can carry whatever casing the operator typed into project_key
    (``_non_git_source_rows`` stores it verbatim), but Jira's API always
    returns the canonical (typically uppercase) key. Rediscovery must match
    that existing row case-insensitively -- update it in place -- instead of
    inserting a case-variant duplicate that then double-schedules the
    project."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    existing = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="eng",  # operator-typed casing, not canonical
        name="eng",
        full_name="eng",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add(existing)
    session.commit()

    with patch(
        DISCOVERY_PATH,
        return_value=[("ENG", "Engineering", "software")],
    ):
        sources = discover_sources_for_integration(session, jira_integration.id)

    assert len(sources) == 1, "must update the existing row, not insert a duplicate"
    total = (
        session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .count()
    )
    assert total == 1
    assert sources[0].external_id == "eng", (
        "existing casing is preserved, not rewritten"
    )
    assert sources[0].name == "Engineering"


def test_jira_discovery_prefers_current_planner_config_sync_options(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 2, P1): PATCH /sync-configs/{id} only
    writes SyncConfiguration.sync_options -- Integration.config is kept in
    sync only for github (a provider-specific special case in
    update_sync_config), so it can go stale for jira the moment an operator
    changes an explicitly-scoped project via PATCH. Discovery must filter by
    the planner config's CURRENT sync_options, not the stale Integration.config
    copy the integration was originally created with."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    # Integration.config still says the OLD project (as if PATCH updated
    # sync_options but never touched Integration.config).
    jira_integration.config = {"project_key": "OLD"}
    jira_planner_config.sync_options = {"project_key": "NEW"}
    session.commit()

    with patch(DISCOVERY_PATH) as mock_discover:
        mock_discover.return_value = [("NEW", "New Project", "software")]
        discover_sources_for_integration(session, jira_integration.id)

    passed_config = mock_discover.call_args.args[0]
    assert passed_config.sync_options == {"project_key": "NEW"}


def test_jira_discovery_falls_back_to_env_credentials(
    session: Session,
    jira_integration: Integration,
    monkeypatch: pytest.MonkeyPatch,
):
    """Codex review (CHAOS-4584 round 5, P1): an integration with no stored
    credential_id must still discover via JIRA_BASE_URL/JIRA_EMAIL/
    JIRA_API_TOKEN env vars -- every other jira credential-resolution path
    in this codebase already supports this, so an env-configured jira
    integration must not silently discover zero projects forever."""
    monkeypatch.setenv("JIRA_BASE_URL", "https://acme.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")

    from dev_health_ops.sync.discovery import discover_sources_for_integration

    assert jira_integration.credential_id is None
    with patch(DISCOVERY_PATH) as mock_discover:
        mock_discover.return_value = []
        discover_sources_for_integration(session, jira_integration.id)

    passed_credentials = mock_discover.call_args.args[1]
    assert passed_credentials == {
        "base_url": "https://acme.atlassian.net",
        "email": "bot@example.com",
        "api_token": "tok",
    }


def test_jira_discovery_no_credential_no_env_returns_empty_credentials(
    session: Session,
    jira_integration: Integration,
    monkeypatch: pytest.MonkeyPatch,
):
    """No stored credential AND no env vars set -> the same '{}'  contract
    as before (no exception, discover_repos_for_config's own
    jira_credentials_from_mapping(None-ish) handles the rest)."""
    monkeypatch.delenv("JIRA_BASE_URL", raising=False)
    monkeypatch.delenv("JIRA_EMAIL", raising=False)
    monkeypatch.delenv("JIRA_API_TOKEN", raising=False)

    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH) as mock_discover:
        mock_discover.return_value = []
        discover_sources_for_integration(session, jira_integration.id)

    assert mock_discover.call_args.args[1] == {}


def test_jira_discovery_dangling_credential_id_fails_closed(
    session: Session,
    jira_integration: Integration,
    monkeypatch: pytest.MonkeyPatch,
):
    """Codex review (CHAOS-4584 round 6, P1): a NON-null credential_id that
    fails to resolve (deleted row, or belongs to a different org) must fail
    closed -- never fall back to process-wide env credentials, which could
    import a DIFFERENT account's Jira projects into this integration. Only
    a genuinely UNSET credential_id (None) may use the env fallback."""
    monkeypatch.setenv("JIRA_BASE_URL", "https://acme.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")

    jira_integration.credential_id = uuid.uuid4()  # dangling: no matching row
    session.commit()

    from contextlib import contextmanager

    @contextmanager
    def _fake_pg_session():
        class _Query:
            def filter(self, *_args, **_kwargs):
                return self

            def one_or_none(self):
                return None

        class _FakeSession:
            def query(self, *_args, **_kwargs):
                return _Query()

        yield _FakeSession()

    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch("dev_health_ops.db.get_postgres_session_sync", _fake_pg_session):
        with patch(DISCOVERY_PATH) as mock_discover:
            mock_discover.return_value = []
            discover_sources_for_integration(session, jira_integration.id)

    assert mock_discover.call_args.args[1] == {}, (
        "dangling credential_id must never fall back to env credentials"
    )


def test_jira_discovery_project_id_wins_identity_over_project_key(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 5, P2): when BOTH project_id and
    project_key are set, _non_git_explicit_source_id's precedence is
    project_id first -- discovery's identity choice must match exactly, or
    a config scoped this way gets a duplicate key-based row on every
    rediscovery."""
    from dev_health_ops.discovery.repos import discover_jira_projects

    creds = type(
        "C",
        (),
        {"base_url": "https://acme.atlassian.net", "email": "e", "api_token": "t"},
    )()
    with patch(
        "dev_health_ops.providers.jira.client.JiraClient.get_all_projects",
        return_value=[{"id": "10002", "key": "ENG", "name": "Engineering"}],
    ):
        result = discover_jira_projects(
            creds, sync_options={"project_id": "10002", "project_key": "ENG"}
        )
    assert result == [("10002", "Engineering", "", "10002")]


def test_jira_discovery_moves_to_new_project_id_despite_stale_project_key(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
    monkeypatch: pytest.MonkeyPatch,
):
    """Codex review (gate round, P1 scenario for the P2 finding): a config
    scoped to project_id=10001 (source ENG) gets its project_id PATCHed to
    10002 while sync_options still carries the stale project_key="ENG" from
    before. Exercises the REAL discover_jira_projects filtering (not just
    a mocked discover_repos_for_config) so the stale-key bug is actually
    reachable: it must find the NEW project (SUP, id=10002) despite the
    stale key -- not return [] and trigger the empty-result safeguard,
    which would leave ENG enabled forever and never discover SUP."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    monkeypatch.setenv("JIRA_BASE_URL", "https://acme.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")

    jira_planner_config.sync_options = {"project_id": "10001"}
    session.commit()
    with patch(
        "dev_health_ops.providers.jira.client.JiraClient.get_all_projects",
        return_value=[
            {"id": "10001", "key": "ENG", "name": "Engineering"},
            {"id": "10002", "key": "SUP", "name": "Support Desk"},
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    # PATCH moves project_id to 10002 but leaves the stale project_key.
    jira_planner_config.sync_options = {"project_id": "10002", "project_key": "ENG"}
    session.commit()
    with patch(
        "dev_health_ops.providers.jira.client.JiraClient.get_all_projects",
        return_value=[
            {"id": "10001", "key": "ENG", "name": "Engineering"},
            {"id": "10002", "key": "SUP", "name": "Support Desk"},
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    by_external = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    }
    assert by_external["10002"].is_enabled is True, "new project must be discovered"
    assert by_external["10001"].is_enabled is False, (
        "old project must be superseded, not left enabled forever"
    )


def test_jira_discovery_preserves_watermark_on_unscoped_project_rename(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 gate round 2, P2): unscoped discovery must
    key an existing row by Jira's immutable project id, not the mutable
    project key. Rename ENG -> NEW (same id=10001): rediscovery must update
    the SAME row's external_id, not create a second enabled row, AND must
    move the existing SyncWatermark row from source_id="ENG" to
    source_id="NEW" so incremental sync doesn't silently restart."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(
        DISCOVERY_PATH,
        return_value=[("ENG", "Engineering", "software", "10001")],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    eng = (
        session.query(IntegrationSource)
        .filter(IntegrationSource.external_id == "ENG")
        .one()
    )
    assert eng.metadata_["jira_project_id"] == "10001"

    watermark = SyncWatermark(
        org_id=_JIRA_ORG_ID,
        repo_id="ENG",
        source_id="ENG",
        target="work-items",
        dataset_key="issues",
        last_synced_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    session.add(watermark)
    session.commit()

    # Jira admin renames the project key (same numeric id).
    with patch(
        DISCOVERY_PATH,
        return_value=[("NEW", "Engineering", "software", "10001")],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    rows = (
        session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    )
    assert [r.external_id for r in rows] == ["NEW"], (
        "rename must update the same row, not create a second enabled one"
    )
    assert rows[0].id == eng.id, "row identity must be preserved across rename"
    assert rows[0].is_enabled is True

    watermarks = (
        session.query(SyncWatermark).filter(SyncWatermark.org_id == _JIRA_ORG_ID).all()
    )
    assert [w.source_id for w in watermarks] == ["NEW"], (
        "watermark must move to the new key, not be orphaned under the old one"
    )
    assert watermarks[0].last_synced_at == datetime(2026, 1, 1, tzinfo=timezone.utc), (
        "watermark's synced-at cursor must survive the rename"
    )


def test_jira_discovery_whitespace_scope_treated_as_unscoped(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 5, P2): a whitespace-only project_key
    must be treated as "no explicit scope" consistently -- both by
    discover_jira_projects's own filtering (already true) and by the
    scope-supersession guard, or supersession would disable the
    whitespace-keyed row while treating every real project as in-scope,
    silently expanding from "one malformed scope" to "sync everything"."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    existing = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="   ",
        name="   ",
        full_name="   ",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add(existing)
    jira_planner_config.sync_options = {"project_key": "   "}
    session.commit()

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):  # SUP, OPS
        discover_sources_for_integration(session, jira_integration.id)

    session.refresh(existing)
    assert existing.is_enabled is True, (
        "whitespace-keyed row must not be superseded -- the guard treats "
        "whitespace scope as unscoped, matching discover_jira_projects"
    )


def test_jira_discovery_caps_and_recovers_against_repo_limit(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 1 P1 + round 2 P1/P2): the repo-limit
    cap must apply to EVERY discovery entry point (not just config-creation
    time), and a capped row must be able to recover automatically once the
    org's allowance grows again -- both exercised here directly against
    discover_sources_for_integration, the seam every entry point shares."""
    from dev_health_ops.metrics.prometheus import JIRA_PROJECT_DISCOVERY_TOTAL
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    license_row = OrgLicense(org_id=uuid.UUID(_JIRA_ORG_ID), tier="community")
    license_row.limits_override = {"max_repos": 2}
    session.add(license_row)
    session.commit()

    before_capped = JIRA_PROJECT_DISCOVERY_TOTAL.labels(
        outcome="capped_by_repo_limit"
    )._value.get()

    with patch(
        DISCOVERY_PATH,
        return_value=[
            ("AAA", "Project A", "software"),
            ("BBB", "Project B", "software"),
            ("CCC", "Project C", "software"),
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    by_external = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    }
    assert len(by_external) == 3, "capping disables, never deletes"
    assert by_external["AAA"].is_enabled is True
    assert by_external["BBB"].is_enabled is True
    assert by_external["CCC"].is_enabled is False
    assert by_external["CCC"].metadata_.get("capped_by_repo_limit") is True
    after_capped = JIRA_PROJECT_DISCOVERY_TOTAL.labels(
        outcome="capped_by_repo_limit"
    )._value.get()
    assert after_capped - before_capped == 1

    # Raise the allowance and re-run discovery -- CCC must recover
    # automatically, since it carries the cap marker (not an operator's own
    # deliberate disable).
    license_row.limits_override = {"max_repos": 3}
    session.commit()

    before_recovered = JIRA_PROJECT_DISCOVERY_TOTAL.labels(
        outcome="recovered_from_repo_limit_cap"
    )._value.get()
    with patch(
        DISCOVERY_PATH,
        return_value=[
            ("AAA", "Project A", "software"),
            ("BBB", "Project B", "software"),
            ("CCC", "Project C", "software"),
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    session.refresh(by_external["CCC"])
    assert by_external["CCC"].is_enabled is True
    assert "capped_by_repo_limit" not in by_external["CCC"].metadata_
    after_recovered = JIRA_PROJECT_DISCOVERY_TOTAL.labels(
        outcome="recovered_from_repo_limit_cap"
    )._value.get()
    assert after_recovered - before_recovered == 1


def test_jira_discovery_cap_never_touches_operator_disabled_rows(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """A row an operator deliberately disabled (no cap marker) must never be
    swept up by the cap-recovery pass, even once headroom exists."""
    from dev_health_ops.sync.discovery import (
        discover_sources_for_integration,
        set_source_enabled,
    )

    license_row = OrgLicense(org_id=uuid.UUID(_JIRA_ORG_ID), tier="enterprise")
    session.add(license_row)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        first_run = discover_sources_for_integration(session, jira_integration.id)

    sup = next(s for s in first_run if s.external_id == "SUP")
    set_source_enabled(session, sup.id, enabled=False)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):
        discover_sources_for_integration(session, jira_integration.id)

    session.refresh(sup)
    assert sup.is_enabled is False, "operator-disabled rows are never auto-recovered"


def test_jira_discovery_supersedes_old_source_on_explicit_scope_change(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 3, P1): PATCH-ing an explicitly-scoped
    jira config from project_key=OLD to NEW must disable OLD's source, not
    leave both tagged+enabled -- otherwise the planner
    (_planner_scoped_source_ids tags by config id, not by "latest scope")
    keeps syncing the project the operator moved away from."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    jira_planner_config.sync_options = {"project_key": "OLD"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("OLD", "Old Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    jira_planner_config.sync_options = {"project_key": "NEW"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("NEW", "New Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    by_external = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    }
    assert by_external["OLD"].is_enabled is False
    assert by_external["OLD"].metadata_.get("superseded_by_scope_change") is True
    assert by_external["NEW"].is_enabled is True


def test_jira_discovery_reverting_scope_reenables_the_original_source(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 4, P1): OLD -> NEW -> OLD must land
    with OLD enabled and NEW disabled -- not both disabled. A prior fix
    correctly disabled OLD on the first scope change; without also
    re-enabling a superseded row discovery reconfirms as the current scope,
    reverting the PATCH would leave zero enabled sources and zero units."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    jira_planner_config.sync_options = {"project_key": "OLD"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("OLD", "Old Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    jira_planner_config.sync_options = {"project_key": "NEW"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("NEW", "New Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    jira_planner_config.sync_options = {"project_key": "OLD"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("OLD", "Old Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    by_external = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    }
    assert by_external["OLD"].is_enabled is True
    assert "superseded_by_scope_change" not in by_external["OLD"].metadata_
    assert by_external["NEW"].is_enabled is False
    assert by_external["NEW"].metadata_.get("superseded_by_scope_change") is True


def test_jira_discovery_empty_result_never_supersedes_scoped_source(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 4, P1): a transient credential/API
    failure returns []  from discover_repos_for_config -- identical to a
    genuinely confirmed "project no longer exists" from Jira's own API.
    An explicitly-scoped config's existing, working source must survive an
    empty result, not get zeroed out on every credential hiccup."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    jira_planner_config.sync_options = {"project_key": "ENG"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("ENG", "Engineering", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    with patch(DISCOVERY_PATH, return_value=[]):  # credential failure
        discover_sources_for_integration(session, jira_integration.id)

    eng = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.integration_id == jira_integration.id,
            IntegrationSource.external_id == "ENG",
        )
        .one()
    )
    assert eng.is_enabled is True
    assert "superseded_by_scope_change" not in (eng.metadata_ or {})


def test_jira_discovery_never_supersedes_across_full_discovery(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """The stale-handling policy (module docstring) must still hold for a
    config with NO explicit scope: a project temporarily absent from one
    discovery run is never auto-disabled -- only an explicit single-project
    scope change makes "not in this run" a durable fact."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    with patch(DISCOVERY_PATH, return_value=_JIRA_TUPLES):  # SUP, OPS
        discover_sources_for_integration(session, jira_integration.id)

    # Next run only returns SUP (OPS temporarily missing from the API).
    with patch(DISCOVERY_PATH, return_value=[("SUP", "Support Desk", "service_desk")]):
        discover_sources_for_integration(session, jira_integration.id)

    ops = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.integration_id == jira_integration.id,
            IntegrationSource.external_id == "OPS",
        )
        .one()
    )
    assert ops.is_enabled is True, "no explicit scope -> never auto-disable"
    assert "superseded_by_scope_change" not in (ops.metadata_ or {})


def test_jira_discovery_repairs_preexisting_case_variant_duplicate(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 3, P1): if two case-variant rows for
    the same project already exist (e.g. from data predating the
    case-insensitive match), discovery must self-repair -- fold the
    duplicate into the surviving row -- instead of the case-insensitive
    filter matching both and one_or_none() raising MultipleResultsFound
    (which would 503 every future discovery run for this integration)."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    older = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="eng",
        name="eng",
        full_name="eng",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    newer = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    session.add_all([older, newer])
    session.commit()

    with patch(DISCOVERY_PATH, return_value=[("ENG", "Engineering", "software")]):
        sources = discover_sources_for_integration(session, jira_integration.id)

    assert len(sources) == 1
    survivors = [
        s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
        if s.is_enabled
    ]
    assert len(survivors) == 1
    disabled = [
        s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
        if not s.is_enabled
    ]
    assert len(disabled) == 1
    assert "duplicate_of_external_id" in disabled[0].metadata_


def test_jira_case_normalization_invariant(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 gate rounds 4-5): ONE invariant -- every
    Jira provider-name and project-key comparison in ``discovery/repos.py``
    and ``sync/discovery.py`` goes through ``jira_key_norm`` (Python-side)
    or ``_provider_matches``/a normalized column comparison (SQLAlchemy
    filters), never a site-local ``.lower()`` or exact-match. Four
    sub-cases, one per finding, each red on that finding's own gate-round
    parent tip:

    (a) #33 P1 -- a mixed-case ``provider="JIRA"`` integration must still
        read the planner-managed config's CURRENT ``sync_options`` (not
        stale ``Integration.config``) on PATCH-triggered rediscovery.
        Red on 7ad1efaea.
    (b) #34 P1 -- case-variant self-repair must never end with the project
        having ZERO enabled sources: the already-enabled candidate must
        survive even when a DISABLED row is the exact-case match.
        Red on 7ad1efaea.
    (c) #35 P2 -- case-variant self-repair must migrate the SyncWatermark
        off the LOSING duplicate onto the survivor, even when the losing
        duplicate (not the exact-case survivor) is the one that actually
        held it. Red on 7ad1efaea.
    (d) #36 P1 (gate round 5) -- a PRE-EXISTING source stored with a
        mixed-case ``provider="JIRA"`` must still be reachable by both the
        upsert loop's candidate lookup AND explicit-scope supersession --
        codex's exact repro: pre-existing ``("JIRA", "SUP", enabled)`` +
        PATCH scope ``SUP -> ENG`` must supersede (disable) SUP, not leave
        it invisibly enabled forever because its provider casing didn't
        match the raw-``==`` filter. Red on 87e2e8645.
    """
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    # Four sub-cases share this org's max_repos budget (each creates its
    # own enabled sources on top of the others') -- give it enough headroom
    # that none of them trips the repo-limit cap, which isn't what any of
    # these sub-cases are testing.
    session.add(
        OrgLicense(
            org_id=uuid.UUID(_JIRA_ORG_ID),
            tier="enterprise",
            limits_override={"max_repos": 100},
        )
    )
    session.commit()

    # --- (a) mixed-case provider="JIRA" --------------------------------
    mixed_case_integration = Integration(
        id=uuid.uuid4(),
        org_id=_JIRA_ORG_ID,
        provider="JIRA",
        name="Test Jira Mixed Case",
        config={"project_key": "OLD"},  # stale Integration.config -- must
        # NOT be read once a planner-managed config exists (see the
        # docstring on _build_config_shim)
        is_active=True,
    )
    session.add(mixed_case_integration)
    session.commit()
    mixed_case_config = SyncConfiguration(
        name="Test Jira Mixed Case",
        provider="JIRA",
        org_id=_JIRA_ORG_ID,
        sync_targets=["work-items"],
        sync_options={"project_key": "ENG"},  # current, authoritative scope
        is_active=True,
        integration_id=mixed_case_integration.id,
        planner_managed=True,
    )
    session.add(mixed_case_config)
    session.commit()

    def _discriminating_discovery(config, credentials):
        # Discriminates which sync_options actually reached
        # discover_repos_for_config -- the stale Integration.config
        # ("OLD") or the planner-managed config's current sync_options
        # ("ENG") -- rather than returning a fixed value regardless of
        # input, which would never exercise the bug.
        scoped_key = (config.sync_options or {}).get("project_key")
        if scoped_key == "ENG":
            return [("ENG", "Engineering", "software")]
        return [("OLD", "Old Project", "software")]

    with patch(DISCOVERY_PATH, side_effect=_discriminating_discovery):
        discover_sources_for_integration(session, mixed_case_integration.id)

    mixed_case_rows = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == mixed_case_integration.id)
        .all()
    }
    assert "ENG" in mixed_case_rows and mixed_case_rows["ENG"].is_enabled is True, (
        "(a) a mixed-case provider integration must still read the "
        "planner-managed config's CURRENT sync_options, not stale "
        "Integration.config -- otherwise the PATCH scope repair silently "
        "no-ops for it"
    )
    assert "OLD" not in mixed_case_rows, (
        "(a) discovery must not have run against the STALE "
        "Integration.config scope at all"
    )

    # --- (b) an already-enabled row must survive case-variant repair ---
    disabled_uppercase = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="ENG",
        name="ENG",
        full_name="ENG",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=False,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    enabled_lowercase = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="eng",
        name="eng",
        full_name="eng",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    session.add_all([disabled_uppercase, enabled_lowercase])
    session.commit()

    with patch(DISCOVERY_PATH, return_value=[("ENG", "Engineering", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    eng_variants = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.integration_id == jira_integration.id,
            IntegrationSource.external_id.in_(["ENG", "eng"]),
        )
        .all()
    )
    enabled_eng_variants = [r for r in eng_variants if r.is_enabled]
    assert len(enabled_eng_variants) == 1, (
        "(b) case-variant repair must never zero out enabled sources for "
        "a project that had one enabled candidate before the repair"
    )
    assert enabled_eng_variants[0].external_id == "eng", (
        "(b) the survivor must be the row that was actually enabled, not "
        "picked by exact-case match regardless of enabled state"
    )

    # --- (c) the watermark must follow the survivor, not the exact-case
    #     match, when they disagree ------------------------------------
    enabled_upper_sup = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="SUP",
        name="SUP",
        full_name="SUP",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    enabled_lower_sup = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="sup",
        name="sup",
        full_name="sup",
        metadata_={"planner_managed_sync_config_id": str(jira_planner_config.id)},
        is_enabled=True,
        discovered_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    session.add_all([enabled_upper_sup, enabled_lower_sup])
    session.commit()
    watermark = SyncWatermark(
        org_id=_JIRA_ORG_ID,
        repo_id="sup",
        source_id="sup",  # on the LOSING duplicate (not the exact-case match)
        target="work-items",
        dataset_key="issues",
        last_synced_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    session.add(watermark)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=[("SUP", "Support Desk", "service_desk")]):
        discover_sources_for_integration(session, jira_integration.id)

    sup_watermarks = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == _JIRA_ORG_ID,
            SyncWatermark.source_id.in_(["SUP", "sup"]),
        )
        .all()
    )
    assert [w.source_id for w in sup_watermarks] == ["SUP"], (
        "(c) the watermark must migrate onto the surviving row, even "
        "though the LOSING duplicate ('sup') -- not the exact-case "
        "survivor ('SUP') -- was the one that actually held it"
    )

    # --- (d) a pre-existing mixed-case-provider source must still be
    #     reachable by BOTH the upsert loop AND scope supersession --------
    scope_change_integration = Integration(
        id=uuid.uuid4(),
        org_id=_JIRA_ORG_ID,
        provider="jira",
        name="Test Jira Scope Change",
        config={},
        is_active=True,
    )
    session.add(scope_change_integration)
    session.commit()
    scope_change_config = SyncConfiguration(
        name="Test Jira Scope Change",
        provider="jira",
        org_id=_JIRA_ORG_ID,
        sync_targets=["work-items"],
        sync_options={"project_key": "SUP"},
        is_active=True,
        integration_id=scope_change_integration.id,
        planner_managed=True,
    )
    session.add(scope_change_config)
    session.commit()
    # codex's exact repro shape: the pre-existing source's provider is
    # stored mixed-case ("JIRA"), unlike every row THIS PR's own discovery
    # mapper creates (always lowercase "jira").
    preexisting_mixed_case_sup = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=scope_change_integration.id,
        provider="JIRA",
        source_type="project",
        external_id="SUP",
        name="SUP",
        full_name="SUP",
        metadata_={
            "planner_managed_sync_config_id": str(scope_change_config.id),
            "explicit_project_scope": True,
        },
        is_enabled=True,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add(preexisting_mixed_case_sup)
    session.commit()

    # PATCH the scope from SUP to ENG.
    scope_change_config.sync_options = {"project_key": "ENG"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("ENG", "Engineering", "software")]):
        discover_sources_for_integration(session, scope_change_integration.id)

    session.refresh(preexisting_mixed_case_sup)
    assert preexisting_mixed_case_sup.is_enabled is False, (
        "(d) a mixed-case-provider pre-existing source must still be "
        "superseded on an explicit scope change -- not invisibly left "
        "enabled forever because its provider casing didn't match a "
        "raw-== filter"
    )
    eng_row_scope_change = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.integration_id == scope_change_integration.id,
            IntegrationSource.external_id == "ENG",
        )
        .one_or_none()
    )
    assert eng_row_scope_change is not None and eng_row_scope_change.is_enabled is True


@pytest.mark.parametrize(
    "a,b",
    [
        ("SUP", "sup"),
        (" SUP ", "SUP"),  # codex's exact gate round 6 repro
        ("SUP", " sup "),
        ("Sup", "sUP"),
        (" Sup ", "SUP"),
        ("SUP", "SUP "),
        ("SUP", " SUP"),
        ("SUP", "SUP"),  # already-clean identity case
        ("SUP", "SUPPORT"),  # must NOT match
        (" S U P ", "SUP"),  # interior whitespace must NOT be collapsed
        ("", " "),  # both normalize to empty -- must still agree
    ],
)
def test_sql_side_comparator_agrees_with_jira_key_norm(
    session: Session, a: str, b: str
):
    """Codex review (CHAOS-4584 gate round 6, P2): a symmetry property test
    for the ONE normalization rule -- for every adversarial value pair
    (mixed case, leading/trailing/interior whitespace, already-clean, both
    empty), the SQL-side comparator (``_normalized_column_matches``, the
    shared implementation ``_provider_matches``/``_jira_key_matches`` both
    delegate to) must agree EXACTLY with the Python-side ``jira_key_norm``.

    Root cause of the round 6 finding: two independent implementations of
    one normalization rule (``func.lower(column)`` on the SQL side vs
    ``.strip().lower()`` in Python) that could drift out of sync -- and
    did, on whitespace. This test executes the ACTUAL SQL expression
    (against the real SQLite engine, not a Python re-implementation of
    what the SQL is supposed to do) for every pair, so a future edit to
    ``_normalized_column_matches`` that reintroduces drift fails here
    structurally, rather than needing its own bespoke regression case."""
    from sqlalchemy import literal

    from dev_health_ops.discovery.repos import jira_key_norm
    from dev_health_ops.sync.discovery import _normalized_column_matches

    python_truth = jira_key_norm(a) == jira_key_norm(b)

    sql_match = (
        session.query(literal(1))
        .filter(_normalized_column_matches(literal(a), b))
        .first()
    )
    sql_truth = sql_match is not None

    assert sql_truth == python_truth, (
        f"SQL-side comparator diverged from jira_key_norm for "
        f"({a!r}, {b!r}): sql_matches={sql_truth} python_equal={python_truth}"
    )


def test_jira_discovery_caps_new_sources_before_preexisting_ones(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 3, P1): capping must prefer sources
    discovered in THIS run over ones that already existed and were already
    relied upon -- an org already at its limit must not have a working,
    pre-existing source silently disabled just because this run also
    discovered new ones."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    license_row = OrgLicense(org_id=uuid.UUID(_JIRA_ORG_ID), tier="community")
    session.add(license_row)
    session.commit()

    with patch(DISCOVERY_PATH, return_value=[("ZZZ", "Existing Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    zzz = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.integration_id == jira_integration.id,
            IntegrationSource.external_id == "ZZZ",
        )
        .one()
    )
    assert zzz.is_enabled is True

    # Community's default max_repos is 3; ZZZ already consumes one slot.
    # Discovering 3 MORE projects overflows by 2 -- both must come from the
    # new batch, never from ZZZ, even though ZZZ sorts after some of them
    # alphabetically (external_id desc would otherwise pick it).
    with patch(
        DISCOVERY_PATH,
        return_value=[
            ("AAA", "Project A", "software"),
            ("BBB", "Project B", "software"),
            ("CCC", "Project C", "software"),
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    session.refresh(zzz)
    assert zzz.is_enabled is True, "pre-existing source must never be capped first"

    by_external = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    }
    enabled_new = [k for k in ("AAA", "BBB", "CCC") if by_external[k].is_enabled]
    assert len(enabled_new) == 2, (
        "exactly 2 of the 3 new projects fit the remaining slot"
    )


def test_jira_discovery_recovers_only_sources_seen_in_current_run(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 3, P2): a capped project Jira no
    longer returns (deleted, credential lost access) must never be
    re-enabled just because headroom opened up on some OTHER project --
    recovery only considers rows this run's discovery actually reconfirmed."""
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    license_row = OrgLicense(org_id=uuid.UUID(_JIRA_ORG_ID), tier="community")
    session.add(license_row)
    session.commit()

    with patch(
        DISCOVERY_PATH,
        return_value=[
            ("AAA", "Project A", "software"),
            ("BBB", "Project B", "software"),
            ("CCC", "Project C", "software"),
            ("DDD", "Project D", "software"),
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    by_external = {
        s.external_id: s
        for s in session.query(IntegrationSource)
        .filter(IntegrationSource.integration_id == jira_integration.id)
        .all()
    }
    assert by_external["DDD"].is_enabled is False  # capped: community max_repos=3

    # Raise the allowance, but DDD has disappeared from Jira in this run.
    license_row.limits_override = {"max_repos": 4}
    session.commit()
    with patch(
        DISCOVERY_PATH,
        return_value=[
            ("AAA", "Project A", "software"),
            ("BBB", "Project B", "software"),
            ("CCC", "Project C", "software"),
        ],
    ):
        discover_sources_for_integration(session, jira_integration.id)

    session.refresh(by_external["DDD"])
    assert by_external["DDD"].is_enabled is False, (
        "must not recover a project this run's discovery didn't return"
    )


def test_set_source_enabled_clears_cap_marker_on_manual_enable(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 3, P2): an operator explicitly
    enabling a cap-disabled row supersedes the automatic bookkeeping -- the
    marker must not linger and confuse a later discovery run's own
    (now-redundant) recovery pass."""
    from dev_health_ops.sync.discovery import set_source_enabled

    source = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="AAA",
        name="Project A",
        full_name="AAA",
        metadata_={"capped_by_repo_limit": True},
        is_enabled=False,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add(source)
    session.commit()

    updated = set_source_enabled(session, source.id, enabled=True)
    assert updated.is_enabled is True
    assert "capped_by_repo_limit" not in updated.metadata_


def test_set_source_enabled_clears_superseded_marker_on_manual_disable(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 5, P2): an operator's explicit
    disable must clear superseded_by_scope_change too -- otherwise
    reverting the scope back to this project later force-re-enables it
    (round 4's fix), overriding the operator's own decision."""
    from dev_health_ops.sync.discovery import set_source_enabled

    source = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="OLD",
        name="Old Project",
        full_name="OLD",
        metadata_={
            "planner_managed_sync_config_id": str(jira_planner_config.id),
            "superseded_by_scope_change": True,
        },
        is_enabled=False,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add(source)
    session.commit()

    updated = set_source_enabled(session, source.id, enabled=False)
    assert "superseded_by_scope_change" not in updated.metadata_

    # Now scope reverts to OLD -- must stay disabled (operator's decision),
    # not force-re-enabled by round 4's "reconfirmed scope" fix.
    from dev_health_ops.sync.discovery import discover_sources_for_integration

    jira_planner_config.sync_options = {"project_key": "OLD"}
    session.commit()
    with patch(DISCOVERY_PATH, return_value=[("OLD", "Old Project", "software")]):
        discover_sources_for_integration(session, jira_integration.id)

    session.refresh(source)
    assert source.is_enabled is False, "operator's explicit disable must survive"


def test_set_source_enabled_rejects_enable_over_repo_limit(
    session: Session,
    jira_integration: Integration,
    jira_planner_config: SyncConfiguration,
):
    """Codex review (CHAOS-4584 round 6, P2): re-enabling a source while
    the org is still at/over its max_repos limit must be rejected up front
    -- otherwise the very next discovery run's rebalance would silently
    undo the operator's own explicit enable, since a marker-less row looks
    like any other ordinary pre-existing source once the cap marker is
    cleared."""
    from dev_health_ops.metrics.prometheus import JIRA_PROJECT_DISCOVERY_TOTAL
    from dev_health_ops.sync.discovery import (
        RepoLimitExceededError,
        set_source_enabled,
    )

    before = JIRA_PROJECT_DISCOVERY_TOTAL.labels(
        outcome="rejected_at_enable_repo_limit"
    )._value.get()

    license_row = OrgLicense(org_id=uuid.UUID(_JIRA_ORG_ID), tier="community")
    session.add(license_row)
    license_row.limits_override = {"max_repos": 1}
    already_enabled = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="AAA",
        name="Project A",
        full_name="AAA",
        metadata_={},
        is_enabled=True,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    capped = IntegrationSource(
        org_id=_JIRA_ORG_ID,
        integration_id=jira_integration.id,
        provider="jira",
        source_type="project",
        external_id="BBB",
        name="Project B",
        full_name="BBB",
        metadata_={"capped_by_repo_limit": True},
        is_enabled=False,
        discovered_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    session.add_all([already_enabled, capped])
    session.commit()

    with pytest.raises(RepoLimitExceededError):
        set_source_enabled(session, capped.id, enabled=True)

    session.refresh(capped)
    assert capped.is_enabled is False, "rejected enable must not mutate the row"
    assert capped.metadata_.get("capped_by_repo_limit") is True

    after = JIRA_PROJECT_DISCOVERY_TOTAL.labels(
        outcome="rejected_at_enable_repo_limit"
    )._value.get()
    assert after == before + 1, (
        "codex review (gate round 3, P3): the rejection must be observable "
        "via telemetry, not just the raised exception"
    )


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
