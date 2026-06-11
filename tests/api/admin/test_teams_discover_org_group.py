"""Tests for org/group resolution in ``GET /teams/discover`` (CHAOS-2266).

Team import from GitHub/GitLab used to hard-require ``config.org`` /
``config.group`` on the stored credential, but nothing ever populated those
fields — the endpoint 400'd for every org. Discovery now resolves the
effective org/group in order:

1. explicit ``org`` / ``group`` query parameter,
2. ``credential.config`` (legacy behaviour),
3. distinct owner/group values derived from the org's active
   ``SyncConfiguration.sync_options`` for the same provider (zero-config
   path — repo sync already knows the owner).
"""

from __future__ import annotations

import importlib
import os
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

from dev_health_ops.api.admin.schemas import DiscoveredTeam
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import (
    GitLabDiscoveryResult,
    IntegrationCredentialsService,
    SyncConfigurationService,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    IntegrationCredential,
    SyncConfiguration,
    TeamMapping,
)
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    Membership,
    TeamMapping,
    IntegrationCredential,
    SyncConfiguration,
)

ORG_ID = str(uuid.uuid4())

_GITHUB_DISCOVER = (
    "dev_health_ops.api.services.configuration.team_discovery."
    "TeamDiscoveryService.discover_github"
)
_GITLAB_DISCOVER = (
    "dev_health_ops.api.services.configuration.team_discovery."
    "TeamDiscoveryService.discover_gitlab"
)


def _team(provider_type: str, team_id: str, name: str | None = None) -> DiscoveredTeam:
    return DiscoveredTeam(
        provider_type=provider_type,
        provider_team_id=team_id,
        name=name or team_id,
    )


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "teams-discover.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_credential(
    session_maker,
    provider: str,
    credentials: dict | None = None,
    config: dict | None = None,
) -> None:
    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        await svc.set(
            provider=provider,
            credentials=credentials or {"token": "test-token"},
            name="default",
            config=config,
        )
        await session.commit()


async def _seed_sync_config(
    session_maker,
    provider: str,
    name: str,
    sync_options: dict,
    is_active: bool = True,
    org_id: str = ORG_ID,
) -> None:
    async with session_maker() as session:
        svc = SyncConfigurationService(session, org_id)
        config = await svc.create(
            name=name,
            provider=provider,
            sync_targets=["git"],
            sync_options=sync_options,
        )
        if not is_active:
            config.is_active = False
        await session.commit()


@pytest_asyncio.fixture
async def client(session_maker):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=ORG_ID,
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_github_query_param_wins_over_config(client, session_maker):
    await _seed_credential(session_maker, "github", config={"org": "config-org"})
    await _seed_sync_config(
        session_maker, "github", "derived-org/repo", {"owner": "derived-org"}
    )

    with patch(_GITHUB_DISCOVER, new=AsyncMock(return_value=[])) as mock_discover:
        response = await client.get(
            "/api/v1/admin/teams/discover?provider=github&org=param-org"
        )

    assert response.status_code == 200, response.text
    mock_discover.assert_awaited_once_with(token="test-token", org_name="param-org")


@pytest.mark.asyncio
async def test_github_falls_back_to_credential_config(client, session_maker):
    await _seed_credential(session_maker, "github", config={"org": "config-org"})
    await _seed_sync_config(
        session_maker, "github", "derived-org/repo", {"owner": "derived-org"}
    )

    with patch(_GITHUB_DISCOVER, new=AsyncMock(return_value=[])) as mock_discover:
        response = await client.get("/api/v1/admin/teams/discover?provider=github")

    assert response.status_code == 200, response.text
    mock_discover.assert_awaited_once_with(token="test-token", org_name="config-org")


@pytest.mark.asyncio
async def test_github_derives_org_from_sync_options(client, session_maker):
    """No query param and no config.org — the existing frontend call shape."""
    await _seed_credential(session_maker, "github")
    await _seed_sync_config(
        session_maker, "github", "acme/api", {"owner": "acme", "repo": "api"}
    )
    await _seed_sync_config(
        session_maker, "github", "acme/web", {"owner": "acme", "repo": "web"}
    )

    teams = [_team("github", "platform")]
    with patch(_GITHUB_DISCOVER, new=AsyncMock(return_value=teams)) as mock_discover:
        response = await client.get("/api/v1/admin/teams/discover?provider=github")

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["total"] == 1
    assert data["teams"][0]["provider_team_id"] == "platform"
    # Distinct owners only: two configs with the same owner -> one call.
    mock_discover.assert_awaited_once_with(token="test-token", org_name="acme")


@pytest.mark.asyncio
async def test_github_derivation_skips_other_provider_and_inactive(
    client, session_maker
):
    await _seed_credential(session_maker, "github")
    await _seed_sync_config(
        session_maker, "gitlab", "glgroup/repo", {"owner": "glgroup"}
    )
    await _seed_sync_config(
        session_maker,
        "github",
        "inactive/repo",
        {"owner": "inactive-org"},
        is_active=False,
    )
    await _seed_sync_config(session_maker, "github", "acme/api", {"owner": "acme"})

    with patch(_GITHUB_DISCOVER, new=AsyncMock(return_value=[])) as mock_discover:
        response = await client.get("/api/v1/admin/teams/discover?provider=github")

    assert response.status_code == 200, response.text
    mock_discover.assert_awaited_once_with(token="test-token", org_name="acme")


@pytest.mark.asyncio
async def test_github_multi_owner_merge_dedupes_by_slug(client, session_maker):
    await _seed_credential(session_maker, "github")
    await _seed_sync_config(session_maker, "github", "org-a/api", {"owner": "org-a"})
    await _seed_sync_config(session_maker, "github", "org-b/web", {"owner": "org-b"})

    results = {
        "org-a": [_team("github", "platform"), _team("github", "shared")],
        "org-b": [_team("github", "shared"), _team("github", "mobile")],
    }

    async def _discover(token: str, org_name: str):
        return results[org_name]

    with patch(_GITHUB_DISCOVER, new=AsyncMock(side_effect=_discover)) as mock_discover:
        response = await client.get("/api/v1/admin/teams/discover?provider=github")

    assert response.status_code == 200, response.text
    data = response.json()
    assert mock_discover.await_count == 2
    called_orgs = {call.kwargs["org_name"] for call in mock_discover.await_args_list}
    assert called_orgs == {"org-a", "org-b"}
    assert data["total"] == 3
    assert [t["provider_team_id"] for t in data["teams"]] == [
        "platform",
        "shared",
        "mobile",
    ]


@pytest.mark.asyncio
async def test_github_400_when_nothing_resolves(client, session_maker):
    await _seed_credential(session_maker, "github")

    response = await client.get("/api/v1/admin/teams/discover?provider=github")

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "?org=" in detail
    assert "repository sync" in detail


@pytest.mark.asyncio
async def test_github_400_when_token_missing(client, session_maker):
    await _seed_credential(session_maker, "github", credentials={"other": "x"})

    response = await client.get("/api/v1/admin/teams/discover?provider=github&org=acme")

    assert response.status_code == 400
    assert "token" in response.json()["detail"]


# ---------------------------------------------------------------------------
# GitLab
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gitlab_query_param_wins(client, session_maker):
    await _seed_credential(session_maker, "gitlab", config={"group": "config-group"})

    with patch(
        _GITLAB_DISCOVER, new=AsyncMock(return_value=GitLabDiscoveryResult(teams=[]))
    ) as mock_discover:
        response = await client.get(
            "/api/v1/admin/teams/discover?provider=gitlab&group=param-group"
        )

    assert response.status_code == 200, response.text
    mock_discover.assert_awaited_once_with(
        token="test-token", group_path="param-group", url="https://gitlab.com"
    )
    # Discovery completed without hitting pagination bounds.
    data = response.json()
    assert data["truncated"] is False
    assert data["warnings"] == []


@pytest.mark.asyncio
async def test_gitlab_derives_group_from_sync_options(client, session_maker):
    await _seed_credential(session_maker, "gitlab")
    # The sync-config UI stores the GitLab group under ``owner``.
    await _seed_sync_config(
        session_maker, "gitlab", "my-group/api", {"owner": "my-group"}
    )

    with patch(
        _GITLAB_DISCOVER, new=AsyncMock(return_value=GitLabDiscoveryResult(teams=[]))
    ) as mock_discover:
        response = await client.get("/api/v1/admin/teams/discover?provider=gitlab")

    assert response.status_code == 200, response.text
    mock_discover.assert_awaited_once_with(
        token="test-token", group_path="my-group", url="https://gitlab.com"
    )


@pytest.mark.asyncio
async def test_gitlab_truncated_discovery_surfaces_in_response(client, session_maker):
    """Partial GitLab walks must be visible to the caller, not silently
    presented as complete (CHAOS-2281 review follow-up)."""
    await _seed_credential(session_maker, "gitlab", config={"group": "big-group"})

    warning = (
        "GitLab team discovery truncated projects for 'big-group' at "
        "500 results; the import may be incomplete."
    )
    result = GitLabDiscoveryResult(
        teams=[_team("gitlab", "big-group/platform")],
        truncated=True,
        warnings=[warning],
    )
    with patch(_GITLAB_DISCOVER, new=AsyncMock(return_value=result)):
        response = await client.get("/api/v1/admin/teams/discover?provider=gitlab")

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["total"] == 1
    assert data["truncated"] is True
    assert data["warnings"] == [warning]


@pytest.mark.asyncio
async def test_gitlab_truncation_aggregates_across_groups(client, session_maker):
    """With multiple derived groups, one truncated walk flags the whole
    response and all warnings are preserved."""
    await _seed_credential(session_maker, "gitlab")
    await _seed_sync_config(session_maker, "gitlab", "grp-a/api", {"owner": "grp-a"})
    await _seed_sync_config(session_maker, "gitlab", "grp-b/web", {"owner": "grp-b"})

    results = {
        "grp-a": GitLabDiscoveryResult(teams=[_team("gitlab", "grp-a/x")]),
        "grp-b": GitLabDiscoveryResult(
            teams=[_team("gitlab", "grp-b/y")],
            truncated=True,
            warnings=["GitLab team discovery truncated subgroups for 'grp-b'"],
        ),
    }

    async def _discover(token: str, group_path: str, url: str):
        return results[group_path]

    with patch(_GITLAB_DISCOVER, new=AsyncMock(side_effect=_discover)):
        response = await client.get("/api/v1/admin/teams/discover?provider=gitlab")

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["total"] == 2
    assert data["truncated"] is True
    assert data["warnings"] == ["GitLab team discovery truncated subgroups for 'grp-b'"]


@pytest.mark.asyncio
async def test_gitlab_400_when_nothing_resolves(client, session_maker):
    await _seed_credential(session_maker, "gitlab")

    response = await client.get("/api/v1/admin/teams/discover?provider=gitlab")

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "?group=" in detail
    assert "repository sync" in detail


# ---------------------------------------------------------------------------
# Org isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_derivation_ignores_other_orgs_sync_configs(client, session_maker):
    await _seed_credential(session_maker, "github")
    await _seed_sync_config(
        session_maker,
        "github",
        "other/repo",
        {"owner": "other-tenant-org"},
        org_id=str(uuid.uuid4()),
    )

    response = await client.get("/api/v1/admin/teams/discover?provider=github")

    assert response.status_code == 400


# ---------------------------------------------------------------------------
# GitLab discovery truncation (service level)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_gitlab_flags_truncation_when_bound_hit(monkeypatch):
    """When the project walk exceeds the discovery bound, the result is
    truncated to the bound and explicitly flagged instead of silently
    presented as complete."""
    from dev_health_ops.api.services.configuration import team_discovery as td

    monkeypatch.setattr(td, "MAX_GITLAB_DISCOVERY_PROJECTS", 2)

    root_group = MagicMock()
    root_group.full_path = "big-group"
    root_group.name = "Big Group"
    root_group.description = None
    root_group.subgroups.list.return_value = iter([])
    root_group.projects.list.return_value = iter(
        SimpleNamespace(path_with_namespace=f"big-group/p{i}") for i in range(5)
    )

    fake_gl = MagicMock()
    fake_gl.groups.get.return_value = root_group

    with patch("gitlab.Gitlab", return_value=fake_gl):
        svc = td.TeamDiscoveryService(None, ORG_ID)
        result = await svc.discover_gitlab(token="t", group_path="big-group")

    assert result.truncated is True
    assert any("truncated projects" in w for w in result.warnings)
    assert len(result.teams) == 1
    assert result.teams[0].associations["repo_patterns"] == [
        "big-group/p0",
        "big-group/p1",
    ]


@pytest.mark.asyncio
async def test_discover_gitlab_not_truncated_under_bound(monkeypatch):
    from dev_health_ops.api.services.configuration import team_discovery as td

    monkeypatch.setattr(td, "MAX_GITLAB_DISCOVERY_PROJECTS", 10)

    root_group = MagicMock()
    root_group.full_path = "small-group"
    root_group.name = "Small Group"
    root_group.description = None
    root_group.subgroups.list.return_value = iter([])
    root_group.projects.list.return_value = iter(
        [SimpleNamespace(path_with_namespace="small-group/only")]
    )

    fake_gl = MagicMock()
    fake_gl.groups.get.return_value = root_group

    with patch("gitlab.Gitlab", return_value=fake_gl):
        svc = td.TeamDiscoveryService(None, ORG_ID)
        result = await svc.discover_gitlab(token="t", group_path="small-group")

    assert result.truncated is False
    assert result.warnings == []
    assert len(result.teams) == 1
