"""Tests for GET /credentials/{credential_id}/repos — connector exception handling.

Covers the fix in 6668b2175 which gracefully handles:
- NotFoundException → empty list (for typeahead with partial/invalid org names)
- AuthenticationException → HTTP 401
- RateLimitException → HTTP 429
- Missing owner param → empty list without calling the connector
"""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.connectors.exceptions import (
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.connectors.models import Repository
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Organization, User

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = [
    User.__table__,
    Organization.__table__,
    OrgLicense.__table__,
    IntegrationCredential.__table__,
]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "cred-repos.db"
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


@pytest_asyncio.fixture
async def seeded_state(session_maker):
    org_id = uuid.uuid4()
    user_id = uuid.uuid4()

    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="team")
    user = User(id=user_id, email="admin@example.com", is_active=True)
    cred = IntegrationCredential(
        org_id=str(org_id),
        provider="github",
        name="default",
        is_active=True,
        credentials_encrypted="fake-encrypted",
        config={"org": "my-org"},
    )

    async with session_maker() as session:
        session.add_all([org, user, cred])
        await session.commit()
        await session.refresh(cred)
        cred_id = cred.id

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
        "cred_id": str(cred_id),
    }


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
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
        yield ac, seeded_state

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_REPOS = [
    Repository(
        id=1,
        name="api-gateway",
        full_name="my-org/api-gateway",
        default_branch="main",
        description="API Gateway service",
        url="https://github.com/my-org/api-gateway",
    ),
    Repository(
        id=2,
        name="web-app",
        full_name="my-org/web-app",
        default_branch="main",
        description=None,
        url="https://github.com/my-org/web-app",
    ),
]

_DECRYPT_PATCH = "dev_health_ops.api.services.settings.IntegrationCredentialsService.get_decrypted_credentials_by_id"
_GH_CONNECTOR = "dev_health_ops.connectors.github.GitHubConnector"


def _mock_credential(provider="github", config=None):
    """Return a (decrypted_creds, credential_obj) tuple for mocking."""
    cred = MagicMock()
    cred.provider = provider
    cred.config = {"org": "my-org"} if config is None else config
    return {"token": "ghp_fake123"}, cred


# ---------------------------------------------------------------------------
# Tests — Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_repos_success(client):
    """Connector returns repos → 200 with DiscoveredReposResponse."""
    ac, state = client

    with (
        patch(_DECRYPT_PATCH, return_value=_mock_credential()) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.return_value = _FAKE_REPOS

        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["provider"] == "github"
    assert body["total"] == 2
    assert body["repos"][0]["name"] == "api-gateway"
    assert body["repos"][1]["name"] == "web-app"


# ---------------------------------------------------------------------------
# Tests — Exception handling (the fix under test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_not_found_returns_empty_list(client):
    """NotFoundException (invalid/partial org name) → 200 with empty repos."""
    ac, state = client

    with (
        patch(_DECRYPT_PATCH, return_value=_mock_credential()) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.side_effect = NotFoundException(
            "GitHub resource not found (404)"
        )

        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "full-"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["repos"] == []
    assert body["total"] == 0
    assert body["provider"] == "github"


@pytest.mark.asyncio
async def test_auth_exception_returns_401(client):
    """AuthenticationException (bad/revoked token) → HTTP 401."""
    ac, state = client

    with (
        patch(_DECRYPT_PATCH, return_value=_mock_credential()) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.side_effect = (
            AuthenticationException("Bad credentials")
        )

        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 401
    assert "Bad credentials" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_rate_limit_returns_429(client):
    """RateLimitException (GitHub API rate limit) → HTTP 429."""
    ac, state = client

    with (
        patch(_DECRYPT_PATCH, return_value=_mock_credential()) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.side_effect = (
            RateLimitException("API rate limit exceeded")
        )

        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 429
    assert "rate limit" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Tests — Missing owner
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_owner_returns_empty_without_calling_connector(client):
    """No owner param and no config.org → 200 empty, connector never called."""
    ac, state = client

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}),  # no org in config
        ) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            # no owner query param
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["repos"] == []
    assert body["total"] == 0
    # Connector should never have been instantiated for the list call
    MockConnector.return_value.list_repositories.assert_not_called()


@pytest.mark.asyncio
async def test_owner_falls_back_to_config_org(client):
    """When owner param is missing, uses config.org from credential."""
    ac, state = client

    with (
        patch(_DECRYPT_PATCH, return_value=_mock_credential()) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.return_value = _FAKE_REPOS

        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            # no owner param — should fall back to config["org"] = "my-org"
        )

    assert resp.status_code == 200
    assert resp.json()["total"] == 2
    MockConnector.return_value.list_repositories.assert_called_once_with(
        org_name="my-org", search=None, max_repos=100
    )


# ---------------------------------------------------------------------------
# Tests — Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_credential_returns_404(client):
    """Non-existent credential ID → 404."""
    ac, _ = client
    fake_id = str(uuid.uuid4())

    with patch(_DECRYPT_PATCH, return_value=(None, None)):
        resp = await ac.get(
            f"/api/v1/admin/credentials/{fake_id}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 404
    assert "Credential not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_unsupported_provider_returns_400(client):
    """Non-github/gitlab provider → 400."""
    ac, state = client

    with patch(
        _DECRYPT_PATCH,
        return_value=_mock_credential(provider="bitbucket"),
    ):
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 400
    assert "not supported" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_missing_token_returns_400(client):
    """Credential decrypted but no token → 400."""
    ac, state = client
    cred = MagicMock()
    cred.provider = "github"
    cred.config = {"org": "my-org"}

    with patch(_DECRYPT_PATCH, return_value=({}, cred)):  # no "token" key
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 400
    assert "missing token" in resp.json()["detail"].lower()
