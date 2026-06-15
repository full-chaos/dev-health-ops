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
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
credentials_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.credentials"
)

_TABLES = tables_of(User, Organization, OrgLicense, IntegrationCredential)

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

_DECRYPT_PATCH = "dev_health_ops.api.services.configuration.IntegrationCredentialsService.get_decrypted_credentials_by_id"
_GH_CONNECTOR = "dev_health_ops.connectors.github.GitHubConnector"
_GL_CONNECTOR = "dev_health_ops.connectors.gitlab.GitLabConnector"
_GH_APP_PROVIDER = "dev_health_ops.connectors.utils.github_app.GitHubAppTokenProvider"
_GL_MEMBERSHIP_HELPER = (
    "dev_health_ops.api.admin.routers.credentials._list_gitlab_membership_repos"
)


def _mock_credential(provider="github", config=None, credentials=None):
    """Return a (decrypted_creds, credential_obj) tuple for mocking."""
    cred = MagicMock()
    cred.provider = provider
    cred.config = {"org": "my-org"} if config is None else config
    return credentials or {"token": "ghp_fake123"}, cred


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


@pytest.mark.asyncio
async def test_list_repos_accepts_github_app_credentials(client):
    ac, state = client
    app_credentials = {
        "app_id": "12345",
        "private_key": "not-a-real-github-app-private-key",
        "installation_id": "67890",
    }

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(credentials=app_credentials),
        ),
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.return_value = _FAKE_REPOS

        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"owner": "my-org"},
        )

    assert resp.status_code == 200
    assert resp.json()["total"] == 2
    _, kwargs = MockConnector.call_args
    assert kwargs["credentials"].is_app_auth is True
    assert kwargs["credentials"].installation_id == "67890"


@pytest.mark.asyncio
async def test_github_app_test_connection_accepts_camel_case_fields():
    class _Response:
        status_code = 200

        def json(self):
            return {"total_count": 3}

    class _AsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return _Response()

    with (
        patch(_GH_APP_PROVIDER) as MockProvider,
        patch("httpx.AsyncClient", return_value=_AsyncClient()),
    ):
        MockProvider.return_value.get_token.return_value = "ghs_installation"
        success, details = await credentials_router_module._test_github_connection(
            {
                "appId": "12345",
                "privateKey": "not-a-real-github-app-private-key",
                "installationId": "67890",
                "baseUrl": "https://api.github.com",
            }
        )

    assert success is True
    assert details == {
        "auth_mode": "github_app",
        "installation_id": "67890",
        "repository_count": 3,
    }
    MockProvider.assert_called_once()


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
        MockConnector.return_value.list_repositories.side_effect = RateLimitException(
            "API rate limit exceeded"
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
async def test_no_owner_github_enumerates_token_wide(client):
    """Blank owner + no config.org → connector called without org_name (token-wide).

    CHAOS-2449: previously returned empty list; now enumerates all accessible repos.
    """
    ac, state = client

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}),  # no org in config
        ) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.return_value = _FAKE_REPOS
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            # no owner query param
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert body["repos"][0]["name"] == "api-gateway"
    # Connector must be called without org_name (None → token-wide)
    MockConnector.return_value.list_repositories.assert_called_once_with(
        org_name=None, search=None, max_repos=100
    )


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


@pytest.mark.asyncio
async def test_no_owner_gitlab_enumerates_membership_scoped(client):
    """Blank owner + no config.group → membership-scoped enumeration (not global).

    CHAOS-2449 review finding 2: blank-owner GitLab must use membership=True
    to avoid returning globally-visible public projects.
    """
    ac, state = client

    gl_repos = [
        Repository(
            id=10,
            name="infra",
            full_name="mygroup/infra",
            default_branch="main",
            description="Infrastructure repo",
            url="https://gitlab.com/mygroup/infra",
        ),
    ]

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(
                provider="gitlab",
                config={},  # no group in config
                credentials={"token": "glpat_fake"},
            ),
        ) as _,
        patch(_GL_CONNECTOR) as MockGLConnector,
        patch(_GL_MEMBERSHIP_HELPER, return_value=gl_repos) as MockMembership,
    ):
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            # no owner query param
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["provider"] == "gitlab"
    assert body["total"] == 1
    assert body["repos"][0]["name"] == "infra"
    # Must use membership-scoped helper, NOT the connector's unscoped list
    MockMembership.assert_called_once()
    MockGLConnector.return_value.list_repositories.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — Blank-owner search safety (review findings)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_owner_github_search_uses_pattern_not_global_search(client):
    """Blank owner + search → client-side pattern filter, NOT global search_repositories.

    Finding 1 (HIGH): passing search= to the connector with no owner triggers a
    global GitHub search that returns repos the credential cannot access.
    Fix: enumerate token-wide repos and apply search as a pattern filter.
    """
    ac, state = client

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}),  # no org in config
        ) as _,
        patch(_GH_CONNECTOR) as MockConnector,
    ):
        MockConnector.return_value.list_repositories.return_value = _FAKE_REPOS
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"search": "api"},  # search but no owner
        )

    assert resp.status_code == 200
    # Must NOT have called search_repositories (global search)
    MockConnector.return_value.search_repositories = MagicMock()
    MockConnector.return_value.search_repositories.assert_not_called()
    # Must have called list_repositories with pattern= (client-side filter), search=None
    call_kwargs = MockConnector.return_value.list_repositories.call_args
    assert call_kwargs is not None
    assert call_kwargs.kwargs.get("search") is None or call_kwargs.args[1:2] == ()
    assert call_kwargs.kwargs.get("pattern") == "*api*"
    assert call_kwargs.kwargs.get("org_name") is None


@pytest.mark.asyncio
async def test_no_owner_github_app_uses_installation_repos(client):
    """Blank owner + GitHub App auth → installation/repositories endpoint.

    Finding 2 (MEDIUM): get_user().get_repos() fails for App installation tokens
    because App tokens have no user surface. Fix: enumerate via the
    installation/repositories REST API.
    """
    ac, state = client
    app_credentials = {
        "app_id": "12345",
        "private_key": "not-a-real-github-app-private-key",
        "installation_id": "67890",
    }

    class _FakeResp:
        status_code = 200

        def json(self):
            return {
                "repositories": [
                    {
                        "id": 1,
                        "name": "install-repo",
                        "full_name": "org/install-repo",
                        "default_branch": "main",
                        "description": "Installation repo",
                        "html_url": "https://github.com/org/install-repo",
                    }
                ],
                "total_count": 1,
            }

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return _FakeResp()

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}, credentials=app_credentials),
        ) as _,
        patch(_GH_CONNECTOR) as MockConnector,
        patch(_GH_APP_PROVIDER) as MockProvider,
        patch("httpx.AsyncClient", return_value=_FakeAsyncClient()),
    ):
        MockProvider.return_value.get_token.return_value = "ghs_installation_token"
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            # no owner — App auth blank-owner path
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["repos"][0]["name"] == "install-repo"
    # The connector's list_repositories must NOT have been called
    # (we bypass it for App auth + blank owner)
    MockConnector.return_value.list_repositories.assert_not_called()


@pytest.mark.asyncio
async def test_no_owner_gitlab_search_uses_membership_scoped_with_pattern(client):
    """Blank owner + search → membership-scoped enumeration with client-side pattern.

    CHAOS-2449 review finding 2: blank-owner GitLab must use membership=True
    (via _list_gitlab_membership_repos) and apply search as a client-side
    pattern filter, never a global public-project search.
    """
    ac, state = client

    gl_repos = [
        Repository(
            id=10,
            name="infra-api",
            full_name="mygroup/infra-api",
            default_branch="main",
            description="Infrastructure API",
            url="https://gitlab.com/mygroup/infra-api",
        ),
    ]

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(
                provider="gitlab",
                config={},  # no group in config
                credentials={"token": "glpat_fake"},
            ),
        ) as _,
        patch(_GL_CONNECTOR) as MockGLConnector,
        patch(_GL_MEMBERSHIP_HELPER, return_value=gl_repos) as MockMembership,
    ):
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            params={"search": "api"},  # search but no owner
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["provider"] == "gitlab"
    assert body["repos"][0]["name"] == "infra-api"
    # Must use membership-scoped helper with search forwarded for client-side filtering
    MockMembership.assert_called_once_with(
        url="https://gitlab.com",
        token="glpat_fake",
        search="api",
        max_repos=100,
    )
    # Must NOT have called the connector's unscoped list
    MockGLConnector.return_value.list_repositories.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — GitLab base_url key fallback (review round 3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gitlab_base_url_key_used_for_self_hosted(client):
    """Credential with only base_url (no url) → discovery uses that host, not gitlab.com.

    CHAOS-2449 review round 3: the GitLab url resolution previously only checked
    decrypted['url'] and config['url'], so a self-hosted credential stored with
    base_url would silently fall back to gitlab.com, disclosing the token to the
    wrong host.
    """
    ac, state = client

    gl_repos = [
        Repository(
            id=20,
            name="self-hosted-repo",
            full_name="mygroup/self-hosted-repo",
            default_branch="main",
            description=None,
            url="https://gitlab.example.com/mygroup/self-hosted-repo",
        ),
    ]

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(
                provider="gitlab",
                config={},
                credentials={
                    "token": "glpat_selfhosted",
                    "base_url": "https://gitlab.example.com",  # only base_url, no url
                },
            ),
        ) as _,
        patch(_GL_CONNECTOR) as MockGLConnector,
        patch(_GL_MEMBERSHIP_HELPER, return_value=gl_repos) as MockMembership,
        patch(
            "dev_health_ops.api.admin.routers.credentials.socket.getaddrinfo",
            return_value=[(2, 1, 6, "", ("8.8.8.8", 0))],
        ),
    ):
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["repos"][0]["name"] == "self-hosted-repo"
    # Membership helper must be called with the self-hosted URL, NOT gitlab.com
    MockMembership.assert_called_once_with(
        url="https://gitlab.example.com",
        token="glpat_selfhosted",
        search=None,
        max_repos=100,
    )
    MockGLConnector.return_value.list_repositories.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — App installation helper error handling (review finding 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_github_app_installation_401_raises_auth_error(client):
    """App installation helper: 401 response → HTTP 401, not a silent empty list."""
    ac, state = client
    app_credentials = {
        "app_id": "12345",
        "private_key": "not-a-real-github-app-private-key",
        "installation_id": "67890",
    }

    class _FakeResp:
        status_code = 401
        text = "Bad credentials"
        headers: dict = {}

        def json(self):
            return {"message": "Bad credentials"}

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return _FakeResp()

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}, credentials=app_credentials),
        ) as _,
        patch(_GH_CONNECTOR),
        patch(_GH_APP_PROVIDER) as MockProvider,
        patch("httpx.AsyncClient", return_value=_FakeAsyncClient()),
    ):
        MockProvider.return_value.get_token.return_value = "ghs_bad_token"
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
        )

    assert resp.status_code == 401
    assert "authentication" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_github_app_installation_429_raises_rate_limit(client):
    """App installation helper: 429 response → HTTP 429, not a silent empty list."""
    ac, state = client
    app_credentials = {
        "app_id": "12345",
        "private_key": "not-a-real-github-app-private-key",
        "installation_id": "67890",
    }

    class _FakeResp:
        status_code = 429
        text = "rate limit exceeded"
        headers: dict = {}

        def json(self):
            return {"message": "rate limit exceeded"}

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return _FakeResp()

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}, credentials=app_credentials),
        ) as _,
        patch(_GH_CONNECTOR),
        patch(_GH_APP_PROVIDER) as MockProvider,
        patch("httpx.AsyncClient", return_value=_FakeAsyncClient()),
    ):
        MockProvider.return_value.get_token.return_value = "ghs_token"
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
        )

    assert resp.status_code == 429
    assert "rate limit" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_github_app_installation_5xx_raises_502(client):
    """App installation helper: 5xx response → HTTP 502, not a silent empty list."""
    ac, state = client
    app_credentials = {
        "app_id": "12345",
        "private_key": "not-a-real-github-app-private-key",
        "installation_id": "67890",
    }

    class _FakeResp:
        status_code = 503
        text = "Service Unavailable"
        headers: dict = {}

        def json(self):
            return {}

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return _FakeResp()

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}, credentials=app_credentials),
        ) as _,
        patch(_GH_CONNECTOR),
        patch(_GH_APP_PROVIDER) as MockProvider,
        patch("httpx.AsyncClient", return_value=_FakeAsyncClient()),
    ):
        MockProvider.return_value.get_token.return_value = "ghs_token"
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
        )

    assert resp.status_code == 502


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
    assert "require either token" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Tests — Security / correctness fixes (review round 4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gitlab_internal_url_rejected_ssrf(client):
    """GitLab credential with an internal/private base_url → HTTP 400 (SSRF guard).

    CHAOS-2449 review round 4 (FINDING 1 HIGH): the GitLab branch of
    list_credential_repos must validate the resolved URL with _validate_external_url
    before constructing GitLabConnector or calling _list_gitlab_membership_repos,
    so a tenant-controlled base_url cannot be used to exfiltrate the GitLab token
    to an internal host.
    """
    ac, state = client

    for internal_url in (
        "http://localhost",
        "http://169.254.169.254",
        "http://192.168.1.1",
    ):
        with (
            patch(
                _DECRYPT_PATCH,
                return_value=_mock_credential(
                    provider="gitlab",
                    config={},
                    credentials={"token": "glpat_fake", "url": internal_url},
                ),
            ) as _,
            patch(_GL_CONNECTOR) as MockGLConnector,
            patch(_GL_MEMBERSHIP_HELPER) as MockMembership,
        ):
            resp = await ac.get(
                f"/api/v1/admin/credentials/{state['cred_id']}/repos",
            )

        assert resp.status_code == 400, (
            f"Expected 400 for {internal_url}, got {resp.status_code}"
        )
        # Connector and membership helper must NOT have been called
        MockGLConnector.assert_not_called()
        MockMembership.assert_not_called()


@pytest.mark.asyncio
async def test_github_app_403_permission_not_rate_limit(client):
    """App installation helper: 403 with x-ratelimit-remaining=4999 and a
    permission-error body → auth/permission error (HTTP 403), NOT 429.

    CHAOS-2449 review round 4 (FINDING 2 MEDIUM): the previous logic treated any
    403 carrying an x-ratelimit-remaining header as a rate limit, causing admins
    to be wrongly told to wait when the real problem is a missing permission or
    SSO requirement.  A 403 is only a rate limit when remaining==0, Retry-After
    is present, or the body explicitly mentions rate/abuse/secondary limiting.
    """
    ac, state = client
    app_credentials = {
        "app_id": "12345",
        "private_key": "not-a-real-github-app-private-key",
        "installation_id": "67890",
    }

    class _FakeResp:
        status_code = 403
        text = "Resource not accessible by integration"
        headers = {"x-ratelimit-remaining": "4999"}

        def json(self):
            return {"message": "Resource not accessible by integration"}

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return _FakeResp()

    with (
        patch(
            _DECRYPT_PATCH,
            return_value=_mock_credential(config={}, credentials=app_credentials),
        ) as _,
        patch(_GH_CONNECTOR),
        patch(_GH_APP_PROVIDER) as MockProvider,
        patch("httpx.AsyncClient", return_value=_FakeAsyncClient()),
    ):
        MockProvider.return_value.get_token.return_value = "ghs_token"
        resp = await ac.get(
            f"/api/v1/admin/credentials/{state['cred_id']}/repos",
        )

    # Must NOT be 429 — remaining=4999 means quota is not exhausted
    assert resp.status_code != 429, (
        "Permission 403 must not be misreported as rate limit"
    )
    # Must be a client-side auth/permission error
    assert resp.status_code in (401, 403)
