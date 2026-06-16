from __future__ import annotations

import importlib
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.integrations.github_app_state import (
    mint_github_app_install_state,
    verify_github_app_install_state,
)
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import GithubAppInstallation, IntegrationCredential
from tests._helpers import tables_of

github_app_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.github_app"
)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "s" * 32)
    monkeypatch.setenv("JWT_SECRET_KEY", "j" * 32)
    db_path = tmp_path / "github-app-install.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(GithubAppInstallation, IntegrationCredential),
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


def _app(session_maker, org_id: str) -> FastAPI:
    app = FastAPI()
    app.include_router(github_app_router_module.router, prefix="/admin")

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[github_app_router_module.get_session] = _session_override
    app.dependency_overrides[github_app_router_module.get_admin_org_id] = lambda: org_id
    return app


class FakeGitHubResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeGitHubClient:
    def __init__(
        self,
        token_responses: list[FakeGitHubResponse],
        installations_response: FakeGitHubResponse,
    ):
        self.token_responses = token_responses
        self.installations_response = installations_response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url: str, headers: dict, data: dict):
        assert url == "https://github.com/login/oauth/access_token"
        assert headers["Accept"] == "application/json"
        assert data["client_id"] == "client-id"
        assert data["client_secret"] == "client-secret"
        return self.token_responses.pop(0)

    async def get(self, url: str, params: dict, headers: dict):
        assert url == "https://api.github.com/user/installations"
        assert params == {"per_page": 100, "page": 1}
        assert headers["Authorization"] == "Bearer user-token"
        assert headers["Accept"] == "application/vnd.github+json"
        assert headers["X-GitHub-Api-Version"] == "2022-11-28"
        return self.installations_response


class FakeStateCache:
    def __init__(self):
        self.values: dict[str, str] = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str):
        self.values[key] = value


class FakeNoRowResult:
    def scalar_one_or_none(self):
        return None


def _patch_cache(monkeypatch) -> FakeStateCache:
    cache = FakeStateCache()
    monkeypatch.setattr(
        github_app_router_module, "create_cache", lambda ttl_seconds: cache
    )
    return cache


def _patch_github_user_installations(
    monkeypatch,
    installations: list[dict],
    token_responses: list[FakeGitHubResponse] | None = None,
) -> None:
    responses = token_responses or [
        FakeGitHubResponse(200, {"access_token": "user-token"})
    ]
    monkeypatch.setattr(
        github_app_router_module.httpx,
        "AsyncClient",
        lambda timeout: FakeGitHubClient(
            responses,
            FakeGitHubResponse(200, {"installations": installations}),
        ),
    )


def _installation(installation_id: int = 987654) -> dict:
    return {
        "id": installation_id,
        "account": {"login": "verified-org", "type": "Organization"},
    }


def _configure_github_app(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_APP_ID", "12345")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "dummy-test-app-key-not-a-pem")
    monkeypatch.setenv("GITHUB_APP_CLIENT_ID", "client-id")
    monkeypatch.setenv("GITHUB_APP_CLIENT_SECRET", "client-secret")


@pytest.mark.asyncio
async def test_install_url_returns_verifiable_state(session_maker, monkeypatch):
    org_id = str(uuid.uuid4())
    monkeypatch.setenv("GITHUB_APP_SLUG", "dev-health-test")
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/admin/integrations/github/install-url")

    assert response.status_code == 200, response.text
    install_url = response.json()["install_url"]
    parsed = urlparse(install_url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "github.com"
    assert parsed.path == "/apps/dev-health-test/installations/new"
    state = parse_qs(parsed.query)["state"][0]
    assert verify_github_app_install_state(state).org_id == org_id


@pytest.mark.asyncio
async def test_install_callback_writes_credential_and_installation(
    session_maker,
    monkeypatch,
):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    _patch_github_user_installations(monkeypatch, [_installation()])
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": "install",
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "connected": True,
        "installation_id": 987654,
        "credential_name": "github-app",
    }
    async with session_maker() as session:
        installation = (
            await session.execute(select(GithubAppInstallation))
        ).scalar_one()
        assert installation.installation_id == 987654
        assert installation.org_id == org_id
        assert installation.account_login == "verified-org"
        assert installation.account_type == "Organization"
        svc = IntegrationCredentialsService(session, org_id)
        credentials = await svc.get_decrypted_credentials("github", "github-app")
        credential = await svc.get("github", "github-app")

    assert credentials == {
        "app_id": "12345",
        "private_key": "dummy-test-app-key-not-a-pem",
        "installation_id": "987654",
    }
    assert credential is not None
    assert credential.is_active is True


@pytest.mark.asyncio
async def test_install_callback_recovers_when_webhook_created_row_concurrently(
    session_maker,
    monkeypatch,
):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    _patch_github_user_installations(monkeypatch, [_installation()])
    async with session_maker() as session:
        installation = GithubAppInstallation()
        installation.installation_id = 987654
        session.add(installation)
        await session.commit()

    original_execute = AsyncSession.execute
    stale_select_consumed = False

    async def stale_first_execute(self, *args, **kwargs):
        nonlocal stale_select_consumed
        if not stale_select_consumed:
            stale_select_consumed = True
            return FakeNoRowResult()
        return await original_execute(self, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", stale_first_execute)
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": "install",
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )

    assert response.status_code == 200, response.text
    async with session_maker() as session:
        installation = (
            await session.execute(select(GithubAppInstallation))
        ).scalar_one()
        credential = (
            await session.execute(select(IntegrationCredential))
        ).scalar_one_or_none()
    assert installation.org_id == org_id
    assert installation.account_login == "verified-org"
    assert credential is not None


@pytest.mark.asyncio
async def test_install_callback_claims_null_org_once_and_rejects_different_org(
    session_maker,
    monkeypatch,
):
    org_a = str(uuid.uuid4())
    org_b = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    _patch_github_user_installations(
        monkeypatch,
        [_installation()],
        token_responses=[
            FakeGitHubResponse(200, {"access_token": "user-token"}),
            FakeGitHubResponse(200, {"access_token": "user-token"}),
        ],
    )
    async with session_maker() as session:
        installation = GithubAppInstallation()
        installation.installation_id = 987654
        session.add(installation)
        await session.commit()

    app_a = _app(session_maker, org_a)
    async with AsyncClient(
        transport=ASGITransport(app=app_a), base_url="http://test"
    ) as client:
        first = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": "install",
                "state": mint_github_app_install_state(org_a),
                "code": "oauth-code-a",
            },
        )

    app_b = _app(session_maker, org_b)
    async with AsyncClient(
        transport=ASGITransport(app=app_b), base_url="http://test"
    ) as client:
        second = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": "install",
                "state": mint_github_app_install_state(org_b),
                "code": "oauth-code-b",
            },
        )

    assert first.status_code == 200, first.text
    assert second.status_code == 409, second.text
    async with session_maker() as session:
        installation = (
            await session.execute(select(GithubAppInstallation))
        ).scalar_one()
        org_a_credential = await IntegrationCredentialsService(session, org_a).get(
            "github", "github-app"
        )
        org_b_credential = await IntegrationCredentialsService(session, org_b).get(
            "github", "github-app"
        )
    assert installation.org_id == org_a
    assert org_a_credential is not None
    assert org_b_credential is None


@pytest.mark.asyncio
async def test_install_callback_rejects_mismatched_org(session_maker, monkeypatch):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(str(uuid.uuid4())),
                "code": "oauth-code",
            },
        )

    assert response.status_code == 403, response.text


@pytest.mark.asyncio
async def test_install_callback_requires_server_private_key(session_maker, monkeypatch):
    org_id = str(uuid.uuid4())
    monkeypatch.setenv("GITHUB_APP_ID", "12345")
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.setenv("GITHUB_APP_CLIENT_ID", "client-id")
    monkeypatch.setenv("GITHUB_APP_CLIENT_SECRET", "client-secret")
    _patch_cache(monkeypatch)
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )

    assert response.status_code == 500, response.text


@pytest.mark.asyncio
async def test_install_callback_requires_code_without_credential(
    session_maker,
    monkeypatch,
):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(org_id),
                "code": None,
            },
        )

    assert response.status_code == 400, response.text
    async with session_maker() as session:
        credential = (
            await session.execute(select(IntegrationCredential))
        ).scalar_one_or_none()
    assert credential is None


@pytest.mark.asyncio
async def test_install_callback_rejects_inaccessible_installation_without_credential(
    session_maker,
    monkeypatch,
):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    _patch_github_user_installations(monkeypatch, [_installation(111111)])
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )

    assert response.status_code == 403, response.text
    async with session_maker() as session:
        credential = (
            await session.execute(select(IntegrationCredential))
        ).scalar_one_or_none()
    assert credential is None


@pytest.mark.asyncio
async def test_install_callback_rejects_installation_bound_to_other_org(
    session_maker,
    monkeypatch,
):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    _patch_github_user_installations(monkeypatch, [_installation()])
    async with session_maker() as session:
        installation = GithubAppInstallation()
        installation.installation_id = 987654
        installation.org_id = str(uuid.uuid4())
        session.add(installation)
        await session.commit()
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )

    assert response.status_code == 409, response.text
    async with session_maker() as session:
        credential = (
            await session.execute(select(IntegrationCredential))
        ).scalar_one_or_none()
    assert credential is None


@pytest.mark.asyncio
async def test_install_callback_rejects_replayed_state(session_maker, monkeypatch):
    org_id = str(uuid.uuid4())
    _configure_github_app(monkeypatch)
    _patch_cache(monkeypatch)
    _patch_github_user_installations(
        monkeypatch,
        [_installation()],
        token_responses=[
            FakeGitHubResponse(200, {"access_token": "user-token"}),
            FakeGitHubResponse(400, {"error": "bad_verification_code"}),
        ],
    )
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )
        second = await client.post(
            "/admin/integrations/github/install-callback",
            json={
                "installation_id": 987654,
                "setup_action": None,
                "state": mint_github_app_install_state(org_id),
                "code": "oauth-code",
            },
        )

    assert first.status_code == 200, first.text
    assert second.status_code == 400, second.text
    assert second.json()["detail"] == "GitHub user authorization could not be verified"


@pytest.mark.asyncio
async def test_install_url_includes_redirect_uri_when_configured(
    session_maker, monkeypatch
):
    org_id = str(uuid.uuid4())
    monkeypatch.setenv("GITHUB_APP_SLUG", "dev-health-test")
    monkeypatch.setenv(
        "GITHUB_APP_CALLBACK_URL",
        "http://localhost:3000/admin/integrations/github-app/callback",
    )
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/admin/integrations/github/install-url")

    assert response.status_code == 200, response.text
    install_url = response.json()["install_url"]
    parsed = urlparse(install_url)
    qs = parse_qs(parsed.query)
    assert qs["redirect_uri"] == [
        "http://localhost:3000/admin/integrations/github-app/callback"
    ]
    assert "state" in qs
    assert verify_github_app_install_state(qs["state"][0]).org_id == org_id


@pytest.mark.asyncio
async def test_install_url_omits_redirect_uri_when_not_configured(
    session_maker, monkeypatch
):
    org_id = str(uuid.uuid4())
    monkeypatch.setenv("GITHUB_APP_SLUG", "dev-health-test")
    monkeypatch.delenv("GITHUB_APP_CALLBACK_URL", raising=False)
    app = _app(session_maker, org_id)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/admin/integrations/github/install-url")

    assert response.status_code == 200, response.text
    install_url = response.json()["install_url"]
    parsed = urlparse(install_url)
    qs = parse_qs(parsed.query)
    assert "redirect_uri" not in qs
    assert "state" in qs
