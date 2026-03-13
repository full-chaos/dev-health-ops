"""Tests for admin credential endpoints."""

from __future__ import annotations

import socket
from datetime import datetime, timezone
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.admin.middleware import get_admin_org_id, require_admin
from dev_health_ops.api.admin.router import get_session, router
from dev_health_ops.api.services.auth import AuthenticatedUser

admin_router_module = import_module("dev_health_ops.api.admin.router")

HEADERS = {}


def _call_validate_external_url(url: str):
    validate_external_url = getattr(admin_router_module, "_validate_external_url")
    return validate_external_url(url)


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def app():
    app = _build_app()
    session = AsyncMock()

    async def _override_get_session():
        yield session

    app.dependency_overrides[get_session] = _override_get_session
    app.dependency_overrides[get_admin_org_id] = lambda: "test-org"
    app.dependency_overrides[require_admin] = lambda: AuthenticatedUser(
        user_id="test-user",
        email="test@example.com",
        org_id="test-org",
        role="owner",
    )
    yield app
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _mock_credential(
    provider: str = "github", name: str = "default"
) -> SimpleNamespace:
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id="cred-1",
        provider=provider,
        name=name,
        is_active=True,
        config={"base_url": "https://api.example.com"},
        last_test_at=None,
        last_test_success=None,
        last_test_error=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.parametrize(
    "url,resolved_ip",
    [
        ("http://169.254.169.254", "169.254.169.254"),
        ("http://10.0.0.1", "10.0.0.1"),
        ("http://192.168.1.1", "192.168.1.1"),
    ],
)
def test_validate_external_url_blocks_private_networks(
    monkeypatch, url: str, resolved_ip: str
):
    def _fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", (resolved_ip, 0))]

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _fake_getaddrinfo)

    is_valid, error = _call_validate_external_url(url)

    assert is_valid is False
    assert error == "Connection to private/internal networks is not allowed"


def test_validate_external_url_blocks_loopback_ip():
    is_valid, error = _call_validate_external_url("http://127.0.0.1")

    assert is_valid is False
    assert error == "Connection to localhost is not allowed"


def test_validate_external_url_allows_public_host(monkeypatch):
    def _fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("140.82.121.5", 0))]

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _fake_getaddrinfo)

    is_valid, error = _call_validate_external_url("https://api.github.com")

    assert is_valid is True
    assert error is None


def test_validate_external_url_blocks_bad_scheme():
    is_valid, error = _call_validate_external_url("ftp://example.com")

    assert is_valid is False
    assert error == "Invalid URL scheme - only http and https are allowed"


def test_validate_external_url_blocks_localhost():
    is_valid, error = _call_validate_external_url("http://localhost")

    assert is_valid is False
    assert error == "Connection to localhost is not allowed"


@pytest.mark.asyncio
async def test_create_credential(client):
    cred = _mock_credential(provider="github", name="primary")

    with patch(
        "dev_health_ops.api.admin.router.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.set.return_value = cred
        mock_svc_cls.return_value = svc

        resp = await client.post(
            "/api/v1/admin/credentials",
            json={
                "provider": "github",
                "name": "primary",
                "credentials": {"token": "ghp_test"},
                "config": {"base_url": "https://api.github.com"},
            },
            headers=HEADERS,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "cred-1"
    assert data["provider"] == "github"
    assert data["name"] == "primary"
    mock_svc_cls.assert_called_once()
    svc.set.assert_awaited_once_with(
        provider="github",
        credentials={"token": "ghp_test"},
        name="primary",
        config={"base_url": "https://api.github.com"},
    )


@pytest.mark.asyncio
async def test_list_credentials(client):
    cred = _mock_credential(provider="jira", name="default")

    with patch(
        "dev_health_ops.api.admin.router.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.list_all.return_value = [cred]
        mock_svc_cls.return_value = svc

        resp = await client.get("/api/v1/admin/credentials", headers=HEADERS)

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["provider"] == "jira"
    assert data[0]["name"] == "default"
    svc.list_all.assert_awaited_once_with(active_only=False)


@pytest.mark.asyncio
async def test_test_connection_inline_persists_when_stored(client):
    cred = _mock_credential(provider="github", name="default")

    with (
        patch(
            "dev_health_ops.api.admin.router.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.router._test_github_connection",
            new_callable=AsyncMock,
        ) as mock_test,
    ):
        svc = AsyncMock()
        svc.get.return_value = cred
        mock_svc_cls.return_value = svc
        mock_test.return_value = (True, {"user": "test"})

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={
                "provider": "github",
                "name": "default",
                "credentials": {"token": "ghp_test"},
            },
            headers=HEADERS,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["details"] == {"user": "test"}
    mock_test.assert_awaited_once_with({"token": "ghp_test"})
    svc.get.assert_awaited_once_with("github", "default")
    svc.update_test_result.assert_awaited_once_with("github", True, None, "default")


@pytest.mark.asyncio
async def test_test_connection_inline_no_persist_when_not_stored(client):
    with (
        patch(
            "dev_health_ops.api.admin.router.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.router._test_jira_connection",
            new_callable=AsyncMock,
        ) as mock_test,
    ):
        svc = AsyncMock()
        svc.get.return_value = None
        mock_svc_cls.return_value = svc
        mock_test.return_value = (True, {"user": "jira-user@example.com"})

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={
                "provider": "jira",
                "name": "default",
                "credentials": {
                    "email": "jira-user@example.com",
                    "token": "jira-token",
                    "url": "https://example.atlassian.net",
                },
            },
            headers=HEADERS,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    svc.get.assert_awaited_once_with("jira", "default")
    svc.update_test_result.assert_not_called()


@pytest.mark.asyncio
async def test_test_connection_db_creds_persists(client):
    cred = _mock_credential(provider="linear", name="default")

    with (
        patch(
            "dev_health_ops.api.admin.router.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.router._test_linear_connection",
            new_callable=AsyncMock,
        ) as mock_test,
    ):
        svc = AsyncMock()
        svc.get_decrypted_credentials.return_value = {"apiKey": "lin_api_key"}
        svc.get.return_value = cred
        mock_svc_cls.return_value = svc
        mock_test.return_value = (True, {"user": "linear-user@example.com"})

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={"provider": "linear", "name": "default"},
            headers=HEADERS,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["details"] == {"user": "linear-user@example.com"}
    svc.get_decrypted_credentials.assert_awaited_once_with("linear", "default")
    svc.update_test_result.assert_awaited_once_with("linear", True, None, "default")


@pytest.mark.asyncio
async def test_test_connection_by_credential_id(client):
    cred = _mock_credential(provider="github", name="default")

    with (
        patch(
            "dev_health_ops.api.admin.router.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.router._test_github_connection",
            new_callable=AsyncMock,
        ) as mock_test,
    ):
        svc = AsyncMock()
        svc.get_decrypted_credentials_by_id.return_value = ({"token": "ghp_test"}, cred)
        svc.get.return_value = cred
        mock_svc_cls.return_value = svc
        mock_test.return_value = (True, {"user": "test-user"})

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={"provider": "github", "credential_id": "cred-1"},
            headers=HEADERS,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["details"] == {"user": "test-user"}
    svc.get_decrypted_credentials_by_id.assert_awaited_once_with("cred-1")
    svc.update_test_result.assert_awaited_once_with("github", True, None, "default")


@pytest.mark.asyncio
async def test_test_connection_by_credential_id_not_found(client):
    with patch(
        "dev_health_ops.api.admin.router.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.get_decrypted_credentials_by_id.return_value = (None, None)
        mock_svc_cls.return_value = svc

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={"provider": "github", "credential_id": "nonexistent-id"},
            headers=HEADERS,
        )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Credential not found"


@pytest.mark.asyncio
async def test_delete_credential(client):
    with patch(
        "dev_health_ops.api.admin.router.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.delete.return_value = True
        mock_svc_cls.return_value = svc

        resp = await client.delete(
            "/api/v1/admin/credentials/github/default",
            headers=HEADERS,
        )

    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}
    svc.delete.assert_awaited_once_with("github", "default")


@pytest.mark.asyncio
async def test_get_credential_not_found(client):
    with patch(
        "dev_health_ops.api.admin.router.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.get.return_value = None
        mock_svc_cls.return_value = svc

        resp = await client.get(
            "/api/v1/admin/credentials/github/missing",
            headers=HEADERS,
        )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Credential not found"
