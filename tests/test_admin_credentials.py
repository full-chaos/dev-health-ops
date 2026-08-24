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

from dev_health_ops.api.admin import get_session, router
from dev_health_ops.api.admin.middleware import get_admin_org_id, require_admin
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import CredentialLookupOutcome

admin_router_module = import_module("dev_health_ops.api.admin.routers.credentials")

HEADERS: dict[str, str] = {}


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
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
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
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
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
            "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.routers.credentials._test_github_connection",
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
            "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.routers.credentials._test_jira_connection",
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
            "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.routers.credentials._test_linear_connection",
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
            "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.routers.credentials._test_github_connection",
            new_callable=AsyncMock,
        ) as mock_test,
    ):
        svc = AsyncMock()
        svc.get_decrypted_credentials_by_id_with_outcome.return_value = (
            {"token": "ghp_test"},
            cred,
            CredentialLookupOutcome.OK,
        )
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
    svc.get_decrypted_credentials_by_id_with_outcome.assert_awaited_once_with("cred-1")
    svc.update_test_result.assert_awaited_once_with("github", True, None, "default")


@pytest.mark.asyncio
async def test_test_connection_by_credential_id_not_found(client):
    """issue 3694 case 1: no row at all (or a different org's row --
    indistinguishable by design). Stays 404, the cross-tenant
    not-found-as-forbidden posture is unchanged."""

    with patch(
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.get_decrypted_credentials_by_id_with_outcome.return_value = (
            None,
            None,
            CredentialLookupOutcome.NOT_FOUND,
        )
        mock_svc_cls.return_value = svc

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={"provider": "github", "credential_id": "nonexistent-id"},
            headers=HEADERS,
        )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Credential not found"


@pytest.mark.asyncio
async def test_test_connection_by_credential_id_no_payload_is_422(client):
    """issue 3694 case 2: the row exists (within this org) but has no
    stored credentials_encrypted payload. Distinct from case 1 -- this is
    NOT "not found", it is "found but unusable" -- a reason-coded 422,
    never a 404 (a 404 here would be a lie: the row genuinely exists)."""

    cred = _mock_credential(provider="github", name="default")
    with patch(
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.get_decrypted_credentials_by_id_with_outcome.return_value = (
            None,
            cred,
            CredentialLookupOutcome.NO_PAYLOAD,
        )
        mock_svc_cls.return_value = svc

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={"provider": "github", "credential_id": "cred-1"},
            headers=HEADERS,
        )

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["reason_code"] == "credential_missing_payload"
    # No secret material of any kind belongs in this response.
    assert "token" not in str(detail).lower()


@pytest.mark.asyncio
async def test_test_connection_by_credential_id_decrypt_failed_is_422(client):
    """issue 3694 case 3: the row exists with a payload, but decryption
    failed (key-mismatch class). Distinct reason code from case 2 -- both
    are "exists but unusable", but a caller diagnosing this needs to tell
    "nothing was ever stored" apart from "something was stored and is now
    unreadable"."""

    cred = _mock_credential(provider="github", name="default")
    with patch(
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
    ) as mock_svc_cls:
        svc = AsyncMock()
        svc.get_decrypted_credentials_by_id_with_outcome.return_value = (
            None,
            cred,
            CredentialLookupOutcome.DECRYPT_FAILED,
        )
        mock_svc_cls.return_value = svc

        resp = await client.post(
            "/api/v1/admin/credentials/test",
            json={"provider": "github", "credential_id": "cred-1"},
            headers=HEADERS,
        )

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["reason_code"] == "credential_unreadable"
    assert detail["reason_code"] != "credential_missing_payload"
    assert "token" not in str(detail).lower()


@pytest.mark.asyncio
async def test_delete_credential(client):
    with patch(
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
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
        "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
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


# ---------------------------------------------------------------------------
# CHAOS-2780 codex HIGH: /credentials/test must never echo a raw secret back
# through the HTTP response, even though the persisted last_test_error was
# already sanitized. Fixture secret assembled at runtime with a neutral name
# per the Gitleaks-safety convention (tests/test_error_sanitize.py).
# ---------------------------------------------------------------------------


def _fake_secret(*parts: str) -> str:
    return "".join(parts)


_LEAK = _fake_secret("ghp_", "FAKEtestconnLEAK1234567890AB")


@pytest.mark.asyncio
async def test_test_connection_sanitizes_exception_in_response_and_persisted_error(
    client,
):
    cred = _mock_credential(provider="github", name="default")

    with (
        patch(
            "dev_health_ops.api.admin.routers.credentials.IntegrationCredentialsService"
        ) as mock_svc_cls,
        patch(
            "dev_health_ops.api.admin.routers.credentials._test_github_connection",
            new_callable=AsyncMock,
        ) as mock_test,
    ):
        svc = AsyncMock()
        svc.get.return_value = cred
        mock_svc_cls.return_value = svc
        mock_test.side_effect = RuntimeError(
            f"403 rate limited -- Authorization: Bearer {_LEAK}"
        )

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
    assert data["success"] is False
    # The HTTP response body must be redacted, not just the persisted row --
    # this is the exact leak: error = str(e) returned verbatim to the caller.
    assert data["error"] is not None
    assert _LEAK not in data["error"]
    assert "Bearer" not in data["error"]
    assert "[REDACTED]" in data["error"]

    # And the value handed to the persistence layer must be the SAME
    # sanitized text (single source of truth, not sanitized twice
    # differently).
    svc.update_test_result.assert_awaited_once()
    persisted_args = svc.update_test_result.await_args.args
    persisted_error = persisted_args[2]
    assert persisted_error == data["error"]
    assert _LEAK not in persisted_error


@pytest.mark.asyncio
async def test_gitlab_connection_helper_sanitizes_raw_response_body(monkeypatch):
    """Direct unit test on the OTHER error-bearing response field codex
    flagged: provider helpers echo up to 200 chars of the raw external HTTP
    response body into details['error']. Some providers echo request
    details (including the submitted credential) back in error bodies, so
    this must be redacted too, not just the top-level exception path.

    CHAOS-2830: _test_gitlab_connection runs its request through
    _validate_external_url first, which does a live socket.getaddrinfo SSRF
    check on the hostname. Mock it the same way the other
    _validate_external_url tests in this file do (see
    test_validate_external_url_allows_public_host above) so this test is
    hermetic and doesn't depend on real DNS resolution -- without this it
    fails offline/no-network with a stray "Cannot resolve hostname" result
    (no 'status' key) before ever reaching the mocked httpx.AsyncClient
    below.
    """

    def _fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("172.65.251.78", 0))]

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _fake_getaddrinfo)

    class _FakeResponse:
        status_code = 401

        def __init__(self, text: str) -> None:
            self.text = text

        def json(self):
            return {}

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> bool:
            return False

        async def get(self, *args, **kwargs):
            return _FakeResponse(
                f"401 Unauthorized: submitted PRIVATE-TOKEN {_LEAK} is invalid"
            )

    with patch("httpx.AsyncClient", _FakeAsyncClient):
        success, details = await admin_router_module._test_gitlab_connection(
            {"token": "placeholder"}
        )

    assert success is False
    assert details["status"] == 401
    assert _LEAK not in details["error"]
    assert "[REDACTED]" in details["error"]


GITLAB_URL_SHAPES = [
    "https://gitlab.com",
    "https://gitlab.com/",
    "https://gitlab.com/api/v4",
    "https://gitlab.example.com",
    "https://gitlab.example.com/gitlab",
    "https://gitlab.example.com/gitlab/",
    "https://gitlab.example.com:8443/gitlab",
]


@pytest.mark.parametrize("stored_url", GITLAB_URL_SHAPES)
def test_gitlab_probe_base_url_matches_what_the_sync_client_would_request(stored_url):
    """CHAOS-4223: the probe must target the runtime's own endpoint.

    The connection test exists to answer "would the sync reach this
    instance", so its URL has to be built the way the sync builds it --
    ``gitlab.Gitlab`` is the authority, not a normalization of our own.
    Deriving the expectation from python-gitlab rather than restating it
    means the two cannot drift apart again: a probe that normalizes a URL
    the runtime would not is a test that passes for an endpoint the sync
    cannot use.
    """
    import gitlab

    expected = gitlab.Gitlab(stored_url, private_token="placeholder")._url
    assert admin_router_module._gitlab_api_base_url(stored_url) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "creds",
    [
        {"token": "placeholder", "url": "https://gitlab.com"},
        {"token": "placeholder", "base_url": "https://gitlab.com"},
        {"token": "placeholder", "gitlab_url": "https://gitlab.com"},
        {"token": "placeholder"},
    ],
    ids=["url", "base_url", "gitlab_url", "no-url"],
)
async def test_gitlab_connection_helper_probes_the_v4_api_for_every_url_alias(
    monkeypatch, creds
):
    """Every alias the credential resolver reads must reach the v4 API.

    The helper used to read only ``url``/``base_url`` and treat the value
    as an API base, so the inline credential modal's default
    ``https://gitlab.com`` was probed at ``https://gitlab.com/user`` -- an
    HTML login page -- and no token could pass. ``gitlab_url`` was ignored
    outright, silently falling back to gitlab.com.
    """

    def _fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("172.65.251.78", 0))]

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _fake_getaddrinfo)

    requested: list[str] = []
    sent_headers: list[dict[str, str]] = []

    class _FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {"username": "probe", "name": "Probe"}

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> bool:
            return False

        async def get(self, url, *args, headers=None, **kwargs):
            requested.append(url)
            sent_headers.append(dict(headers or {}))
            return _FakeResponse()

    with patch("httpx.AsyncClient", _FakeAsyncClient):
        success, details = await admin_router_module._test_gitlab_connection(creds)

    assert requested == ["https://gitlab.com/api/v4/user"]
    assert sent_headers == [{"PRIVATE-TOKEN": "placeholder"}]
    assert success is True
    assert details["user"] == "probe"


@pytest.mark.asyncio
async def test_gitlab_connection_helper_keeps_a_subpath_install_on_its_own_path(
    monkeypatch,
):
    """A GitLab served under a subpath keeps that subpath.

    python-gitlab appends ``/api/v4`` to whatever path the stored URL
    carries, so ``https://host/gitlab`` is a real, supported instance.
    Discarding the path would send the probe -- and the token -- to
    whatever else runs at that host.
    """

    def _fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("172.65.251.78", 0))]

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _fake_getaddrinfo)

    requested: list[str] = []

    class _FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {"username": "probe", "name": "Probe"}

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> bool:
            return False

        async def get(self, url, *args, **kwargs):
            requested.append(url)
            return _FakeResponse()

    with patch("httpx.AsyncClient", _FakeAsyncClient):
        success, _ = await admin_router_module._test_gitlab_connection(
            {"token": "placeholder", "gitlab_url": "https://gitlab.example.com/gitlab"}
        )

    assert requested == ["https://gitlab.example.com/gitlab/api/v4/user"]
    assert success is True


def test_build_safe_url_drops_userinfo():
    """Credentials embedded in a stored base URL never reach the wire.

    Only the hostname is validated, so a ``user:password@`` prefix would
    survive into the request and be replayed by the HTTP client as an
    Authorization header alongside the provider token this request exists
    to test.
    """
    built = admin_router_module._build_safe_url(
        "https://someone:secret@gitlab.example.com:8443/gitlab/api/v4", "user"
    )
    assert built == "https://gitlab.example.com:8443/gitlab/api/v4/user"
    assert "secret" not in built


@pytest.mark.asyncio
async def test_gitlab_connection_helper_never_reroutes_an_unusable_url_to_gitlab_com(
    monkeypatch,
):
    """An unusable stored URL must fail, not silently become gitlab.com.

    Falling back to the public host when the configured one cannot be
    resolved would send a self-hosted instance's token to a third party.
    """

    def _unresolvable(*_args, **_kwargs):
        raise socket.gaierror("no such host")

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _unresolvable)

    class _ExplodingAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("no request may be made for an unusable URL")

    with patch("httpx.AsyncClient", _ExplodingAsyncClient):
        success, details = await admin_router_module._test_gitlab_connection(
            {"token": "placeholder", "url": "https://gitlab.example.com:notaport"}
        )

    assert success is False
    assert "gitlab.example.com" in details["error"]
