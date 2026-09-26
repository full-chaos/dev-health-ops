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
    "https://[2001:db8::1]:8443/gitlab",
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


@pytest.mark.parametrize(
    "url",
    [
        "https://someone:secret@gitlab.example.com/gitlab",
        "https://someone@ghe.example.com/api/v3",
        "https://someone:secret@acme.atlassian.net",
    ],
    ids=["gitlab", "github", "jira"],
)
def test_validate_external_url_rejects_embedded_credentials(url):
    """A stored base URL may never carry userinfo.

    Only the hostname is validated, and every HTTP client reached from
    this module replays userinfo as an Authorization header -- leaking the
    embedded secret and displacing the provider token the request exists
    to test. Refusing it once here covers the callers that rebuild the URL
    through _build_safe_url and the GitHub App token exchange that does
    not, which is why this is a validation rule rather than a scrub at
    each consumer.
    """
    is_valid, error = _call_validate_external_url(url)
    assert is_valid is False
    assert error == "Credentials must not be embedded in the URL"


@pytest.mark.asyncio
async def test_gitlab_connection_helper_refuses_a_url_with_embedded_credentials(
    monkeypatch,
):
    """The refusal reaches the provider helper, not just the guard."""

    def _fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("172.65.251.78", 0))]

    monkeypatch.setattr(admin_router_module.socket, "getaddrinfo", _fake_getaddrinfo)

    class _ExplodingAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("no request may be made for a URL carrying userinfo")

    with patch("httpx.AsyncClient", _ExplodingAsyncClient):
        success, details = await admin_router_module._test_gitlab_connection(
            {"token": "placeholder", "url": "https://someone:secret@gitlab.example.com"}
        )

    assert success is False
    assert details["error"] == "Credentials must not be embedded in the URL"


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
    # Exact, not a substring match: the point is that the error names the
    # CONFIGURED host, and a substring check would also pass for a URL that
    # merely contains it somewhere.
    assert details["error"] == "Cannot resolve hostname: gitlab.example.com"
