from __future__ import annotations

import importlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.routing import APIRoute
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.router import router as admin_router
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.models.git import Base
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.models.settings import (
    IntegrationCredential,
    PagerDutyOAuthAuthorizationRequest,
    ProviderOAuthCredential,
    ProviderOAuthRevocation,
)
from dev_health_ops.providers.pagerduty.auth import PagerDutyAuth
from dev_health_ops.providers.pagerduty.models import Service
from dev_health_ops.providers.pagerduty.oauth import (
    READ_SCOPES,
    OAuthTokens,
    PagerDutyOAuthConfig,
)
from tests._helpers import tables_of

pagerduty_router = importlib.import_module("dev_health_ops.api.admin.routers.pagerduty")
pagerduty_services_router = importlib.import_module(
    "dev_health_ops.api.admin.routers.pagerduty_services"
)
credentials_router = importlib.import_module(
    "dev_health_ops.api.admin.routers.credentials"
)
pagerduty_revocations = importlib.import_module(
    "dev_health_ops.providers.pagerduty.oauth_revocations"
)

_ORG_ID = "00000000-0000-0000-0000-000000003024"
_USER_ID = "pagerduty-oauth-test-user"
_CONFIG = PagerDutyOAuthConfig(
    client_id="test-client-id",
    client_secret="test-client-secret",
    redirect_uri="https://app.example.test/pagerduty/callback",
)


@pytest_asyncio.fixture
async def session_maker(
    tmp_path,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'pagerduty-oauth.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection,
                tables=tables_of(
                    IntegrationCredential,
                    PagerDutyOAuthAuthorizationRequest,
                    ProviderOAuthCredential,
                    ProviderOAuthRevocation,
                    PagerDutyWebhookBinding,
                ),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "pagerduty-oauth-test-encryption-key")
    monkeypatch.setattr(pagerduty_revocations, "revoke_token", AsyncMock())

    # The PagerDuty setup, status, preflight, binding and services routes are
    # Go-served (their Python bodies are the refusal stub, pinned in
    # test_pagerduty_go_served_sentinel.py); only the generic credential routes
    # this module still exercises are mounted.
    app = FastAPI()
    app.include_router(credentials_router.router, prefix="/api/v1/admin")

    async def session_override() -> AsyncIterator[AsyncSession]:
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[credentials_router.get_session] = session_override
    app.dependency_overrides[credentials_router.get_admin_org_id] = lambda: _ORG_ID

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client

    app.dependency_overrides.clear()


async def _authorize(client: AsyncClient, *, datasets: list[str] | None = None) -> str:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/authorize",
        json={},
    )
    assert response.status_code == 200
    return parse_qs(urlparse(response.json()["authorize_url"]).query)["state"][0]


async def _persisted_counts(
    session_maker: async_sessionmaker[AsyncSession],
) -> tuple[int, int]:
    async with session_maker() as session:
        oauth_credentials = list(
            (await session.execute(select(ProviderOAuthCredential))).scalars()
        )
        integration_credentials = list(
            (await session.execute(select(IntegrationCredential))).scalars()
        )
    return len(oauth_credentials), len(integration_credentials)


def test_pagerduty_routes_are_registered_with_admin_router() -> None:
    registered_paths = {
        route.path for route in admin_router.routes if isinstance(route, APIRoute)
    }

    assert "/api/v1/admin/integrations/pagerduty/authorize" in registered_paths
    assert "/api/v1/admin/integrations/pagerduty/callback" in registered_paths


@pytest.mark.asyncio
async def test_credential_probe_does_not_require_users_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.providers.pagerduty.client import PagerDutyClient

    list_incidents = AsyncMock(return_value=[])
    monkeypatch.setattr(PagerDutyClient, "list_incidents", list_incidents)

    success, details = await credentials_router._test_pagerduty_connection(
        {
            "access_token": "oauth-access-token",
            "region": "us",
            "enabled_datasets": ["incidents"],
            "granted_scopes": ["incidents.read"],
        }
    )

    assert success is True
    assert details == {"records_checked": 0, "missing_scopes": []}
    list_incidents.assert_awaited_once()


@pytest.mark.asyncio
async def test_credential_probe_selects_authorized_dataset_when_not_stored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.providers.pagerduty.client import PagerDutyClient

    list_services = AsyncMock(return_value=[])
    monkeypatch.setattr(PagerDutyClient, "list_services", list_services)

    success, details = await credentials_router._test_pagerduty_connection(
        {
            "access_token": "oauth-access-token",
            "region": "us",
            "granted_scopes": ["services.read"],
        }
    )

    assert success is True
    assert details == {"records_checked": 0, "missing_scopes": []}
    list_services.assert_awaited_once()


@pytest.mark.asyncio
async def test_service_rejects_pagerduty_live_tokens(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        service = IntegrationCredentialsService(session, _ORG_ID)
        with pytest.raises(ValueError, match="tokens"):
            await service.set(
                "pagerduty",
                {
                    "auth_mode": "api_token",
                    "api_token": "api-token",
                    "subdomain": "acme",
                    "region": "us",
                    "access_token": "forbidden",
                },
            )
        with pytest.raises(ValueError, match="tokens"):
            await service.set(
                "pagerduty",
                {
                    "auth_mode": "api_token",
                    "api_token": "api-token",
                    "subdomain": "acme",
                    "region": "us",
                },
                config={"refresh_token": "forbidden"},
            )


@pytest.mark.asyncio
async def test_generic_pagerduty_credential_routes_are_rejected(
    client: AsyncClient,
) -> None:
    created = await client.post(
        "/api/v1/admin/credentials",
        json={"provider": "pagerduty", "credentials": {"api_token": "forbidden"}},
    )
    updated = await client.patch(
        "/api/v1/admin/credentials/pagerduty/default",
        json={"credentials": {"api_token": "forbidden"}},
    )

    assert created.status_code == 400
    assert updated.status_code == 400


class _RecordingPagerDutyClient:
    """Stand-in PagerDuty client that records calls and closes for preflight tests."""

    instances: list[_RecordingPagerDutyClient] = []
    fail_methods: set[str] = set()

    def __init__(self, auth: object, *, region: str = "us", transport: object = None):
        self.auth = auth
        self.region = region
        self.closed = 0
        self.calls: list[str] = []
        type(self).instances.append(self)

    async def close(self) -> None:
        self.closed += 1

    async def _probe(self, name: str) -> list[object]:
        self.calls.append(name)
        if name in type(self).fail_methods:
            raise RuntimeError(f"boom:{name}")
        return [object()]

    async def list_incidents(self) -> list[object]:
        return await self._probe("list_incidents")

    async def list_services(self) -> list[object]:
        return await self._probe("list_services")

    async def list_users(self) -> list[object]:
        return await self._probe("list_users")


def _install_recording_client(
    monkeypatch: pytest.MonkeyPatch,
) -> type[_RecordingPagerDutyClient]:
    _RecordingPagerDutyClient.instances.clear()
    _RecordingPagerDutyClient.fail_methods = set()
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.client.PagerDutyClient",
        _RecordingPagerDutyClient,
    )
    return _RecordingPagerDutyClient


@pytest.mark.asyncio
async def test_preflight_helper_blocks_missing_scope_without_calling_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.api.admin.routers.credentials import _test_pagerduty_connection

    client_cls = _install_recording_client(monkeypatch)
    ok, detail = await _test_pagerduty_connection(
        {
            "access_token": "tok",
            "enabled_datasets": ["incidents"],
            "granted_scopes": [],
            "region": "us",
        }
    )

    assert ok is False
    assert "incidents.read" in detail["missing_scopes"]
    instance = client_cls.instances[-1]
    assert instance.calls == []
    assert instance.closed == 1


@pytest.mark.asyncio
async def test_preflight_helper_closes_client_on_success_and_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.api.admin.routers.credentials import _test_pagerduty_connection

    client_cls = _install_recording_client(monkeypatch)
    ok, detail = await _test_pagerduty_connection(
        {
            "access_token": "tok",
            "enabled_datasets": ["incidents"],
            "granted_scopes": ["incidents.read"],
            "region": "us",
        }
    )
    assert ok is True
    assert detail["records_checked"] == 1
    success_instance = client_cls.instances[-1]
    assert success_instance.calls == ["list_incidents"]
    assert success_instance.closed == 1

    client_cls.fail_methods = {"list_incidents"}
    with pytest.raises(RuntimeError, match="boom:list_incidents"):
        await _test_pagerduty_connection(
            {
                "access_token": "tok",
                "enabled_datasets": ["incidents"],
                "granted_scopes": ["incidents.read"],
                "region": "us",
            }
        )
    error_instance = client_cls.instances[-1]
    assert error_instance.closed == 1


@pytest.mark.asyncio
async def test_preflight_helper_rejects_unknown_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.api.admin.routers.credentials import _test_pagerduty_connection

    client_cls = _install_recording_client(monkeypatch)
    ok, detail = await _test_pagerduty_connection(
        {
            "access_token": "tok",
            "enabled_datasets": ["not_a_dataset"],
            "granted_scopes": [],
            "region": "us",
        }
    )

    assert ok is False
    assert "Unknown PagerDuty datasets" in detail["error"]
    assert client_cls.instances[-1].closed == 1


@pytest.mark.asyncio
async def test_preflight_helper_api_token_skips_oauth_scope_math(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.api.admin.routers.credentials import _test_pagerduty_connection

    client_cls = _install_recording_client(monkeypatch)
    ok, detail = await _test_pagerduty_connection(
        {
            "api_token": "tok",
            "enabled_datasets": ["incidents"],
            "granted_scopes": [],
            "region": "us",
        }
    )

    assert ok is True
    assert detail["missing_scopes"] == []
    instance = client_cls.instances[-1]
    assert instance.calls == ["list_incidents"]
    assert instance.closed == 1


async def _connect_oauth(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    *,
    datasets: list[str] | None = None,
    granted: set[str] | None = None,
) -> None:
    state = await _authorize(client, datasets=datasets or ["incidents"])
    connected_tokens = OAuthTokens(
        access_token="access-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=READ_SCOPES,
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=connected_tokens)
    )
    connect = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )
    assert connect.status_code == 200


@pytest.mark.asyncio
async def test_failed_replacement_revoke_is_retained_and_retried(
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    from dev_health_ops.providers.pagerduty.oauth_revocations import (
        PagerDutyOAuthRevocationRepository,
    )

    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "pagerduty-oauth-test-encryption-key")
    async with session_maker() as session:
        revocations = PagerDutyOAuthRevocationRepository(session, _ORG_ID, "operations")
        await revocations.enqueue("old-refresh-token", purpose="replacement")
        await session.commit()
        response = httpx.Response(
            503, request=httpx.Request("POST", "https://identity.pagerduty.test/revoke")
        )
        monkeypatch.setattr(
            pagerduty_revocations,
            "revoke_token",
            AsyncMock(
                side_effect=httpx.HTTPStatusError(
                    "unavailable", request=response.request, response=response
                )
            ),
        )

        assert await revocations.retry_pending(_CONFIG) is False
        await session.commit()
        pending = list(
            (await session.execute(select(ProviderOAuthRevocation))).scalars()
        )
        assert len(pending) == 1
        assert pending[0].status == "pending"
        assert pending[0].attempts == 1
        assert pending[0].token_encrypted != "old-refresh-token"

        monkeypatch.setattr(pagerduty_revocations, "revoke_token", AsyncMock())
        assert await revocations.retry_pending(_CONFIG) is True
        await session.commit()
        assert (
            await session.execute(select(ProviderOAuthRevocation))
        ).scalar_one_or_none() is None


def test_validate_pagerduty_descriptor_guards_config_secrets_and_mode() -> None:
    from dev_health_ops.api.services.configuration.integration_credentials import (
        _validate_pagerduty_descriptor,
    )

    api_token_creds = {
        "auth_mode": "api_token",
        "api_token": "token",
        "subdomain": "acme",
        "region": "us",
    }
    # A secret smuggled into the non-secret config is rejected.
    with pytest.raises(ValueError):
        _validate_pagerduty_descriptor(
            api_token_creds,
            {"auth_mode": "api_token", "region": "us", "client_secret": "leak"},
        )
    # An unknown config key is rejected.
    with pytest.raises(ValueError):
        _validate_pagerduty_descriptor(
            api_token_creds,
            {"auth_mode": "api_token", "region": "us", "surprise": "x"},
        )
    # A config auth_mode that disagrees with the credentials is rejected.
    with pytest.raises(ValueError):
        _validate_pagerduty_descriptor(
            api_token_creds,
            {"auth_mode": "oauth", "region": "us", "subdomain": "acme"},
        )
    # The dedicated api-token payload itself remains valid.
    _validate_pagerduty_descriptor(
        api_token_creds,
        {"auth_mode": "api_token", "region": "us", "subdomain": "acme"},
    )


def test_validate_pagerduty_descriptor_requires_matching_config_auth_mode() -> None:
    from dev_health_ops.api.services.configuration.integration_credentials import (
        _validate_pagerduty_descriptor,
    )

    creds = {
        "auth_mode": "api_token",
        "api_token": "token",
        "subdomain": "acme",
        "region": "us",
    }
    # Absent, empty, or null config auth_mode no longer satisfies the invariant.
    bad_configs: list[dict[str, object] | None] = [
        None,
        {},
        {"auth_mode": None, "region": "us", "subdomain": "acme"},
    ]
    for bad_config in bad_configs:
        with pytest.raises(ValueError):
            _validate_pagerduty_descriptor(creds, bad_config)


class _ServiceDiscoveryClient:
    instances: list[_ServiceDiscoveryClient] = []
    services: list[Service] = []
    error: Exception | None = None
    events: list[str] | None = None

    def __init__(self, auth: PagerDutyAuth, *, region: str = "us") -> None:
        self.auth = auth
        self.region = region
        self.closed = 0
        type(self).instances.append(self)

    async def list_services(self) -> list[Service]:
        events = type(self).events
        if events is not None:
            events.append("list")
        error = type(self).error
        if error is not None:
            raise error
        return type(self).services

    async def close(self) -> None:
        self.closed += 1


async def _save_api_token_credential(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    org_id: str = _ORG_ID,
    name: str = "operations",
) -> None:
    async with session_maker() as session:
        await IntegrationCredentialsService(session, org_id).set(
            provider="pagerduty",
            name=name,
            credentials={
                "auth_mode": "api_token",
                "api_token": "secret-token",
                "subdomain": "acme",
                "region": "us",
            },
            config={
                "auth_mode": "api_token",
                "subdomain": "acme",
                "region": "us",
            },
        )
        await session.commit()


async def _save_oauth_descriptor(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    name: str,
    oauth_credential_name: str,
    binding_id: str,
) -> None:
    async with session_maker() as session:
        await IntegrationCredentialsService(session, _ORG_ID).set(
            provider="pagerduty",
            name=name,
            credentials={
                "auth_mode": "oauth",
                "oauth_credential_name": oauth_credential_name,
                "oauth_binding_id": binding_id,
                "subdomain": "acme",
                "region": "eu",
                "account_id": "acme",
            },
            config={
                "auth_mode": "oauth",
                "subdomain": "acme",
                "region": "eu",
                "account_id": "acme",
                "granted_scopes": ["services.read"],
            },
        )
        await session.commit()
