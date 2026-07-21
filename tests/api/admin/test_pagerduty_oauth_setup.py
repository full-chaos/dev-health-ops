from __future__ import annotations

import hashlib
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
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import (
    IntegrationCredential,
    PagerDutyOAuthAuthorizationRequest,
    ProviderOAuthCredential,
)
from dev_health_ops.providers.pagerduty.auth import PagerDutyAuth
from dev_health_ops.providers.pagerduty.models import Service
from dev_health_ops.providers.pagerduty.oauth import OAuthTokens, PagerDutyOAuthConfig
from dev_health_ops.providers.pagerduty.oauth_storage import (
    PagerDutyOAuthCredentialRepository,
)
from tests._helpers import tables_of

pagerduty_router = importlib.import_module("dev_health_ops.api.admin.routers.pagerduty")
pagerduty_services_router = importlib.import_module(
    "dev_health_ops.api.admin.routers.pagerduty_services"
)
credentials_router = importlib.import_module(
    "dev_health_ops.api.admin.routers.credentials"
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
    monkeypatch.setattr(
        pagerduty_router.PagerDutyOAuthConfig,
        "from_env",
        classmethod(lambda _: _CONFIG),
    )
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=True),
    )

    app = FastAPI()
    app.include_router(pagerduty_router.router, prefix="/api/v1/admin")
    app.include_router(pagerduty_services_router.router, prefix="/api/v1/admin")
    app.include_router(credentials_router.router, prefix="/api/v1/admin")

    async def session_override() -> AsyncIterator[AsyncSession]:
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[pagerduty_router.get_session] = session_override
    app.dependency_overrides[pagerduty_router.get_admin_org_id] = lambda: _ORG_ID
    app.dependency_overrides[pagerduty_services_router.get_session] = session_override
    app.dependency_overrides[pagerduty_services_router.get_admin_org_id] = lambda: (
        _ORG_ID
    )
    app.dependency_overrides[pagerduty_router.get_admin_user] = lambda: (
        AuthenticatedUser(
            user_id=_USER_ID,
            email="admin@example.test",
            org_id=_ORG_ID,
            role="owner",
        )
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client

    app.dependency_overrides.clear()


async def _authorize(client: AsyncClient, *, datasets: list[str]) -> str:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/authorize",
        json={
            "credential_name": "operations",
            "region": "eu",
            "subdomain": "acme",
            "enabled_datasets": datasets,
        },
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


@pytest.mark.asyncio
async def test_authorize_returns_url_and_persists_hashed_request(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents", "services"])

    async with session_maker() as session:
        persisted = (
            await session.execute(select(PagerDutyOAuthAuthorizationRequest))
        ).scalar_one()

    assert persisted.state_hash == hashlib.sha256(state.encode()).hexdigest()
    assert persisted.state_hash != state
    assert persisted.credential_name == "operations"
    assert persisted.region == "eu"
    assert persisted.subdomain == "acme"
    assert persisted.enabled_datasets == ["incidents", "services"]
    assert persisted.initiated_by == _USER_ID


@pytest.mark.asyncio
async def test_authorize_rejects_unknown_dataset(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/authorize",
        json={"subdomain": "acme", "enabled_datasets": ["unknown"]},
    )

    assert response.status_code == 400
    async with session_maker() as session:
        assert (
            await session.execute(select(PagerDutyOAuthAuthorizationRequest))
        ).scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_authorize_rejects_missing_registered_app_config(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    monkeypatch.setattr(
        pagerduty_router.PagerDutyOAuthConfig,
        "from_env",
        classmethod(lambda _: None),
    )

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/authorize",
        json={"subdomain": "acme", "enabled_datasets": ["incidents"]},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "PAGER_DUTY_CLIENT_ID is not configured"
    async with session_maker() as session:
        assert (
            await session.execute(select(PagerDutyOAuthAuthorizationRequest))
        ).scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_callback_persists_oauth_token_and_safe_descriptor(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents", "services"])
    tokens = OAuthTokens(
        access_token="access-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read", "Services.read"}),
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "connected": True,
        "credential_name": "operations",
        "region": "eu",
        "subdomain": "acme",
        "granted_scopes": ["Incidents.read", "Services.read"],
    }
    async with session_maker() as session:
        oauth_credential = (
            await session.execute(select(ProviderOAuthCredential))
        ).scalar_one()
        descriptor = await IntegrationCredentialsService(
            session, _ORG_ID
        ).get_decrypted_credentials("pagerduty", "operations")
        integration_credential = (
            await session.execute(select(IntegrationCredential))
        ).scalar_one()

    assert oauth_credential.account_id == "acme"
    assert oauth_credential.account_display == "acme"
    assert integration_credential.is_active is True
    assert descriptor is not None
    assert descriptor == {
        "auth_mode": "oauth",
        "oauth_credential_name": "operations",
        "oauth_binding_id": oauth_credential.binding_id,
        "subdomain": "acme",
        "region": "eu",
        "account_id": "acme",
    }
    assert "access_token" not in descriptor
    assert "refresh_token" not in descriptor
    assert integration_credential.config == {
        "auth_mode": "oauth",
        "region": "eu",
        "subdomain": "acme",
        "account_id": "acme",
        "granted_scopes": ["Incidents.read", "Services.read"],
    }


@pytest.mark.asyncio
async def test_callback_rejects_unknown_state_without_persistence(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": "unknown-state", "code": "authorization-code"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid or expired PagerDuty OAuth state"
    assert await _persisted_counts(session_maker) == (0, 0)


def test_pagerduty_routes_are_registered_with_admin_router() -> None:
    registered_paths = {
        route.path for route in admin_router.routes if isinstance(route, APIRoute)
    }

    assert "/api/v1/admin/integrations/pagerduty/authorize" in registered_paths
    assert "/api/v1/admin/integrations/pagerduty/callback" in registered_paths


@pytest.mark.asyncio
async def test_callback_rejects_replayed_state_without_another_persistence(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    exchange = AsyncMock(return_value=tokens)
    monkeypatch.setattr(pagerduty_router, "exchange_code", exchange)

    first_response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )
    replay_response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    assert first_response.status_code == 200
    assert replay_response.status_code == 400
    assert await _persisted_counts(session_maker) == (1, 1)
    exchange.assert_awaited_once()


@pytest.mark.asyncio
async def test_callback_rejects_denied_authorization(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents"])

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "error": "access_denied"},
    )

    assert response.status_code == 400
    assert await _persisted_counts(session_maker) == (0, 0)


@pytest.mark.asyncio
async def test_callback_revokes_and_rejects_missing_required_scope(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Services.read"}),
    )
    revoke = AsyncMock()
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    monkeypatch.setattr(pagerduty_router, "revoke_token", revoke)

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    assert response.status_code == 400
    assert "Incidents.read" in response.json()["detail"]
    revoke.assert_awaited_once_with(_CONFIG, "refresh-token")
    assert await _persisted_counts(session_maker) == (0, 0)

    replay = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )
    assert replay.status_code == 400


@pytest.mark.asyncio
async def test_status_returns_non_secret_oauth_metadata(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/status",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["connected"] is True
    assert payload["auth_mode"] == "oauth"
    assert payload["subdomain"] == "acme"
    assert payload["granted_scopes"] == ["Incidents.read"]
    assert "access_token" not in payload
    assert "refresh_token" not in payload


@pytest.mark.asyncio
async def test_status_returns_not_connected_when_descriptor_is_absent(
    client: AsyncClient,
) -> None:
    response = await client.get("/api/v1/admin/integrations/pagerduty/status")

    assert response.status_code == 200
    assert response.json()["connected"] is False


@pytest.mark.asyncio
async def test_disconnect_deactivates_descriptor_without_deleting_it(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    revoke = AsyncMock()
    monkeypatch.setattr(pagerduty_router, "revoke_token", revoke)
    await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )
    repeated_response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )

    assert response.status_code == 200
    assert repeated_response.status_code == 200
    # Remote revocation is best-effort and happens after the local removal commits.
    revoke.assert_awaited()
    async with session_maker() as session:
        descriptor = await IntegrationCredentialsService(session, _ORG_ID).get(
            "pagerduty", "operations"
        )
        metadata = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get_status_metadata()
    # Descriptor is deactivated (not deleted); all decryptable secret storage is gone.
    assert descriptor is not None
    assert descriptor.is_active is False
    assert descriptor.credentials_encrypted is None
    assert metadata is None


@pytest.mark.asyncio
async def test_preflight_reports_dataset_scopes_without_users_requirement(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/preflight",
        json={
            "credential_name": "operations",
            "enabled_datasets": ["incidents", "users"],
        },
    )

    assert response.status_code == 200
    datasets = {
        dataset["requested"]: dataset for dataset in response.json()["datasets"]
    }
    assert datasets["incidents"] == {
        "requested": "incidents",
        "required_scopes": ["Incidents.read"],
        "granted": True,
        "missing": [],
    }
    assert datasets["users"]["granted"] is False
    assert datasets["users"]["missing"] == ["Users.read"]


@pytest.mark.asyncio
async def test_client_credentials_persists_exact_descriptor(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/client-credentials",
        json={
            "credential_name": "automation",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "subdomain": "acme",
            "region": "eu",
        },
    )

    assert response.status_code == 200
    async with session_maker() as session:
        service = IntegrationCredentialsService(session, _ORG_ID)
        descriptor = await service.get_decrypted_credentials("pagerduty", "automation")
        credential = await service.get("pagerduty", "automation")
    assert descriptor == {
        "auth_mode": "client_credentials",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "subdomain": "acme",
        "region": "eu",
    }
    assert credential is not None
    assert credential.config == {
        "auth_mode": "client_credentials",
        "region": "eu",
        "subdomain": "acme",
    }


@pytest.mark.asyncio
async def test_api_token_persists_exact_descriptor(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/api-token",
        json={
            "credential_name": "personal",
            "api_token": "api-token",
            "subdomain": "acme",
            "region": "us",
        },
    )

    assert response.status_code == 200
    async with session_maker() as session:
        service = IntegrationCredentialsService(session, _ORG_ID)
        descriptor = await service.get_decrypted_credentials("pagerduty", "personal")
        credential = await service.get("pagerduty", "personal")
    assert descriptor == {
        "auth_mode": "api_token",
        "api_token": "api-token",
        "subdomain": "acme",
        "region": "us",
    }
    assert credential is not None
    assert credential.config == {
        "auth_mode": "api_token",
        "region": "us",
        "subdomain": "acme",
    }


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
            "granted_scopes": ["Incidents.read"],
        }
    )

    assert success is True
    assert details == {"records_checked": 0, "missing_scopes": []}
    list_incidents.assert_awaited_once()


@pytest.mark.asyncio
async def test_callback_burns_state_after_denied_authorization(
    client: AsyncClient,
) -> None:
    state = await _authorize(client, datasets=["incidents"])

    denied = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "error": "access_denied"},
    )
    replay = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    assert denied.status_code == 400
    assert replay.status_code == 400


@pytest.mark.asyncio
async def test_callback_burns_state_and_sanitizes_invalid_code_error(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    response = httpx.Response(
        400,
        content=b"provider-secret-response",
        request=httpx.Request("POST", "https://identity.pagerduty.test/oauth/token"),
    )
    monkeypatch.setattr(
        pagerduty_router,
        "exchange_code",
        AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "invalid grant", request=response.request, response=response
            )
        ),
    )

    invalid_code = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )
    replay = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    assert invalid_code.status_code == 400
    assert "provider-secret-response" not in invalid_code.text
    assert replay.status_code == 400


@pytest.mark.asyncio
async def test_callback_sanitizes_upstream_exchange_failure(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    response = httpx.Response(
        503,
        content=b"provider-secret-response",
        request=httpx.Request("POST", "https://identity.pagerduty.test/oauth/token"),
    )
    monkeypatch.setattr(
        pagerduty_router,
        "exchange_code",
        AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "unavailable", request=response.request, response=response
            )
        ),
    )

    upstream_error = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    assert upstream_error.status_code == 502
    assert "provider-secret-response" not in upstream_error.text


@pytest.mark.asyncio
async def test_callback_burns_state_and_revokes_after_descriptor_write_failure(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    revoke = AsyncMock()
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    monkeypatch.setattr(pagerduty_router, "revoke_token", revoke)
    monkeypatch.setattr(
        pagerduty_router.IntegrationCredentialsService,
        "set",
        AsyncMock(side_effect=RuntimeError("descriptor write failure")),
    )

    with pytest.raises(RuntimeError, match="descriptor write failure"):
        await client.post(
            "/api/v1/admin/integrations/pagerduty/callback",
            json={"state": state, "code": "authorization-code"},
        )
    replay = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    revoke.assert_awaited_once_with(_CONFIG, "refresh-token")
    assert replay.status_code == 400


@pytest.mark.asyncio
async def test_callback_burns_state_and_revokes_after_persistence_commit_failure(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        refresh_token="refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    revoke = AsyncMock()
    original_commit = AsyncSession.commit
    commits = 0

    async def commit_with_failure(session: AsyncSession) -> None:
        nonlocal commits
        commits += 1
        if commits == 2:
            raise RuntimeError("persistence commit failure")
        await original_commit(session)

    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    monkeypatch.setattr(pagerduty_router, "revoke_token", revoke)
    monkeypatch.setattr(AsyncSession, "commit", commit_with_failure)

    with pytest.raises(RuntimeError, match="persistence commit failure"):
        await client.post(
            "/api/v1/admin/integrations/pagerduty/callback",
            json={"state": state, "code": "authorization-code"},
        )
    replay = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    revoke.assert_awaited_once_with(_CONFIG, "refresh-token")
    assert replay.status_code == 400


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


@pytest.mark.asyncio
async def test_disconnect_removes_oauth_row_without_descriptor_or_oauth_config(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    state = await _authorize(client, datasets=["incidents"])
    tokens = OAuthTokens(
        access_token="access-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )
    async with session_maker() as session:
        descriptor = await IntegrationCredentialsService(session, _ORG_ID).get(
            "pagerduty", "operations"
        )
        assert descriptor is not None
        await session.delete(descriptor)
        await session.commit()
    monkeypatch.setattr(
        pagerduty_router.PagerDutyOAuthConfig,
        "from_env",
        classmethod(lambda _: None),
    )

    disconnected = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )
    repeated = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )
    async with session_maker() as session:
        metadata = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get_status_metadata()

    assert disconnected.status_code == 200
    assert repeated.status_code == 200
    assert metadata is None


@pytest.mark.asyncio
async def test_non_oauth_modes_remove_existing_oauth_binding(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    tokens = OAuthTokens(
        access_token="access-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    monkeypatch.setattr(
        pagerduty_router, "exchange_code", AsyncMock(return_value=tokens)
    )
    first_state = await _authorize(client, datasets=["incidents"])
    await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": first_state, "code": "authorization-code"},
    )
    monkeypatch.setattr(
        pagerduty_router.PagerDutyOAuthConfig,
        "from_env",
        classmethod(lambda _: None),
    )
    await client.post(
        "/api/v1/admin/integrations/pagerduty/api-token",
        json={
            "credential_name": "operations",
            "api_token": "api-token",
            "subdomain": "acme",
            "region": "us",
        },
    )
    async with session_maker() as session:
        after_api_token = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get_status_metadata()

    monkeypatch.setattr(
        pagerduty_router.PagerDutyOAuthConfig,
        "from_env",
        classmethod(lambda _: _CONFIG),
    )
    second_state = await _authorize(client, datasets=["incidents"])
    await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": second_state, "code": "authorization-code"},
    )
    monkeypatch.setattr(
        pagerduty_router.PagerDutyOAuthConfig,
        "from_env",
        classmethod(lambda _: None),
    )
    await client.post(
        "/api/v1/admin/integrations/pagerduty/client-credentials",
        json={
            "credential_name": "operations",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "subdomain": "acme",
            "region": "us",
        },
    )
    async with session_maker() as session:
        after_client_credentials = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get_status_metadata()

    assert after_api_token is None
    assert after_client_credentials is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/integrations/pagerduty/authorize",
            {"credential_name": " ", "subdomain": "acme", "enabled_datasets": []},
        ),
        (
            "/integrations/pagerduty/client-credentials",
            {
                "credential_name": "automation",
                "client_id": "id",
                "client_secret": "secret",
                "subdomain": " ",
                "region": "us",
            },
        ),
        (
            "/integrations/pagerduty/api-token",
            {
                "credential_name": " ",
                "api_token": "token",
                "subdomain": "acme",
                "region": "us",
            },
        ),
    ],
)
async def test_setup_rejects_blank_required_text(
    client: AsyncClient, path: str, payload: dict[str, object]
) -> None:
    response = await client.post(f"/api/v1/admin{path}", json=payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_client_credentials_strips_descriptor_text(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/client-credentials",
        json={
            "credential_name": " automation ",
            "client_id": "id",
            "client_secret": "secret",
            "subdomain": " acme ",
            "region": "us",
        },
    )
    async with session_maker() as session:
        descriptor = await IntegrationCredentialsService(
            session, _ORG_ID
        ).get_decrypted_credentials("pagerduty", "automation")

    assert response.status_code == 200
    assert descriptor is not None
    assert descriptor["subdomain"] == "acme"


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
    assert "Incidents.read" in detail["missing_scopes"]
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
            "granted_scopes": ["Incidents.read"],
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
                "granted_scopes": ["Incidents.read"],
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
        granted_scopes=frozenset(granted or {"Incidents.read"}),
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
async def test_callback_rejects_missing_subdomain_before_exchange(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await pagerduty_router.PagerDutyAuthorizationRequestStore(session).create(
            org_id=_ORG_ID,
            state="state-without-subdomain",
            credential_name="operations",
            code_verifier="verifier",
            enabled_datasets=["incidents"],
            region="eu",
            subdomain=None,
            initiated_by=_USER_ID,
        )
        await session.commit()
    exchange = AsyncMock(side_effect=AssertionError("exchange must not run"))
    monkeypatch.setattr(pagerduty_router, "exchange_code", exchange)

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": "state-without-subdomain", "code": "authorization-code"},
    )

    assert response.status_code == 400
    exchange.assert_not_awaited()


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


@pytest.mark.asyncio
async def test_disconnect_deletes_oauth_row_even_with_corrupt_ciphertext(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _connect_oauth(client, monkeypatch)
    async with session_maker() as session:
        row = (await session.execute(select(ProviderOAuthCredential))).scalar_one()
        row.token_encrypted = "corrupt-not-decryptable"
        await session.commit()
    monkeypatch.setattr(pagerduty_router, "revoke_token", AsyncMock())

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )

    assert response.status_code == 200
    async with session_maker() as session:
        metadata = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get_status_metadata()
    assert metadata is None


@pytest.mark.asyncio
async def test_disconnect_commit_failure_rolls_back_all_local_secret_cleanup(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _connect_oauth(client, monkeypatch)
    async with session_maker() as session:
        descriptor_before = (
            await session.execute(
                select(IntegrationCredential).where(
                    IntegrationCredential.org_id == _ORG_ID,
                    IntegrationCredential.provider == "pagerduty",
                    IntegrationCredential.name == "operations",
                )
            )
        ).scalar_one()
        oauth_before = (
            await session.execute(
                select(ProviderOAuthCredential).where(
                    ProviderOAuthCredential.org_id == _ORG_ID,
                    ProviderOAuthCredential.provider == "pagerduty",
                    ProviderOAuthCredential.credential_name == "operations",
                )
            )
        ).scalar_one()
        descriptor_ciphertext = descriptor_before.credentials_encrypted
        oauth_ciphertext = oauth_before.token_encrypted

    async def fail_commit(_session: AsyncSession) -> None:
        raise RuntimeError("disconnect commit failure")

    revoke = AsyncMock()
    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    monkeypatch.setattr(pagerduty_router, "revoke_token", revoke)

    with pytest.raises(RuntimeError, match="disconnect commit failure"):
        await client.post(
            "/api/v1/admin/integrations/pagerduty/disconnect",
            json={"credential_name": "operations"},
        )

    revoke.assert_not_awaited()
    async with session_maker() as session:
        descriptor_after = (
            await session.execute(
                select(IntegrationCredential).where(
                    IntegrationCredential.org_id == _ORG_ID,
                    IntegrationCredential.provider == "pagerduty",
                    IntegrationCredential.name == "operations",
                )
            )
        ).scalar_one()
        oauth_after = (
            await session.execute(
                select(ProviderOAuthCredential).where(
                    ProviderOAuthCredential.org_id == _ORG_ID,
                    ProviderOAuthCredential.provider == "pagerduty",
                    ProviderOAuthCredential.credential_name == "operations",
                )
            )
        ).scalar_one()
    assert descriptor_after.is_active is True
    assert descriptor_after.credentials_encrypted == descriptor_ciphertext
    assert oauth_after.token_encrypted == oauth_ciphertext


@pytest.mark.asyncio
async def test_disconnect_preserves_unrelated_connector_credentials(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _connect_oauth(client, monkeypatch)
    standby_tokens = OAuthTokens(
        access_token="standby-access-token",
        refresh_token="standby-refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=frozenset({"Incidents.read"}),
    )
    async with session_maker() as session:
        service = IntegrationCredentialsService(session, _ORG_ID)
        standby_descriptor = await service.set(
            "pagerduty",
            {
                "auth_mode": "oauth",
                "oauth_credential_name": "standby",
                "oauth_binding_id": "standby-binding",
                "subdomain": "acme",
                "region": "us",
                "account_id": "acme",
            },
            name="standby",
            config={
                "auth_mode": "oauth",
                "subdomain": "acme",
                "region": "us",
                "account_id": "acme",
                "granted_scopes": ["Incidents.read"],
            },
        )
        github_descriptor = await service.set(
            "github",
            {"token": "unrelated-github-token"},
            name="source-control",
        )
        await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "standby"
        ).create_or_replace(standby_tokens, binding_id="standby-binding")
        await session.commit()
        standby_ciphertext = standby_descriptor.credentials_encrypted
        github_ciphertext = github_descriptor.credentials_encrypted
    monkeypatch.setattr(pagerduty_router, "revoke_token", AsyncMock())

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )

    assert response.status_code == 200
    async with session_maker() as session:
        service = IntegrationCredentialsService(session, _ORG_ID)
        standby_after = await service.get("pagerduty", "standby")
        github_after = await service.get("github", "source-control")
        standby_oauth = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "standby"
        ).get()
    assert standby_after is not None
    assert standby_after.is_active is True
    assert standby_after.credentials_encrypted == standby_ciphertext
    assert standby_oauth is not None
    assert standby_oauth.tokens == standby_tokens
    assert github_after is not None
    assert github_after.is_active is True
    assert github_after.credentials_encrypted == github_ciphertext


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("credential_name", "credentials", "config"),
    [
        (
            "api-token",
            {
                "auth_mode": "api_token",
                "api_token": "legacy-token",
                "subdomain": "acme",
                "region": "us",
            },
            {"auth_mode": "api_token", "subdomain": "acme", "region": "us"},
        ),
        (
            "client-credentials",
            {
                "auth_mode": "client_credentials",
                "client_id": "legacy-client-id",
                "client_secret": "legacy-client-secret",
                "subdomain": "acme",
                "region": "us",
            },
            {
                "auth_mode": "client_credentials",
                "subdomain": "acme",
                "region": "us",
            },
        ),
    ],
)
async def test_disconnect_deactivates_and_clears_legacy_descriptor_secrets(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
    credential_name: str,
    credentials: dict[str, str],
    config: dict[str, str],
) -> None:
    import json

    from dev_health_ops.core.encryption import encrypt_value

    async with session_maker() as session:
        session.add(
            IntegrationCredential(
                provider="pagerduty",
                name=credential_name,
                org_id=_ORG_ID,
                credentials_encrypted=encrypt_value(json.dumps(credentials)),
                config=config,
                is_active=True,
            )
        )
        await session.commit()

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": credential_name},
    )

    assert response.status_code == 200
    async with session_maker() as session:
        descriptor = await IntegrationCredentialsService(session, _ORG_ID).get(
            "pagerduty", credential_name
        )
    assert descriptor is not None
    assert descriptor.is_active is False
    assert descriptor.credentials_encrypted is None


@pytest.mark.asyncio
async def test_disconnect_revokes_only_after_local_removal_committed(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _connect_oauth(client, monkeypatch)
    observed: dict[str, bool] = {}

    async def spy_revoke(config: object, token: str) -> None:
        async with session_maker() as session:
            meta = await PagerDutyOAuthCredentialRepository(
                session, _ORG_ID, "operations"
            ).get_status_metadata()
        observed["row_present_at_revoke"] = meta is not None

    monkeypatch.setattr(pagerduty_router, "revoke_token", spy_revoke)

    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )

    assert response.status_code == 200
    # Revocation ran, and by the time it ran the local row was already committed-gone.
    assert observed.get("row_present_at_revoke") is False


@pytest.mark.asyncio
async def test_disconnect_rejects_whitespace_credential_name(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "   "},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_preflight_rejects_whitespace_credential_name(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/preflight",
        json={"credential_name": "   ", "enabled_datasets": ["incidents"]},
    )
    assert response.status_code == 422


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


@pytest.mark.asyncio
async def test_remove_oauth_binding_reraises_unexpected_error_after_delete(
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "pagerduty-oauth-test-encryption-key")
    async with session_maker() as session:
        repository = PagerDutyOAuthCredentialRepository(session, _ORG_ID, "operations")
        await repository.create_or_replace(
            OAuthTokens(
                access_token="access-token",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
            binding_id="binding-id",
        )
        await session.commit()

        async def boom() -> None:
            raise RuntimeError("programming error")

        monkeypatch.setattr(repository, "get", boom)
        # An unexpected (non-ValueError) failure must propagate, not be swallowed...
        with pytest.raises(RuntimeError, match="programming error"):
            await pagerduty_router._remove_oauth_binding(repository, _CONFIG)
        # ...while the local row is still deleted via the finally clause.
        assert await repository.get_status_metadata() is None


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
                "granted_scopes": ["Services.read"],
            },
        )
        await session.commit()


@pytest.mark.asyncio
async def test_services_returns_resolved_names_sorted_and_closes_client(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _save_api_token_credential(session_maker)
    _ServiceDiscoveryClient.instances.clear()
    _ServiceDiscoveryClient.error = None
    _ServiceDiscoveryClient.services = [
        Service(id="P2", name=None, status="disabled"),
        Service(id="P1", name="Checkout", status="active"),
        Service(id="P3", name="Alerts", status="active"),
    ]
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "credential_name": "operations",
        "services": [
            {
                "external_id": "P3",
                "display_name": "Alerts",
                "name_resolved": True,
                "status": "active",
            },
            {
                "external_id": "P1",
                "display_name": "Checkout",
                "name_resolved": True,
                "status": "active",
            },
            {
                "external_id": "P2",
                "display_name": "PagerDuty service P2",
                "name_resolved": False,
                "status": "disabled",
            },
        ],
    }
    instance = _ServiceDiscoveryClient.instances[-1]
    assert instance.region == "us"
    assert instance.closed == 1


@pytest.mark.asyncio
async def test_services_are_scoped_to_current_org(
    client: AsyncClient,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _save_api_token_credential(session_maker, org_id="another-org")

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_services_close_client_when_provider_fails(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _save_api_token_credential(session_maker)
    _ServiceDiscoveryClient.instances.clear()
    _ServiceDiscoveryClient.error = APIException("provider unavailable")
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 502
    assert response.json() == {
        "detail": "PagerDuty services are temporarily unavailable"
    }
    assert _ServiceDiscoveryClient.instances[-1].closed == 1


@pytest.mark.asyncio
async def test_services_maps_authentication_failure_to_reconnect_response(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _save_api_token_credential(session_maker)
    monkeypatch.setattr(
        _ServiceDiscoveryClient,
        "error",
        AuthenticationException("provider rejected secret-token"),
    )
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "PagerDuty credential is no longer authorized"}
    assert "secret-token" not in response.text


@pytest.mark.asyncio
async def test_services_maps_rate_limit_with_retry_after(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _save_api_token_credential(session_maker)
    monkeypatch.setattr(
        _ServiceDiscoveryClient,
        "error",
        RateLimitException(retry_after_seconds=17.5),
    )
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "18"
    assert response.json() == {"detail": "PagerDuty rate limit exceeded"}


@pytest.mark.asyncio
async def test_services_oauth_uses_descriptor_referenced_credential(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _connect_oauth(
        client,
        monkeypatch,
        datasets=["services"],
        granted={"Services.read"},
    )
    async with session_maker() as session:
        metadata = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get_status_metadata()
    assert metadata is not None
    assert metadata.binding_id is not None
    await _save_oauth_descriptor(
        session_maker,
        name="service-catalog",
        oauth_credential_name="operations",
        binding_id=metadata.binding_id,
    )
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )
    monkeypatch.setattr(_ServiceDiscoveryClient, "error", None)
    monkeypatch.setattr(_ServiceDiscoveryClient, "services", [])

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "service-catalog"},
    )

    assert response.status_code == 200
    assert _ServiceDiscoveryClient.instances[-1].auth.headers() == {
        "Authorization": "Bearer access-token"
    }


@pytest.mark.asyncio
async def test_services_oauth_rejects_descriptor_binding_mismatch(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    await _connect_oauth(
        client,
        monkeypatch,
        datasets=["services"],
        granted={"Services.read"},
    )
    await _save_oauth_descriptor(
        session_maker,
        name="operations",
        oauth_credential_name="operations",
        binding_id="stale-binding",
    )
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )
    monkeypatch.setattr(_ServiceDiscoveryClient, "error", None)
    monkeypatch.setattr(_ServiceDiscoveryClient, "services", [])

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": "PagerDuty OAuth credential must be reconnected"
    }


@pytest.mark.asyncio
async def test_services_commit_oauth_rotation_before_provider_io(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _connect_oauth(
        client,
        monkeypatch,
        datasets=["services"],
        granted={"Services.read"},
    )
    events: list[str] = []
    original_commit = AsyncSession.commit

    async def record_commit(session: AsyncSession) -> None:
        events.append("commit")
        await original_commit(session)

    async def access_token(*_args: object, **_kwargs: object) -> str:
        events.append("token")
        return "rotated-access-token"

    monkeypatch.setattr(AsyncSession, "commit", record_commit)
    monkeypatch.setattr(
        pagerduty_services_router, "get_valid_access_token", access_token
    )
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )
    _ServiceDiscoveryClient.instances.clear()
    _ServiceDiscoveryClient.error = None
    _ServiceDiscoveryClient.services = []
    _ServiceDiscoveryClient.events = events

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "operations"},
    )

    assert response.status_code == 200
    assert events[:3] == ["token", "commit", "list"]
    assert _ServiceDiscoveryClient.instances[-1].closed == 1
    _ServiceDiscoveryClient.events = None


@pytest.mark.asyncio
async def test_services_materializes_client_credentials_without_exposing_secret(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        await IntegrationCredentialsService(session, _ORG_ID).set(
            provider="pagerduty",
            name="automation",
            credentials={
                "auth_mode": "client_credentials",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "subdomain": "acme",
                "region": "eu",
            },
            config={
                "auth_mode": "client_credentials",
                "subdomain": "acme",
                "region": "eu",
            },
        )
        await session.commit()
    token_exchange = AsyncMock(
        return_value=OAuthTokens(
            access_token="machine-access-token",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    monkeypatch.setattr(pagerduty_services_router, "client_credentials", token_exchange)
    monkeypatch.setattr(
        pagerduty_services_router, "PagerDutyClient", _ServiceDiscoveryClient
    )
    _ServiceDiscoveryClient.instances.clear()
    _ServiceDiscoveryClient.error = None
    _ServiceDiscoveryClient.services = []

    response = await client.get(
        "/api/v1/admin/integrations/pagerduty/services",
        params={"credential_name": "automation"},
    )

    assert response.status_code == 200
    instance = _ServiceDiscoveryClient.instances[-1]
    assert instance.region == "eu"
    assert instance.auth.headers() == {"Authorization": "Bearer machine-access-token"}
    assert "client-secret" not in response.text
