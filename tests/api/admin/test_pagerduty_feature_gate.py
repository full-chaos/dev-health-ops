from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.core.encryption import decrypt_value
from dev_health_ops.models.settings import ProviderOAuthRevocation
from dev_health_ops.providers.pagerduty.oauth import READ_SCOPES, OAuthTokens
from dev_health_ops.providers.pagerduty.oauth_storage import (
    PagerDutyOAuthCredentialRepository,
)
from tests.api.admin.test_pagerduty_oauth_setup import (
    _ORG_ID,
    _authorize,
    pagerduty_revocations,
    pagerduty_router,
)

pytest_plugins = ("tests.api.admin.test_pagerduty_oauth_setup",)


@pytest.fixture(autouse=True)
def _canonical_incident_feature_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=True),
        raising=False,
    )


@pytest.mark.asyncio
async def test_authorize_rejects_when_canonical_incident_feature_is_off(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=False),
        raising=False,
    )

    # When
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/authorize",
        json={},
    )

    # Then
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_status_remains_available_when_canonical_incident_feature_is_off(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=False),
        raising=False,
    )

    # When
    response = await client.get("/api/v1/admin/integrations/pagerduty/status")

    # Then
    assert response.status_code == 200
    assert response.json()["connected"] is False


@pytest.mark.asyncio
async def test_preflight_remains_available_when_canonical_incident_feature_is_off(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=False),
        raising=False,
    )

    # When
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/preflight",
        json={"credential_name": "operations", "enabled_datasets": ["incidents"]},
    )

    # Then
    assert response.status_code == 200
    assert response.json()["connected"] is False


@pytest.mark.asyncio
async def test_disconnect_clears_secrets_when_canonical_incident_feature_is_off(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    # Given
    async with session_maker() as session:
        await IntegrationCredentialsService(session, _ORG_ID).set(
            provider="pagerduty",
            name="operations",
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
            is_active=True,
        )
        await session.commit()
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=False),
        raising=False,
    )

    # When
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/disconnect",
        json={"credential_name": "operations"},
    )

    # Then
    assert response.status_code == 200
    async with session_maker() as session:
        descriptor = await IntegrationCredentialsService(session, _ORG_ID).get(
            "pagerduty", "operations"
        )
    assert descriptor is not None
    assert descriptor.is_active is False
    assert descriptor.credentials_encrypted is None


@pytest.mark.asyncio
async def test_started_oauth_callback_completes_after_feature_flip(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    # Given
    first_state = await _authorize(client, datasets=["incidents"])
    first_tokens = OAuthTokens(
        access_token="first-access-token",
        refresh_token="first-refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=READ_SCOPES,
    )
    second_tokens = OAuthTokens(
        access_token="second-access-token",
        refresh_token="second-refresh-token",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=READ_SCOPES,
    )
    monkeypatch.setattr(
        pagerduty_router,
        "exchange_code",
        AsyncMock(side_effect=[first_tokens, second_tokens]),
    )
    first_response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": first_state, "code": "first-authorization-code"},
    )
    assert first_response.status_code == 200
    state = await _authorize(client, datasets=["incidents"])
    response = httpx.Response(
        401,
        request=httpx.Request("POST", "https://identity.pagerduty.test/revoke"),
    )
    monkeypatch.setattr(
        pagerduty_revocations,
        "revoke_token",
        AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "unauthorized", request=response.request, response=response
            )
        ),
    )
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=False),
        raising=False,
    )

    # When
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/callback",
        json={"state": state, "code": "authorization-code"},
    )

    # Then
    assert response.status_code == 200
    assert response.json()["connected"] is True
    async with session_maker() as session:
        current = await PagerDutyOAuthCredentialRepository(
            session, _ORG_ID, "operations"
        ).get()
        pending = list(
            (await session.execute(select(ProviderOAuthRevocation))).scalars()
        )
    assert current is not None
    assert current.tokens == second_tokens
    assert len(pending) == 1
    assert pending[0].purpose == "replacement"
    assert pending[0].status == "pending"
    assert pending[0].attempts == 1
    assert pending[0].last_error == "remote_revoke_failed"
    assert decrypt_value(pending[0].token_encrypted) == "first-refresh-token"
