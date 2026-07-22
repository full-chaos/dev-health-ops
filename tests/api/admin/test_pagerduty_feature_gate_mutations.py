from __future__ import annotations

import uuid
from unittest.mock import ANY, AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.models.settings import (
    IntegrationCredential,
    PagerDutyOAuthAuthorizationRequest,
)
from tests.api.admin.test_pagerduty_oauth_setup import (
    _ORG_ID,
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
@pytest.mark.parametrize(
    ("path", "payload"),
    (
        (
            "/api/v1/admin/integrations/pagerduty/authorize",
            {},
        ),
        (
            "/api/v1/admin/integrations/pagerduty/client-credentials",
            {
                "credential_name": "operations",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "subdomain": "acme",
                "region": "us",
            },
        ),
        (
            "/api/v1/admin/integrations/pagerduty/api-token",
            {
                "credential_name": "operations",
                "api_token": "api-token",
                "subdomain": "acme",
                "region": "us",
            },
        ),
    ),
)
async def test_feature_off_blocks_new_pagerduty_credentials_before_persistence(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
    path: str,
    payload: dict[str, str | list[str]],
) -> None:
    # Given
    evaluator = AsyncMock(return_value=False)
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        evaluator,
        raising=False,
    )

    # When
    response = await client.post(path, json=payload)

    # Then
    assert response.status_code == 403
    assert response.json()["detail"] == (
        "Canonical incident ingestion is not enabled for this organization"
    )
    evaluator.assert_awaited_once_with(
        ANY,
        uuid.UUID(_ORG_ID),
        "canonical_incident_ingestion",
    )
    async with session_maker() as session:
        assert await session.scalar(select(PagerDutyOAuthAuthorizationRequest)) is None
        assert await session.scalar(select(IntegrationCredential)) is None


@pytest.mark.asyncio
async def test_pagerduty_gate_fails_closed_when_evaluator_storage_fails(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(side_effect=SQLAlchemyError("feature store unavailable")),
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
async def test_feature_off_blocks_reconnect_without_reactivating_descriptor(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    # Given
    async with session_maker() as session:
        descriptor = await IntegrationCredentialsService(session, _ORG_ID).set(
            provider="pagerduty",
            name="operations",
            credentials={
                "auth_mode": "api_token",
                "api_token": "old-token",
                "subdomain": "acme",
                "region": "us",
            },
            config={
                "auth_mode": "api_token",
                "subdomain": "acme",
                "region": "us",
            },
            is_active=False,
        )
        descriptor.credentials_encrypted = None
        await session.commit()
    monkeypatch.setattr(
        pagerduty_router,
        "is_org_feature_enabled_async",
        AsyncMock(return_value=False),
        raising=False,
    )

    # When
    response = await client.post(
        "/api/v1/admin/integrations/pagerduty/api-token",
        json={
            "credential_name": "operations",
            "api_token": "new-token",
            "subdomain": "acme",
            "region": "us",
        },
    )

    # Then
    assert response.status_code == 403
    async with session_maker() as session:
        stored_descriptor = await IntegrationCredentialsService(session, _ORG_ID).get(
            "pagerduty", "operations"
        )
    assert stored_descriptor is not None
    assert stored_descriptor.is_active is False
    assert stored_descriptor.credentials_encrypted is None
