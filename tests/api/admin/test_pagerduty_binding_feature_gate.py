from __future__ import annotations

import importlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding

bindings_router = importlib.import_module(
    "dev_health_ops.api.admin.routers.pagerduty_bindings"
)


@pytest.mark.asyncio
async def test_binding_create_fails_closed_before_storing_a_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = AsyncMock(
        side_effect=HTTPException(status_code=403, detail="feature disabled")
    )
    monkeypatch.setattr(bindings_router, "_require_canonical_incident_ingestion", gate)
    app = FastAPI()
    app.include_router(bindings_router.router, prefix="/api/v1/admin")

    async def session_override():
        yield None

    app.dependency_overrides[bindings_router.get_session] = session_override
    app.dependency_overrides[bindings_router.get_admin_org_id] = lambda: str(uuid4())

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/admin/integrations/pagerduty/webhook-bindings",
            json={
                "integration_source_id": str(uuid4()),
                "credential_id": str(uuid4()),
                "provider_subscription_id": "subscription-1",
                "signing_secret": "secret",
            },
        )

    assert response.status_code == 403
    gate.assert_awaited_once()


def test_binding_response_preserves_detached_credential_history() -> None:
    # Given
    binding = PagerDutyWebhookBinding(
        id=uuid4(),
        org_id=uuid4(),
        integration_source_id=uuid4(),
        credential_id=None,
        provider_subscription_id="subscription-1",
        signing_secret_encrypted="encrypted",
        signing_secret_key_version="v1",
        status="inactive",
        created_at=datetime.now(UTC),
    )

    # When
    response = bindings_router._response(binding)

    # Then
    assert response.credential_id is None
