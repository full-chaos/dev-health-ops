from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dev_health_ops.providers.pagerduty.oauth import (
    READ_SCOPES,
    OAuthTokens,
    PagerDutyOAuthConfig,
)
from dev_health_ops.providers.pagerduty.sync_auth import (
    hydrate_pagerduty_credentials_async,
)


@pytest.mark.anyio
async def test_async_oauth_hydration_uses_the_running_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def postgres_session():
        yield SimpleNamespace()

    class Repository:
        def __init__(
            self,
            session: SimpleNamespace,
            org_id: str,
            credential_name: str,
            *,
            expected_binding_id: str | None = None,
        ) -> None:
            assert session is not None
            assert org_id == "org-1"
            assert credential_name == "primary"
            assert expected_binding_id == "binding-1"

        async def get(self) -> SimpleNamespace:
            return SimpleNamespace(
                tokens=OAuthTokens(
                    access_token="oauth-token",
                    expires_at=datetime.now(UTC) + timedelta(hours=1),
                    granted_scopes=READ_SCOPES,
                )
            )

    monkeypatch.setenv("PAGER_DUTY_CLIENT_ID", "client-id")
    monkeypatch.setenv("PAGER_DUTY_CLIENT_SECRET", "client-secret")
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth.get_postgres_session",
        postgres_session,
    )
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth.PagerDutyOAuthCredentialRepository",
        Repository,
    )

    hydrated = await hydrate_pagerduty_credentials_async(
        {
            "auth_mode": "oauth",
            "oauth_credential_name": "primary",
            "oauth_binding_id": "binding-1",
        },
        org_id="org-1",
    )

    assert hydrated["access_token"] == "oauth-token"
    assert hydrated["granted_scopes"] == sorted(READ_SCOPES)


@pytest.mark.anyio
async def test_async_client_credentials_hydration_mints_scoped_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def client_credentials(
        config: PagerDutyOAuthConfig,
        *,
        scopes: set[str],
        subdomain: str,
        region: str,
    ) -> OAuthTokens:
        assert config.client_id == "client-id"
        assert config.client_secret == "client-secret"
        assert scopes == set(READ_SCOPES)
        assert subdomain == "acme"
        assert region == "eu"
        return OAuthTokens(
            access_token="machine-token",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            granted_scopes=READ_SCOPES,
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.client_credentials",
        client_credentials,
    )

    hydrated = await hydrate_pagerduty_credentials_async(
        {
            "auth_mode": "client_credentials",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "subdomain": "acme",
            "region": "eu",
        },
        org_id="org-1",
    )

    assert hydrated["access_token"] == "machine-token"
    assert hydrated["granted_scopes"] == sorted(READ_SCOPES)
