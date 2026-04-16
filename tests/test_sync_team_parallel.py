"""Assert sync_team_drift dispatches provider discovery concurrently."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_providers_discovered_concurrently(monkeypatch):
    from dev_health_ops.workers import sync_team as mod

    active = 0
    peak = 0

    async def slow_discover(*_args, **_kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            return []
        finally:
            active -= 1

    fake_creds = MagicMock()
    fake_creds.get = AsyncMock(
        return_value=MagicMock(config={"org": "o", "group": "g", "url": "https://j"})
    )
    fake_creds.get_decrypted_credentials = AsyncMock(
        return_value={"token": "t", "email": "e@x", "api_token": "a"}
    )

    fake_discovery = MagicMock()
    fake_discovery.discover_github = slow_discover
    fake_discovery.discover_gitlab = slow_discover
    fake_discovery.discover_jira = slow_discover

    fake_drift = MagicMock()
    fake_drift.run_drift_sync = AsyncMock(return_value={"provider": "x"})

    class _FakeSession:
        async def __aenter__(self):
            return MagicMock(commit=AsyncMock())

        async def __aexit__(self, *a):
            return False

        async def commit(self):
            return None

    with (
        patch(
            "dev_health_ops.api.services.settings.IntegrationCredentialsService",
            return_value=fake_creds,
        ),
        patch(
            "dev_health_ops.api.services.settings.TeamDiscoveryService",
            return_value=fake_discovery,
        ),
        patch(
            "dev_health_ops.api.services.settings.TeamDriftSyncService",
            return_value=fake_drift,
        ),
        patch("dev_health_ops.db.get_postgres_session", lambda: _FakeSession()),
    ):
        # Call the async body directly via the helper we'll add
        result = await mod._discover_and_sync_all(org_id="org-1")

    assert peak >= 3, f"Expected 3 concurrent providers, observed peak={peak}"
    assert len(result["results"]) == 3
