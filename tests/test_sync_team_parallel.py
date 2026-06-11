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
    # Tracks live DB sessions opened via get_postgres_session.
    sessions = {"open": 0, "peak_open": 0}
    open_during_discovery = 0

    async def slow_discover(*_args, **_kwargs):
        nonlocal active, peak, open_during_discovery
        active += 1
        peak = max(peak, active)
        # CHAOS-2066 invariant: no DB connection may be held while the slow
        # external discovery call is in flight.
        open_during_discovery = max(open_during_discovery, sessions["open"])
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

    async def slow_discover_gitlab(*args, **kwargs):
        from dev_health_ops.api.services.configuration import GitLabDiscoveryResult

        teams = await slow_discover(*args, **kwargs)
        return GitLabDiscoveryResult(teams=teams)

    fake_discovery = MagicMock()
    fake_discovery.discover_github = slow_discover
    fake_discovery.discover_gitlab = slow_discover_gitlab
    fake_discovery.discover_jira = slow_discover

    fake_drift = MagicMock()
    fake_drift.run_drift_sync = AsyncMock(return_value={"provider": "x"})

    class _FakeSession:
        async def __aenter__(self):
            sessions["open"] += 1
            sessions["peak_open"] = max(sessions["peak_open"], sessions["open"])
            return MagicMock(commit=AsyncMock())

        async def __aexit__(self, *a):
            sessions["open"] -= 1
            return False

        async def commit(self):
            return None

    with (
        patch(
            "dev_health_ops.api.services.configuration.IntegrationCredentialsService",
            return_value=fake_creds,
        ),
        patch(
            "dev_health_ops.api.services.configuration.TeamDiscoveryService",
            return_value=fake_discovery,
        ),
        patch(
            "dev_health_ops.api.services.configuration.TeamDriftSyncService",
            return_value=fake_drift,
        ),
        patch("dev_health_ops.db.get_postgres_session", lambda: _FakeSession()),
    ):
        result = await mod._discover_and_sync_all(org_id="org-1")

    assert peak >= 3, f"Expected 3 concurrent providers, observed peak={peak}"
    assert len(result["results"]) == 3
    # CHAOS-2066: the connection is scoped to DB ops only -- never held during
    # discovery, and never more than one open at a time per job.
    assert open_during_discovery == 0, (
        f"DB connection held during discovery (open={open_during_discovery})"
    )
    assert sessions["peak_open"] <= 1, (
        f"More than one session open at once (peak={sessions['peak_open']})"
    )
    assert sessions["open"] == 0, "Session leaked (not closed)"
