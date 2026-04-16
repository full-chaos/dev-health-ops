"""Assert discover_members_jira_bulk runs per-project lookups concurrently."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest


@pytest.mark.asyncio
async def test_bulk_members_concurrent(monkeypatch):
    from dev_health_ops.api.services import settings as mod

    active = 0
    peak = 0

    async def slow_single(self, email, api_token, url, project_key):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            return [
                # 1 member per project
                MagicMock(provider_identity=f"user-{project_key}")
            ]
        finally:
            active -= 1

    monkeypatch.setattr(mod.TeamMembershipService, "discover_members_jira", slow_single)

    svc = mod.TeamMembershipService(session=MagicMock(), org_id="org-1")
    out = await svc.discover_members_jira_bulk(
        email="e@x",
        api_token="t",
        url="https://j",
        project_keys=["P1", "P2", "P3", "P4", "P5", "P6"],
        concurrency=5,
    )

    # Members flattened across projects
    assert len(out) == 6
    # >=5 in-flight at peak (limited by semaphore)
    assert peak >= 5
    # Not more than 5 at once
    assert peak <= 5


@pytest.mark.asyncio
async def test_bulk_members_honours_concurrency_cap(monkeypatch):
    from dev_health_ops.api.services import settings as mod

    active = 0
    peak = 0

    async def slow_single(self, email, api_token, url, project_key):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.02)
            return []
        finally:
            active -= 1

    monkeypatch.setattr(mod.TeamMembershipService, "discover_members_jira", slow_single)

    svc = mod.TeamMembershipService(session=MagicMock(), org_id="o")
    await svc.discover_members_jira_bulk(
        email="e",
        api_token="t",
        url="u",
        project_keys=[str(i) for i in range(10)],
        concurrency=2,
    )
    assert peak == 2
