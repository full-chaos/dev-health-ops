"""Assert sync_team_drift dispatches provider discovery concurrently.

Covers the full capability registry: github, gitlab, jira, linear, ms-teams.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
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
        return_value=MagicMock(
            config={
                "org": "o",
                "group": "g",
                "url": "https://j",
            }
        )
    )
    fake_creds.get_decrypted_credentials = AsyncMock(
        return_value={
            "token": "t",
            "email": "e@x",
            "api_token": "a",
            "api_key": "lk",
            "tenant_id": "tid",
            "client_id": "cid",
            "client_secret": "csec",
        }
    )

    async def slow_discover_gitlab(*args, **kwargs):
        from dev_health_ops.api.services.configuration import GitLabDiscoveryResult

        teams = await slow_discover(*args, **kwargs)
        return GitLabDiscoveryResult(teams=teams)

    fake_discovery = MagicMock()
    fake_discovery.discover_github = slow_discover
    fake_discovery.discover_gitlab = slow_discover_gitlab
    fake_discovery.discover_jira = slow_discover
    fake_discovery.discover_linear = slow_discover
    fake_discovery.discover_ms_teams = slow_discover

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

    assert peak >= 5, f"Expected 5 concurrent providers, observed peak={peak}"
    assert len(result["results"]) == 5
    # CHAOS-2066: the connection is scoped to DB ops only -- never held during
    # discovery, and never more than one open at a time per job.
    assert open_during_discovery == 0, (
        f"DB connection held during discovery (open={open_during_discovery})"
    )
    assert sessions["peak_open"] <= 1, (
        f"More than one session open at once (peak={sessions['peak_open']})"
    )
    assert sessions["open"] == 0, "Session leaked (not closed)"


def test_worker_reads_team_capability_registry():
    import inspect

    from dev_health_ops.workers import sync_team as mod

    source = inspect.getsource(mod._discover_and_sync_all)
    assert "org_drift_capable_providers()" in source
    assert '("github", "gitlab", "jira", "linear", "ms-teams")' not in source


def test_reconcile_team_members_uses_clickhouse_uri_and_preserves_org_scope():
    import inspect

    from dev_health_ops.workers import sync_team as mod

    source = inspect.getsource(mod.reconcile_team_members)
    assert "require_clickhouse_uri()" in source
    assert "bridge_teams_to_clickhouse" in source
    assert "_get_db_url" not in source
    assert 'org_id=str(getattr(team, "org_id"' in source


def test_reconcile_team_members_matches_bridge_member_facets(monkeypatch):
    from dev_health_ops.providers import team_bridge
    from dev_health_ops.workers import sync_team as mod

    identity_mappings = [
        SimpleNamespace(
            canonical_id="u1",
            email="alice@example.com",
            display_name="Alice Example",
            provider_identities={"github": ["alice-gh"], "jira": ["alice-jira"]},
            team_ids=["team-1"],
        ),
        SimpleNamespace(
            canonical_id="u2",
            email=None,
            display_name="Bob Example",
            provider_identities={"linear": ["bob-linear"]},
            team_ids=["team-1"],
        ),
    ]
    team_mapping = SimpleNamespace(
        team_id="team-1",
        name="Team One",
        description="A team",
        project_keys=[],
        repo_patterns=[],
        updated_at=datetime.now(timezone.utc),
    )

    class Scalars:
        def __init__(self, rows):
            self.rows = rows

        def scalars(self):
            return self

        def all(self):
            return self.rows

    class BridgeSession:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            self.calls += 1
            return Scalars([team_mapping] if self.calls == 1 else identity_mappings)

    class FakeStore:
        inserted: list[list[dict[str, object]]] = []

        def __init__(self, _uri):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def insert_teams(self, teams: list[dict[str, object]]):
            self.__class__.inserted.append(list(teams))

    monkeypatch.setattr(
        team_bridge, "get_postgres_session_sync", lambda: BridgeSession()
    )
    monkeypatch.setattr(team_bridge, "ClickHouseStore", FakeStore)
    monkeypatch.setattr(
        team_bridge, "_clickhouse_uri", lambda _db_url=None: "clickhouse://test"
    )
    monkeypatch.setattr(mod, "require_clickhouse_uri", lambda: "clickhouse://test")

    getattr(mod.reconcile_team_members, "run")(org_id="org-1")
    reconcile_payload_members = FakeStore.inserted.pop()[0]["members"]
    assert isinstance(reconcile_payload_members, list)
    reconcile_members = set(reconcile_payload_members)

    team_bridge.bridge_teams_to_clickhouse(org_id="org-1")
    bridge_payload_members = FakeStore.inserted.pop()[0]["members"]
    assert isinstance(bridge_payload_members, list)
    bridge_members = set(bridge_payload_members)

    assert (
        reconcile_members
        == bridge_members
        == {
            "u1",
            "alice@example.com",
            "alice-gh",
            "alice-jira",
            "u2",
            "bob-linear",
            "Bob Example",
        }
    )
