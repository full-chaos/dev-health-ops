"""
Unit tests for Linear team sync and discovery functionality.

Tests cover:
- sync_teams() with provider="linear" (happy path, archived, empty members, pagination)
- TeamDiscoveryService.discover_linear() (happy path, empty, associations, client close)
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_linear_team(
    *,
    key: str = "ENG",
    name: str = "Engineering",
    team_id: str | None = None,
    description: str | None = None,
    members: list[dict[str, Any]] | None = None,
    archived: bool = False,
    has_next_page: bool = False,
) -> dict[str, Any]:
    """Build a minimal Linear team dict as returned by iter_teams()."""
    if team_id is None:
        team_id = f"team-{key.lower()}"
    member_nodes = members if members is not None else []
    return {
        "id": team_id,
        "key": key,
        "name": name,
        "description": description,
        "archivedAt": "2024-01-01T00:00:00Z" if archived else None,
        "members": {
            "nodes": member_nodes,
            "pageInfo": {
                "hasNextPage": has_next_page,
                "endCursor": "cursor-abc" if has_next_page else None,
            },
        },
    }


def _mock_member(
    *,
    name: str = "Alice",
    email: str = "alice@example.com",
    active: bool = True,
) -> dict[str, Any]:
    return {
        "id": f"user-{name.lower()}",
        "name": name,
        "email": email,
        "active": active,
    }


def _make_ns(
    provider: str = "linear", db: str = "sqlite+aiosqlite:///:memory:"
) -> argparse.Namespace:
    """Build a minimal argparse.Namespace for sync_teams()."""
    ns = argparse.Namespace()
    ns.provider = provider
    ns.db = db
    ns.db_type = None
    ns.path = None
    ns.owner = None
    ns.auth = None
    ns.org = None
    return ns


# ---------------------------------------------------------------------------
# Tests for sync_teams() with provider="linear"
# ---------------------------------------------------------------------------


class TestLinearTeamSync:
    """Tests for the linear branch inside sync_teams()."""

    @patch("dev_health_ops.providers.teams._bridge_teams_to_postgres")
    @patch("dev_health_ops.providers.teams.resolve_sink_uri")
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_happy_path_two_teams(
        self,
        mock_client_class: MagicMock,
        mock_resolve_sink: MagicMock,
        mock_bridge: MagicMock,
    ) -> None:
        """Happy path: two active teams with members → two Team objects created."""
        from dev_health_ops.providers.teams import sync_teams

        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_resolve_sink.return_value = "clickhouse://localhost:8123/default"
        mock_client.iter_teams.return_value = [
            _mock_linear_team(
                key="ENG",
                name="Engineering",
                members=[_mock_member(name="Alice", email="alice@example.com")],
            ),
            _mock_linear_team(
                key="PROD",
                name="Product",
                members=[_mock_member(name="Bob", email="bob@example.com")],
            ),
        ]

        ns = _make_ns()
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None
            result = sync_teams(ns)

        assert result == 0
        # asyncio.run was called (to persist teams)
        mock_run.assert_called_once()
        # bridge was called
        mock_bridge.assert_called_once()
        # Verify the teams list passed to bridge has 2 items with correct IDs
        teams_arg = mock_bridge.call_args[0][0]
        assert len(teams_arg) == 2
        team_ids = {t.id for t in teams_arg}
        assert "linear:ENG" in team_ids
        assert "linear:PROD" in team_ids

    @patch("dev_health_ops.providers.teams._bridge_teams_to_postgres")
    @patch("dev_health_ops.providers.teams.resolve_sink_uri")
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_archived_teams_skipped(
        self,
        mock_client_class: MagicMock,
        mock_resolve_sink: MagicMock,
        mock_bridge: MagicMock,
    ) -> None:
        """Archived teams (archivedAt set) must be skipped; only active teams synced."""
        from dev_health_ops.providers.teams import sync_teams

        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_resolve_sink.return_value = "clickhouse://localhost:8123/default"
        mock_client.iter_teams.return_value = [
            _mock_linear_team(key="ARCH", name="Archived Team", archived=True),
            _mock_linear_team(
                key="ENG",
                name="Engineering",
                members=[_mock_member()],
            ),
        ]

        ns = _make_ns()
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None
            result = sync_teams(ns)

        assert result == 0
        teams_arg = mock_bridge.call_args[0][0]
        assert len(teams_arg) == 1
        assert teams_arg[0].id == "linear:ENG"

    @patch("dev_health_ops.providers.teams._bridge_teams_to_postgres")
    @patch("dev_health_ops.providers.teams.resolve_sink_uri")
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_empty_members_creates_team_with_empty_list(
        self,
        mock_client_class: MagicMock,
        mock_resolve_sink: MagicMock,
        mock_bridge: MagicMock,
    ) -> None:
        """A team with no members should still be created with members=[]."""
        from dev_health_ops.providers.teams import sync_teams

        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_resolve_sink.return_value = "clickhouse://localhost:8123/default"
        mock_client.iter_teams.return_value = [
            _mock_linear_team(key="EMPTY", name="Empty Team", members=[]),
        ]

        ns = _make_ns()
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None
            result = sync_teams(ns)

        assert result == 0
        teams_arg = mock_bridge.call_args[0][0]
        assert len(teams_arg) == 1
        assert teams_arg[0].id == "linear:EMPTY"
        assert teams_arg[0].members == []

    @patch("dev_health_ops.providers.teams._bridge_teams_to_postgres")
    @patch("dev_health_ops.providers.teams.resolve_sink_uri")
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_member_pagination_calls_get_team_members(
        self,
        mock_client_class: MagicMock,
        mock_resolve_sink: MagicMock,
        mock_bridge: MagicMock,
    ) -> None:
        """When pageInfo.hasNextPage=True, get_team_members() must be called and its results used."""
        from dev_health_ops.providers.teams import sync_teams

        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_resolve_sink.return_value = "clickhouse://localhost:8123/default"

        # Team with hasNextPage=True — only 1 member in initial nodes
        initial_member = _mock_member(name="Alice", email="alice@example.com")
        extra_member = _mock_member(name="Bob", email="bob@example.com")

        mock_client.iter_teams.return_value = [
            _mock_linear_team(
                key="BIG",
                name="Big Team",
                team_id="team-big",
                members=[initial_member],
                has_next_page=True,
            ),
        ]
        # get_team_members returns both members (already filtered for active)
        mock_client.get_team_members.return_value = [initial_member, extra_member]

        ns = _make_ns()
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None
            result = sync_teams(ns)

        assert result == 0
        # get_team_members must have been called with the team's id
        mock_client.get_team_members.assert_called_once_with("team-big")
        teams_arg = mock_bridge.call_args[0][0]
        assert len(teams_arg) == 1
        # Both members should be present
        assert "alice@example.com" in teams_arg[0].members
        assert "bob@example.com" in teams_arg[0].members

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_linear_config_error_returns_1(
        self,
        mock_client_class: MagicMock,
    ) -> None:
        """If LinearClient.from_env() raises ValueError, sync_teams returns 1."""
        from dev_health_ops.providers.teams import sync_teams

        mock_client_class.from_env.side_effect = ValueError("LINEAR_API_KEY not set")

        ns = _make_ns()
        result = sync_teams(ns)

        assert result == 1

    @patch("dev_health_ops.providers.teams._bridge_teams_to_postgres")
    @patch("dev_health_ops.providers.teams.resolve_sink_uri")
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_member_email_fallback_to_name(
        self,
        mock_client_class: MagicMock,
        mock_resolve_sink: MagicMock,
        mock_bridge: MagicMock,
    ) -> None:
        """Members without email fall back to name; members with neither are excluded."""
        from dev_health_ops.providers.teams import sync_teams

        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_resolve_sink.return_value = "clickhouse://localhost:8123/default"
        mock_client.iter_teams.return_value = [
            _mock_linear_team(
                key="ENG",
                name="Engineering",
                members=[
                    {
                        "id": "u1",
                        "name": "Alice",
                        "email": "alice@example.com",
                        "active": True,
                    },
                    {
                        "id": "u2",
                        "name": "Bob",
                        "email": None,
                        "active": True,
                    },  # no email → use name
                    {
                        "id": "u3",
                        "name": "",
                        "email": None,
                        "active": True,
                    },  # no email, no name → excluded
                ],
            ),
        ]

        ns = _make_ns()
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None
            result = sync_teams(ns)

        assert result == 0
        teams_arg = mock_bridge.call_args[0][0]
        assert len(teams_arg) == 1
        members = teams_arg[0].members
        assert "alice@example.com" in members
        assert "Bob" in members
        # Empty-string member should be excluded
        assert "" not in members


# ---------------------------------------------------------------------------
# Tests for TeamDiscoveryService.discover_linear()
# ---------------------------------------------------------------------------


class TestDiscoverLinear:
    """Tests for TeamDiscoveryService.discover_linear()."""

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_discover_linear_happy_path(self, mock_client_class: MagicMock) -> None:
        """discover_linear() returns DiscoveredTeam objects with correct provider fields."""
        from dev_health_ops.api.services.settings import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.iter_teams.return_value = [
            {
                "id": "team-eng",
                "key": "ENG",
                "name": "Engineering",
                "description": "Eng team",
            },
            {"id": "team-prod", "key": "PROD", "name": "Product", "description": None},
        ]

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        result = asyncio.run(service.discover_linear(api_key="test-key"))

        assert len(result) == 2
        eng = next(t for t in result if t.provider_team_id == "ENG")
        assert eng.provider_type == "linear"
        assert eng.name == "Engineering"
        assert eng.description == "Eng team"

        prod = next(t for t in result if t.provider_team_id == "PROD")
        assert prod.provider_type == "linear"
        assert prod.name == "Product"

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_discover_linear_empty_workspace(
        self, mock_client_class: MagicMock
    ) -> None:
        """discover_linear() returns empty list when workspace has no teams."""
        from dev_health_ops.api.services.settings import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.iter_teams.return_value = []

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        result = asyncio.run(service.discover_linear(api_key="test-key"))

        assert result == []

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_discover_linear_associations_set(
        self, mock_client_class: MagicMock
    ) -> None:
        """discover_linear() sets associations with provider_org='linear'."""
        from dev_health_ops.api.services.settings import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.iter_teams.return_value = [
            {
                "id": "team-eng",
                "key": "ENG",
                "name": "Engineering",
                "description": None,
            },
        ]

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        result = asyncio.run(service.discover_linear(api_key="test-key"))

        assert len(result) == 1
        assert result[0].associations == {"provider_org": "linear"}

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_discover_linear_client_closed_on_success(
        self, mock_client_class: MagicMock
    ) -> None:
        """discover_linear() always closes the client (via finally block)."""
        from dev_health_ops.api.services.settings import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.iter_teams.return_value = [
            {
                "id": "team-eng",
                "key": "ENG",
                "name": "Engineering",
                "description": None,
            },
        ]

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        asyncio.run(service.discover_linear(api_key="test-key"))

        mock_client.close.assert_called_once()
