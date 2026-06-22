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
    ns.sink = "clickhouse"
    ns.analytics_db = "clickhouse://example.test:8123/default"
    ns.path = None
    ns.owner = None
    ns.auth = None
    ns.org = "org-1"
    return ns


# ---------------------------------------------------------------------------
# Tests for sync_teams() with provider="linear"
# ---------------------------------------------------------------------------


class _RecordingStore:
    """ClickHouseStore stand-in that records the teams inserted (CS5).

    The org-scoped ``sync_teams`` path writes ClickHouse directly via
    ``insert_teams`` — these tests assert the provider-built ``Team`` objects
    that reach that call.
    """

    last: _RecordingStore | None = None

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.org_id: str | None = None
        self.inserted_teams: list[Any] = []
        _RecordingStore.last = self

    async def __aenter__(self) -> _RecordingStore:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None

    async def insert_teams(self, teams: list[Any]) -> None:
        self.inserted_teams.extend(teams)

    async def insert_jira_project_ops_team_links(self, links: list[Any]) -> None:
        pass

    async def get_all_teams(self) -> list[Any]:
        return [
            argparse.Namespace(id=getattr(t, "id", None), org_id=self.org_id)
            for t in self.inserted_teams
        ]


def _run_linear_sync(mock_client_class: MagicMock) -> tuple[int, list[Any]]:
    """Run org-scoped sync_teams(linear) against a recording ClickHouse store."""
    from dev_health_ops.providers.teams import sync_teams

    _RecordingStore.last = None
    with (
        patch(
            "dev_health_ops.storage.clickhouse.ClickHouseStore",
            _RecordingStore,
        ),
        patch("dev_health_ops.providers.teams.validate_sink"),
        patch(
            "dev_health_ops.providers.teams.resolve_sink_uri",
            return_value="clickhouse://localhost:8123/default",
        ),
    ):
        result = sync_teams(_make_ns())
    store = _RecordingStore.last
    inserted: list[Any] = store.inserted_teams if store is not None else []
    return result, inserted


class TestLinearTeamSync:
    """Tests for the linear branch inside sync_teams() (ClickHouse-direct)."""

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_happy_path_two_teams(self, mock_client_class: MagicMock) -> None:
        """Two active teams with members → two Team objects written to ClickHouse."""
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
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

        result, inserted = _run_linear_sync(mock_client_class)

        assert result == 0
        assert len(inserted) == 2
        team_ids = {t.id for t in inserted}
        assert "linear:ENG" in team_ids
        assert "linear:PROD" in team_ids
        # The org-scoped path tags each team with the org_id.
        assert all(getattr(t, "org_id", None) == "org-1" for t in inserted)

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_archived_teams_skipped(self, mock_client_class: MagicMock) -> None:
        """Archived teams (archivedAt set) must be skipped; only active teams synced."""
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_client.iter_teams.return_value = [
            _mock_linear_team(key="ARCH", name="Archived Team", archived=True),
            _mock_linear_team(
                key="ENG",
                name="Engineering",
                members=[_mock_member()],
            ),
        ]

        result, inserted = _run_linear_sync(mock_client_class)

        assert result == 0
        assert len(inserted) == 1
        assert inserted[0].id == "linear:ENG"

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_empty_members_creates_team_with_empty_list(
        self, mock_client_class: MagicMock
    ) -> None:
        """A team with no members should still be created with members=[]."""
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        mock_client.iter_teams.return_value = [
            _mock_linear_team(key="EMPTY", name="Empty Team", members=[]),
        ]

        result, inserted = _run_linear_sync(mock_client_class)

        assert result == 0
        assert len(inserted) == 1
        assert inserted[0].id == "linear:EMPTY"
        assert inserted[0].members == []

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_member_pagination_calls_get_team_members(
        self, mock_client_class: MagicMock
    ) -> None:
        """When pageInfo.hasNextPage=True, get_team_members() must be called and its results used."""
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client

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
        mock_client.get_team_members.return_value = [initial_member, extra_member]

        result, inserted = _run_linear_sync(mock_client_class)

        assert result == 0
        mock_client.get_team_members.assert_called_once_with("team-big")
        assert len(inserted) == 1
        assert "alice@example.com" in inserted[0].members
        assert "bob@example.com" in inserted[0].members

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_partial_member_pagination_is_marked_incomplete(
        self, mock_client_class: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
        initial_member = _mock_member(name="Alice", email="alice@example.com")
        mock_client.iter_teams.return_value = [
            _mock_linear_team(
                key="BIG",
                name="Big Team",
                team_id="team-big",
                members=[initial_member],
                has_next_page=True,
            ),
        ]
        mock_client.get_team_members.side_effect = RuntimeError("linear timeout")

        result, inserted = _run_linear_sync(mock_client_class)

        assert result == 0
        assert inserted[0].members == ["alice@example.com"]
        assert getattr(inserted[0], "members_complete") is False

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

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_member_email_fallback_to_name(self, mock_client_class: MagicMock) -> None:
        """Members without email fall back to name; members with neither are excluded."""
        mock_client = MagicMock()
        mock_client_class.from_env.return_value = mock_client
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

        result, inserted = _run_linear_sync(mock_client_class)

        assert result == 0
        assert len(inserted) == 1
        members = inserted[0].members
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
        from dev_health_ops.api.services.configuration import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
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
        service.org_id = "test-org"
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
        from dev_health_ops.api.services.configuration import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
        mock_client.iter_teams.return_value = []

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        service.org_id = "test-org"
        result = asyncio.run(service.discover_linear(api_key="test-key"))

        assert result == []

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_discover_linear_associations_set(
        self, mock_client_class: MagicMock
    ) -> None:
        """discover_linear() sets project_keys (= team key) for work-item
        attribution, plus provider_org."""
        from dev_health_ops.api.services.configuration import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
        mock_client.iter_teams.return_value = [
            {
                "id": "team-eng",
                "key": "ENG",
                "name": "Engineering",
                "description": None,
            },
        ]

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        service.org_id = "test-org"
        result = asyncio.run(service.discover_linear(api_key="test-key"))

        assert len(result) == 1
        assert result[0].associations == {
            "project_keys": ["ENG"],
            "provider_org": "linear",
        }

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_discover_linear_client_closed_on_success(
        self, mock_client_class: MagicMock
    ) -> None:
        """discover_linear() always closes the client (via context manager __exit__)."""
        from dev_health_ops.api.services.configuration import TeamDiscoveryService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
        mock_client.iter_teams.return_value = [
            {
                "id": "team-eng",
                "key": "ENG",
                "name": "Engineering",
                "description": None,
            },
        ]

        service = TeamDiscoveryService.__new__(TeamDiscoveryService)
        service.org_id = "test-org"
        asyncio.run(service.discover_linear(api_key="test-key"))

        # Context-manager protocol guarantees cleanup via __exit__
        mock_client.__exit__.assert_called_once()
