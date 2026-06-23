"""Tests for team member propagation (CHAOS-2264).

Covers:
- ``_members_by_team`` — confirmed identity mappings → ClickHouse ``teams.members``
  (previously the analytics bridge hardcoded ``members: []``, discarding
  confirmed memberships and breaking the membership-based attribution fallback
  for UI-imported teams)
- ``TeamMembershipService.discover_members_linear`` — Linear team member
  discovery for the existing discover→confirm flow
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from dev_health_ops.api.services.configuration.team_member_resolver import (
    members_by_team as _members_by_team,
)

# ---------------------------------------------------------------------------
# _members_by_team
# ---------------------------------------------------------------------------


def _identity(**kwargs: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "canonical_id": None,
        "display_name": None,
        "email": None,
        "provider_identities": {},
        "team_ids": [],
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestMembersByTeam:
    def test_collects_all_identity_facets_per_team(self) -> None:
        mappings = [
            _identity(
                canonical_id="alice@example.com",
                email="alice@example.com",
                provider_identities={"github": ["alice-gh"], "linear": ["alice-lin"]},
                team_ids=["CHAOS"],
            )
        ]
        result = _members_by_team(mappings)
        assert result == {
            "CHAOS": {"alice@example.com", "alice-gh", "alice-lin"},
        }

    def test_identity_in_multiple_teams(self) -> None:
        mappings = [
            _identity(email="bob@example.com", team_ids=["CHAOS", "gh:platform"])
        ]
        result = _members_by_team(mappings)
        assert result["CHAOS"] == {"bob@example.com"}
        assert result["gh:platform"] == {"bob@example.com"}

    def test_display_name_only_used_without_email(self) -> None:
        with_email = _identity(
            email="carol@example.com", display_name="Carol", team_ids=["t1"]
        )
        without_email = _identity(display_name="Dave", team_ids=["t1"])
        result = _members_by_team([with_email, without_email])
        assert "Carol" not in result["t1"]
        assert "Dave" in result["t1"]
        assert "carol@example.com" in result["t1"]

    def test_no_team_ids_or_no_identities_skipped(self) -> None:
        mappings = [
            _identity(email="erin@example.com", team_ids=[]),
            _identity(team_ids=["t1"]),
        ]
        assert _members_by_team(mappings) == {}


# ---------------------------------------------------------------------------
# discover_members_linear
# ---------------------------------------------------------------------------


def _linear_team(
    *,
    key: str = "ENG",
    members: list[dict[str, Any]] | None = None,
    has_next_page: bool = False,
) -> dict[str, Any]:
    return {
        "id": f"team-{key.lower()}",
        "key": key,
        "name": key.title(),
        "members": {
            "nodes": members or [],
            "pageInfo": {"hasNextPage": has_next_page},
        },
    }


class TestDiscoverMembersLinear:
    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_happy_path_filters_inactive_and_uses_email(
        self, mock_client_class: MagicMock
    ) -> None:
        from dev_health_ops.api.services.configuration import TeamMembershipService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
        mock_client.iter_teams.return_value = [
            _linear_team(
                key="ENG",
                members=[
                    {
                        "id": "u1",
                        "name": "Alice",
                        "email": "alice@x.io",
                        "active": True,
                    },
                    {"id": "u2", "name": "Bob", "email": None, "active": True},
                    {"id": "u3", "name": "Gone", "email": "gone@x.io", "active": False},
                ],
            ),
        ]

        service = TeamMembershipService.__new__(TeamMembershipService)
        service.org_id = "test-org"
        members = asyncio.run(
            service.discover_members_linear(api_key="k", team_key="ENG")
        )

        assert [m.provider_identity for m in members] == ["alice@x.io", "u2"]
        assert members[0].provider_type == "linear"
        assert members[0].email == "alice@x.io"
        assert members[1].display_name == "Bob"

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_paginates_when_first_page_incomplete(
        self, mock_client_class: MagicMock
    ) -> None:
        from dev_health_ops.api.services.configuration import TeamMembershipService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
        mock_client.iter_teams.return_value = [
            _linear_team(key="ENG", members=[], has_next_page=True),
        ]
        mock_client.get_team_members.return_value = [
            {"id": "u9", "name": "Zoe", "email": "zoe@x.io", "active": True},
        ]

        service = TeamMembershipService.__new__(TeamMembershipService)
        service.org_id = "test-org"
        members = asyncio.run(
            service.discover_members_linear(api_key="k", team_key="ENG")
        )

        mock_client.get_team_members.assert_called_once_with("team-eng")
        assert [m.provider_identity for m in members] == ["zoe@x.io"]

    @patch("dev_health_ops.providers.linear.client.LinearClient")
    def test_strips_linear_prefix_and_unknown_team_returns_empty(
        self, mock_client_class: MagicMock
    ) -> None:
        from dev_health_ops.api.services.configuration import TeamMembershipService

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.__enter__.return_value = mock_client
        mock_client.iter_teams.return_value = [
            _linear_team(
                key="ENG",
                members=[
                    {"id": "u1", "name": "Alice", "email": "alice@x.io", "active": True}
                ],
            ),
        ]

        service = TeamMembershipService.__new__(TeamMembershipService)
        service.org_id = "test-org"

        prefixed = asyncio.run(
            service.discover_members_linear(api_key="k", team_key="linear:ENG")
        )
        assert [m.provider_identity for m in prefixed] == ["alice@x.io"]

        missing = asyncio.run(
            service.discover_members_linear(api_key="k", team_key="NOPE")
        )
        assert missing == []
