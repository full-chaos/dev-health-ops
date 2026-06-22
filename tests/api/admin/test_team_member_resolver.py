from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
    ClickHouseIdentity,
)
from dev_health_ops.api.services.configuration.team_member_resolver import (
    members_by_team,
)


def _identity(**kwargs) -> ClickHouseIdentity:
    defaults: dict[str, Any] = dict(
        identity_uuid=uuid.uuid4(),
        display_name=None,
        email=None,
        provider_identities={},
        team_ids=[],
        is_active=True,
        updated_at=datetime(2026, 5, 20, tzinfo=UTC),
        org_id="org-1",
    )
    defaults.update(kwargs)
    return ClickHouseIdentity(**defaults)


def test_members_by_team_preserves_confirmed_identity_facets():
    identities = [
        _identity(
            canonical_id="u1",
            email="alice@example.com",
            display_name="Alice Example",
            provider_identities={"github": ["alice-gh"], "jira": ["alice-jira"]},
            team_ids=["gh:platform"],
        ),
        _identity(
            canonical_id="u2",
            display_name="Bob Example",
            provider_identities={"github": ["bob-gh"]},
            team_ids=["gh:platform"],
        ),
    ]

    resolved = members_by_team(identities)

    assert resolved["gh:platform"] == {
        "u1",
        "alice@example.com",
        "alice-gh",
        "alice-jira",
        "u2",
        "bob-gh",
        "Bob Example",
    }
    assert "unmapped-login" not in resolved["gh:platform"]
