from __future__ import annotations

from dev_health_ops.api.services.configuration.team_member_resolver import (
    members_by_team,
)
from dev_health_ops.models.settings import IdentityMapping


def test_members_by_team_preserves_confirmed_identity_facets():
    identities = [
        IdentityMapping(
            canonical_id="u1",
            org_id="org-1",
            email="alice@example.com",
            display_name="Alice Example",
            provider_identities={"github": ["alice-gh"], "jira": ["alice-jira"]},
            team_ids=["gh:platform"],
        ),
        IdentityMapping(
            canonical_id="u2",
            org_id="org-1",
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
