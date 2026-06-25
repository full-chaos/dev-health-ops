from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import pytest

from dev_health_ops.api.services.configuration.clickhouse_identity_drift import (
    FIELD_MEMBER_FALLBACK,
    FIELD_TEAM_MEMBERSHIP,
    split_memberships_for_review,
)
from dev_health_ops.metrics.schemas import TeamMembershipRecord

ORG_ID = "org-1"
_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeIdentityDriftStore:
    def __init__(self) -> None:
        self.manual_memberships: list[dict[str, Any]] = []
        self.member_fallbacks: list[dict[str, Any]] = []
        self.drift_changes: list[dict[str, Any]] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        org_id = str((parameters or {}).get("org_id") or "")
        if "FROM team_memberships" in query:
            return [
                dict(row)
                for row in self.manual_memberships
                if row.get("org_id") == org_id
            ]
        if "FROM manual_attribution_fallbacks" in query:
            return [
                dict(row)
                for row in self.member_fallbacks
                if row.get("org_id") == org_id
            ]
        if "FROM team_drift_changes" in query:
            return [
                dict(row) for row in self.drift_changes if row.get("org_id") == org_id
            ]
        return []

    async def insert_team_drift_changes(self, rows: list[dict[str, Any]]) -> None:
        self.drift_changes.extend(dict(row) for row in rows)


@pytest.mark.parametrize("provider", ["github", "gitlab", "jira", "linear"])
def test_manual_membership_conflict_is_flagged_and_withheld(provider: str) -> None:
    store = FakeIdentityDriftStore()
    member_id = f"{provider}:alice"
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": provider,
            "team_id": "manual-team",
            "member_id": member_id,
            "raw_provider_user_id": "alice@example.com",
            "raw_email": "alice@example.com",
            "source": "manual",
            "is_primary": 1,
            "specificity": 100,
            "priority": 5,
            "valid_from": _NOW,
            "valid_to": None,
            "updated_at": _NOW,
        }
    )
    rows = [_membership(provider=provider, team_id="team-1", member_id=member_id)]

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert len(store.drift_changes) == 1
    change = store.drift_changes[0]
    assert change["entity_type"] == "identity"
    assert change["entity_id"] == "team-1"
    assert change["field"] == FIELD_TEAM_MEMBERSHIP
    assert json.loads(change["new_value_json"])["member_id"] == member_id


def test_matching_manual_membership_does_not_flag() -> None:
    store = FakeIdentityDriftStore()
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": "linear",
            "team_id": "team-1",
            "member_id": "linear:alice",
            "source": "manual",
        }
    )
    rows = [_membership(team_id="team-1", member_id="linear:alice")]

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == rows
    assert store.drift_changes == []


@pytest.mark.parametrize("provider", ["github", "gitlab", "jira", "linear"])
def test_member_fallback_conflict_is_flagged_and_withheld(provider: str) -> None:
    store = FakeIdentityDriftStore()
    store.member_fallbacks.append(
        {
            "org_id": ORG_ID,
            "provider": provider,
            "scope_type": "member",
            "scope_id": "alice@example.com",
            "team_id": "fallback-team",
            "team_name": "Fallback",
            "reason": "manual fallback",
            "priority": 100,
            "valid_from": _NOW,
            "valid_to": None,
            "created_by": "admin",
            "created_at": _NOW,
            "updated_at": _NOW,
        }
    )
    rows = [
        _membership(provider=provider, team_id="team-1", member_id=f"{provider}:alice")
    ]

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert len(store.drift_changes) == 1
    assert store.drift_changes[0]["field"] == FIELD_MEMBER_FALLBACK


def test_member_fallback_scope_match_is_normalized() -> None:
    store = FakeIdentityDriftStore()
    store.member_fallbacks.append(
        {
            "org_id": ORG_ID,
            "provider": "linear",
            "scope_type": "member",
            "scope_id": " ALICE@EXAMPLE.COM ",
            "team_id": "fallback-team",
            "team_name": "Fallback",
            "reason": "manual fallback",
            "priority": 100,
            "valid_from": _NOW,
            "valid_to": None,
            "created_by": "admin",
            "created_at": _NOW,
            "updated_at": _NOW,
        }
    )
    rows = [_membership(team_id="team-1", member_id="linear:alice")]

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert store.drift_changes[0]["field"] == FIELD_MEMBER_FALLBACK


def test_dismissed_identity_change_is_not_reinserted() -> None:
    store = FakeIdentityDriftStore()
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": "linear",
            "team_id": "manual-team",
            "member_id": "linear:alice",
            "source": "manual",
        }
    )
    rows = [_membership(team_id="team-1", member_id="linear:alice")]
    asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )
    store.drift_changes[0]["status"] = "dismissed"

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert len(store.drift_changes) == 1


def test_pending_identity_change_resolves_when_conflict_disappears() -> None:
    store = FakeIdentityDriftStore()
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": "linear",
            "team_id": "manual-team",
            "member_id": "linear:alice",
            "source": "manual",
        }
    )
    rows = [_membership(team_id="team-1", member_id="linear:alice")]
    asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )
    change_id = store.drift_changes[0]["change_id"]
    store.manual_memberships[0]["team_id"] = "team-1"

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == rows
    assert store.drift_changes[-1]["change_id"] == change_id
    assert store.drift_changes[-1]["status"] == "resolved"


def test_empty_observed_team_snapshot_resolves_stale_identity_change() -> None:
    store = FakeIdentityDriftStore()
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": "linear",
            "team_id": "manual-team",
            "member_id": "linear:alice",
            "source": "manual",
        }
    )
    rows = [_membership(team_id="team-1", member_id="linear:alice")]
    asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            observed_team_ids=[("linear", "team-1")],
            discovered_at=_NOW,
        )
    )
    change_id = store.drift_changes[0]["change_id"]

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=[],
            observed_team_ids=[("linear", "team-1")],
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert store.drift_changes[-1]["change_id"] == change_id
    assert store.drift_changes[-1]["status"] == "resolved"


def test_unobserved_team_snapshot_does_not_resolve_stale_identity_change() -> None:
    store = FakeIdentityDriftStore()
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": "github",
            "team_id": "gh:manual-team",
            "member_id": "gh:alice",
            "source": "manual",
        }
    )
    rows = [_membership(provider="github", team_id="gh:team-1", member_id="gh:alice")]
    asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            observed_team_ids=[("github", "gh:team-1")],
            discovered_at=_NOW,
        )
    )
    change_id = store.drift_changes[0]["change_id"]

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=[],
            observed_team_ids=[("github", "gh:other-team")],
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert [row for row in store.drift_changes if row["change_id"] == change_id] == [
        store.drift_changes[0]
    ]


def test_pending_identity_change_supersedes_when_conflict_value_changes() -> None:
    store = FakeIdentityDriftStore()
    store.manual_memberships.append(
        {
            "org_id": ORG_ID,
            "provider": "linear",
            "team_id": "manual-team-a",
            "member_id": "linear:alice",
            "source": "manual",
        }
    )
    rows = [_membership(team_id="team-1", member_id="linear:alice")]
    asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )
    old_change_id = store.drift_changes[0]["change_id"]
    store.manual_memberships[0]["team_id"] = "manual-team-b"

    safe_rows = asyncio.run(
        split_memberships_for_review(
            store=store,
            org_id=ORG_ID,
            rows=rows,
            discovered_at=_NOW,
        )
    )

    assert safe_rows == []
    assert store.drift_changes[1]["status"] == "pending"
    assert store.drift_changes[1]["change_id"] != old_change_id
    assert store.drift_changes[2]["change_id"] == old_change_id
    assert store.drift_changes[2]["status"] == "superseded"


def _membership(
    *, provider: str = "linear", team_id: str, member_id: str
) -> TeamMembershipRecord:
    return TeamMembershipRecord(
        org_id=ORG_ID,
        provider=provider,
        team_id=team_id,
        member_id=member_id,
        raw_provider_user_id="alice@example.com",
        raw_email="alice@example.com",
        source="native",
        is_primary=1,
        specificity=100,
        priority=10,
        valid_from=_NOW,
        updated_at=_NOW,
    )
