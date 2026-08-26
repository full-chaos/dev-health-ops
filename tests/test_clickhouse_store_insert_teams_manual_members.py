"""Regression test for a codex adversarial review round 2 HIGH finding
(2026-08-26): ``ClickHouseStore.insert_teams`` used to write
``manual_members`` as ``[]`` whenever a caller omitted the key/attribute.
``teams`` is a ReplacingMergeTree keyed on ``(org_id, id)``, so a caller
that never learned about the CHAOS-4321 provenance column --
``providers/teams.py``'s ``sync_teams`` CLI path was the concrete example
found, whose ``Team`` SQLAlchemy model has no ``manual_members`` attribute
at all -- could become the ``FINAL`` winner and silently WIPE an admin
override this ticket depends on staying authoritative.

Fixed by having ``insert_teams`` distinguish "caller omitted the field"
(``"manual_members" not in item`` / ``not hasattr(item, "manual_members")``)
from "caller explicitly passed an empty list", and backfilling the omitted
case from the CURRENT ClickHouse value for that ``(org_id, id)`` in one
batched query before the write. Verified once live against a real
ClickHouse scratch database (an admin-added manual_members entry survived a
subsequent dict-shaped insert_teams call that omitted the key entirely, and
a genuinely new team still correctly defaulted to ``[]``) before writing
this fake-client unit test to pin the same contract for CI.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

from dev_health_ops.storage.clickhouse import ClickHouseStore

ORG_ID = "org-acme"
NOW = datetime(2026, 8, 26, tzinfo=timezone.utc)


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _FakeClient:
    """Models just enough of clickhouse_connect's sync client for
    insert_teams's own logic plus _preserve_existing_manual_members's
    lookup query -- a dict keyed by (org_id, id), REPLACING semantics
    (mirrors ReplacingMergeTree FINAL for a single-writer test)."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.insert_calls: list[tuple[str, list[list[Any]], list[str]]] = []
        self.query_calls = 0

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> _Result:
        self.query_calls += 1
        params = parameters or {}
        assert "FROM teams FINAL" in sql
        org_ids = set(params.get("org_ids") or [])
        ids = set(params.get("ids") or [])
        rows = [
            (org_id, team_id, row["manual_members"])
            for (org_id, team_id), row in self.rows.items()
            if org_id in org_ids and team_id in ids
        ]
        return _Result(rows)

    def insert(
        self,
        table: str,
        matrix: list[list[Any]],
        *,
        column_names: list[str],
        **_kwargs: Any,
    ) -> None:
        assert table == "teams"
        self.insert_calls.append((table, matrix, column_names))
        for values in matrix:
            row = dict(zip(column_names, values, strict=True))
            self.rows[(str(row["org_id"]), str(row["id"]))] = row


@dataclass
class _LegacyTeamRow:
    """Shape of providers/teams.py's Team SQLAlchemy model as far as
    insert_teams's object branch is concerned: no manual_members attribute
    at all -- this is the exact class of caller codex found."""

    id: str
    name: str
    org_id: str
    members: list[str] = field(default_factory=list)
    team_uuid: Any = None
    description: str | None = None
    is_active: int = 1
    updated_at: datetime = NOW


def _store() -> tuple[ClickHouseStore, _FakeClient]:
    store = ClickHouseStore("clickhouse://fake")
    client = _FakeClient()
    store.client = client
    return store, client


@pytest.mark.asyncio
async def test_dict_caller_omitting_manual_members_preserves_the_existing_value() -> (
    None
):
    store, client = _store()
    # Seed an existing admin override, as ClickHouseTeamAdminService would.
    client.rows[(ORG_ID, "team-guarded")] = {
        "id": "team-guarded",
        "org_id": ORG_ID,
        "members": ["alice@example.com"],
        "manual_members": ["alice@example.com"],
    }

    await store.insert_teams(
        [
            {
                "id": "team-guarded",
                "name": "Guarded Team",
                "members": ["alice@example.com", "bob"],
                "org_id": ORG_ID,
                "updated_at": NOW,
                # manual_members deliberately OMITTED -- the exact shape of
                # providers/teams.py's sync_teams call.
            }
        ]
    )

    row = client.rows[(ORG_ID, "team-guarded")]
    assert row["members"] == ["alice@example.com", "bob"], "members should update"
    assert row["manual_members"] == ["alice@example.com"], (
        "manual_members must survive an insert_teams call that omitted it"
    )


@pytest.mark.asyncio
async def test_object_caller_omitting_manual_members_preserves_the_existing_value() -> (
    None
):
    """Same guarantee for the object branch (providers/teams.py passes
    Team model instances, not dicts)."""
    store, client = _store()
    client.rows[(ORG_ID, "team-guarded")] = {
        "id": "team-guarded",
        "org_id": ORG_ID,
        "members": ["alice@example.com"],
        "manual_members": ["alice@example.com"],
    }

    await store.insert_teams(
        [
            _LegacyTeamRow(
                id="team-guarded",
                name="Guarded Team",
                org_id=ORG_ID,
                members=["alice@example.com", "bob"],
                team_uuid=uuid.uuid4(),
            )
        ]
    )

    row = client.rows[(ORG_ID, "team-guarded")]
    assert row["members"] == ["alice@example.com", "bob"]
    assert row["manual_members"] == ["alice@example.com"]


@pytest.mark.asyncio
async def test_explicitly_empty_manual_members_is_honored_not_treated_as_omitted() -> (
    None
):
    """An explicit [] (a real admin removal, e.g. via remove_members) must
    NOT trigger the preserve-existing lookup -- omitted and explicitly-empty
    are different things."""
    store, client = _store()
    client.rows[(ORG_ID, "team-guarded")] = {
        "id": "team-guarded",
        "org_id": ORG_ID,
        "members": ["alice@example.com"],
        "manual_members": ["alice@example.com"],
    }

    await store.insert_teams(
        [
            {
                "id": "team-guarded",
                "name": "Guarded Team",
                "members": [],
                "manual_members": [],  # explicit removal
                "org_id": ORG_ID,
                "updated_at": NOW,
            }
        ]
    )

    row = client.rows[(ORG_ID, "team-guarded")]
    assert row["manual_members"] == [], "an explicit [] must be honored, not overridden"


@pytest.mark.asyncio
async def test_brand_new_team_omitting_manual_members_defaults_to_empty() -> None:
    """A team with no prior row has nothing to preserve -- must not error,
    must not query for something that can't exist, must land at []."""
    store, client = _store()

    await store.insert_teams(
        [
            {
                "id": "team-brand-new",
                "name": "New Team",
                "org_id": ORG_ID,
                "updated_at": NOW,
            }
        ]
    )

    row = client.rows[(ORG_ID, "team-brand-new")]
    assert row["manual_members"] == []


@pytest.mark.asyncio
async def test_no_lookup_query_when_every_caller_supplies_manual_members() -> None:
    """The batched preserve-lookup must not fire at all when every row
    explicitly provides manual_members (the shape ClickHouseTeamAdminService
    and ClickHouseTeamDriftProjector's AUTO_APPLY_POLICY branch always use)
    -- no wasted query."""
    store, client = _store()

    await store.insert_teams(
        [
            {
                "id": "team-explicit",
                "name": "Explicit Team",
                "org_id": ORG_ID,
                "updated_at": NOW,
                "manual_members": ["carol@example.com"],
            }
        ]
    )

    assert client.rows[(ORG_ID, "team-explicit")]["manual_members"] == [
        "carol@example.com"
    ]
    assert client.query_calls == 0, "no preserve-lookup should fire here"
