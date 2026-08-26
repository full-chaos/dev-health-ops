"""Contract tests for ClickHouse migration 079 (CHAOS-4321 teams.manual_members).

Verified live once against a real ClickHouse scratch database during
development (raw-SQL-seeded "legacy" identity + team rows, confirmed the
backfill populates manual_members correctly and a second run is a no-op) --
these are the fast, hermetic unit-level tests that pin the same contract for
CI, using a small fake client rather than a live container.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)
MIGRATION = "079_teams_manual_members.py"

_TEAMS_COLUMNS = (
    "id",
    "team_uuid",
    "name",
    "description",
    "members",
    "manual_members",
    "project_keys",
    "repo_patterns",
    "is_active",
    "updated_at",
    "last_synced",
    "org_id",
    "provider",
    "native_team_key",
    "parent_team_id",
    "source_id",
)
_IDENTITIES_COLUMNS = (
    "org_id",
    "canonical_id",
    "email",
    "display_name",
    "provider_identities",
    "team_ids",
)


def _load_migration() -> ModuleType:
    path = MIGRATIONS_DIR / MIGRATION
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None, f"cannot load {MIGRATION}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.result_rows = rows


def _str_list(value: object) -> list[str]:
    assert isinstance(value, list)
    return [str(item) for item in value]


class _FakeTeamsClient:
    """Models just enough of clickhouse_connect's sync client for upgrade().

    Rows are keyed by (org_id, id); a `.insert()` REPLACES the row for that
    key (mirroring ReplacingMergeTree(updated_at) FINAL semantics for a
    single-writer test -- there is never a concurrent older version to
    out-race here).
    """

    def __init__(
        self,
        *,
        teams: list[dict[str, object]],
        identities: list[dict[str, object]],
    ) -> None:
        self.teams: dict[tuple[str, str], dict[str, object]] = {
            (str(row["org_id"]), str(row["id"])): dict(row) for row in teams
        }
        self.identities = identities
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str) -> _Result:
        if "FROM identities FINAL" in sql:
            rows = [
                tuple(row.get(column) for column in _IDENTITIES_COLUMNS)
                for row in self.identities
            ]
            return _Result(rows)
        if "FROM teams FINAL" in sql:
            rows = [
                tuple(row.get(column) for column in _TEAMS_COLUMNS)
                for row in self.teams.values()
            ]
            return _Result(rows)
        raise AssertionError(f"unexpected query: {sql}")

    def insert(
        self, table: str, matrix: list[list[object]], *, column_names: list[str]
    ) -> None:
        assert table == "teams"
        self.inserts.append((table, matrix, column_names))
        for values in matrix:
            row = dict(zip(column_names, values, strict=True))
            self.teams[(str(row["org_id"]), str(row["id"]))] = row


def _team_row(
    *,
    team_id: str,
    org_id: str = "org-acme",
    members: list[str] | None = None,
    manual_members: list[str] | None = None,
) -> dict[str, object]:
    now = datetime(2026, 8, 1, tzinfo=timezone.utc)
    return {
        "id": team_id,
        "team_uuid": "00000000-0000-0000-0000-000000000001",
        "name": f"{team_id} name",
        "description": None,
        "members": members or [],
        "manual_members": manual_members or [],
        "project_keys": [],
        "repo_patterns": [],
        "is_active": 1,
        "updated_at": now,
        "last_synced": now,
        "org_id": org_id,
        "provider": "",
        "native_team_key": None,
        "parent_team_id": None,
        "source_id": None,
    }


def _identity_row(
    *,
    canonical_id: str,
    team_ids: list[str],
    org_id: str = "org-acme",
    email: str | None = None,
    provider_identities: dict[str, list[str]] | None = None,
) -> dict[str, object]:
    return {
        "org_id": org_id,
        "canonical_id": canonical_id,
        "email": email,
        "display_name": None,
        "provider_identities": json.dumps(provider_identities or {}),
        "team_ids": team_ids,
    }


def test_migration_079_declares_upgrade_and_adds_the_column() -> None:
    migration = _load_migration()
    assert callable(getattr(migration, "upgrade", None))

    client = _FakeTeamsClient(teams=[], identities=[])
    migration.upgrade(client)

    assert any("manual_members" in cmd for cmd in client.commands)
    assert any("ADD COLUMN IF NOT EXISTS" in cmd for cmd in client.commands)


def test_migration_079_backfills_manual_members_from_identity_team_ids() -> None:
    """Positive case: an identity admin-mapped (team_ids) to a legacy team
    with an empty manual_members seeds it from member_facets()."""
    migration = _load_migration()
    client = _FakeTeamsClient(
        teams=[_team_row(team_id="team-legacy", members=["stray-provider-facet"])],
        identities=[
            _identity_row(
                canonical_id="legacy:alice",
                email="alice@example.com",
                provider_identities={"github": ["alice"], "jira": ["alice-jira-1"]},
                team_ids=["team-legacy"],
            )
        ],
    )

    migration.upgrade(client)

    row = client.teams[("org-acme", "team-legacy")]
    manual_members = _str_list(row["manual_members"])
    members = _str_list(row["members"])
    assert set(manual_members) == {
        "legacy:alice",
        "alice@example.com",
        "alice",
        "alice-jira-1",
    }
    # Mirrored into legacy `members` too (union, nothing dropped) so the
    # pre-CHAOS-4321 TeamResolver reader keeps working unchanged.
    assert "stray-provider-facet" in members
    assert set(manual_members) <= set(members)


def test_migration_079_is_idempotent_on_rerun() -> None:
    migration = _load_migration()
    client = _FakeTeamsClient(
        teams=[_team_row(team_id="team-legacy")],
        identities=[
            _identity_row(canonical_id="legacy:alice", team_ids=["team-legacy"])
        ],
    )

    migration.upgrade(client)
    first = dict(client.teams[("org-acme", "team-legacy")])
    insert_count_after_first = len(client.inserts)

    migration.upgrade(client)
    second = client.teams[("org-acme", "team-legacy")]

    assert second["manual_members"] == first["manual_members"]
    assert second["members"] == first["members"]
    # The second run must not re-insert an unchanged row.
    assert len(client.inserts) == insert_count_after_first


def test_migration_079_drift_approved_only_membership_has_no_backfill_signal() -> None:
    """KNOWN GAP, documented in the migration's own module docstring: a team
    whose members came ONLY from drift-approval (teams.members/
    team_memberships written directly, identities.team_ids never touched)
    has no admin-only signal this backfill can use -- manual_members stays
    empty. It falls into the provider-fallback tier, same attribution as
    before this PR, until an admin re-approves or re-saves from the panel."""
    migration = _load_migration()
    client = _FakeTeamsClient(
        teams=[_team_row(team_id="team-drift-only", members=["drift-approved-facet"])],
        identities=[],  # no identity ever declared team_ids for this team
    )

    migration.upgrade(client)

    row = client.teams[("org-acme", "team-drift-only")]
    assert row["manual_members"] == []
    assert row["members"] == ["drift-approved-facet"]


def test_migration_079_skips_teams_with_no_matching_admin_identity() -> None:
    """A team nobody's identities.team_ids names at all must not be touched
    -- no wasted ReplacingMergeTree write, no spurious updated_at bump."""
    migration = _load_migration()
    other_team = _team_row(team_id="team-untouched", members=["someone"])
    client = _FakeTeamsClient(
        teams=[other_team],
        identities=[
            _identity_row(canonical_id="legacy:bob", team_ids=["team-elsewhere"])
        ],
    )

    migration.upgrade(client)

    assert client.inserts == []
    assert client.teams[("org-acme", "team-untouched")] == other_team


def test_migration_079_frozen_member_facets_still_agrees_with_the_live_function() -> (
    None
):
    """team-lead ruling (2026-08-26): a migration must be frozen at
    authorship -- the same doctrine applied to migration 0112's
    capability-map freeze. Migration 079 no longer imports
    ClickHouseTeamAdminService.member_facets (that import dragged the whole
    api.services.configuration package tree, and its eager __init__.py
    re-export cascade, into the migration's dependency closure -- see
    ci/requirements-clickhouse-migrations.txt's own comments). It carries a
    private, frozen copy (_member_facets_at_079) instead.

    A frozen copy can silently drift from the live function it was copied
    from. This test is the guard: it asserts the two agree on a fixture
    identity set that exercises every branch (email present, canonical_id
    only, multiple provider identities as both list and bare-string values,
    display_name used only when email is absent, empty/falsy inputs). A
    future edit to the live member_facets that this copy does NOT mirror
    must fail this test, not silently change what future migration runs
    would have computed -- migration 079 itself must never change to chase
    that drift; a NEW migration is how the fix ships if one is ever needed.
    """
    from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
        member_facets,
    )

    migration = _load_migration()

    fixture_identities: list[dict[str, Any]] = [
        # email + canonical_id + provider identities (list-shaped)
        {
            "canonical_id": "github:alice",
            "email": "alice@example.com",
            "display_name": "Alice",
            "provider_identities": {
                "github": ["gh-alice"],
                "jira": ["jira-alice-1", "jira-alice-2"],
            },
        },
        # canonical_id only, no email -- display_name must be included
        {
            "canonical_id": "gitlab:bob",
            "email": None,
            "display_name": "Bob B",
            "provider_identities": {},
        },
        # email present -- display_name must NOT be included
        {
            "canonical_id": "linear:carol",
            "email": "carol@example.com",
            "display_name": "Carol C",
            "provider_identities": {"linear": ["linear-carol"]},
        },
        # provider_identities value is a bare string, not a list
        {
            "canonical_id": "jira:dave",
            "email": None,
            "display_name": None,
            "provider_identities": {"jira": "jira-dave-solo"},
        },
        # everything falsy except canonical_id
        {
            "canonical_id": "github:eve",
            "email": None,
            "display_name": None,
            "provider_identities": None,
        },
        # empty provider_identities values (falsy list entries dropped)
        {
            "canonical_id": "github:frank",
            "email": "",
            "display_name": "",
            "provider_identities": {"github": ["", "gh-frank", None]},
        },
    ]

    for identity in fixture_identities:
        live = member_facets(
            canonical_id=identity["canonical_id"],
            email=identity["email"],
            display_name=identity["display_name"],
            provider_identities=identity["provider_identities"],
        )
        frozen = migration._member_facets_at_079(
            canonical_id=identity["canonical_id"],
            email=identity["email"],
            display_name=identity["display_name"],
            provider_identities=identity["provider_identities"],
        )
        assert frozen == live, (
            f"frozen copy diverged from the live function for "
            f"{identity['canonical_id']!r}: frozen={frozen!r} live={live!r}"
        )
