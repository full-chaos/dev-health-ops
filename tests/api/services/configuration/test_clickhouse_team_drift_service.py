"""Service-level tests for ``ClickHouseTeamDriftService`` (CHAOS-2622).

Covers the *decision* surface (approve / dismiss by ``change_id``) that sits in
front of the drift-aware projector. The projector's value-fingerprint /
no-resurrection / supersede / resolve lifecycle is already asserted in
``test_clickhouse_team_drift_projector.py`` and is intentionally NOT duplicated
here. This file proves:

* ``approve`` applies the *provider-observed* value into the ``teams`` catalog
  (via ``ClickHouseTeamAdminService.create_or_update``) and writes a status row
  flipping the change to ``approved``;
* ``dismiss`` writes a ``dismissed`` status row and leaves the catalog untouched;
* ``approve_all`` / ``dismiss_all`` decide every pending change for the team;
* an explicit ``change_ids`` list decides only the requested subset;
* a no-op call (no ``change_ids`` and not ``*_all``) decides nothing.

The fake store implements exactly the surface the service touches:
``client.query`` (returning ``column_names`` + ``result_rows`` like
clickhouse-connect), ``_lock``, ``insert_teams`` and
``insert_team_drift_changes``. It mirrors the ``FakeProjectorStore`` pattern but
adds ``column_names`` because the drift service zips columns by name.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, cast

import pytest

from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    _TEAM_COLUMNS,
)
from dev_health_ops.api.services.configuration.clickhouse_team_drift import (
    _OBSERVATION_COLUMNS,
    ClickHouseTeamDriftService,
)

ORG_ID = "org-1"
_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

_PENDING_ALIASES = (
    "change_id",
    "team_id",
    "team_name",
    "provider",
    "native_team_key",
    "change_type",
    "field",
    "old_value_json",
    "new_value_json",
    "first_seen_at",
    "last_seen_at",
)


class _Result:
    def __init__(
        self, column_names: tuple[str, ...], rows: list[tuple[Any, ...]]
    ) -> None:
        self.column_names = list(column_names)
        self.result_rows = rows


class _FakeDriftClient:
    def __init__(self, store: FakeDriftStore) -> None:
        self._store = store

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> _Result:
        params = parameters or {}
        # Order matters: the pending query also contains "teams FINAL" (LEFT JOIN).
        if "team_drift_changes" in query:
            team_id = params.get("team_id")
            rows = [
                tuple(row[col] for col in _PENDING_ALIASES)
                for row in self._store.pending
                if team_id is None or row["team_id"] == team_id
            ]
            return _Result(_PENDING_ALIASES, rows)
        if "team_provider_observations" in query:
            obs = self._store.observations.get(str(params.get("team_id", "")))
            if obs is None:
                return _Result(_OBSERVATION_COLUMNS, [])
            return _Result(
                _OBSERVATION_COLUMNS,
                [tuple(obs.get(col) for col in _OBSERVATION_COLUMNS)],
            )
        if "teams FINAL" in query:
            team_id = params.get("team_id")
            rows = [
                tuple(team[col] for col in _TEAM_COLUMNS)
                for tid, team in self._store.teams.items()
                if team_id is None or tid == team_id
            ]
            return _Result(_TEAM_COLUMNS, rows)
        return _Result((), [])


class FakeDriftStore:
    """In-memory stand-in for ``ClickHouseStore`` (drift-service surface only)."""

    def __init__(self) -> None:
        self.teams: dict[str, dict[str, Any]] = {}
        self.observations: dict[str, dict[str, Any]] = {}
        self.pending: list[dict[str, Any]] = []
        self.drift_inserts: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self.client = _FakeDriftClient(self)

    async def insert_teams(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            team_id = str(row["id"])
            team_uuid = row.get("team_uuid") or uuid.uuid5(
                uuid.NAMESPACE_URL, f"team:{ORG_ID}:{team_id}"
            )
            if not isinstance(team_uuid, uuid.UUID):
                team_uuid = uuid.UUID(str(team_uuid))
            self.teams[team_id] = {
                "id": team_id,
                "team_uuid": team_uuid,
                "name": str(row.get("name"))
                if row.get("name") is not None
                else team_id,
                "description": row.get("description"),
                "members": list(row.get("members") or []),
                "project_keys": list(row.get("project_keys") or []),
                "repo_patterns": list(row.get("repo_patterns") or []),
                "is_active": int(row.get("is_active", 1) or 0),
                "updated_at": row.get("updated_at") or _NOW,
                "org_id": str(row.get("org_id") or ORG_ID),
            }

    async def insert_team_drift_changes(self, rows: list[dict[str, Any]]) -> None:
        self.drift_inserts.extend(dict(row) for row in rows)


def _seed_catalog_team(
    store: FakeDriftStore, *, name: str, description: str | None
) -> None:
    store.teams["team-1"] = {
        "id": "team-1",
        "team_uuid": uuid.uuid5(uuid.NAMESPACE_URL, f"team:{ORG_ID}:team-1"),
        "name": name,
        "description": description,
        "members": ["alice@example.com"],
        "project_keys": ["TEAM"],
        "repo_patterns": ["org/repo"],
        "is_active": 1,
        "updated_at": _NOW,
        "org_id": ORG_ID,
    }


def _seed_observation(store: FakeDriftStore, **overrides: Any) -> None:
    obs = {
        "team_id": "team-1",
        "name": "Platform",
        "description": "Observed description",
        "members_json": json.dumps(["alice@example.com"]),
        "project_keys_json": json.dumps(["TEAM"]),
        "repo_patterns_json": json.dumps(["org/repo"]),
        "is_active": 1,
        "parent_team_id": None,
    }
    obs.update(overrides)
    store.observations["team-1"] = obs


def _pending(change_id: str, *, field: str, old: Any, new: Any) -> dict[str, Any]:
    return {
        "change_id": change_id,
        "team_id": "team-1",
        "team_name": "Platform",
        "provider": "linear",
        "native_team_key": "TEAM",
        "change_type": "field_changed",
        "field": field,
        "old_value_json": json.dumps(old, separators=(",", ":")),
        "new_value_json": json.dumps(new, separators=(",", ":")),
        "first_seen_at": _NOW,
        "last_seen_at": _NOW,
    }


def _service(store: FakeDriftStore) -> ClickHouseTeamDriftService:
    return ClickHouseTeamDriftService(cast(Any, store), ORG_ID)


def test_approve_applies_observed_value_into_catalog_and_marks_approved() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Observed description")
    _seed_observation(store, name="Platform")
    store.pending = [_pending("chg-name", field="name", old="Old", new="Platform")]

    result = asyncio.run(
        _service(store).approve(
            team_id="team-1", change_ids=["chg-name"], decided_by="admin"
        )
    )

    # Catalog now carries the provider-observed value.
    assert store.teams["team-1"]["name"] == "Platform"
    # Status row flips to approved.
    assert len(store.drift_inserts) == 1
    assert store.drift_inserts[0]["status"] == "approved"
    assert store.drift_inserts[0]["change_id"] == "chg-name"
    assert store.drift_inserts[0]["decided_by"] == "admin"
    assert result == {"approved": 1, "change_ids": ["chg-name"]}


def test_dismiss_marks_dismissed_without_touching_catalog() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Observed description")
    _seed_observation(store, name="Platform")
    store.pending = [_pending("chg-name", field="name", old="Old", new="Platform")]

    result = asyncio.run(
        _service(store).dismiss(
            team_id="team-1", change_ids=["chg-name"], decided_by="admin"
        )
    )

    # Catalog is untouched by a dismiss.
    assert store.teams["team-1"]["name"] == "Old"
    assert len(store.drift_inserts) == 1
    assert store.drift_inserts[0]["status"] == "dismissed"
    assert result == {"dismissed": 1, "change_ids": ["chg-name"]}


def test_approve_all_decides_every_pending_change_for_the_team() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Old desc")
    _seed_observation(store, name="Platform", description="New desc")
    store.pending = [
        _pending("chg-name", field="name", old="Old", new="Platform"),
        _pending("chg-desc", field="description", old="Old desc", new="New desc"),
    ]

    result = asyncio.run(
        _service(store).approve(team_id="team-1", approve_all=True, decided_by="admin")
    )

    assert store.teams["team-1"]["name"] == "Platform"
    assert store.teams["team-1"]["description"] == "New desc"
    assert result["approved"] == 2
    assert set(result["change_ids"]) == {"chg-name", "chg-desc"}
    assert {row["status"] for row in store.drift_inserts} == {"approved"}
    assert len(store.drift_inserts) == 2


def test_dismiss_all_decides_every_pending_change_for_the_team() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Old desc")
    _seed_observation(store, name="Platform", description="New desc")
    store.pending = [
        _pending("chg-name", field="name", old="Old", new="Platform"),
        _pending("chg-desc", field="description", old="Old desc", new="New desc"),
    ]

    result = asyncio.run(
        _service(store).dismiss(team_id="team-1", dismiss_all=True, decided_by="admin")
    )

    # Nothing applied.
    assert store.teams["team-1"]["name"] == "Old"
    assert store.teams["team-1"]["description"] == "Old desc"
    assert result["dismissed"] == 2
    assert {row["status"] for row in store.drift_inserts} == {"dismissed"}


def test_change_ids_decides_only_the_requested_subset() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Old desc")
    _seed_observation(store, name="Platform", description="New desc")
    store.pending = [
        _pending("chg-name", field="name", old="Old", new="Platform"),
        _pending("chg-desc", field="description", old="Old desc", new="New desc"),
    ]

    result = asyncio.run(
        _service(store).approve(
            team_id="team-1", change_ids=["chg-name"], decided_by="admin"
        )
    )

    # Only the name change was applied; description untouched.
    assert store.teams["team-1"]["name"] == "Platform"
    assert store.teams["team-1"]["description"] == "Old desc"
    assert result == {"approved": 1, "change_ids": ["chg-name"]}
    assert [row["change_id"] for row in store.drift_inserts] == ["chg-name"]


def test_approve_uses_empty_observed_name_instead_of_falling_back() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Observed description")
    _seed_observation(store, name="")
    store.pending = [_pending("chg-name", field="name", old="Old", new="")]

    result = asyncio.run(
        _service(store).approve(
            team_id="team-1", change_ids=["chg-name"], decided_by="admin"
        )
    )

    assert store.teams["team-1"]["name"] == ""
    assert result == {"approved": 1, "change_ids": ["chg-name"]}


def test_approve_fails_without_provider_observation() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Observed description")
    store.pending = [_pending("chg-name", field="name", old="Old", new="Platform")]

    with pytest.raises(ValueError, match="provider observation"):
        asyncio.run(
            _service(store).approve(
                team_id="team-1", change_ids=["chg-name"], decided_by="admin"
            )
        )

    assert store.teams["team-1"]["name"] == "Old"
    assert store.drift_inserts == []


def test_approve_without_change_ids_and_not_all_is_a_noop() -> None:
    store = FakeDriftStore()
    _seed_catalog_team(store, name="Old", description="Old desc")
    _seed_observation(store, name="Platform")
    store.pending = [_pending("chg-name", field="name", old="Old", new="Platform")]

    result = asyncio.run(_service(store).approve(team_id="team-1", decided_by="admin"))

    assert store.teams["team-1"]["name"] == "Old"
    assert store.drift_inserts == []
    assert result == {"approved": 0, "change_ids": []}
