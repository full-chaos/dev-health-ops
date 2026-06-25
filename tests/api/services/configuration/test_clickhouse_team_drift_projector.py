from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
    ClickHouseTeamDriftProjector,
    change_id_for_team_field,
)


class FakeProjectorStore:
    def __init__(self) -> None:
        self.policy_rows: dict[str, dict[str, Any]] = {}
        self.team_rows: dict[str, dict[str, Any]] = {}
        self.drift_rows: dict[str, dict[str, Any]] = {}
        self.observations: list[dict[str, Any]] = []
        self.drift_inserts: list[dict[str, Any]] = []

    async def insert_teams(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            self.team_rows[str(row["id"])] = dict(row)

    async def insert_team_provider_observations(
        self, rows: list[dict[str, Any]]
    ) -> None:
        self.observations.extend(dict(row) for row in rows)

    async def insert_team_drift_changes(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            stored = dict(row)
            self.drift_inserts.append(stored)
            self.drift_rows[str(stored["change_id"])] = stored

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "FROM team_sync_policies" in query:
            row = self.policy_rows.get(str(parameters["team_id"]))
            return [dict(row)] if row else []
        if "FROM teams FINAL" in query:
            row = self.team_rows.get(str(parameters["team_id"]))
            return [dict(row)] if row else []
        if "FROM team_drift_changes" in query:
            return [
                dict(row)
                for row in self.drift_rows.values()
                if row["org_id"] == parameters["org_id"]
                and row["entity_id"] == parameters["team_id"]
                and row["provider"] == parameters["provider"]
            ]
        return []


def test_auto_apply_policy_writes_observation_and_catalog() -> None:
    store = FakeProjectorStore()
    team_writes: list[dict[str, Any]] = []

    async def write_teams(rows: list[dict[str, Any]]) -> None:
        team_writes.extend(dict(row) for row in rows)

    row = _team_row(name="Platform")
    asyncio.run(
        ClickHouseTeamDriftProjector(
            store=cast(Any, store),
            org_id="org-1",
            team_writer=write_teams,
        ).project_team(row)
    )

    assert store.observations[0]["team_id"] == "team-1"
    assert store.observations[0]["name"] == "Platform"
    assert team_writes == [row]
    assert store.drift_inserts == []


def test_flag_policy_emits_value_fingerprinted_pending_change() -> None:
    store = _store_with_flag_policy()
    store.team_rows["team-1"] = _catalog_row(name="Old")

    asyncio.run(_project(store, _team_row(name="New")))

    expected_change_id = change_id_for_team_field(
        org_id="org-1",
        team_id="team-1",
        field="name",
        old_value_json=json.dumps("Old", separators=(",", ":")),
        new_value_json=json.dumps("New", separators=(",", ":")),
    )
    assert store.drift_inserts[0]["change_id"] == expected_change_id
    assert store.drift_inserts[0]["status"] == "pending"
    assert store.drift_inserts[0]["old_value_json"] == '"Old"'
    assert store.drift_inserts[0]["new_value_json"] == '"New"'


def test_decided_change_is_not_reinserted_as_pending() -> None:
    store = _store_with_flag_policy()
    store.team_rows["team-1"] = _catalog_row(name="Old")

    asyncio.run(_project(store, _team_row(name="New")))
    change_id = store.drift_inserts[0]["change_id"]
    store.drift_rows[change_id] = {
        **store.drift_rows[change_id],
        "status": "dismissed",
        "updated_at": datetime.now(timezone.utc),
    }

    asyncio.run(_project(store, _team_row(name="New")))

    assert store.drift_rows[change_id]["status"] == "dismissed"
    assert [row["status"] for row in store.drift_inserts] == ["pending"]


def test_changed_provider_value_supersedes_prior_pending_change() -> None:
    store = _store_with_flag_policy()
    store.team_rows["team-1"] = _catalog_row(name="Old")

    asyncio.run(_project(store, _team_row(name="New")))
    first_change_id = store.drift_inserts[0]["change_id"]
    asyncio.run(_project(store, _team_row(name="Newer")))
    latest_change_id = store.drift_inserts[-1]["change_id"]

    assert store.drift_rows[first_change_id]["status"] == "superseded"
    assert store.drift_rows[latest_change_id]["status"] == "pending"
    assert [row["status"] for row in store.drift_inserts] == [
        "pending",
        "superseded",
        "pending",
    ]


def test_disappeared_drift_resolves_prior_pending_change() -> None:
    store = _store_with_flag_policy()
    store.team_rows["team-1"] = _catalog_row(name="Old")

    asyncio.run(_project(store, _team_row(name="New")))
    change_id = store.drift_inserts[0]["change_id"]
    asyncio.run(_project(store, _team_row(name="Old")))

    assert store.drift_rows[change_id]["status"] == "resolved"
    assert [row["status"] for row in store.drift_inserts] == ["pending", "resolved"]


async def _project(store: FakeProjectorStore, row: dict[str, Any]) -> None:
    await ClickHouseTeamDriftProjector(
        store=cast(Any, store), org_id="org-1"
    ).project_team(row)


def _store_with_flag_policy() -> FakeProjectorStore:
    store = FakeProjectorStore()
    store.policy_rows["team-1"] = {
        "sync_policy": 1,
        "managed_fields": ["name"],
    }
    return store


def _team_row(*, name: str) -> dict[str, Any]:
    return {
        "id": "team-1",
        "name": name,
        "description": "Team description",
        "members": ["alice@example.com"],
        "project_keys": ["TEAM"],
        "repo_patterns": ["org/repo"],
        "is_active": True,
        "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "org_id": "org-1",
        "provider": "linear",
        "native_team_key": "TEAM",
        "parent_team_id": None,
    }


def _catalog_row(*, name: str) -> dict[str, Any]:
    return {
        "id": "team-1",
        "name": name,
        "description": "Team description",
        "members": ["alice@example.com"],
        "project_keys": ["TEAM"],
        "repo_patterns": ["org/repo"],
        "is_active": 1,
        "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "org_id": "org-1",
        "provider": "linear",
        "native_team_key": "TEAM",
        "parent_team_id": None,
    }
