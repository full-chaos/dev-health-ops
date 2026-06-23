"""Routing invariants for ``sync_teams`` (CHAOS-2600 CS5).

ClickHouse is the team system of record. Both the org-scoped and no-org
paths write ClickHouse directly via ``insert_teams``; neither projects to
Postgres ``TeamMapping`` nor goes through the removed analytics bridge.

* Org-scoped (``ns.org`` set): opens a ``ClickHouseStore`` directly, tags it
  with the org_id, and inserts the teams (and any ops links). No
  ``run_with_store``, no Postgres projection.
* No-org (``ns.org`` is None): uses ``run_with_store`` with ``org_id=None``
  (unchanged from before CS5).
"""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import yaml


def _make_ns(
    *,
    provider: str = "config",
    org: str | None = None,
    allow_empty: bool = False,
    path: str | None = None,
) -> argparse.Namespace:
    ns = argparse.Namespace()
    ns.provider = provider
    ns.org = org
    ns.allow_empty = allow_empty
    ns.path = path
    ns.db = "sqlite+aiosqlite:///:memory:"
    ns.sink = "clickhouse"
    ns.analytics_db = "clickhouse://example.test:8123/default"
    ns.owner = None
    ns.auth = None
    return ns


def _write_config(tmp_path: Any, team_id: str = "team-a") -> str:
    config_file = tmp_path / "teams.yaml"
    config_file.write_text(
        yaml.dump({"teams": [{"team_id": team_id, "team_name": "Team A"}]})
    )
    return str(config_file)


class _RecordingStore:
    """Stand-in ClickHouseStore that records org_id + inserted teams."""

    instances: list[_RecordingStore] = []

    def __init__(self, db_uri: str, *args: Any, **kwargs: Any) -> None:
        self.db_uri = db_uri
        self.org_id: str | None = None
        self.inserted_teams: list[Any] = []
        self.inserted_ops_links: list[Any] = []
        _RecordingStore.instances.append(self)

    async def __aenter__(self) -> _RecordingStore:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def insert_teams(self, teams: list[Any]) -> None:
        self.inserted_teams.extend(teams)

    async def insert_jira_project_ops_team_links(self, links: list[Any]) -> None:
        self.inserted_ops_links.extend(links)

    async def get_all_teams(self) -> list[Any]:
        # Echo back the inserted teams (org-tagged) so the persisted-count
        # verification in sync_teams sees them.
        return [
            SimpleNamespace(
                id=getattr(t, "id", None)
                or (t.get("id") if isinstance(t, dict) else None),
                org_id=self.org_id,
            )
            for t in self.inserted_teams
        ]


class TestOrgScopedRouting:
    def test_org_scoped_writes_clickhouse_directly_with_org_id(
        self, tmp_path: Any
    ) -> None:
        from dev_health_ops.providers.teams import sync_teams

        _RecordingStore.instances = []
        with (
            patch(
                "dev_health_ops.storage.clickhouse.ClickHouseStore",
                _RecordingStore,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://example.test:8123/default",
            ),
        ):
            ns = _make_ns(org="org-1", path=_write_config(tmp_path))
            result = sync_teams(ns)

        assert result == 0
        assert len(_RecordingStore.instances) == 1
        store = _RecordingStore.instances[0]
        # The store is org-scoped and the team rows are tagged with the org_id.
        assert store.org_id == "org-1"
        assert store.db_uri == "clickhouse://example.test:8123/default"
        assert {getattr(t, "id", None) for t in store.inserted_teams} == {"team-a"}
        assert all(getattr(t, "org_id", None) == "org-1" for t in store.inserted_teams)

    def test_org_scoped_never_projects_to_postgres(self, tmp_path: Any) -> None:
        from dev_health_ops.providers import teams as teams_module
        from dev_health_ops.providers.teams import sync_teams

        # No Postgres projection helper exists anymore; assert ClickHouse is the
        # only write path (run_with_store is the no-org branch and must not run).
        assert not hasattr(teams_module, "_project_teams_to_postgres")

        _RecordingStore.instances = []
        with (
            patch(
                "dev_health_ops.storage.clickhouse.ClickHouseStore",
                _RecordingStore,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://example.test:8123/default",
            ),
            patch("dev_health_ops.storage.run_with_store") as mock_run_with_store,
        ):
            ns = _make_ns(org="org-7", path=_write_config(tmp_path))
            result = sync_teams(ns)

        assert result == 0
        mock_run_with_store.assert_not_called()

    def test_org_scoped_write_failure_exits_one(self, tmp_path: Any) -> None:
        from dev_health_ops.providers.teams import sync_teams

        class _FailingStore(_RecordingStore):
            async def insert_teams(self, teams: list[Any]) -> None:
                raise RuntimeError("clickhouse down")

        with (
            patch(
                "dev_health_ops.storage.clickhouse.ClickHouseStore",
                _FailingStore,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://example.test:8123/default",
            ),
        ):
            ns = _make_ns(org="org-1", path=_write_config(tmp_path))
            result = sync_teams(ns)

        assert result == 1

    def test_org_scoped_zero_persisted_exits_one(self, tmp_path: Any) -> None:
        from dev_health_ops.providers.teams import sync_teams

        class _EmptyStore(_RecordingStore):
            async def get_all_teams(self) -> list[Any]:
                return []

        with (
            patch(
                "dev_health_ops.storage.clickhouse.ClickHouseStore",
                _EmptyStore,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://example.test:8123/default",
            ),
        ):
            ns = _make_ns(org="org-1", path=_write_config(tmp_path))
            result = sync_teams(ns)

        assert result == 1


class TestNoOrgRouting:
    def test_no_org_calls_run_with_store_and_insert_teams(self, tmp_path: Any) -> None:
        from dev_health_ops.providers.teams import sync_teams

        inserted: list[Any] = []

        async def _fake_run_with_store(
            _db_uri: str, _db_type: str, handler: Any, org_id: Any = None
        ) -> int:
            class _FakeStore:
                async def ensure_tables(self) -> None:
                    pass

                async def insert_teams(self, teams: list) -> None:
                    inserted.extend(teams)

                async def get_all_teams(self) -> list:
                    return [SimpleNamespace(id="team-a", org_id=None)]

            return await handler(_FakeStore())

        with (
            patch(
                "dev_health_ops.storage.run_with_store",
                side_effect=_fake_run_with_store,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
            patch(
                "dev_health_ops.providers.teams.detect_db_type",
                return_value="clickhouse",
            ),
        ):
            ns = _make_ns(org=None, path=_write_config(tmp_path))
            result = sync_teams(ns)

        assert result == 0
        assert [getattr(team, "id", None) for team in inserted] == ["team-a"]

    def test_no_org_run_with_store_receives_org_id_none(self, tmp_path: Any) -> None:
        from dev_health_ops.providers.teams import sync_teams

        captured_org_ids: list[Any] = []

        async def _spy_run_with_store(
            _db_uri: str, _db_type: str, handler: Any, org_id: Any = None
        ) -> int:
            captured_org_ids.append(org_id)

            class _FakeStore:
                async def ensure_tables(self) -> None:
                    pass

                async def insert_teams(self, teams: list) -> None:
                    pass

                async def get_all_teams(self) -> list:
                    return [SimpleNamespace(id="team-a", org_id=None)]

            return await handler(_FakeStore())

        with (
            patch(
                "dev_health_ops.storage.run_with_store",
                side_effect=_spy_run_with_store,
            ),
            patch("dev_health_ops.providers.teams.validate_sink"),
            patch(
                "dev_health_ops.providers.teams.resolve_sink_uri",
                return_value="clickhouse://localhost:8123/default",
            ),
            patch(
                "dev_health_ops.providers.teams.detect_db_type",
                return_value="clickhouse",
            ),
        ):
            ns = _make_ns(org=None, path=_write_config(tmp_path))
            sync_teams(ns)

        assert captured_org_ids == [None], (
            "run_with_store must be called with org_id=None on the no-org path"
        )
