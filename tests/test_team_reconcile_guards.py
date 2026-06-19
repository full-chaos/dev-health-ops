from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IdentityMapping, TeamMapping
from tests._helpers import tables_of


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


def _write_config(path: Path) -> None:
    import yaml

    path.write_text(
        yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]}),
        encoding="utf-8",
    )


def _seed_sync_sqlite(db_path: Path, rows: list[Any]) -> str:
    uri = f"sqlite:///{db_path}"
    engine = create_engine(uri)
    Base.metadata.create_all(engine, tables=tables_of(TeamMapping, IdentityMapping))
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    with Session() as session:
        session.add_all(rows)
        session.commit()
    engine.dispose()
    return f"sqlite+aiosqlite:///{db_path}"


class _FakeClickHouseStore:
    teams: list[Any] = []
    inserted_batches: list[list[Any]] = []

    def __init__(self, _db_url: str) -> None:
        self.org_id: str | None = None

    async def __aenter__(self) -> _FakeClickHouseStore:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return list(self.teams)

    async def insert_teams(self, teams: list[Any]) -> None:
        self.inserted_batches.append(list(teams))


def test_guard_org_sync_projects_before_clickhouse_bridge(tmp_path: Path) -> None:
    from dev_health_ops.providers.teams import sync_teams

    config_file = tmp_path / "teams.yaml"
    _write_config(config_file)
    order: list[str] = []

    async def _projection(teams_data: list[Any], ns: Any) -> dict[str, Any]:
        order.append("projection")
        return {"projected": len(teams_data)}

    def _bridge(**_kwargs: Any) -> int:
        order.append("bridge")
        return 1

    with (
        patch("dev_health_ops.providers.teams._project_teams_to_postgres", _projection),
        patch(
            "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse", _bridge
        ),
        patch("dev_health_ops.storage.run_with_store") as run_with_store,
    ):
        result = sync_teams(_make_ns(org="org-1", path=str(config_file)))

    assert result == 0
    assert order == ["projection", "bridge"]
    run_with_store.assert_not_called()


def test_guard_no_org_sync_writes_clickhouse_directly(tmp_path: Path) -> None:
    from dev_health_ops.providers import teams as teams_provider

    config_file = tmp_path / "teams.yaml"
    _write_config(config_file)
    inserted: list[Any] = []

    class _Store:
        async def ensure_tables(self) -> None:
            return None

        async def insert_teams(self, teams: list[Any]) -> None:
            inserted.extend(teams)

        async def get_all_teams(self) -> list[Any]:
            return inserted

    async def _run_with_store(
        _db_uri: str,
        _db_type: str,
        handler: Any,
        *,
        org_id: str | None = None,
    ) -> int:
        return await handler(_Store())

    with patch("dev_health_ops.storage.run_with_store", _run_with_store):
        result = teams_provider.sync_teams(_make_ns(path=str(config_file)))

    assert result == 0
    assert [team.id for team in inserted] == ["team-a"]


def test_guard_org_bridge_members_use_confirmed_identity_facets_only(
    tmp_path: Path,
) -> None:
    from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

    pg_url = _seed_sync_sqlite(
        tmp_path / "members.db",
        [
            TeamMapping(team_id="team-a", name="Team A", org_id="org-1"),
            IdentityMapping(
                canonical_id="u1",
                org_id="org-1",
                email="alice@example.com",
                provider_identities={"github": ["alice-gh"]},
                team_ids=["team-a"],
            ),
        ],
    )
    _FakeClickHouseStore.inserted_batches = []

    with patch(
        "dev_health_ops.providers.team_bridge.ClickHouseStore", _FakeClickHouseStore
    ):
        bridge_teams_to_clickhouse(
            org_id="org-1",
            db_url="clickhouse://example.test:8123/default",
            postgres_db_url=pg_url,
        )

    members = set(_FakeClickHouseStore.inserted_batches[-1][0]["members"])
    assert {"u1", "alice-gh", "alice@example.com"}.issubset(members)
    assert "unmapped-login" not in members


def test_guard_last_writer_keeps_identity_members_and_curated_fields(
    tmp_path: Path,
) -> None:
    from dev_health_ops.providers.teams import sync_teams

    pg_url = _seed_sync_sqlite(
        tmp_path / "last-writer.db",
        [
            TeamMapping(
                team_id="team-a",
                name="Team A",
                org_id="org-1",
                repo_patterns=["curated/*"],
                project_keys=["CUR"],
                extra_data={"provider_type": "config", "provider_team_id": "team-a"},
                managed_fields=["name", "description", "repo_patterns", "project_keys"],
                sync_policy=0,
            ),
            IdentityMapping(
                canonical_id="u1",
                org_id="org-1",
                provider_identities={"github": ["alice-gh"]},
                team_ids=["team-a"],
            ),
        ],
    )
    config_file = tmp_path / "teams.yaml"
    _write_config(config_file)
    _FakeClickHouseStore.inserted_batches = []

    with patch(
        "dev_health_ops.providers.team_bridge.ClickHouseStore", _FakeClickHouseStore
    ):
        ns = _make_ns(org="org-1", path=str(config_file))
        ns.db = pg_url
        result = sync_teams(ns)

    assert result == 0
    payload = _FakeClickHouseStore.inserted_batches[-1][0]
    assert set(payload["members"]) == {"u1", "alice-gh"}
    assert payload["project_keys"] == ["CUR"]
    assert payload["repo_patterns"] == ["curated/*"]


@pytest.mark.asyncio
async def test_guard_health_check_flags_org_clickhouse_rows_without_mapping(
    tmp_path: Path,
) -> None:
    from types import SimpleNamespace

    from dev_health_ops.providers.team_reconcile import find_unmapped_clickhouse_teams

    pg_url = _seed_sync_sqlite(
        tmp_path / "health.db",
        [TeamMapping(team_id="mapped", name="Mapped", org_id="org-1")],
    )
    _FakeClickHouseStore.teams = [
        SimpleNamespace(id="mapped", name="Mapped", org_id="org-1"),
        SimpleNamespace(id="missing", name="Missing", org_id="org-1"),
        SimpleNamespace(id="other", name="Other", org_id="org-2"),
    ]

    with patch(
        "dev_health_ops.providers.team_reconcile.ClickHouseStore", _FakeClickHouseStore
    ):
        missing = await find_unmapped_clickhouse_teams(
            "org-1",
            db_url="clickhouse://example.test:8123/default",
            postgres_db_url=pg_url,
        )

    assert missing == ["missing"]


def test_guard_drift_provider_registry_covers_required_providers() -> None:
    from dev_health_ops.providers.team_capabilities import team_provider_capabilities

    capabilities = {c.provider: c for c in team_provider_capabilities()}
    required = {"github", "gitlab", "jira", "linear", "ms-teams"}

    assert required.issubset(capabilities)
    unsupported = [
        provider
        for provider in required
        if not capabilities[provider].supports_org_drift_discovery
    ]
    assert unsupported == []


@pytest.mark.asyncio
async def test_reconcile_command_creates_missing_mapping_then_rerun_is_noop(
    tmp_path: Path,
) -> None:
    from types import SimpleNamespace

    from dev_health_ops.providers.team_reconcile import (
        reconcile_clickhouse_teams_to_postgres,
    )

    pg_url = _seed_sync_sqlite(tmp_path / "reconcile.db", [])
    _FakeClickHouseStore.teams = [
        SimpleNamespace(
            id="team-a",
            name="Team A",
            description="From ClickHouse",
            org_id="org-1",
            project_keys=["A"],
            repo_patterns=["a/*"],
        )
    ]

    with (
        patch(
            "dev_health_ops.providers.team_reconcile.ClickHouseStore",
            _FakeClickHouseStore,
        ),
        patch(
            "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
            return_value=1,
        ),
    ):
        first = await reconcile_clickhouse_teams_to_postgres(
            "org-1",
            db_url="clickhouse://example.test:8123/default",
            postgres_db_url=pg_url,
        )
        second = await reconcile_clickhouse_teams_to_postgres(
            "org-1",
            db_url="clickhouse://example.test:8123/default",
            postgres_db_url=pg_url,
        )

    engine = create_engine(pg_url.replace("sqlite+aiosqlite://", "sqlite://"))
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    with Session() as session:
        mappings = session.query(TeamMapping).all()
    engine.dispose()

    assert first["created"] == 1
    assert first["missing_after"] == []
    assert second["created"] == 0
    assert second["existing"] == 1
    assert second["missing_before"] == []
    assert [(m.team_id, m.project_keys, m.repo_patterns) for m in mappings] == [
        ("team-a", ["A"], ["a/*"])
    ]


def test_cli_org_path_consults_team_capability_registry(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from dev_health_ops.providers.teams import sync_teams

    config_file = tmp_path / "teams.yaml"
    _write_config(config_file)

    async def _projection(teams_data: list[Any], ns: Any) -> dict[str, Any]:
        return {"projected": len(teams_data)}

    with (
        patch("dev_health_ops.providers.teams._project_teams_to_postgres", _projection),
        patch(
            "dev_health_ops.providers.team_bridge.bridge_teams_to_clickhouse",
            return_value=1,
        ),
        patch(
            "dev_health_ops.providers.team_capabilities.org_drift_capable_providers",
            return_value=(),
        ) as capable,
        patch(
            "dev_health_ops.providers.team_capabilities.team_provider_capabilities",
            return_value=(),
        ) as capabilities,
        caplog.at_level("INFO"),
    ):
        result = sync_teams(_make_ns(org="org-1", path=str(config_file)))

    assert result == 0
    capable.assert_called_once()
    capabilities.assert_called_once()
    assert "unsupported for org drift discovery" in caplog.text
