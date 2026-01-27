import asyncio
import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select

from dev_health_ops.models.teams import JiraProjectOpsTeamLink, Team
from dev_health_ops.storage import SQLAlchemyStore, MongoStore, ClickHouseStore
from dev_health_ops.fixtures.generator import SyntheticDataGenerator


@pytest.mark.asyncio
async def test_team_model():
    """Test Team model instantiation."""
    team = Team(
        id="team-a",
        name="Team Alpha",
        description="A test team",
        members=["alice@example.com", "bob@example.com"],
    )
    assert team.id == "team-a"
    assert team.name == "Team Alpha"
    assert "alice@example.com" in team.members
    assert isinstance(team.updated_at, datetime)


@pytest.mark.asyncio
async def test_sqlalchemy_store_teams():
    """Test Team storage in SQLAlchemy (SQLite)."""
    store = SQLAlchemyStore("sqlite+aiosqlite:///:memory:")
    async with store:
        await store.ensure_tables()

        teams = [
            Team(id="t1", name="Team 1", members=["m1"]),
            Team(id="t2", name="Team 2", members=["m2", "m3"]),
        ]

        await store.insert_teams(teams)

        retrieved = await store.get_all_teams()
        assert len(retrieved) == 2
        ids = {t.id for t in retrieved}
        assert "t1" in ids
        assert "t2" in ids

        # Test update
        updated_team = Team(id="t1", name="Team 1 Updated", members=["m1", "m4"])
        await store.insert_teams([updated_team])

        # Expire session to ensure we fetch from DB
        store.session.expire_all()

        retrieved = await store.get_all_teams()
        t1 = next(t for t in retrieved if t.id == "t1")
        assert t1.name == "Team 1 Updated"
        assert "m4" in t1.members

        links = [
            JiraProjectOpsTeamLink(
                project_key="OPS",
                ops_team_id="team-1",
                project_name="Ops Project",
                ops_team_name="Primary Ops",
            )
        ]
        await store.insert_jira_project_ops_team_links(links)

        result = await store.session.execute(select(JiraProjectOpsTeamLink))
        rows = list(result.scalars().all())
        assert len(rows) == 1
        assert rows[0].project_key == "OPS"
        assert rows[0].ops_team_id == "team-1"


@pytest.mark.asyncio
async def test_mongo_store_teams():
    """Test Team storage in Mongo (mocked)."""
    with patch("dev_health_ops.storage.AsyncIOMotorClient") as mock_client:
        store = MongoStore("mongodb://localhost:27017", db_name="testdb")
        store.db = mock_client.return_value["testdb"]

        # Mock insert_teams (uses _upsert_many)
        store.db["teams"].bulk_write = AsyncMock()

        teams = [Team(id="t1", name="Team 1")]
        await store.insert_teams(teams)
        assert store.db["teams"].bulk_write.called

        store.db["jira_project_ops_team_links"].bulk_write = AsyncMock()
        links = [
            JiraProjectOpsTeamLink(
                project_key="OPS",
                ops_team_id="team-1",
                project_name="Ops Project",
                ops_team_name="Primary Ops",
            )
        ]
        await store.insert_jira_project_ops_team_links(links)
        assert store.db["jira_project_ops_team_links"].bulk_write.called

        # Mock get_all_teams
        mock_cursor = MagicMock()
        mock_cursor.__aiter__.return_value = [
            {
                "id": "t1",
                "team_uuid": uuid.uuid4(),
                "name": "Team 1",
                "members": ["m1"],
                "updated_at": datetime.now(timezone.utc),
            }
        ]
        store.db["teams"].find.return_value = mock_cursor

        retrieved = await store.get_all_teams()
        assert len(retrieved) == 1
        assert retrieved[0].id == "t1"


@pytest.mark.asyncio
async def test_clickhouse_store_teams():
    """Test Team storage in ClickHouse (mocked)."""
    with patch("clickhouse_connect.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        store = ClickHouseStore("clickhouse://localhost")
        await store.__aenter__()

        # Mock insert_teams
        teams = [Team(id="t1", name="Team 1")]
        await store.insert_teams(teams)
        assert mock_client.insert.called

        # Mock get_all_teams
        mock_result = MagicMock()
        mock_result.result_rows = [
            (
                "t1",
                str(uuid.uuid4()),
                "Team 1",
                "Desc",
                ["m1"],
                datetime.now(timezone.utc),
            )
        ]
        mock_client.query.return_value = mock_result

        retrieved = await store.get_all_teams()
        assert len(retrieved) == 1
        assert retrieved[0].id == "t1"

        links = [
            JiraProjectOpsTeamLink(
                project_key="OPS",
                ops_team_id="team-1",
                project_name="Ops Project",
                ops_team_name="Primary Ops",
            )
        ]
        await store.insert_jira_project_ops_team_links(links)
        assert mock_client.insert.called


def test_synthetic_teams_generation():
    """Test synthetic team generation."""
    generator = SyntheticDataGenerator()
    teams = generator.generate_teams(count=3)
    assert len(teams) == 3
    for team in teams:
        assert team.id.startswith("team-")
        assert len(team.members) > 0


@pytest.mark.asyncio
async def test_cli_sync_teams_synthetic():
    """Test CLI sync teams command with synthetic provider."""
    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    with patch("asyncio.run") as mock_run:
        with patch("dev_health_ops.storage.resolve_db_type") as mock_resolve:
            mock_resolve.return_value = "sqlite"

            ns = MagicMock()
            ns.db = "sqlite:///:memory:"
            ns.db_type = None
            ns.provider = "synthetic"
            ns.path = None

            result = _cmd_sync_teams(ns)

            assert result == 0
            assert mock_run.called
            coro = mock_run.call_args[0][0]
            assert asyncio.iscoroutine(coro)
            coro.close()


@pytest.mark.asyncio
async def test_load_team_resolver_from_store_accepts_id_name_dicts():
    """Ensure team resolver handles dicts keyed by id/name."""
    from dev_health_ops.providers.teams import load_team_resolver_from_store

    class FakeStore:
        async def get_all_teams(self):
            return [
                {
                    "id": "team-a",
                    "name": "Team Alpha",
                    "members": ["alice@example.com"],
                }
            ]

    resolver = await load_team_resolver_from_store(FakeStore())
    team_id, team_name = resolver.resolve("alice@example.com")
    assert team_id == "team-a"
    assert team_name == "Team Alpha"
