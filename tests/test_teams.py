import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.models.teams import JiraProjectOpsTeamLink, Team
from dev_health_ops.storage import ClickHouseStore, SQLAlchemyStore


@pytest.mark.asyncio
async def test_team_model():
    """Test Team model instantiation."""
    team = Team(
        id="team-a",
        name="Team Alpha",
        description="A test team",
        members=["alice@example.com", "bob@example.com"],
    )
    assert str(getattr(team, "id")) == "team-a"
    assert str(getattr(team, "name")) == "Team Alpha"
    assert "alice@example.com" in (team.members or [])
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
        assert store.session is not None
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

        assert store.session is not None
        result = await store.session.execute(select(JiraProjectOpsTeamLink))
        rows = list(result.scalars().all())
        assert len(rows) == 1
        assert str(getattr(rows[0], "project_key")) == "OPS"
        assert str(getattr(rows[0], "ops_team_id")) == "team-1"


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
                "org-1",
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
        assert str(getattr(retrieved[0], "id")) == "t1"

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
    """Test synthetic team generation uses curated, believable identities."""
    from dev_health_ops.fixtures.demo_identity import DEMO_TEAMS

    generator = SyntheticDataGenerator()
    teams = generator.generate_teams(count=3)
    assert len(teams) == 3
    rendered = [(str(getattr(t, "id")), str(getattr(t, "name"))) for t in teams]
    assert rendered == list(DEMO_TEAMS[:3])
    for team in teams:
        # Never the legacy generic 'team-N'/'Team N' scaffolding.
        assert not str(getattr(team, "id")).startswith("team-")
        assert not str(getattr(team, "name")).startswith("Team ")
        assert len(list(getattr(team, "members") or [])) > 0


def test_cli_sync_teams_synthetic():
    """Test CLI sync teams command with synthetic provider."""
    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    class FakeStore:
        def __init__(self):
            self.teams = []

        async def ensure_tables(self):
            return None

        async def insert_teams(self, teams):
            self.teams = teams

        async def get_all_teams(self):
            return self.teams

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id is None
        return await handler(FakeStore())

    with (
        patch("dev_health_ops.storage.run_with_store", new=fake_run_with_store),
        patch("dev_health_ops.providers.teams.validate_sink") as mock_validate,
        patch(
            "dev_health_ops.providers.teams.resolve_sink_uri",
            return_value="clickhouse://localhost:8123/default",
        ) as mock_resolve_sink,
        patch(
            "dev_health_ops.providers.teams.detect_db_type",
            return_value="clickhouse",
        ) as mock_detect,
    ):
        ns = MagicMock()
        ns.db = "sqlite:///:memory:"
        ns.sink = "clickhouse"
        ns.analytics_db = None
        ns.provider = "synthetic"
        ns.path = None
        ns.org = None
        ns.allow_empty = False

        result = _cmd_sync_teams(ns)

        assert result == 0
        mock_validate.assert_called_once_with(ns)
        mock_resolve_sink.assert_called_once_with(ns)
        mock_detect.assert_called_once_with("clickhouse://localhost:8123/default")


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


def test_cli_sync_teams_empty_exits_one(tmp_path):
    """Test that sync_teams exits 1 when no teams are found and allow_empty is False."""
    import yaml

    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    config_file = tmp_path / "empty_teams.yaml"
    config_file.write_text(yaml.dump({"teams": []}))

    ns = MagicMock()
    ns.provider = "config"
    ns.path = str(config_file)
    ns.allow_empty = False

    result = _cmd_sync_teams(ns)
    assert result == 1


def test_cli_sync_teams_empty_allow_empty_exits_zero(tmp_path):
    """Test that sync_teams exits 0 when no teams are found and allow_empty is True."""
    import yaml

    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    config_file = tmp_path / "empty_teams.yaml"
    config_file.write_text(yaml.dump({"teams": []}))

    ns = MagicMock()
    ns.provider = "config"
    ns.path = str(config_file)
    ns.allow_empty = True

    result = _cmd_sync_teams(ns)
    assert result == 0


def test_cli_sync_teams_non_empty_zero_persisted_exits_one(tmp_path):
    import yaml

    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    config_file = tmp_path / "teams.yaml"
    config_file.write_text(
        yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
    )

    class FakeStore:
        async def ensure_tables(self):
            return None

        async def insert_teams(self, teams):
            assert len(teams) == 1

        async def get_all_teams(self):
            return []

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id is None
        return await handler(FakeStore())

    with (
        patch("dev_health_ops.storage.run_with_store", new=fake_run_with_store),
        patch("dev_health_ops.providers.teams.validate_sink"),
        patch(
            "dev_health_ops.providers.teams.resolve_sink_uri",
            return_value="clickhouse://localhost:8123/default",
        ),
        patch(
            "dev_health_ops.providers.teams.detect_db_type", return_value="clickhouse"
        ),
    ):
        ns = MagicMock()
        ns.provider = "config"
        ns.path = str(config_file)
        ns.org = None
        ns.allow_empty = False

        result = _cmd_sync_teams(ns)

    assert result == 1


def test_cli_sync_teams_allow_empty_overrides_zero_persisted(tmp_path):
    import yaml

    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    config_file = tmp_path / "teams.yaml"
    config_file.write_text(
        yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
    )

    class FakeStore:
        async def ensure_tables(self):
            return None

        async def insert_teams(self, teams):
            assert len(teams) == 1

        async def get_all_teams(self):
            return []

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id is None
        return await handler(FakeStore())

    with (
        patch("dev_health_ops.storage.run_with_store", new=fake_run_with_store),
        patch("dev_health_ops.providers.teams.validate_sink"),
        patch(
            "dev_health_ops.providers.teams.resolve_sink_uri",
            return_value="clickhouse://localhost:8123/default",
        ),
        patch(
            "dev_health_ops.providers.teams.detect_db_type", return_value="clickhouse"
        ),
    ):
        ns = MagicMock()
        ns.provider = "config"
        ns.path = str(config_file)
        ns.org = None
        ns.allow_empty = True

        result = _cmd_sync_teams(ns)

    assert result == 0


class _RecordingCHStore:
    """ClickHouseStore stand-in recording org_id + inserted teams (CS5)."""

    last: "_RecordingCHStore | None" = None

    def __init__(self, db_uri, *args, **kwargs):
        self.db_uri = db_uri
        self.org_id = None
        self.inserted_teams: list = []
        self.inserted_ops_links: list = []
        _RecordingCHStore.last = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def insert_teams(self, teams):
        self.inserted_teams.extend(teams)

    async def insert_jira_project_ops_team_links(self, links):
        self.inserted_ops_links.extend(links)

    async def get_all_teams(self):
        return [
            MagicMock(id=getattr(t, "id", None), org_id=self.org_id)
            for t in self.inserted_teams
        ]


def test_cli_sync_teams_org_scoped_write_failure_exits_one(tmp_path):
    import yaml

    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    config_file = tmp_path / "teams.yaml"
    config_file.write_text(
        yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
    )

    class _FailingStore(_RecordingCHStore):
        async def insert_teams(self, teams):
            raise RuntimeError("clickhouse down")

    with patch(
        "dev_health_ops.storage.clickhouse.ClickHouseStore",
        _FailingStore,
    ):
        ns = MagicMock()
        ns.provider = "config"
        ns.path = str(config_file)
        ns.org = "org-1"
        ns.db = "sqlite:///semantic.db"
        ns.allow_empty = False
        ns.sink = "clickhouse"
        ns.analytics_db = "clickhouse://example.test:8123/default"

        result = _cmd_sync_teams(ns)

    assert result == 1


def test_cli_sync_teams_org_scoped_writes_clickhouse_directly(tmp_path):
    """Org-scoped sync writes ClickHouse directly (CS5).

    No Postgres projection and no run_with_store (the no-org branch) for an
    org-scoped run; the store is tagged with the org_id.
    """
    import yaml

    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    config_file = tmp_path / "teams.yaml"
    config_file.write_text(
        yaml.dump({"teams": [{"team_id": "team-a", "team_name": "Team A"}]})
    )

    _RecordingCHStore.last = None
    with (
        patch(
            "dev_health_ops.storage.clickhouse.ClickHouseStore",
            _RecordingCHStore,
        ),
        patch("dev_health_ops.storage.run_with_store") as mock_run_with_store,
    ):
        ns = MagicMock()
        ns.provider = "config"
        ns.path = str(config_file)
        ns.org = "org-1"
        ns.db = "sqlite:///semantic.db"
        ns.allow_empty = False
        ns.sink = "clickhouse"
        ns.analytics_db = "clickhouse://example.test:8123/default"

        result = _cmd_sync_teams(ns)

    assert result == 0
    mock_run_with_store.assert_not_called()
    store = _RecordingCHStore.last
    assert store is not None
    assert store.org_id == "org-1"
    assert {getattr(t, "id", None) for t in store.inserted_teams} == {"team-a"}


def test_cli_sync_teams_org_scoped_jira_ops_links_written_to_clickhouse():
    from atlassian.canonical_models import (
        CanonicalProjectWithOpsgenieTeams,
        JiraProject,
        OpsgenieTeamRef,
    )

    from dev_health_ops.models.teams import JiraProjectOpsTeamLink
    from dev_health_ops.providers.teams import sync_teams as _cmd_sync_teams

    project = CanonicalProjectWithOpsgenieTeams(
        project=JiraProject(cloud_id="cloud-1", key="OPS", name="Ops Project"),
        opsgenie_teams=[OpsgenieTeamRef(id="team-1", name="Ops Team")],
    )
    captured: list[JiraProjectOpsTeamLink] = []

    class FakeClickHouseStore:
        def __init__(self, db_uri, *args, **kwargs):
            self.db_uri = db_uri
            self.org_id = None
            self._inserted_teams: list = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def insert_teams(self, teams):
            self._inserted_teams.extend(teams)

        async def insert_jira_project_ops_team_links(self, links):
            captured.extend(links)

        async def get_all_teams(self):
            return [
                MagicMock(id=getattr(t, "id", None), org_id=self.org_id)
                for t in self._inserted_teams
            ]

    with (
        patch(
            "dev_health_ops.providers.jira.atlassian_compat.get_atlassian_cloud_id",
            return_value="cloud-1",
        ),
        patch(
            "dev_health_ops.providers.jira.atlassian_compat.build_atlassian_graphql_client"
        ),
        patch(
            "atlassian.graph.api.jira_projects.iter_projects_with_opsgenie_linkable_teams",
            return_value=iter([project]),
        ),
        patch(
            "dev_health_ops.storage.clickhouse.ClickHouseStore",
            FakeClickHouseStore,
        ),
    ):
        ns = MagicMock()
        ns.provider = "jira-ops"
        ns.path = None
        ns.org = "org-1"
        ns.db = "sqlite:///semantic.db"
        ns.allow_empty = False
        ns.sink = "clickhouse"
        ns.analytics_db = "clickhouse://example.test:8123/default"

        result = _cmd_sync_teams(ns)

    assert result == 0
    assert len(captured) == 1
    assert captured[0].project_key == "OPS"
    assert captured[0].ops_team_id == "team-1"


@pytest.mark.asyncio
async def test_import_ms_teams_uses_prefixed_team_id():
    from dev_health_ops.api.admin.schemas import DiscoveredTeam
    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )

    captured: dict = {}

    class FakeTeamMappingService:
        def __init__(self, _session, _org_id):
            pass

        async def get(self, _team_id):
            return None

        async def create_or_update(self, **kwargs):
            captured.update(kwargs)

    with patch(
        "dev_health_ops.api.services.configuration.team_discovery.TeamMappingService",
        FakeTeamMappingService,
    ):
        result = await TeamDiscoveryService(
            MagicMock(spec=AsyncSession), "org-1"
        ).import_teams(
            [
                DiscoveredTeam(
                    provider_type="ms-teams",
                    provider_team_id="graph-team-id",
                    name="Platform Team",
                )
            ]
        )

    assert result["imported"] == 1
    assert captured["team_id"] == "ms-teams:graph-team-id"
    assert captured["extra_data"]["provider_team_id"] == "graph-team-id"
