from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.configuration.team_drift_sync import (
    PROVIDER_MANAGED_FIELDS,
    TeamDriftSyncService,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IdentityMapping, TeamMapping
from tests._helpers import tables_of


@dataclass
class ProviderTeam:
    id: str
    name: str
    description: str | None = None
    repo_patterns: list[str] = field(default_factory=list)
    project_keys: list[str] = field(default_factory=list)
    members: list[str] = field(default_factory=list)
    members_complete: bool = False


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "team_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(TeamMapping, IdentityMapping),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_project_provider_teams_creates_full_team_mapping(session_maker):
    async with session_maker() as session:
        service = TeamDriftSyncService(session, "org-1")
        result = await service.project_provider_teams(
            "github",
            [
                ProviderTeam(
                    id="gh:platform",
                    name="Platform",
                    description="Platform team",
                    repo_patterns=["platform/*"],
                    project_keys=["PLAT"],
                )
            ],
        )

        mapping = await session.scalar(
            select(TeamMapping).where(TeamMapping.team_id == "gh:platform")
        )

    assert result["created"] == 1
    assert result["projected"] == 1
    assert mapping is not None
    assert mapping.name == "Platform"
    assert mapping.description == "Platform team"
    assert mapping.is_active is True
    assert mapping.repo_patterns == ["platform/*"]
    assert mapping.project_keys == ["PLAT"]
    assert mapping.managed_fields == PROVIDER_MANAGED_FIELDS
    assert mapping.extra_data == {
        "provider_type": "github",
        "provider_team_id": "platform",
        "last_discovered_at": mapping.extra_data["last_discovered_at"],
        "sync_source": "provider-projection",
    }


@pytest.mark.asyncio
async def test_project_provider_teams_preserves_curated_empty_provider_values(
    session_maker,
):
    async with session_maker() as session:
        session.add(
            TeamMapping(
                team_id="gh:platform",
                name="Old Platform",
                org_id="org-1",
                description="Old description",
                repo_patterns=["curated/*"],
                project_keys=["CUR"],
                extra_data={"provider_type": "github", "provider_team_id": "platform"},
                managed_fields=list(PROVIDER_MANAGED_FIELDS),
                sync_policy=0,
            )
        )
        await session.flush()

        service = TeamDriftSyncService(session, "org-1")
        await service.project_provider_teams(
            "github",
            [ProviderTeam(id="gh:platform", name="New Platform")],
        )
        mapping = await session.scalar(
            select(TeamMapping).where(TeamMapping.team_id == "gh:platform")
        )

    assert mapping is not None
    assert mapping.name == "New Platform"
    assert mapping.description is None
    assert mapping.repo_patterns == ["curated/*"]
    assert mapping.project_keys == ["CUR"]


@pytest.mark.asyncio
async def test_project_provider_teams_explicit_replace_allows_empty_provider_values(
    session_maker,
):
    async with session_maker() as session:
        session.add(
            TeamMapping(
                team_id="gh:platform",
                name="Old Platform",
                org_id="org-1",
                repo_patterns=["curated/*"],
                project_keys=["CUR"],
                extra_data={"provider_type": "github", "provider_team_id": "platform"},
                managed_fields=list(PROVIDER_MANAGED_FIELDS),
                sync_policy=0,
            )
        )
        await session.flush()

        service = TeamDriftSyncService(session, "org-1")
        await service.project_provider_teams(
            "github",
            [ProviderTeam(id="gh:platform", name="New Platform")],
            replace_empty_provider_values=True,
        )
        mapping = await session.scalar(
            select(TeamMapping).where(TeamMapping.team_id == "gh:platform")
        )

    assert mapping is not None
    assert mapping.repo_patterns == []
    assert mapping.project_keys == []


@pytest.mark.asyncio
async def test_project_provider_teams_sync_policy_one_flags_without_applying(
    session_maker,
):
    async with session_maker() as session:
        session.add(
            TeamMapping(
                team_id="gh:platform",
                name="Old Platform",
                org_id="org-1",
                repo_patterns=["curated/*"],
                project_keys=["CUR"],
                extra_data={"provider_type": "github", "provider_team_id": "platform"},
                managed_fields=list(PROVIDER_MANAGED_FIELDS),
                sync_policy=1,
            )
        )
        await session.flush()

        service = TeamDriftSyncService(session, "org-1")
        result = await service.project_provider_teams(
            "github",
            [
                ProviderTeam(
                    id="gh:platform",
                    name="New Platform",
                    repo_patterns=["provider/*"],
                    project_keys=["PROV"],
                )
            ],
        )
        mapping = await session.scalar(
            select(TeamMapping).where(TeamMapping.team_id == "gh:platform")
        )

    assert result["flagged"] == 1
    assert mapping is not None
    assert mapping.name == "Old Platform"
    assert mapping.repo_patterns == ["curated/*"]
    assert mapping.project_keys == ["CUR"]
    pending = mapping.flagged_changes["pending"]
    assert {change["field"] for change in pending} == {
        "name",
        "repo_patterns",
        "project_keys",
    }


@pytest.mark.asyncio
async def test_project_provider_teams_does_not_seed_raw_member_identities(
    session_maker,
):
    async with session_maker() as session:
        service = TeamDriftSyncService(session, "org-1")
        await service.project_provider_teams(
            "github",
            [ProviderTeam(id="gh:platform", name="Platform", members=["alice-gh"])],
        )
        identities = list((await session.execute(select(IdentityMapping))).scalars())

    assert identities == []
