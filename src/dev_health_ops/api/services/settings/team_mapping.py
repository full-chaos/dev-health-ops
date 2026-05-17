"""Team mapping service.

CRUD for the ``TeamMapping`` model that ties a team_id to provider data
(repo patterns, project keys, free-form extra_data).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import TeamMapping


class TeamMappingService:
    """Service for managing team mappings."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def get(self, team_id: str) -> TeamMapping | None:
        """Get a team mapping by team ID."""
        stmt = select(TeamMapping).where(
            TeamMapping.org_id == self.org_id,
            TeamMapping.team_id == team_id,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_or_update(
        self,
        team_id: str,
        name: str,
        description: str | None = None,
        repo_patterns: list[str] | None = None,
        project_keys: list[str] | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> TeamMapping:
        """Create or update a team mapping."""
        mapping: Any | None = await self.get(team_id)

        if mapping is None:
            mapping = TeamMapping(
                team_id=team_id,
                name=name,
                org_id=self.org_id,
                description=description,
                repo_patterns=repo_patterns or [],
                project_keys=project_keys or [],
                extra_data=extra_data or {},
            )
            self.session.add(mapping)
        else:
            mapping.name = name
            if description is not None:
                mapping.description = description
            if repo_patterns is not None:
                mapping.repo_patterns = repo_patterns
            if project_keys is not None:
                mapping.project_keys = project_keys
            if extra_data is not None:
                mapping.extra_data = extra_data

        await self.session.flush()
        return mapping

    async def list_all(self, active_only: bool = True) -> list[TeamMapping]:
        """List all team mappings."""
        stmt = select(TeamMapping).where(
            TeamMapping.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(TeamMapping.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete(self, team_id: str) -> bool:
        """Delete a team mapping."""
        mapping = await self.get(team_id)
        if mapping is None:
            return False

        await self.session.delete(mapping)
        await self.session.flush()
        return True
