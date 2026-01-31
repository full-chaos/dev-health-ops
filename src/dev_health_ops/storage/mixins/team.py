from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import select


class TeamMixin:
    async def insert_teams(self, teams: List[Any]) -> None:
        from dev_health_ops.models.teams import Team

        if not teams:
            return

        rows: List[Dict[str, Any]] = []
        for item in teams:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(
                    {
                        "id": item.id,
                        "team_uuid": item.team_uuid,
                        "name": item.name,
                        "description": item.description,
                        "members": item.members,
                        "updated_at": item.updated_at,
                    }
                )

        await self._upsert_many(
            Team,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "team_uuid",
                "name",
                "description",
                "members",
                "updated_at",
            ],
        )

    async def insert_jira_project_ops_team_links(self, links: List[Any]) -> None:
        from dev_health_ops.models.teams import JiraProjectOpsTeamLink

        if not links:
            return

        rows: List[Dict[str, Any]] = []
        for item in links:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(
                    {
                        "project_key": item.project_key,
                        "ops_team_id": item.ops_team_id,
                        "project_name": item.project_name,
                        "ops_team_name": item.ops_team_name,
                        "updated_at": item.updated_at,
                    }
                )

        await self._upsert_many(
            JiraProjectOpsTeamLink,
            rows,
            conflict_columns=["project_key", "ops_team_id"],
            update_columns=[
                "project_name",
                "ops_team_name",
                "updated_at",
            ],
        )

    async def get_all_teams(self) -> List[Any]:
        from dev_health_ops.models.teams import Team

        assert self.session is not None
        result = await self.session.execute(select(Team))
        return list(result.scalars().all())

    async def get_jira_project_ops_team_links(self) -> List[Any]:
        from dev_health_ops.models.teams import JiraProjectOpsTeamLink

        assert self.session is not None
        result = await self.session.execute(select(JiraProjectOpsTeamLink))
        return list(result.scalars().all())
