"""Team drift sync service.

Compares freshly discovered teams against the stored ``TeamMapping`` rows
and either auto-applies field changes (``sync_policy == 0``) or flags them
for review (``sync_policy == 1``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import TeamMapping

from .team_mapping import TeamMappingService

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import DiscoveredTeam

PROVIDER_MANAGED_FIELDS = ["name", "description", "repo_patterns", "project_keys"]
logger = logging.getLogger(__name__)


@dataclass
class _ProviderDiscoveredTeam:
    provider_type: str
    provider_team_id: str
    name: str
    description: str | None = None
    member_count: int | None = None
    associations: dict[str, Any] = field(default_factory=dict)


class TeamDriftSyncService:
    """Compares discovered teams against stored TeamMappings and flags/merges changes."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def project_provider_teams(
        self,
        provider: str,
        teams_data: list[Any],
        *,
        replace_empty_provider_values: bool = False,
    ) -> dict[str, Any]:
        """Project provider-discovered teams into Postgres TeamMapping rows.

        This entrypoint intentionally creates missing TeamMapping rows for CLI
        provider projections so org-scoped syncs have a control-plane record to
        bridge into ClickHouse. Worker/admin drift discovery keeps its approval
        workflow: missing provider teams are reported as ``new_available`` by
        ``run_drift_sync`` until an admin imports them. Both paths share the
        same field diff and merge implementation for existing mappings.
        """
        normalized_provider = str(provider or "config").lower()
        discovered_teams: list[Any] = []
        for team in teams_data:
            discovered = self._provider_team_to_discovered(normalized_provider, team)
            if not discovered.associations.get("team_id"):
                logger.warning(
                    "Dropping provider team without derivable team_id "
                    "(provider=%s, provider_team_id=%r, name=%r, item=%r)",
                    normalized_provider,
                    getattr(discovered, "provider_team_id", None),
                    getattr(discovered, "name", None),
                    team,
                )
                continue
            discovered_teams.append(discovered)
        if not discovered_teams:
            return {
                "provider": normalized_provider,
                "projected": 0,
                "created": 0,
                "auto_applied": 0,
                "flagged": 0,
                "new_available": 0,
                "provider_removed": 0,
            }

        team_svc = TeamMappingService(self.session, self.org_id)
        existing_teams = await team_svc.list_all(active_only=True)
        provider_lookup: dict[str, Any] = {}
        team_lookup: dict[str, Any] = {}
        for team in existing_teams:
            ed = dict(team.extra_data or {})
            if ed.get("provider_type") == normalized_provider:
                provider_lookup[ed.get("provider_team_id", "")] = team
            team_lookup[str(team.team_id)] = team

        now = datetime.now(timezone.utc)
        created = 0
        for discovered in discovered_teams:
            team_id = str(discovered.associations.get("team_id", "")).strip()
            existing = provider_lookup.get(
                discovered.provider_team_id
            ) or team_lookup.get(team_id)
            extra_data = self._provider_extra_data(
                existing,
                provider=normalized_provider,
                provider_team_id=discovered.provider_team_id,
                now=now,
            )
            if existing is None:
                associations = discovered.associations or {}
                self.session.add(
                    TeamMapping(
                        team_id=team_id,
                        name=discovered.name,
                        org_id=self.org_id,
                        description=discovered.description,
                        repo_patterns=list(associations.get("repo_patterns", [])),
                        project_keys=list(associations.get("project_keys", [])),
                        extra_data=extra_data,
                        managed_fields=list(PROVIDER_MANAGED_FIELDS),
                        is_active=True,
                    )
                )
                created += 1
                continue
            existing.extra_data = extra_data
            existing.is_active = True
            existing.last_drift_sync_at = now

        await self.session.flush()
        result = await self.run_drift_sync(
            normalized_provider,
            discovered_teams,
            replace_empty_provider_values=replace_empty_provider_values,
        )
        result["created"] = created
        result["projected"] = await self._count_projected_team_ids(
            [
                str(team.associations.get("team_id", "")).strip()
                for team in discovered_teams
            ]
        )
        return result

    async def run_drift_sync(
        self,
        provider: str,
        discovered_teams: list[DiscoveredTeam],
        *,
        replace_empty_provider_values: bool = False,
    ) -> dict[str, Any]:
        team_svc = TeamMappingService(self.session, self.org_id)
        existing_teams: list[Any] = await team_svc.list_all(active_only=True)

        provider_lookup: dict[str, Any] = {}
        for team in existing_teams:
            ed: dict[str, Any] = dict(team.extra_data or {})
            if ed.get("provider_type") == provider:
                provider_lookup[ed.get("provider_team_id", "")] = team

        discovered_lookup: dict[str, DiscoveredTeam] = {
            t.provider_team_id: t for t in discovered_teams
        }

        now = datetime.now(timezone.utc)
        auto_applied = 0
        flagged = 0
        new_available = 0
        provider_removed = 0

        for disc_team in discovered_teams:
            existing = provider_lookup.get(disc_team.provider_team_id)
            if existing is None:
                new_available += 1
                continue

            changes = self._compute_field_diffs(
                existing,
                disc_team,
                replace_empty_provider_values=replace_empty_provider_values,
            )
            if not changes:
                existing.last_drift_sync_at = now
                continue

            if existing.sync_policy == 0:
                self._apply_changes(existing, changes, now)
                auto_applied += 1
            elif existing.sync_policy == 1:
                current_flagged = dict(existing.flagged_changes or {})
                current_flagged["pending"] = current_flagged.get("pending", [])
                for change in changes:
                    change["discovered_at"] = now.isoformat()
                    current_flagged["pending"].append(change)
                existing.flagged_changes = current_flagged
                flagged += 1

            existing.last_drift_sync_at = now

        for provider_team_id, existing in provider_lookup.items():
            if provider_team_id not in discovered_lookup:
                current_flagged = dict(existing.flagged_changes or {})
                current_flagged["pending"] = current_flagged.get("pending", [])
                current_flagged["pending"].append(
                    {
                        "change_type": "provider_removed",
                        "discovered_at": now.isoformat(),
                    }
                )
                existing.flagged_changes = current_flagged
                existing.last_drift_sync_at = now
                provider_removed += 1

        await self.session.flush()

        return {
            "provider": provider,
            "auto_applied": auto_applied,
            "flagged": flagged,
            "new_available": new_available,
            "provider_removed": provider_removed,
        }

    def _compute_field_diffs(
        self,
        existing: TeamMapping,
        discovered: DiscoveredTeam,
        *,
        replace_empty_provider_values: bool = False,
    ) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        managed: list[str] = list(existing.managed_fields or [])
        associations = discovered.associations or {}

        field_map = {
            "name": discovered.name,
            "description": discovered.description,
            "repo_patterns": associations.get("repo_patterns", []),
            "project_keys": associations.get("project_keys", []),
        }

        for field_name in managed:
            if field_name not in field_map:
                continue
            new_val = field_map[field_name]
            old_val = getattr(existing, field_name, None)

            if (
                field_name in {"repo_patterns", "project_keys"}
                and not replace_empty_provider_values
                and isinstance(old_val, list)
                and old_val
                and isinstance(new_val, list)
                and not new_val
            ):
                continue

            if isinstance(old_val, list) and isinstance(new_val, list):
                if sorted(old_val) == sorted(new_val):
                    continue
            elif old_val == new_val:
                continue

            changes.append(
                {
                    "change_type": "field_changed",
                    "field": field_name,
                    "old_value": old_val,
                    "new_value": new_val,
                }
            )

        return changes

    async def _count_projected_team_ids(self, team_ids: list[str]) -> int:
        expected_ids = {team_id for team_id in team_ids if team_id}
        if not expected_ids:
            return 0
        result = await self.session.execute(
            select(TeamMapping.team_id).where(
                TeamMapping.org_id == self.org_id,
                TeamMapping.team_id.in_(expected_ids),
            )
        )
        return len(set(result.scalars()))

    def _provider_team_to_discovered(self, provider: str, team: Any) -> Any:
        input_associations = self._team_value(team, "associations", {}) or {}
        associations = (
            dict(input_associations) if isinstance(input_associations, dict) else {}
        )
        team_id = str(
            self._team_value(
                team,
                "id",
                associations.get(
                    "team_id", self._team_value(team, "provider_team_id", "")
                ),
            )
            or ""
        ).strip()
        provider_team_id = str(
            self._team_value(team, "provider_team_id", "")
            or self._provider_team_id(team_id, provider)
        ).strip()
        team_name = str(self._team_value(team, "name", team_id) or team_id)
        associations.setdefault("team_id", team_id)
        associations.setdefault(
            "repo_patterns", self._team_string_list(team, "repo_patterns")
        )
        associations.setdefault(
            "project_keys", self._project_keys_for_team(team_id, provider, team)
        )
        members = self._team_value(team, "members", []) or []
        member_count = self._team_value(team, "member_count", None)
        if member_count is None:
            member_count = len(members) if isinstance(members, list) else None
        return _ProviderDiscoveredTeam(
            provider_type=provider,
            provider_team_id=provider_team_id,
            name=team_name,
            description=self._team_value(team, "description", None),
            member_count=member_count,
            associations=associations,
        )

    def _provider_extra_data(
        self,
        existing: Any | None,
        *,
        provider: str,
        provider_team_id: str,
        now: datetime,
    ) -> dict[str, Any]:
        extra_data = dict(getattr(existing, "extra_data", {}) or {})
        extra_data.update(
            {
                "provider_type": provider,
                "provider_team_id": provider_team_id,
                "last_discovered_at": now.isoformat(),
                "sync_source": "provider-projection",
            }
        )
        return extra_data

    @staticmethod
    def _team_value(team: Any, field: str, default: Any = None) -> Any:
        if isinstance(team, dict):
            return team.get(field, default)
        return getattr(team, field, default)

    def _team_string_list(self, team: Any, field: str) -> list[str]:
        value = self._team_value(team, field, [])
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if item]

    def _project_keys_for_team(
        self, team_id: str, provider: str, team: Any
    ) -> list[str]:
        configured = self._team_string_list(team, "project_keys")
        if configured:
            return configured
        if provider == "jira":
            return [team_id]
        if provider == "linear" and team_id.startswith("linear:"):
            return [team_id.removeprefix("linear:")]
        return []

    @staticmethod
    def _provider_team_id(team_id: str, provider: str) -> str:
        if provider == "github" and team_id.startswith("gh:"):
            return team_id.removeprefix("gh:")
        if provider == "gitlab" and team_id.startswith("gl:"):
            return team_id.removeprefix("gl:")
        if provider == "linear" and team_id.startswith("linear:"):
            return team_id.removeprefix("linear:")
        if provider == "ms-teams" and team_id.startswith("ms-teams:"):
            return team_id.removeprefix("ms-teams:")
        return team_id

    def _apply_changes(
        self,
        existing: TeamMapping,
        changes: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        for change in changes:
            field = change.get("field")
            if field and hasattr(existing, field):
                setattr(existing, field, change["new_value"])
        setattr(existing, "last_drift_sync_at", now)
        ed = dict(existing.extra_data or {})
        ed["last_discovered_at"] = now.isoformat()
        setattr(existing, "extra_data", ed)

    async def approve_changes(
        self,
        team_id: str,
        change_indices: list[int] | None = None,
    ) -> dict[str, Any]:
        team_svc = TeamMappingService(self.session, self.org_id)
        team: Any | None = await team_svc.get(team_id)
        if team is None:
            return {"error": "Team not found"}

        flagged: dict[str, Any] = dict(team.flagged_changes or {})
        pending: list[dict[str, Any]] = list(flagged.get("pending", []))

        if not pending:
            return {"approved": 0}

        now = datetime.now(timezone.utc)
        to_approve = (
            pending
            if change_indices is None
            else [pending[i] for i in change_indices if i < len(pending)]
        )

        applied = 0
        for change in to_approve:
            ct = change.get("change_type")
            if ct == "field_changed":
                field = change.get("field")
                if field and hasattr(team, field):
                    setattr(team, field, change["new_value"])
                    applied += 1
            elif ct == "new_team_available":
                pass
            elif ct == "provider_removed":
                applied += 1

        if change_indices is None:
            flagged["pending"] = []
        else:
            flagged["pending"] = [
                p for i, p in enumerate(pending) if i not in change_indices
            ]

        team.flagged_changes = flagged if flagged.get("pending") else None
        team.last_drift_sync_at = now
        await self.session.flush()

        return {"approved": applied}

    async def dismiss_changes(
        self,
        team_id: str,
        change_indices: list[int] | None = None,
    ) -> dict[str, Any]:
        team_svc = TeamMappingService(self.session, self.org_id)
        team: Any | None = await team_svc.get(team_id)
        if team is None:
            return {"error": "Team not found"}

        flagged: dict[str, Any] = dict(team.flagged_changes or {})
        pending: list[dict[str, Any]] = list(flagged.get("pending", []))

        if not pending:
            return {"dismissed": 0}

        count = (
            len(pending)
            if change_indices is None
            else len([i for i in change_indices if i < len(pending)])
        )

        if change_indices is None:
            flagged["pending"] = []
        else:
            flagged["pending"] = [
                p for i, p in enumerate(pending) if i not in change_indices
            ]

        team.flagged_changes = flagged if flagged.get("pending") else None
        await self.session.flush()

        return {"dismissed": count}

    async def get_all_pending_changes(self) -> list[dict[str, Any]]:
        team_svc = TeamMappingService(self.session, self.org_id)
        teams: list[Any] = await team_svc.list_all(active_only=True)

        all_changes: list[dict[str, Any]] = []
        for team in teams:
            flagged: dict[str, Any] = dict(team.flagged_changes or {})
            pending: list[dict[str, Any]] = list(flagged.get("pending", []))
            for change in pending:
                all_changes.append(
                    {
                        "team_id": team.team_id,
                        "team_name": team.name,
                        **change,
                    }
                )

        return all_changes
