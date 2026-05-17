"""Team drift sync service.

Compares freshly discovered teams against the stored ``TeamMapping`` rows
and either auto-applies field changes (``sync_policy == 0``) or flags them
for review (``sync_policy == 1``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import TeamMapping

from .team_mapping import TeamMappingService

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import DiscoveredTeam


class TeamDriftSyncService:
    """Compares discovered teams against stored TeamMappings and flags/merges changes."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def run_drift_sync(
        self,
        provider: str,
        discovered_teams: list[DiscoveredTeam],
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

            changes = self._compute_field_diffs(existing, disc_team)
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
