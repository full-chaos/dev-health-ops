from __future__ import annotations

from typing import Any, Dict, List

from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsAlertModel,
    AtlassianOpsIncidentModel,
    AtlassianOpsScheduleModel,
)


class AtlassianOpsMixin:
    async def insert_atlassian_ops_incidents(self, incidents: List[Any]) -> None:
        if not incidents:
            return

        rows: List[Dict[str, Any]] = []
        for item in incidents:
            rows.append(
                {
                    "id": item.id,
                    "url": item.url,
                    "summary": item.summary,
                    "description": item.description,
                    "status": item.status,
                    "severity": item.severity,
                    "created_at": item.created_at,
                    "provider_id": item.provider_id,
                    "last_synced": item.last_synced,
                }
            )

        await self._upsert_many(
            AtlassianOpsIncidentModel,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "url",
                "summary",
                "description",
                "status",
                "severity",
                "created_at",
                "provider_id",
                "last_synced",
            ],
        )

    async def insert_atlassian_ops_alerts(self, alerts: List[Any]) -> None:
        if not alerts:
            return

        rows: List[Dict[str, Any]] = []
        for item in alerts:
            rows.append(
                {
                    "id": item.id,
                    "status": item.status,
                    "priority": item.priority,
                    "created_at": item.created_at,
                    "acknowledged_at": item.acknowledged_at,
                    "snoozed_at": item.snoozed_at,
                    "closed_at": item.closed_at,
                    "last_synced": item.last_synced,
                }
            )

        await self._upsert_many(
            AtlassianOpsAlertModel,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "status",
                "priority",
                "created_at",
                "acknowledged_at",
                "snoozed_at",
                "closed_at",
                "last_synced",
            ],
        )

    async def insert_atlassian_ops_schedules(self, schedules: List[Any]) -> None:
        if not schedules:
            return

        rows: List[Dict[str, Any]] = []
        for item in schedules:
            rows.append(
                {
                    "id": item.id,
                    "name": item.name,
                    "timezone": item.timezone,
                    "last_synced": item.last_synced,
                }
            )

        await self._upsert_many(
            AtlassianOpsScheduleModel,
            rows,
            conflict_columns=["id"],
            update_columns=[
                "name",
                "timezone",
                "last_synced",
            ],
        )
