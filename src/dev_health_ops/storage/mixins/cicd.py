from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dev_health_ops.models.git import CiPipelineRun, Deployment, Incident, SecurityAlert


class CicdMixin:
    async def insert_ci_pipeline_runs(self, runs: list[CiPipelineRun]) -> None:
        if not runs:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in runs:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "run_id": item.get("run_id"),
                    "status": item.get("status"),
                    "queued_at": item.get("queued_at"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "run_id": item.run_id,
                    "status": item.status,
                    "queued_at": getattr(item, "queued_at", None),
                    "started_at": item.started_at,
                    "finished_at": getattr(item, "finished_at", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            CiPipelineRun,
            rows,
            conflict_columns=["repo_id", "run_id"],
            update_columns=[
                "status",
                "queued_at",
                "started_at",
                "finished_at",
                "last_synced",
            ],
        )

    async def insert_deployments(self, deployments: list[Deployment]) -> None:
        if not deployments:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in deployments:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "deployment_id": item.get("deployment_id"),
                    "status": item.get("status"),
                    "environment": item.get("environment"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "deployed_at": item.get("deployed_at"),
                    "merged_at": item.get("merged_at"),
                    "pull_request_number": item.get("pull_request_number"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "deployment_id": item.deployment_id,
                    "status": item.status,
                    "environment": getattr(item, "environment", None),
                    "started_at": getattr(item, "started_at", None),
                    "finished_at": getattr(item, "finished_at", None),
                    "deployed_at": getattr(item, "deployed_at", None),
                    "merged_at": getattr(item, "merged_at", None),
                    "pull_request_number": getattr(item, "pull_request_number", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            Deployment,
            rows,
            conflict_columns=["repo_id", "deployment_id"],
            update_columns=[
                "status",
                "environment",
                "started_at",
                "finished_at",
                "deployed_at",
                "merged_at",
                "pull_request_number",
                "last_synced",
            ],
        )

    async def insert_incidents(self, incidents: list[Incident]) -> None:
        if not incidents:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in incidents:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "incident_id": item.get("incident_id"),
                    "status": item.get("status"),
                    "started_at": item.get("started_at"),
                    "resolved_at": item.get("resolved_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "incident_id": item.incident_id,
                    "status": item.status,
                    "started_at": item.started_at,
                    "resolved_at": getattr(item, "resolved_at", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            Incident,
            rows,
            conflict_columns=["repo_id", "incident_id"],
            update_columns=[
                "status",
                "started_at",
                "resolved_at",
                "last_synced",
            ],
        )

    async def insert_security_alerts(self, alerts: list[SecurityAlert]) -> None:
        if not alerts:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in alerts:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "alert_id": item.get("alert_id"),
                    "source": item.get("source"),
                    "severity": item.get("severity"),
                    "state": item.get("state"),
                    "package_name": item.get("package_name"),
                    "cve_id": item.get("cve_id"),
                    "url": item.get("url"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "created_at": item.get("created_at"),
                    "fixed_at": item.get("fixed_at"),
                    "dismissed_at": item.get("dismissed_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": item.repo_id,
                    "alert_id": item.alert_id,
                    "source": item.source,
                    "severity": getattr(item, "severity", None),
                    "state": getattr(item, "state", None),
                    "package_name": getattr(item, "package_name", None),
                    "cve_id": getattr(item, "cve_id", None),
                    "url": getattr(item, "url", None),
                    "title": getattr(item, "title", None),
                    "description": getattr(item, "description", None),
                    "created_at": item.created_at,
                    "fixed_at": getattr(item, "fixed_at", None),
                    "dismissed_at": getattr(item, "dismissed_at", None),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            SecurityAlert,
            rows,
            conflict_columns=["repo_id", "alert_id"],
            update_columns=[
                "source",
                "severity",
                "state",
                "package_name",
                "cve_id",
                "url",
                "title",
                "description",
                "created_at",
                "fixed_at",
                "dismissed_at",
                "last_synced",
            ],
        )
