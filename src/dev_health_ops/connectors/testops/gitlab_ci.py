from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote_plus
from uuid import UUID

from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow

from .base import BasePipelineAdapter, PipelineSyncBatch


class GitLabCIAdapter(BasePipelineAdapter):
    provider = "gitlab_ci"
    token_env_var = "GITLAB_TOKEN"

    @property
    def default_headers(self) -> dict[str, str]:
        return {"PRIVATE-TOKEN": self.token}

    @staticmethod
    def _encode_project(project_id: int | str) -> str:
        return quote_plus(str(project_id), safe="")

    @staticmethod
    def _map_pipeline_status(status: str | None) -> str | None:
        mapping = {
            "success": "success",
            "failed": "failure",
            "canceled": "cancelled",
            "cancelled": "cancelled",
            "manual": "queued",
            "scheduled": "queued",
            "pending": "queued",
            "created": "queued",
            "waiting_for_resource": "queued",
            "preparing": "queued",
            "running": "running",
        }
        if status == "skipped":
            return "cancelled"
        return mapping.get(status or "", status)

    @staticmethod
    def _map_job_status(status: str | None) -> str | None:
        mapping = {
            "success": "success",
            "failed": "failure",
            "canceled": "cancelled",
            "cancelled": "cancelled",
            "manual": "skipped",
            "skipped": "skipped",
            "pending": "running",
            "created": "running",
            "waiting_for_resource": "running",
            "preparing": "running",
            "running": "running",
        }
        return mapping.get(status or "", status)

    @staticmethod
    def _runner_type(job: dict[str, Any]) -> str | None:
        runner = job.get("runner")
        if isinstance(runner, dict):
            runner_type = runner.get("runner_type")
            if runner_type:
                return str(runner_type)
        tag_list = {str(tag).lower() for tag in job.get("tag_list") or []}
        if "self-hosted" in tag_list:
            return "self-hosted"
        if tag_list:
            return "hosted"
        return None

    async def fetch_pipeline_data(
        self,
        *,
        project_id: int | str,
        repo_id: UUID,
        org_id: str | None = None,
        since_date: datetime | None = None,
        until_date: datetime | None = None,
        last_synced: datetime | None = None,
        **_: Any,
    ) -> PipelineSyncBatch:
        effective_since = since_date or last_synced
        params: dict[str, Any] = {"order_by": "updated_at", "sort": "desc"}
        if effective_since is not None:
            params["updated_after"] = effective_since.isoformat()
        if until_date is not None:
            params["updated_before"] = until_date.isoformat()

        encoded_project = self._encode_project(project_id)
        pipelines = await self._paginate(
            f"/projects/{encoded_project}/pipelines",
            params=params,
        )

        pipeline_rows: list[PipelineRunExtendedRow] = []
        job_rows: list[JobRunRow] = []
        cursor_candidates: list[datetime] = []

        for pipeline in pipelines:
            created_at = self.parse_datetime(pipeline.get("created_at"))
            started_at = self.parse_datetime(pipeline.get("started_at")) or created_at
            if started_at is None:
                continue
            if effective_since and started_at < effective_since:
                continue
            if until_date and started_at > until_date:
                continue

            finished_at = self.parse_datetime(pipeline.get("finished_at"))
            status = self._map_pipeline_status(pipeline.get("status"))
            pipeline_row: PipelineRunExtendedRow = {
                "repo_id": repo_id,
                "run_id": str(pipeline.get("id")),
                "pipeline_name": pipeline.get("name") or pipeline.get("ref"),
                "provider": self.provider,
                "status": status,
                "queued_at": created_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": self.seconds_between(started_at, finished_at),
                "queue_seconds": self.seconds_between(created_at, started_at),
                "retry_count": 0,
                "cancel_reason": None,
                "trigger_source": self.coerce_trigger_source(pipeline.get("source")),
                "commit_hash": pipeline.get("sha"),
                "branch": pipeline.get("ref"),
                "pr_number": None,
                "team_id": None,
                "service_id": None,
            }
            if org_id:
                pipeline_row["org_id"] = org_id
            pipeline_rows.append(pipeline_row)

            cursor_candidates.append(finished_at or started_at)

            jobs = await self._paginate(
                f"/projects/{encoded_project}/pipelines/{pipeline.get('id')}/jobs",
                params={"include_retried": True},
            )
            for job in jobs:
                job_started_at = self.parse_datetime(job.get("started_at"))
                job_finished_at = self.parse_datetime(job.get("finished_at"))
                job_row: JobRunRow = {
                    "repo_id": repo_id,
                    "run_id": str(pipeline.get("id")),
                    "job_id": str(job.get("id")),
                    "job_name": str(job.get("name") or "job"),
                    "stage": job.get("stage"),
                    "status": self._map_job_status(job.get("status")),
                    "started_at": job_started_at,
                    "finished_at": job_finished_at,
                    "duration_seconds": self.seconds_between(
                        job_started_at, job_finished_at
                    ),
                    "runner_type": self._runner_type(job),
                    "retry_attempt": 0,
                }
                if isinstance(job.get("retried"), bool) and job.get("retried"):
                    job_row["retry_attempt"] = 1
                if org_id:
                    job_row["org_id"] = org_id
                job_rows.append(job_row)

        cursor = max(cursor_candidates) if cursor_candidates else effective_since
        return PipelineSyncBatch(
            pipeline_runs=pipeline_rows,
            job_runs=job_rows,
            last_synced_cursor=cursor,
        )
