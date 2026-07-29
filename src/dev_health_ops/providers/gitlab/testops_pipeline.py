from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote_plus
from uuid import UUID

from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow
from dev_health_ops.providers._base import BasePipelineAdapter, PipelineSyncBatch
from dev_health_ops.providers._http import GITLAB_DIAGNOSTIC_HEADER_NAMES
from dev_health_ops.providers.ci_acceptance import project_checks
from dev_health_ops.providers.gitlab.budget import GITLAB_USAGE_RESOLVER


class GitLabCIAdapter(BasePipelineAdapter):
    provider = "gitlab_ci"
    token_env_var = "GITLAB_TOKEN"
    usage_resolver = GITLAB_USAGE_RESOLVER
    diagnostic_header_names = GITLAB_DIAGNOSTIC_HEADER_NAMES

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

    async def _pipeline_requirement(
        self, encoded_project: str
    ) -> tuple[set[str] | None, str]:
        """Read the project merge policy; denial remains unknown."""

        client = await self._get_client()
        response = await client.get(f"/projects/{encoded_project}")
        self._record_response_usage(
            response, operation="tests:GET /projects/{project_id}"
        )
        if response.status_code != 200:
            return None, f"gitlab.project_merge_policy.http_{response.status_code}"
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or "only_allow_merge_if_pipeline_succeeds" not in payload
        ):
            return None, "gitlab.project_merge_policy.missing_field"
        required = bool(payload["only_allow_merge_if_pipeline_succeeds"])
        return ({"pipeline"} if required else set()), "gitlab.project_merge_policy"

    async def _merge_request_iid(
        self, *, encoded_project: str, pipeline: dict[str, Any]
    ) -> int | None:
        """Resolve an MR pipeline to its repo-local IID without parsing refs."""

        if pipeline.get("source") != "merge_request_event":
            return None
        embedded = pipeline.get("merge_request")
        embedded_iid = embedded.get("iid") if isinstance(embedded, dict) else None
        if embedded_iid is not None:
            try:
                return int(embedded_iid)
            except (TypeError, ValueError):
                return None
        sha = pipeline.get("sha")
        if not sha:
            return None
        client = await self._get_client()
        encoded_sha = quote_plus(str(sha), safe="")
        response = await client.get(
            f"/projects/{encoded_project}/repository/commits/{encoded_sha}/merge_requests"
        )
        self._record_response_usage(
            response,
            operation="tests:GET /projects/{project_id}/repository/commits/{sha}/merge_requests",
        )
        if response.status_code != 200:
            return None
        payload = response.json()
        if not isinstance(payload, list):
            return None
        exact: list[dict[str, Any]] = []
        candidates = [item for item in payload if isinstance(item, dict)]
        for item in candidates:
            diff_refs = item.get("diff_refs")
            if item.get("sha") == sha or (
                isinstance(diff_refs, dict) and diff_refs.get("head_sha") == sha
            ):
                exact.append(item)
        selected = exact or candidates
        iids: set[int] = set()
        for item in selected:
            try:
                iids.add(int(item["iid"]))
            except (KeyError, TypeError, ValueError):
                continue
        return next(iter(iids)) if len(iids) == 1 else None

    async def fetch_pipeline_data(  # type: ignore[override]
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
        required_names, requirement_provenance = await self._pipeline_requirement(
            encoded_project
        )
        pipelines = await self._paginate(
            f"/projects/{encoded_project}/pipelines",
            params=params,
            operation=f"tests:GET /projects/{project_id}/pipelines",
        )

        pipeline_rows: list[PipelineRunExtendedRow] = []
        job_rows: list[JobRunRow] = []
        acceptance_rows = []
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
            merge_request_iid = await self._merge_request_iid(
                encoded_project=encoded_project, pipeline=pipeline
            )
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
                "pr_number": merge_request_iid,
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
                operation=f"tests:GET /projects/{project_id}/pipelines/{{id}}/jobs",
            )
            projected_jobs: list[dict[str, Any]] = [
                {"name": "pipeline", "status": pipeline.get("status")}
            ]
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
                projected_jobs.append(
                    {"name": job_row["job_name"], "status": job_row["status"]}
                )

            acceptance_rows.extend(
                project_checks(
                    repo_id=repo_id,
                    org_id=org_id,
                    run_id=str(pipeline.get("id")),
                    provider=self.provider,
                    observed_at=finished_at or started_at,
                    jobs=projected_jobs,
                    required_names=required_names,
                    provenance=requirement_provenance,
                    target_branch=str(pipeline.get("ref"))
                    if pipeline.get("ref")
                    else None,
                    pr_number=merge_request_iid,
                    source_url=str(pipeline.get("web_url"))
                    if pipeline.get("web_url")
                    else None,
                )
            )

        cursor = max(cursor_candidates) if cursor_candidates else effective_since
        return PipelineSyncBatch(
            pipeline_runs=pipeline_rows,
            job_runs=job_rows,
            acceptance_checks=acceptance_rows,
            last_synced_cursor=cursor,
        )
