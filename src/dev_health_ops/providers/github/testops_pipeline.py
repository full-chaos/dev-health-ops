from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote
from uuid import UUID

from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow
from dev_health_ops.providers._base import BasePipelineAdapter, PipelineSyncBatch
from dev_health_ops.providers._http import GITHUB_DIAGNOSTIC_HEADER_NAMES
from dev_health_ops.providers.ci_acceptance import project_checks
from dev_health_ops.providers.github.budget import GITHUB_USAGE_RESOLVER


class GitHubActionsAdapter(BasePipelineAdapter):
    provider = "github_actions"
    token_env_var = "GITHUB_TOKEN"
    usage_resolver = GITHUB_USAGE_RESOLVER
    diagnostic_header_names = GITHUB_DIAGNOSTIC_HEADER_NAMES

    @property
    def default_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    @staticmethod
    def _map_pipeline_status(status: str | None, conclusion: str | None) -> str | None:
        if conclusion == "success":
            return "success"
        if conclusion in {"failure", "startup_failure", "action_required"}:
            return "failure"
        if conclusion in {"cancelled", "neutral"}:
            return "cancelled"
        if conclusion == "timed_out":
            return "timeout"
        if status in {"in_progress", "requested", "waiting", "pending"}:
            return "running"
        if status == "queued":
            return "queued"
        return conclusion or status

    @staticmethod
    def _map_job_status(status: str | None, conclusion: str | None) -> str | None:
        if conclusion == "skipped":
            return "skipped"
        return GitHubActionsAdapter._map_pipeline_status(status, conclusion)

    @staticmethod
    def _runner_type(job: dict[str, Any]) -> str | None:
        labels = {str(label).lower() for label in job.get("labels") or []}
        if "self-hosted" in labels:
            return "self-hosted"
        if labels:
            return "hosted"
        return None

    async def _required_contexts(
        self, *, owner: str, repo: str, branch: str
    ) -> tuple[set[str] | None, str]:
        """Read branch policy without converting denial/absence into optional."""

        client = await self._get_client()
        path = (
            f"/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/branches/"
            f"{quote(branch, safe='')}/protection/required_status_checks"
        )
        response = await client.get(path)
        self._record_response_usage(
            response,
            operation="tests:GET /repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks",
        )
        if response.status_code != 200:
            return None, f"github.branch_protection.http_{response.status_code}"
        payload = response.json()
        if not isinstance(payload, dict):
            return None, "github.branch_protection.invalid_payload"
        names = {str(value) for value in payload.get("contexts") or [] if value}
        for item in payload.get("checks") or []:
            if isinstance(item, dict) and item.get("context"):
                names.add(str(item["context"]))
        return names, "github.branch_protection.required_status_checks"

    async def fetch_pipeline_data(  # type: ignore[override]
        self,
        *,
        owner: str,
        repo: str,
        repo_id: UUID,
        org_id: str | None = None,
        since_date: datetime | None = None,
        until_date: datetime | None = None,
        last_synced: datetime | None = None,
        **_: Any,
    ) -> PipelineSyncBatch:
        effective_since = since_date or last_synced
        params: dict[str, Any] = {}
        if effective_since or until_date:
            start = (effective_since.isoformat() if effective_since else "*").replace(
                "+00:00", "Z"
            )
            end = (until_date.isoformat() if until_date else "*").replace("+00:00", "Z")
            params["created"] = f"{start}..{end}"

        workflow_runs = await self._paginate(
            f"/repos/{owner}/{repo}/actions/runs",
            params=params,
            data_key="workflow_runs",
            operation=f"tests:GET /repos/{owner}/{repo}/actions/runs",
        )

        pipeline_rows: list[PipelineRunExtendedRow] = []
        job_rows: list[JobRunRow] = []
        acceptance_rows = []
        cursor_candidates: list[datetime] = []
        required_by_branch: dict[str, tuple[set[str] | None, str]] = {}

        for workflow_run in workflow_runs:
            created_at = self.parse_datetime(workflow_run.get("created_at"))
            started_at = (
                self.parse_datetime(workflow_run.get("run_started_at")) or created_at
            )
            if started_at is None:
                continue
            if effective_since and started_at < effective_since:
                continue
            if until_date and started_at > until_date:
                continue

            finished_at = self.parse_datetime(workflow_run.get("updated_at"))
            status = self._map_pipeline_status(
                workflow_run.get("status"), workflow_run.get("conclusion")
            )
            pull_requests = workflow_run.get("pull_requests") or []
            pr_number = None
            if pull_requests and isinstance(pull_requests[0], dict):
                pr_number = pull_requests[0].get("number")

            pipeline_row: PipelineRunExtendedRow = {
                "repo_id": repo_id,
                "run_id": str(workflow_run.get("id")),
                "pipeline_name": workflow_run.get("name"),
                "provider": self.provider,
                "status": status,
                "queued_at": created_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": self.seconds_between(started_at, finished_at),
                "queue_seconds": self.seconds_between(created_at, started_at),
                "retry_count": max(0, int(workflow_run.get("run_attempt") or 1) - 1),
                "cancel_reason": None,
                "trigger_source": self.coerce_trigger_source(workflow_run.get("event")),
                "commit_hash": workflow_run.get("head_sha"),
                "branch": workflow_run.get("head_branch"),
                "pr_number": pr_number,
                "team_id": None,
                "service_id": None,
            }
            if org_id:
                pipeline_row["org_id"] = org_id
            pipeline_rows.append(pipeline_row)

            if finished_at is not None:
                cursor_candidates.append(finished_at)
            else:
                cursor_candidates.append(started_at)

            jobs = await self._paginate(
                f"/repos/{owner}/{repo}/actions/runs/{workflow_run.get('id')}/jobs",
                data_key="jobs",
                operation=f"tests:GET /repos/{owner}/{repo}/actions/runs/{{id}}/jobs",
            )
            projected_jobs: list[dict[str, Any]] = []
            for job in jobs:
                job_started_at = self.parse_datetime(job.get("started_at"))
                job_finished_at = self.parse_datetime(job.get("completed_at"))
                job_row: JobRunRow = {
                    "repo_id": repo_id,
                    "run_id": str(workflow_run.get("id")),
                    "job_id": str(job.get("id")),
                    "job_name": str(job.get("name") or "job"),
                    "stage": None,
                    "status": self._map_job_status(
                        job.get("status"), job.get("conclusion")
                    ),
                    "started_at": job_started_at,
                    "finished_at": job_finished_at,
                    "duration_seconds": self.seconds_between(
                        job_started_at, job_finished_at
                    ),
                    "runner_type": self._runner_type(job),
                    "retry_attempt": max(
                        0, int(workflow_run.get("run_attempt") or 1) - 1
                    ),
                }
                if org_id:
                    job_row["org_id"] = org_id
                job_rows.append(job_row)
                projected_jobs.append(
                    {
                        "name": job_row["job_name"],
                        "status": job_row["status"],
                    }
                )

            target_branch = None
            if pull_requests and isinstance(pull_requests[0], dict):
                base = pull_requests[0].get("base")
                if isinstance(base, dict) and base.get("ref"):
                    target_branch = str(base["ref"])
            required_names: set[str] | None = None
            provenance = "github.branch_protection.target_branch_unavailable"
            if target_branch:
                if target_branch not in required_by_branch:
                    required_by_branch[target_branch] = await self._required_contexts(
                        owner=owner, repo=repo, branch=target_branch
                    )
                required_names, provenance = required_by_branch[target_branch]
            acceptance_rows.extend(
                project_checks(
                    repo_id=repo_id,
                    org_id=org_id,
                    run_id=str(workflow_run.get("id")),
                    provider=self.provider,
                    observed_at=finished_at or started_at,
                    jobs=projected_jobs,
                    required_names=required_names,
                    provenance=provenance,
                    target_branch=target_branch,
                    pr_number=int(pr_number) if pr_number is not None else None,
                    source_url=str(workflow_run.get("html_url"))
                    if workflow_run.get("html_url")
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
