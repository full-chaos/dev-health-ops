from __future__ import annotations

import importlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import httpx
import pytest

from dev_health_ops.metrics.sinks.ingestion import IngestionSink

_github_actions_module = cast(
    Any, importlib.import_module("dev_health_ops.connectors.testops.github_actions")
)
_gitlab_ci_module = cast(
    Any, importlib.import_module("dev_health_ops.connectors.testops.gitlab_ci")
)
_testops_base_module = cast(
    Any, importlib.import_module("dev_health_ops.connectors.testops.base")
)
_testops_processor_module = cast(
    Any, importlib.import_module("dev_health_ops.processors.testops_pipeline")
)

GitHubActionsAdapter = _github_actions_module.GitHubActionsAdapter
GitLabCIAdapter = _gitlab_ci_module.GitLabCIAdapter
PipelineSyncBatch = _testops_base_module.PipelineSyncBatch
TestOpsPipelineProcessor = _testops_processor_module.TestOpsPipelineProcessor
PipelineProcessor = TestOpsPipelineProcessor
PipelineProcessor.__test__ = False

if TYPE_CHECKING:
    from dev_health_ops.connectors.testops.base import (
        PipelineSyncBatch as PipelineSyncBatchType,
    )
    from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_github_actions_adapter_maps_runs_and_jobs() -> None:
    repo_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/repos/acme/api/actions/runs":
            return httpx.Response(
                200,
                json={
                    "workflow_runs": [
                        {
                            "id": 101,
                            "name": "ci",
                            "status": "completed",
                            "conclusion": "success",
                            "created_at": "2026-04-01T10:00:00Z",
                            "run_started_at": "2026-04-01T10:02:00Z",
                            "updated_at": "2026-04-01T10:07:00Z",
                            "run_attempt": 2,
                            "event": "pull_request",
                            "head_sha": "abc123",
                            "head_branch": "feature/testops",
                            "pull_requests": [{"number": 17}],
                        }
                    ]
                },
            )
        if request.url.path == "/repos/acme/api/actions/runs/101/jobs":
            return httpx.Response(
                200,
                json={
                    "jobs": [
                        {
                            "id": 501,
                            "name": "pytest",
                            "status": "completed",
                            "conclusion": "success",
                            "started_at": "2026-04-01T10:02:30Z",
                            "completed_at": "2026-04-01T10:05:00Z",
                            "labels": ["ubuntu-latest"],
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    adapter = GitHubActionsAdapter(
        base_url="https://api.github.test",
        token="token",
        transport=httpx.MockTransport(handler),
    )
    batch = await adapter.fetch_pipeline_data(
        owner="acme",
        repo="api",
        repo_id=repo_id,
        org_id="org-1",
        since_date=_dt("2026-04-01T00:00:00Z"),
    )
    await adapter.close()

    assert len(batch.pipeline_runs) == 1
    assert len(batch.job_runs) == 1
    pipeline = batch.pipeline_runs[0]
    job = batch.job_runs[0]
    assert pipeline["status"] == "success"
    assert pipeline["trigger_source"] == "pr"
    assert pipeline["queue_seconds"] == 120.0
    assert pipeline["duration_seconds"] == 300.0
    assert pipeline["retry_count"] == 1
    assert pipeline["pr_number"] == 17
    assert pipeline["org_id"] == "org-1"
    assert job["status"] == "success"
    assert job["duration_seconds"] == 150.0
    assert job["runner_type"] == "hosted"
    assert batch.last_synced_cursor == _dt("2026-04-01T10:07:00Z")


@pytest.mark.asyncio
async def test_gitlab_ci_adapter_handles_pagination_and_incremental_sync() -> None:
    repo_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/projects/group%2Fapi/pipelines?" in url:
            page = request.url.params.get("page")
            if page == "1":
                return httpx.Response(
                    200,
                    headers={"x-next-page": "2"},
                    json=[
                        {
                            "id": 301,
                            "name": "pipeline-main",
                            "status": "failed",
                            "created_at": "2026-04-02T09:00:00Z",
                            "started_at": "2026-04-02T09:01:00Z",
                            "finished_at": "2026-04-02T09:03:00Z",
                            "source": "schedule",
                            "sha": "def456",
                            "ref": "main",
                        }
                    ],
                )
            if page == "2":
                return httpx.Response(200, json=[])
        if "/projects/group%2Fapi/pipelines/301/jobs?" in url:
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 401,
                        "name": "unit",
                        "stage": "test",
                        "status": "failed",
                        "started_at": "2026-04-02T09:01:30Z",
                        "finished_at": "2026-04-02T09:02:30Z",
                        "runner": {"runner_type": "self-hosted"},
                        "retried": True,
                    }
                ],
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    adapter = GitLabCIAdapter(
        base_url="https://gitlab.example/api/v4",
        token="token",
        transport=httpx.MockTransport(handler),
    )
    batch = await adapter.fetch_pipeline_data(
        project_id="group/api",
        repo_id=repo_id,
        last_synced=_dt("2026-04-01T00:00:00Z"),
    )
    await adapter.close()

    assert len(batch.pipeline_runs) == 1
    assert len(batch.job_runs) == 1
    pipeline = batch.pipeline_runs[0]
    job = batch.job_runs[0]
    assert pipeline["status"] == "failure"
    assert pipeline["trigger_source"] == "schedule"
    assert pipeline["duration_seconds"] == 120.0
    assert pipeline["queue_seconds"] == 60.0
    assert job["status"] == "failure"
    assert job["retry_attempt"] == 1
    assert job["runner_type"] == "self-hosted"
    assert batch.last_synced_cursor == _dt("2026-04-02T09:03:00Z")


class _FakeStore:
    def __init__(self) -> None:
        self.pipeline_runs: list[PipelineRunExtendedRow] = []
        self.job_runs: list[JobRunRow] = []

    async def insert_testops_pipeline_runs(self, runs):
        self.pipeline_runs.extend(runs)

    async def insert_testops_job_runs(self, jobs):
        self.job_runs.extend(jobs)


class _FakeAdapter:
    def __init__(self, batch: PipelineSyncBatchType) -> None:
        self.batch = batch
        self.received_kwargs: dict[str, object] | None = None

    async def fetch_pipeline_data(self, **kwargs):
        self.received_kwargs = kwargs
        return self.batch


@pytest.mark.asyncio
async def test_pipeline_processor_uses_backfill_or_incremental_cursor() -> None:
    store = _FakeStore()
    sink = IngestionSink(store)
    processor = TestOpsPipelineProcessor(sink)
    repo_id = uuid4()
    batch = PipelineSyncBatch(
        pipeline_runs=[
            {
                "repo_id": repo_id,
                "run_id": "run-1",
                "provider": "github_actions",
                "status": "success",
                "queued_at": _dt("2026-04-03T10:00:00Z"),
                "started_at": _dt("2026-04-03T10:01:00Z"),
                "finished_at": _dt("2026-04-03T10:02:00Z"),
            }
        ],
        job_runs=[
            {
                "repo_id": repo_id,
                "run_id": "run-1",
                "job_id": "job-1",
                "job_name": "pytest",
                "status": "success",
                "started_at": _dt("2026-04-03T10:01:00Z"),
                "finished_at": _dt("2026-04-03T10:02:00Z"),
            }
        ],
        last_synced_cursor=_dt("2026-04-03T10:02:00Z"),
    )
    adapter = _FakeAdapter(batch)

    result = await processor.fetch_and_store(
        adapter,
        since_date=_dt("2026-04-01T00:00:00Z"),
        last_synced=_dt("2026-04-02T00:00:00Z"),
        repo_id=repo_id,
        owner="acme",
        repo="api",
    )

    assert adapter.received_kwargs is not None
    assert adapter.received_kwargs["since_date"] == _dt("2026-04-01T00:00:00Z")
    assert result.pipeline_runs == 1
    assert result.job_runs == 1
    assert result.last_synced_cursor == _dt("2026-04-03T10:02:00Z")
    assert len(store.pipeline_runs) == 1
    assert len(store.job_runs) == 1
