from __future__ import annotations

from typing import Any

from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow
from dev_health_ops.models.git import (
    CiPipelineRun,
    Deployment,
    GitBlame,
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
    GitPullRequestReview,
    Incident,
    Repo,
    SecurityAlert,
)


class IngestionSink:
    def __init__(self, store: Any) -> None:
        self._store = store

    async def insert_repo(self, repo: Repo) -> None:
        await self._store.insert_repo(repo)

    async def insert_git_commit_data(self, commits: list[GitCommit]) -> None:
        await self._store.insert_git_commit_data(commits)

    async def insert_git_commit_stats(self, stats: list[GitCommitStat]) -> None:
        await self._store.insert_git_commit_stats(stats)

    async def insert_git_file_data(self, files: list[GitFile]) -> None:
        await self._store.insert_git_file_data(files)

    async def insert_blame_data(self, blame_data: list[GitBlame]) -> None:
        await self._store.insert_blame_data(blame_data)

    async def insert_git_pull_requests(
        self, pull_requests: list[GitPullRequest]
    ) -> None:
        await self._store.insert_git_pull_requests(pull_requests)

    async def insert_git_pull_request_reviews(
        self, reviews: list[GitPullRequestReview]
    ) -> None:
        await self._store.insert_git_pull_request_reviews(reviews)

    async def insert_ci_pipeline_runs(self, runs: list[CiPipelineRun]) -> None:
        await self._store.insert_ci_pipeline_runs(runs)

    async def insert_testops_pipeline_runs(
        self, runs: list[PipelineRunExtendedRow]
    ) -> None:
        await self._store.insert_testops_pipeline_runs(runs)

    async def insert_testops_job_runs(self, jobs: list[JobRunRow]) -> None:
        await self._store.insert_testops_job_runs(jobs)

    async def insert_deployments(self, deployments: list[Deployment]) -> None:
        await self._store.insert_deployments(deployments)

    async def insert_incidents(self, incidents: list[Incident]) -> None:
        await self._store.insert_incidents(incidents)

    async def insert_security_alerts(self, alerts: list[SecurityAlert]) -> None:
        await self._store.insert_security_alerts(alerts)
