"""StorageProtocol — formal interface between processors and storage backends.

Processors (github.py, gitlab.py, local.py) call methods on a ``store`` object
that is typed as ``Any``.  This module introduces a ``runtime_checkable``
Protocol so that:

1. Static type checkers (mypy, pyright) can validate that a store implementation
   satisfies the contract expected by processors.
2. Tests can pass lightweight fakes that implement only the required subset.
3. We preserve the existing duck-typed call sites — no concrete inheritance
   required from ClickHouseStore / MongoStore / etc.

DEPRECATION NOTICE
------------------
MongoDB and SQLite storage backends are deprecated for analytics (see AGENTS.md).
ClickHouseStore is the only supported implementation.  The Protocol reflects the
*current* full interface used by GitHub and GitLab processors.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

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


@runtime_checkable
class GitSyncStore(Protocol):
    """Minimal storage interface required by GitHub and GitLab processors.

    Implementations: ClickHouseStore (supported), MongoStore (deprecated).
    All methods are async.
    """

    async def insert_repo(self, repo: Repo) -> None:
        """Upsert a repository record."""
        ...

    async def insert_git_commit_data(self, commit_data: list[GitCommit]) -> None:
        """Insert a batch of commit records."""
        ...

    async def insert_git_commit_stats(self, commit_stats: list[GitCommitStat]) -> None:
        """Insert a batch of per-file commit stat records."""
        ...

    async def insert_blame_data(self, data_batch: list[GitBlame]) -> None:
        """Insert a batch of git blame records."""
        ...

    async def insert_git_file_data(self, file_data: list[GitFile]) -> None:
        """Insert a batch of file records."""
        ...

    async def insert_git_pull_requests(self, pr_data: list[GitPullRequest]) -> None:
        """Insert a batch of pull/merge request records."""
        ...

    async def insert_git_pull_request_reviews(
        self, review_data: list[GitPullRequestReview]
    ) -> None:
        """Insert a batch of PR review records."""
        ...

    async def insert_ci_pipeline_runs(self, runs: list[CiPipelineRun]) -> None:
        """Insert a batch of CI pipeline run records."""
        ...

    async def insert_testops_pipeline_runs(
        self, runs: list[PipelineRunExtendedRow]
    ) -> None:
        """Insert a batch of extended TestOps pipeline run records."""
        ...

    async def insert_testops_job_runs(self, jobs: list[JobRunRow]) -> None:
        """Insert a batch of TestOps job run records."""
        ...

    async def insert_deployments(self, deployments: list[Deployment]) -> None:
        """Insert a batch of deployment records."""
        ...

    async def insert_incidents(self, incidents: list[Incident]) -> None:
        """Insert a batch of incident records."""
        ...

    async def insert_security_alerts(self, alerts: list[SecurityAlert]) -> None:
        """Insert a batch of security alert records."""
        raise NotImplementedError

    async def insert_test_suite_results(self, suites: list[object]) -> None:
        """Insert a batch of normalized test suite records."""
        ...

    async def insert_test_case_results(self, cases: list[object]) -> None:
        """Insert a batch of normalized test case records."""
        ...

    async def insert_coverage_snapshots(self, snapshots: list[object]) -> None:
        """Insert a batch of normalized coverage snapshots."""
        ...

    async def has_any_git_files(self, repo_id: object) -> bool:
        """Return True if any git file records exist for the given repo."""
        ...

    async def has_any_git_commit_stats(self, repo_id: object) -> bool:
        """Return True if any commit stat records exist for the given repo."""
        ...

    async def has_any_git_blame(self, repo_id: object) -> bool:
        """Return True if any blame records exist for the given repo."""
        ...
