"""Code-dataset window upper-bound honoring (CHAOS-2573).

Before CHAOS-2573 the GitHub/GitLab code-dataset adapters threaded only
``since=window_start`` into the processors; ``window_end`` was dropped, so a
backfill chunk ``[chunk_start, chunk_end]`` fetched ``[chunk_start, now)`` and
over-fetched every chunk up to the present. These tests lock the upper bound:
commits push ``until`` server-side, and list-returning datasets are trimmed by
an inclusive post-fetch filter.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.processors.github import (
    _filter_after,
    _sync_github_test_reports,
)
from dev_health_ops.processors.gitlab import (
    _fetch_gitlab_commits_sync,
    _fetch_gitlab_test_reports_sync,
    _sync_gitlab_test_reports,
)
from dev_health_ops.processors.gitlab import (
    _filter_after as _gitlab_filter_after,
)

SINCE = datetime(2026, 1, 10, tzinfo=timezone.utc)
UNTIL = datetime(2026, 1, 12, tzinfo=timezone.utc)


def test_gitlab_commits_fetch_passes_window_to_code_client() -> None:
    connector = MagicMock()
    client = _async_cm(MagicMock())
    client.get_commits = AsyncMock(return_value=[])
    client.drain_usage_observations = MagicMock(return_value=[])

    with patch(
        "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
        return_value=client,
    ):
        _fetch_gitlab_commits_sync(
            connector, 123, None, "repo-1", since=SINCE, until=UNTIL
        )

    client.get_commits.assert_awaited_once_with(
        123, max_commits=None, since=SINCE, until=UNTIL
    )


def test_gitlab_commits_fetch_omits_until_when_none() -> None:
    connector = MagicMock()
    client = _async_cm(MagicMock())
    client.get_commits = AsyncMock(return_value=[])
    client.drain_usage_observations = MagicMock(return_value=[])

    with patch(
        "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
        return_value=client,
    ):
        _fetch_gitlab_commits_sync(connector, 123, None, "repo-1", since=SINCE)

    client.get_commits.assert_awaited_once_with(
        123, max_commits=None, since=SINCE, until=None
    )


def _record(ts: datetime | None, field: str = "started_at") -> SimpleNamespace:
    return SimpleNamespace(**{field: ts})


def test_filter_after_drops_post_window_records() -> None:
    in_window = _record(datetime(2026, 1, 11, tzinfo=timezone.utc))
    post_window = _record(datetime(2026, 1, 20, tzinfo=timezone.utc))

    kept = _filter_after([in_window, post_window], UNTIL, "started_at")

    assert kept == [in_window]


def test_filter_after_boundary_is_inclusive() -> None:
    # A record dated exactly at the upper bound is kept (inclusive, mirrors the
    # inclusive lower bound applied for ``since``).
    boundary = _record(UNTIL)

    kept = _filter_after([boundary], UNTIL, "started_at")

    assert kept == [boundary]


def test_filter_after_no_upper_bound_returns_all() -> None:
    records = [_record(datetime(2026, 1, 20, tzinfo=timezone.utc))]

    assert _filter_after(records, None, "started_at") is records


def test_filter_after_falls_back_through_fields() -> None:
    # deployments date off ``deployed_at`` first, then ``started_at``.
    rec = SimpleNamespace(
        deployed_at=None, started_at=datetime(2026, 1, 20, tzinfo=timezone.utc)
    )

    kept = _filter_after([rec], UNTIL, "deployed_at", "started_at")

    assert kept == []


def test_filter_after_keeps_records_without_timestamp() -> None:
    rec = _record(None)

    assert _filter_after([rec], UNTIL, "started_at") == [rec]


def test_gitlab_filter_after_matches_github_semantics() -> None:
    post_window = _record(datetime(2026, 1, 20, tzinfo=timezone.utc))

    assert _gitlab_filter_after([post_window], UNTIL, "started_at") == []


def test_gitlab_test_reports_fetch_skips_post_window_pipelines() -> None:
    # CHAOS-2573 (Codex review): coverage_members carry no timestamp and are
    # ingested unconditionally, so a post-window pipeline must be dropped at the
    # source -- before its native report OR coverage artifacts are collected.
    # CHAOS-2773 CS12: pipeline listing + test_report/jobs now ride the
    # canonical, instrumented ``GitLabCodeClient`` instead of python-gitlab's
    # ``gl_project.pipelines.list()`` + ``connector.rest_client.*``.
    in_window = {
        "id": 1,
        "ref": "main",
        "created_at": "2026-01-11T00:00:00Z",
        "started_at": "2026-01-11T00:00:00Z",
        "finished_at": "2026-01-11T01:00:00Z",
    }
    post_window = {
        "id": 2,
        "ref": "main",
        "created_at": "2026-01-20T00:00:00Z",
        "started_at": "2026-01-20T00:00:00Z",
        "finished_at": "2026-01-20T01:00:00Z",
    }
    connector = MagicMock()
    client = _async_cm(MagicMock())
    client.iter_pipelines_since = AsyncMock(return_value=[post_window, in_window])
    client.get_pipeline_test_report = AsyncMock(
        return_value={"test_suites": [{"name": "suite"}]}
    )
    client.iter_pipeline_jobs = AsyncMock(return_value=[])  # no jobs -> no coverage
    client.drain_usage_observations = MagicMock(return_value=[])

    with patch(
        "dev_health_ops.processors.gitlab._gitlab_code_client_from_connector",
        return_value=client,
    ):
        test_reports, coverage_members = _fetch_gitlab_test_reports_sync(
            connector, 123, SINCE, "main", 50, UNTIL
        )

    assert {run_id for run_id, *_ in test_reports} == {"1"}
    # The post-window pipeline is skipped before any report/coverage fetch.
    queried_ids = {
        call.args[1] for call in client.get_pipeline_test_report.await_args_list
    }
    assert queried_ids == {1}


def _async_cm(target: MagicMock) -> MagicMock:
    target.__aenter__ = AsyncMock(return_value=target)
    target.__aexit__ = AsyncMock(return_value=False)
    return target


@pytest.mark.asyncio
async def test_github_test_reports_forwards_until_date_to_testops() -> None:
    # CHAOS-2573 (Codex review): the TestOps pipeline/job path must forward
    # window_end so post-window pipeline/job rows are not persisted.
    loop = asyncio.get_running_loop()
    proc = MagicMock()
    proc.fetch_and_store = AsyncMock(
        return_value=SimpleNamespace(pipeline_runs=0, job_runs=0)
    )
    adapter = _async_cm(MagicMock())

    with (
        patch(
            "dev_health_ops.providers.github.testops_pipeline.GitHubActionsAdapter",
            return_value=adapter,
        ),
        patch(
            "dev_health_ops.processors.testops_pipeline.TestOpsPipelineProcessor",
            return_value=proc,
        ),
        patch(
            "dev_health_ops.processors.github._fetch_github_test_artifacts_sync",
            return_value=[],
        ),
    ):
        await _sync_github_test_reports(
            connector=MagicMock(),
            gh_repo=MagicMock(default_branch="main"),
            owner="o",
            repo_name="r",
            repo_id="repo-1",
            org_id="org-1",
            ingestion_sink=MagicMock(),
            loop=loop,
            since=SINCE,
            until=UNTIL,
        )

    proc.fetch_and_store.assert_awaited_once()
    assert proc.fetch_and_store.await_args.kwargs["until_date"] == UNTIL


@pytest.mark.asyncio
async def test_gitlab_test_reports_forwards_until_date_to_testops() -> None:
    loop = asyncio.get_running_loop()
    proc = MagicMock()
    proc.fetch_and_store = AsyncMock(
        return_value=SimpleNamespace(pipeline_runs=0, job_runs=0)
    )
    adapter = _async_cm(MagicMock())

    with (
        patch(
            "dev_health_ops.providers.gitlab.testops_pipeline.GitLabCIAdapter",
            return_value=adapter,
        ),
        patch(
            "dev_health_ops.processors.testops_pipeline.TestOpsPipelineProcessor",
            return_value=proc,
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_test_reports_sync",
            return_value=([], []),
        ),
    ):
        await _sync_gitlab_test_reports(
            connector=MagicMock(),
            gl_project=MagicMock(default_branch="main"),
            project_id=123,
            token="t",
            repo_id="repo-1",
            org_id="org-1",
            ingestion_sink=MagicMock(),
            loop=loop,
            since=SINCE,
            until=UNTIL,
        )

    proc.fetch_and_store.assert_awaited_once()
    assert proc.fetch_and_store.await_args.kwargs["until_date"] == UNTIL


@pytest.mark.asyncio
async def test_gitlab_test_reports_drains_ci_adapter_usage_into_sink() -> None:
    """CHAOS-2773 CS12: GitLabCIAdapter now opts into BasePipelineAdapter's
    usage instrumentation (shared foundation with CHAOS-2806/CS5) -- its
    drained observations must flow into the caller-owned usage_sink in the
    finally: block, on the success path."""
    loop = asyncio.get_running_loop()
    proc = MagicMock()
    proc.fetch_and_store = AsyncMock(
        return_value=SimpleNamespace(pipeline_runs=0, job_runs=0)
    )
    adapter = _async_cm(MagicMock())
    adapter.drain_usage_observations = MagicMock(
        return_value=[{"route_family": "tests"}]
    )
    usage_sink: list[dict[str, object]] = []

    with (
        patch(
            "dev_health_ops.providers.gitlab.testops_pipeline.GitLabCIAdapter",
            return_value=adapter,
        ),
        patch(
            "dev_health_ops.processors.testops_pipeline.TestOpsPipelineProcessor",
            return_value=proc,
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_test_reports_sync",
            return_value=([], []),
        ),
    ):
        await _sync_gitlab_test_reports(
            connector=MagicMock(),
            gl_project=MagicMock(default_branch="main"),
            project_id=123,
            token="t",
            repo_id="repo-1",
            org_id="org-1",
            ingestion_sink=MagicMock(),
            loop=loop,
            since=SINCE,
            usage_sink=usage_sink,
        )

    adapter.drain_usage_observations.assert_called_once()
    assert usage_sink == [{"route_family": "tests"}]


@pytest.mark.asyncio
async def test_gitlab_test_reports_drains_ci_adapter_usage_on_failure_path() -> None:
    """CHAOS-2773 CS12: the adapter's usage is drained in the finally: block
    even when fetch_and_store raises -- the CS2 contract requires BOTH the
    success and failure path to preserve partial observations."""
    loop = asyncio.get_running_loop()
    proc = MagicMock()
    proc.fetch_and_store = AsyncMock(side_effect=RuntimeError("boom"))
    adapter = _async_cm(MagicMock())
    adapter.drain_usage_observations = MagicMock(
        return_value=[{"route_family": "tests"}]
    )
    usage_sink: list[dict[str, object]] = []

    with (
        patch(
            "dev_health_ops.providers.gitlab.testops_pipeline.GitLabCIAdapter",
            return_value=adapter,
        ),
        patch(
            "dev_health_ops.processors.testops_pipeline.TestOpsPipelineProcessor",
            return_value=proc,
        ),
        patch(
            "dev_health_ops.processors.gitlab._fetch_gitlab_test_reports_sync",
            return_value=([], []),
        ),
    ):
        await _sync_gitlab_test_reports(
            connector=MagicMock(),
            gl_project=MagicMock(default_branch="main"),
            project_id=123,
            token="t",
            repo_id="repo-1",
            org_id="org-1",
            ingestion_sink=MagicMock(),
            loop=loop,
            since=SINCE,
            usage_sink=usage_sink,
        )

    adapter.drain_usage_observations.assert_called_once()
    assert usage_sink == [{"route_family": "tests"}]
