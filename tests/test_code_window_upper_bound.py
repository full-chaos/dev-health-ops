"""Code-dataset window upper-bound honoring (CHAOS-2573).

Before CHAOS-2573 the GitHub/GitLab code-dataset adapters threaded only
``since=window_start`` into the processors; ``window_end`` was dropped, so a
backfill chunk ``[chunk_start, chunk_end]`` fetched ``[chunk_start, now)`` and
over-fetched every chunk up to the present. These tests lock the upper bound:
commits push ``until`` server-side, and list-returning datasets are trimmed by
an inclusive post-fetch filter.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from dev_health_ops.processors.github import (
    _fetch_github_commits_sync,
    _filter_after,
)
from dev_health_ops.processors.gitlab import (
    _fetch_gitlab_commits_sync,
    _fetch_gitlab_test_reports_sync,
)
from dev_health_ops.processors.gitlab import (
    _filter_after as _gitlab_filter_after,
)

SINCE = datetime(2026, 1, 10, tzinfo=timezone.utc)
UNTIL = datetime(2026, 1, 12, tzinfo=timezone.utc)


def test_github_commits_fetch_passes_until_to_get_commits() -> None:
    gh_repo = MagicMock()
    gh_repo.get_commits.return_value = []

    _fetch_github_commits_sync(gh_repo, None, "repo-1", since=SINCE, until=UNTIL)

    gh_repo.get_commits.assert_called_once_with(since=SINCE, until=UNTIL)


def test_github_commits_fetch_omits_until_when_none() -> None:
    gh_repo = MagicMock()
    gh_repo.get_commits.return_value = []

    _fetch_github_commits_sync(gh_repo, None, "repo-1", since=SINCE)

    gh_repo.get_commits.assert_called_once_with(since=SINCE)


def test_gitlab_commits_fetch_passes_until_to_commits_list() -> None:
    gl_project = MagicMock()
    gl_project.commits.list.return_value = []

    _fetch_gitlab_commits_sync(gl_project, None, "repo-1", since=SINCE, until=UNTIL)

    assert gl_project.commits.list.call_count == 1
    params = gl_project.commits.list.call_args.kwargs
    assert params["until"] == UNTIL.isoformat().replace("+00:00", "Z")
    assert params["since"] == SINCE.isoformat().replace("+00:00", "Z")


def test_gitlab_commits_fetch_omits_until_when_none() -> None:
    gl_project = MagicMock()
    gl_project.commits.list.return_value = []

    _fetch_gitlab_commits_sync(gl_project, None, "repo-1", since=SINCE)

    params = gl_project.commits.list.call_args.kwargs
    assert "until" not in params


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
    in_window = SimpleNamespace(
        id=1,
        ref="main",
        created_at="2026-01-11T00:00:00Z",
        started_at="2026-01-11T00:00:00Z",
        finished_at="2026-01-11T01:00:00Z",
    )
    post_window = SimpleNamespace(
        id=2,
        ref="main",
        created_at="2026-01-20T00:00:00Z",
        started_at="2026-01-20T00:00:00Z",
        finished_at="2026-01-20T01:00:00Z",
    )
    gl_project = MagicMock()
    gl_project.pipelines.list.return_value = [post_window, in_window]
    connector = MagicMock()
    connector.rest_client.get_pipeline_test_report.return_value = {
        "test_suites": [{"name": "suite"}]
    }
    connector.rest_client.get_list.return_value = []  # no jobs -> no coverage

    test_reports, coverage_members = _fetch_gitlab_test_reports_sync(
        connector, gl_project, 123, SINCE, "main", 50, UNTIL
    )

    assert {run_id for run_id, *_ in test_reports} == {"1"}
    # The post-window pipeline is skipped before any report/coverage fetch.
    queried_ids = {
        call.args[1]
        for call in connector.rest_client.get_pipeline_test_report.call_args_list
    }
    assert queried_ids == {1}
