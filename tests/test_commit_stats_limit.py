"""Tests for the window-aware commit-stats limit.

Historically both Git providers capped per-file commit-stat ingestion at 50
commits per sync, which truncated coverage *inside* the initial-sync window
and starved churn/hotspot/bus-factor daily metrics with partial days.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.metrics.sinks.ingestion import IngestionSink
from dev_health_ops.models.git import Repo
from dev_health_ops.processors import github
from dev_health_ops.processors.base_git import resolve_commit_stats_limit

SINCE = datetime(2026, 5, 13, tzinfo=timezone.utc)


def test_window_bounded_sync_covers_all_commits():
    assert resolve_commit_stats_limit(300, None, SINCE) == 300


def test_window_bounded_sync_respects_hard_cap(monkeypatch):
    monkeypatch.setenv("COMMIT_STATS_MAX_COMMITS", "200")
    assert resolve_commit_stats_limit(5000, None, SINCE) == 200


def test_unbounded_sync_keeps_conservative_default():
    assert resolve_commit_stats_limit(5000, None, None) == 50


def test_max_commits_caps_both_modes():
    assert resolve_commit_stats_limit(300, 10, SINCE) == 10
    assert resolve_commit_stats_limit(300, 10, None) == 10


def test_hard_cap_default_is_300():
    assert resolve_commit_stats_limit(5000, None, SINCE) == 300


def test_windowed_truncation_predicate():
    # Truncation for the max_commits cap is decided by ``window_truncated`` (the
    # fetch peeked one commit past the cap), NOT by counting: a complete window of
    # exactly ``max_commits`` commits is indistinguishable from a truncated one by
    # count alone, so counting (``>= max_commits``) would false-skip the exact-size
    # complete case. ``raw_commit_count > stats_limit`` still catches the hard-cap
    # (300) case for uncapped fetches.
    # signature: (raw_commit_count, window_truncated, max_commits, since)
    trunc = github._windowed_commit_stats_truncated
    assert trunc(2, False, 10, SINCE) is False  # undersized + complete -> write
    assert trunc(2, True, 2, SINCE) is True  # cap hit AND more beyond -> truncated
    assert trunc(2, False, 2, SINCE) is False  # exact-size COMPLETE window -> write
    assert trunc(10, True, 10, SINCE) is True  # cap hit AND more beyond
    assert trunc(2, False, None, SINCE) is False
    assert trunc(301, False, None, SINCE) is True  # over hard cap (300)
    assert trunc(50, False, 100, None) is False  # full-history is never truncated


class _RecordingSink:
    def __init__(self) -> None:
        self.stats: list[Any] = []

    async def insert_git_commit_stats(self, stats):
        self.stats.extend(stats)


def _run_sync_commit_stats(monkeypatch, *, raw_commits, since):
    """Drive ``_sync_github_commit_stats`` with a pre-fetched commit list."""
    fetch_calls: list[int] = []

    async def _fake_fetch(
        connector, owner, repo_name, raw, repo_id, max_stats, window, usage_sink
    ):
        fetch_calls.append(max_stats)
        return [
            SimpleNamespace(commit_hash=getattr(c, "sha", c)) for c in raw[:max_stats]
        ]

    monkeypatch.setattr(github, "_fetch_github_commit_stats_async", _fake_fetch)
    sink = _RecordingSink()

    async def _drive():
        return await github._sync_github_commit_stats(
            connector=object(),
            owner="acme",
            repo_name="widgets",
            db_repo=cast(Repo, SimpleNamespace(id="repo-1")),
            ingestion_sink=cast(IngestionSink, sink),
            max_commits=None,
            since=since,
            raw_commits=list(raw_commits),
        )

    written = asyncio.run(_drive())
    return written, sink.stats, fetch_calls


def test_over_cap_window_skips_detail_and_writes_no_stats(monkeypatch):
    # A windowed (since-bound) sync whose commit list exceeds the hard cap must
    # skip every per-commit detail/file access and persist zero stats rather
    # than write a partial day. (Regression: the skip existed only in the
    # backfill path, not the normal _sync_github_commit_stats path.)
    raw_commits = [SimpleNamespace(sha=f"sha-{i}") for i in range(301)]
    written, stats, fetch_calls = _run_sync_commit_stats(
        monkeypatch, raw_commits=raw_commits, since=SINCE
    )
    assert written == 0
    assert stats == []
    assert fetch_calls == []  # detail fetch never invoked


def test_full_history_capped_sample_still_writes_stats(monkeypatch):
    # since=None full-history syncs use a capped sample (50) and MUST still
    # write stats even when the commit list exceeds the cap.
    raw_commits = [SimpleNamespace(sha=f"sha-{i}") for i in range(5000)]
    written, stats, fetch_calls = _run_sync_commit_stats(
        monkeypatch, raw_commits=raw_commits, since=None
    )
    assert fetch_calls == [50]  # capped sample fetched
    assert written == 50
    assert len(stats) == 50


def test_fetch_commit_stats_drains_usage_sink_on_rate_limit(monkeypatch):
    usage_sink: list[dict[str, Any]] = []

    class _FakeClient:
        async def get_commit_file_stats(self, owner, repo_name, sha):
            raise RateLimitException("limited", retry_after_seconds=42.0)

        def drain_usage_observations(self):
            return [{"route_family": "commit_stats", "request_count": 1}]

        async def close(self):
            return None

    monkeypatch.setattr(
        github, "_github_code_client_from_connector", lambda connector: _FakeClient()
    )

    async def _drive():
        return await github._fetch_github_commit_stats_async(
            connector=object(),
            owner="acme",
            repo_name="widgets",
            raw_commits=[SimpleNamespace(sha="sha-1")],
            repo_id="repo-1",
            max_stats=1,
            since=None,
            usage_sink=usage_sink,
        )

    try:
        asyncio.run(_drive())
    except RateLimitException as exc:
        assert exc.retry_after_seconds == 42.0
    else:
        raise AssertionError("expected RateLimitException")

    assert usage_sink == [{"route_family": "commit_stats", "request_count": 1}]


def test_fetch_commit_stats_skips_non_rate_commit_failure(monkeypatch):
    usage_sink: list[dict[str, Any]] = []

    class _FakeClient:
        async def get_commit_file_stats(self, owner, repo_name, sha):
            if sha == "bad-sha":
                raise RuntimeError("detail failed")
            return [
                SimpleNamespace(
                    commit_hash=sha,
                    file_path="src/app.py",
                    additions=2,
                    deletions=1,
                    old_file_mode="unknown",
                    new_file_mode="unknown",
                )
            ]

        def drain_usage_observations(self):
            return [{"route_family": "commit_stats", "request_count": 3}]

        async def close(self):
            return None

    monkeypatch.setattr(
        github, "_github_code_client_from_connector", lambda connector: _FakeClient()
    )

    async def _drive():
        return await github._fetch_github_commit_stats_async(
            connector=object(),
            owner="acme",
            repo_name="widgets",
            raw_commits=[
                SimpleNamespace(sha="good-1"),
                SimpleNamespace(sha="bad-sha"),
                SimpleNamespace(sha="good-2"),
            ],
            repo_id="repo-1",
            max_stats=3,
            since=None,
            usage_sink=usage_sink,
        )

    stats = asyncio.run(_drive())

    assert [stat.commit_hash for stat in stats] == ["good-1", "good-2"]
    assert usage_sink == [{"route_family": "commit_stats", "request_count": 3}]
