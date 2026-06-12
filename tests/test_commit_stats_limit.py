"""Tests for the window-aware commit-stats limit.

Historically both Git providers capped per-file commit-stat ingestion at 50
commits per sync, which truncated coverage *inside* the initial-sync window
and starved churn/hotspot/bus-factor daily metrics with partial days.
"""

from __future__ import annotations

from datetime import datetime, timezone

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
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


def test_hard_cap_default_is_1000():
    assert resolve_commit_stats_limit(5000, None, SINCE) == 1000
