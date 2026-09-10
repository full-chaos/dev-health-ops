"""Unit tests for the post-sync complexity historical-window contract (CHAOS-2888).

``run_complexity_db_job`` scans *current* persisted file contents/blame and
writes ``repo_complexity_daily`` for every day in the requested backfill
window. Before this change, ``_dispatch_post_sync_tasks`` always enqueued
``run_complexity_job`` with only ``{"org_id": org_id}`` for any git sync,
including historical backfills. Because ``run_complexity_job`` defaults
``day``/``backfill_days`` to today/1 when absent, a historical backfill sync
silently wrote *today's* file-content complexity across the historical date
range it did not intend to touch -- fabricating a flat/incorrect historical
complexity trend.

The corrected contract (CHAOS-2888 plan, Workstream A):

- Complexity is enqueued only for a *current single-day* sync:
  ``metrics_backfill_days in (None, 1)`` and ``metrics_day`` is either absent
  or equals ``utc_today()``. When enqueued for an explicit window, the
  dispatch carries the explicit ``day``/``backfill_days=1`` rather than
  relying on the task's implicit today/1 defaults.
- Any other window (multi-day, or single-day but not today) is a historical
  backfill: complexity is skipped and a ``historical_complexity_unsupported``
  warning is logged with the requested date range.

CHAOS-3093: ``_dispatch_post_sync_tasks`` no longer chains complexity ahead of
an investment-materialize step -- that chain partner
(``dispatch_investment_materialize_partitioned``, workers/work_graph_tasks.py)
was deleted outright as dead-into-the-void Celery machinery with no consumer
since 2026-08-19, and Go's investment.materialize route is river-only. What
survives is a standalone ``celery_app.send_task(...)`` for
``run_complexity_job``, same dead-into-the-void status but kept as a named
send_task pending its own eventual retirement (CHAOS-4427). These tests prove
the seam without a live ClickHouse by patching that ``send_task`` call.
"""

from __future__ import annotations

import logging
from datetime import date
from unittest.mock import patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import that otherwise ERRORs isolated collection (mirrors CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.workers.post_sync_dispatch import _dispatch_post_sync_tasks

_COMPLEXITY_TASK = "dev_health_ops.workers.tasks.run_complexity_job"


def _run_dispatch(**kwargs):
    """Drive _dispatch_post_sync_tasks with send_task patched.

    Returns the send_task mock.
    """
    with patch(
        "dev_health_ops.workers.post_sync_dispatch.celery_app.send_task"
    ) as mock_send_task:
        _dispatch_post_sync_tasks(**kwargs)
    return mock_send_task


def _complexity_calls(mock_send_task) -> list:
    return [
        call
        for call in mock_send_task.call_args_list
        if call.args[0] == _COMPLEXITY_TASK
    ]


def _freeze_today(monkeypatch, today: date) -> None:
    monkeypatch.setattr(
        "dev_health_ops.workers.post_sync_dispatch.utc_today",
        lambda: today,
    )


def test_current_single_day_sync_enqueues_complexity_with_explicit_date(
    monkeypatch,
) -> None:
    """A current single-day sync (from_date == to_date == today) enqueues
    complexity with the explicit day/backfill_days=1, not the task's
    implicit-today defaults."""
    _freeze_today(monkeypatch, date(2026, 3, 5))

    mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["git"],
        org_id="org-123",
        from_date="2026-03-05",
        to_date="2026-03-05",
    )

    calls = _complexity_calls(mock_send_task)
    assert len(calls) == 1
    assert calls[0].kwargs["kwargs"] == {
        "org_id": "org-123",
        "day": "2026-03-05",
        "backfill_days": 1,
    }
    assert calls[0].kwargs["queue"] == "metrics"


def test_current_sync_without_explicit_window_still_enqueues_complexity(
    monkeypatch,
) -> None:
    """No from_date/to_date/metrics_day at all is still "current" (regression
    guard): complexity keeps enqueuing with just org_id, matching the task's
    own implicit today/1 defaults."""
    _freeze_today(monkeypatch, date(2026, 3, 5))

    mock_send_task = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs"],
        org_id="org-123",
    )

    calls = _complexity_calls(mock_send_task)
    assert len(calls) == 1
    assert calls[0].kwargs["kwargs"] == {"org_id": "org-123"}


def test_historical_single_day_sync_skips_complexity_dispatch(
    monkeypatch, caplog
) -> None:
    """A single historical day (from_date == to_date, but not today) must not
    enqueue complexity -- it would write today's file contents onto that past
    date."""
    _freeze_today(monkeypatch, date(2026, 3, 5))

    with caplog.at_level(logging.WARNING):
        mock_send_task = _run_dispatch(
            provider="github",
            sync_targets=["git"],
            org_id="org-123",
            from_date="2026-01-01",
            to_date="2026-01-01",
        )

    assert _complexity_calls(mock_send_task) == []
    assert "historical_complexity_unsupported" in caplog.text
    assert "2026-01-01" in caplog.text


def test_historical_multi_day_backfill_skips_complexity_but_keeps_daily_window(
    caplog,
) -> None:
    """A multi-day historical backfill must not enqueue complexity (it would
    fabricate a flat historical trend from current file contents)."""
    with caplog.at_level(logging.WARNING):
        mock_send_task = _run_dispatch(
            provider="github",
            sync_targets=["git"],
            org_id="org-123",
            from_date="2026-01-01",
            to_date="2026-01-14",
        )

    assert _complexity_calls(mock_send_task) == []
    assert "historical_complexity_unsupported" in caplog.text
    assert "2026-01-01" in caplog.text
    assert "2026-01-14" in caplog.text


def test_historical_backfill_skips_complexity_for_explicit_metrics_kwargs(
    monkeypatch, caplog
) -> None:
    """The historical/current determination is based on the final
    metrics_day/metrics_backfill_days values -- including when a caller
    passes them explicitly (bypassing from_date/to_date window derivation)."""
    _freeze_today(monkeypatch, date(2026, 3, 5))

    with caplog.at_level(logging.WARNING):
        mock_send_task = _run_dispatch(
            provider="github",
            sync_targets=["git"],
            org_id="org-123",
            metrics_day="2026-01-14",
            metrics_backfill_days=14,
        )

    assert _complexity_calls(mock_send_task) == []
    assert "historical_complexity_unsupported" in caplog.text
