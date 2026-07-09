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
  signature carries the explicit ``day``/``backfill_days=1`` rather than
  relying on the task's implicit today/1 defaults.
- Any other window (multi-day, or single-day but not today) is a historical
  backfill: complexity is skipped and a ``historical_complexity_unsupported``
  warning is logged with the requested date range. ``run_daily_metrics``
  still receives the full historical window -- only complexity is gated.

These tests prove the seam without a live ClickHouse: they patch the same
Celery ``chain`` / ``signature`` factories the sibling dispatch tests patch.
"""

from __future__ import annotations

import logging
from datetime import date
from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import that otherwise ERRORs isolated collection (mirrors CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.workers.post_sync_dispatch import _dispatch_post_sync_tasks

_COMPLEXITY_TASK = "dev_health_ops.workers.tasks.run_complexity_job"
_DAILY_METRICS_TASK = "dev_health_ops.workers.tasks.run_daily_metrics"
_WORK_GRAPH_TASK = "dev_health_ops.workers.tasks.run_work_graph_build"
_INVESTMENT_TASK = (
    "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned"
)


def _run_dispatch(**kwargs):
    """Drive _dispatch_post_sync_tasks with chain/signature/send_task patched.

    Returns (signature_mock, chain_mock, chain_instance_mock, send_task_mock).
    """
    with (
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.signature"
        ) as mock_signature,
        patch("dev_health_ops.workers.post_sync_dispatch.chain") as mock_chain,
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.send_task"
        ) as mock_send_task,
    ):

        def _make_sig(name, **sig_kwargs):
            sig = MagicMock(name=f"sig:{name}")
            sig.task_name = name
            sig.sig_kwargs = sig_kwargs
            return sig

        mock_signature.side_effect = _make_sig
        chain_instance = MagicMock(name="chain_instance")
        mock_chain.return_value = chain_instance
        _dispatch_post_sync_tasks(**kwargs)
    return mock_signature, mock_chain, chain_instance, mock_send_task


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

    _, mock_chain, chain_instance, _ = _run_dispatch(
        provider="github",
        sync_targets=["git"],
        org_id="org-123",
        from_date="2026-03-05",
        to_date="2026-03-05",
    )

    complexity_sig, daily_sig, build_sig, materialize_sig = mock_chain.call_args.args
    assert complexity_sig.task_name == _COMPLEXITY_TASK
    assert complexity_sig.sig_kwargs["kwargs"] == {
        "org_id": "org-123",
        "day": "2026-03-05",
        "backfill_days": 1,
    }
    assert complexity_sig.sig_kwargs["queue"] == "metrics"
    assert complexity_sig.sig_kwargs.get("immutable") is True
    assert daily_sig.task_name == _DAILY_METRICS_TASK
    assert daily_sig.sig_kwargs["kwargs"] == {
        "org_id": "org-123",
        "day": "2026-03-05",
        "backfill_days": 1,
    }
    chain_instance.apply_async.assert_called_once_with()


def test_current_sync_without_explicit_window_still_enqueues_complexity(
    monkeypatch,
) -> None:
    """No from_date/to_date/metrics_day at all is still "current" (regression
    guard): complexity keeps enqueuing with just org_id, matching the task's
    own implicit today/1 defaults."""
    _freeze_today(monkeypatch, date(2026, 3, 5))

    _, mock_chain, chain_instance, _ = _run_dispatch(
        provider="github",
        sync_targets=["git", "prs"],
        org_id="org-123",
    )

    complexity_sig, daily_sig, build_sig, materialize_sig = mock_chain.call_args.args
    assert complexity_sig.task_name == _COMPLEXITY_TASK
    assert complexity_sig.sig_kwargs["kwargs"] == {"org_id": "org-123"}
    chain_instance.apply_async.assert_called_once_with()


def test_historical_single_day_sync_skips_complexity_dispatch(
    monkeypatch, caplog
) -> None:
    """A single historical day (from_date == to_date, but not today) must not
    enqueue complexity -- it would write today's file contents onto that past
    date. Daily metrics still receives the historical day/backfill_days=1."""
    _freeze_today(monkeypatch, date(2026, 3, 5))

    with caplog.at_level(logging.WARNING):
        _, mock_chain, chain_instance, _ = _run_dispatch(
            provider="github",
            sync_targets=["git"],
            org_id="org-123",
            from_date="2026-01-01",
            to_date="2026-01-01",
        )

    chain_sigs = mock_chain.call_args.args
    task_names = [sig.task_name for sig in chain_sigs]
    assert _COMPLEXITY_TASK not in task_names
    daily_sig = chain_sigs[0]
    assert daily_sig.task_name == _DAILY_METRICS_TASK
    assert daily_sig.sig_kwargs["kwargs"] == {
        "org_id": "org-123",
        "day": "2026-01-01",
        "backfill_days": 1,
    }
    chain_instance.apply_async.assert_called_once_with()
    assert "historical_complexity_unsupported" in caplog.text
    assert "2026-01-01" in caplog.text


def test_historical_multi_day_backfill_skips_complexity_but_keeps_daily_window(
    caplog,
) -> None:
    """A multi-day historical backfill must not enqueue complexity (it would
    fabricate a flat historical trend from current file contents), but
    run_daily_metrics keeps the full requested historical window."""
    with caplog.at_level(logging.WARNING):
        _, mock_chain, chain_instance, _ = _run_dispatch(
            provider="github",
            sync_targets=["git"],
            org_id="org-123",
            from_date="2026-01-01",
            to_date="2026-01-14",
        )

    chain_sigs = mock_chain.call_args.args
    task_names = [sig.task_name for sig in chain_sigs]
    assert _COMPLEXITY_TASK not in task_names
    daily_sig = chain_sigs[0]
    assert daily_sig.task_name == _DAILY_METRICS_TASK
    assert daily_sig.sig_kwargs["kwargs"] == {
        "org_id": "org-123",
        "day": "2026-01-14",
        "backfill_days": 14,
    }
    build_sig, materialize_sig = chain_sigs[1], chain_sigs[2]
    assert build_sig.task_name == _WORK_GRAPH_TASK
    assert materialize_sig.task_name == _INVESTMENT_TASK
    chain_instance.apply_async.assert_called_once_with()
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
        _, mock_chain, _, _ = _run_dispatch(
            provider="github",
            sync_targets=["git"],
            org_id="org-123",
            metrics_day="2026-01-14",
            metrics_backfill_days=14,
        )

    chain_sigs = mock_chain.call_args.args
    task_names = [sig.task_name for sig in chain_sigs]
    assert _COMPLEXITY_TASK not in task_names
    assert "historical_complexity_unsupported" in caplog.text
