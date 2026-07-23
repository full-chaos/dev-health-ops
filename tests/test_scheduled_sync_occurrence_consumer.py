"""Rollout wiring for the default-off scheduled-occurrence consumer."""

from __future__ import annotations

import importlib
import os
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

_FLAG = "SYNC_SCHEDULED_OCCURRENCE_CONSUMER_ENABLED"
_TASK_NAME = "dev_health_ops.workers.tasks.consume_pending_scheduled_sync_occurrences"
_SCHEDULE_NAME = "consume-pending-scheduled-sync-occurrences"
_EMPTY_COUNTS = {
    "scanned": 0,
    "completed": 0,
    "retried": 0,
    "quarantined": 0,
    "already_completed": 0,
    "errors": 0,
}


def _run_task() -> dict[str, int]:
    from dev_health_ops.workers.sync_scheduler import (
        consume_pending_scheduled_sync_occurrences,
    )

    return consume_pending_scheduled_sync_occurrences.run()


def test_disabled_consumer_does_not_open_postgres(monkeypatch) -> None:
    from dev_health_ops import db

    monkeypatch.delenv(_FLAG, raising=False)
    get_session = MagicMock()
    monkeypatch.setattr(db, "get_postgres_session_sync", get_session)

    assert _run_task() == _EMPTY_COUNTS
    get_session.assert_not_called()


def test_enabled_consumer_reconciles_and_commits_outer_transaction(monkeypatch) -> None:
    from dev_health_ops import db
    from dev_health_ops.workers import sync_scheduler

    monkeypatch.setenv(_FLAG, "true")
    session = MagicMock()
    counts = {
        "scanned": 3,
        "completed": 2,
        "retried": 1,
        "quarantined": 0,
        "already_completed": 0,
        "errors": 0,
    }

    @contextmanager
    def session_context():
        yield session

    get_session = MagicMock(side_effect=session_context)
    reconcile = MagicMock(return_value=counts)
    monkeypatch.setattr(db, "get_postgres_session_sync", get_session)
    monkeypatch.setattr(
        sync_scheduler, "reconcile_pending_scheduled_sync_occurrences", reconcile
    )

    assert _run_task() == counts
    get_session.assert_called_once_with()
    reconcile.assert_called_once_with(session)
    session.commit.assert_called_once_with()
    session.rollback.assert_not_called()


def test_enabled_consumer_rolls_back_logs_and_propagates(monkeypatch, caplog) -> None:
    from dev_health_ops import db
    from dev_health_ops.workers import sync_scheduler

    monkeypatch.setenv(_FLAG, "true")
    session = MagicMock()

    @contextmanager
    def session_context():
        try:
            yield session
        except Exception:
            session.rollback()
            raise

    monkeypatch.setattr(db, "get_postgres_session_sync", session_context)
    monkeypatch.setattr(
        sync_scheduler,
        "reconcile_pending_scheduled_sync_occurrences",
        MagicMock(side_effect=RuntimeError("reconcile failed")),
    )

    with pytest.raises(RuntimeError, match="reconcile failed"):
        _run_task()

    session.commit.assert_not_called()
    session.rollback.assert_called_once_with()
    assert "sync_scheduler.pending_occurrence_consumer_failed" in caplog.text


def test_beat_entry_is_default_off_and_uses_scheduler_queue_when_enabled(
    monkeypatch,
) -> None:
    from dev_health_ops.workers import config as worker_config

    original = os.environ.get(_FLAG)
    try:
        monkeypatch.delenv(_FLAG, raising=False)
        disabled_config = importlib.reload(worker_config)
        assert _SCHEDULE_NAME not in disabled_config.beat_schedule

        monkeypatch.setenv(_FLAG, "yes")
        enabled_config = importlib.reload(worker_config)
        assert enabled_config.beat_schedule[_SCHEDULE_NAME] == {
            "task": _TASK_NAME,
            "schedule": 300.0,
            "options": {"queue": "scheduler"},
        }
    finally:
        if original is None:
            monkeypatch.delenv(_FLAG, raising=False)
        else:
            monkeypatch.setenv(_FLAG, original)
        importlib.reload(worker_config)


def test_consumer_task_is_exported_and_registered() -> None:
    from dev_health_ops.workers import tasks
    from dev_health_ops.workers.celery_app import celery_app

    assert "consume_pending_scheduled_sync_occurrences" in tasks.__all__
    assert _TASK_NAME in celery_app.tasks
