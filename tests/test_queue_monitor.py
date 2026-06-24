"""Queue depth/age telemetry (CHAOS-2299).

monitor_queue_depths reads broker queue depths (and the oldest message's
enqueue timestamp, stamped by the before_task_publish signal) and emits one
structured log line per non-empty queue plus a warning above thresholds.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from dev_health_ops.workers import queue_monitor
from dev_health_ops.workers.queue_monitor import (
    QUEUE_AGE_WARNING_SECONDS,
    QUEUE_DEPTH_WARNING_THRESHOLD,
    monitor_queue_depths,
)


def _kombu_payload(enqueued_at: str | None = None) -> str:
    """A kombu redis message envelope as it sits on the broker list."""
    headers = {"task": "dev_health_ops.workers.tasks.dispatch_sync_run", "id": "t-1"}
    if enqueued_at is not None:
        headers["enqueued_at"] = enqueued_at
    return json.dumps(
        {"body": "", "content-type": "application/json", "headers": headers}
    )


class _FakeRedisClient:
    def __init__(self, tails: dict[str, str]):
        self._tails = tails

    def lindex(self, queue: str, index: int):
        assert index == -1  # oldest message: kombu LPUSHes, consumers BRPOP
        return self._tails.get(queue)


class _FakeRedisChannel:
    """Mimics the kombu redis virtual channel surface the monitor touches."""

    def __init__(self, depths: dict[str, int], tails: dict[str, str] | None = None):
        self._depths = depths
        self.client = _FakeRedisClient(tails or {})

    def _size(self, queue: str) -> int:
        return self._depths.get(queue, 0)


class _FakeAmqpChannel:
    """A transport without ``_size``/``client``: depth via passive declare."""

    def __init__(self, depths: dict[str, int]):
        self._depths = depths

    def queue_declare(self, queue: str, passive: bool = False):
        if queue not in self._depths:
            raise Exception(f"NOT_FOUND - no queue '{queue}'")
        return SimpleNamespace(message_count=self._depths[queue])


def _run_monitor(monkeypatch, channel, queues: list[str]) -> dict:
    @contextmanager
    def _fake_connection():
        yield SimpleNamespace(default_channel=channel)

    monkeypatch.setattr(queue_monitor, "task_queues", {q: {} for q in queues})
    monkeypatch.setattr(
        queue_monitor.celery_app, "connection_or_acquire", _fake_connection
    )

    task = monitor_queue_depths
    task.push_request(id="queue-monitor-test")
    try:
        return task()
    finally:
        task.pop_request()


class TestMonitorQueueDepths:
    def test_empty_queues_emit_nothing(self, monkeypatch, caplog):
        channel = _FakeRedisChannel(depths={})
        with caplog.at_level(logging.INFO, logger=queue_monitor.__name__):
            result = _run_monitor(monkeypatch, channel, ["sync", "sync.github"])

        assert result == {"queues": []}
        assert not [r for r in caplog.records if r.message == "queue_depth"]

    def test_non_empty_queue_logs_depth_and_age(self, monkeypatch, caplog):
        enqueued = datetime.now(timezone.utc) - timedelta(seconds=120)
        channel = _FakeRedisChannel(
            depths={"sync.linear": 3},
            tails={"sync.linear": _kombu_payload(enqueued.isoformat())},
        )

        with caplog.at_level(logging.INFO, logger=queue_monitor.__name__):
            result = _run_monitor(
                monkeypatch, channel, ["sync", "sync.linear", "sync.github"]
            )

        assert len(result["queues"]) == 1
        stats = result["queues"][0]
        assert stats["queue"] == "sync.linear"
        assert stats["depth"] == 3
        assert 110 <= stats["oldest_age_seconds"] <= 300

        records = [r for r in caplog.records if r.message == "queue_depth"]
        assert len(records) == 1
        assert records[0].queue == "sync.linear"
        assert records[0].depth == 3
        assert records[0].oldest_age_seconds == stats["oldest_age_seconds"]
        # Under both thresholds: no warning.
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_depth_warning_above_threshold(self, monkeypatch, caplog):
        channel = _FakeRedisChannel(
            depths={"sync.github": QUEUE_DEPTH_WARNING_THRESHOLD + 1}
        )

        with caplog.at_level(logging.INFO, logger=queue_monitor.__name__):
            result = _run_monitor(monkeypatch, channel, ["sync.github"])

        warnings = [r for r in caplog.records if r.message == "queue_backlog"]
        assert len(warnings) == 1
        assert warnings[0].queue == "sync.github"
        assert warnings[0].depth == QUEUE_DEPTH_WARNING_THRESHOLD + 1
        # No enqueued_at header on the tail message: depth-only is acceptable.
        assert result["queues"][0]["oldest_age_seconds"] is None

    def test_age_warning_above_threshold(self, monkeypatch, caplog):
        stale = datetime.now(timezone.utc) - timedelta(
            seconds=QUEUE_AGE_WARNING_SECONDS + 60
        )
        channel = _FakeRedisChannel(
            depths={"sync.jira": 1},
            tails={"sync.jira": _kombu_payload(stale.isoformat())},
        )

        with caplog.at_level(logging.INFO, logger=queue_monitor.__name__):
            _run_monitor(monkeypatch, channel, ["sync.jira"])

        warnings = [r for r in caplog.records if r.message == "queue_backlog"]
        assert len(warnings) == 1
        assert warnings[0].oldest_age_seconds > QUEUE_AGE_WARNING_SECONDS

    def test_message_without_enqueued_at_reports_depth_only(self, monkeypatch):
        """Messages published before the enqueued_at stamp shipped (or by
        foreign producers) must not break the probe."""
        channel = _FakeRedisChannel(
            depths={"sync": 2}, tails={"sync": _kombu_payload(None)}
        )

        result = _run_monitor(monkeypatch, channel, ["sync"])

        assert result["queues"] == [
            {"queue": "sync", "depth": 2, "oldest_age_seconds": None}
        ]

    def test_unparseable_tail_message_reports_depth_only(self, monkeypatch):
        channel = _FakeRedisChannel(depths={"sync": 1}, tails={"sync": "not-json{"})

        result = _run_monitor(monkeypatch, channel, ["sync"])

        assert result["queues"][0]["depth"] == 1
        assert result["queues"][0]["oldest_age_seconds"] is None

    def test_non_redis_transport_reports_depth_only(self, monkeypatch):
        channel = _FakeAmqpChannel(depths={"sync.gitlab": 5})

        result = _run_monitor(monkeypatch, channel, ["sync.gitlab", "metrics"])

        assert result["queues"] == [
            {"queue": "sync.gitlab", "depth": 5, "oldest_age_seconds": None}
        ]

    def test_probe_errors_count_queue_as_empty(self, monkeypatch):
        class _BrokenChannel:
            def _size(self, queue):
                raise RuntimeError("broker hiccup")

        result = _run_monitor(monkeypatch, _BrokenChannel(), ["sync"])

        assert result == {"queues": []}


class TestEnqueuedAtStamp:
    def test_before_task_publish_stamps_header(self):
        from dev_health_ops.workers.celery_app import _stamp_enqueued_at

        headers: dict = {"task": "x"}
        before = datetime.now(timezone.utc)
        _stamp_enqueued_at(headers=headers)

        stamped = datetime.fromisoformat(headers["enqueued_at"])
        assert before <= stamped <= datetime.now(timezone.utc)

    def test_existing_stamp_is_preserved(self):
        from dev_health_ops.workers.celery_app import _stamp_enqueued_at

        headers = {"enqueued_at": "2026-01-01T00:00:00+00:00"}
        _stamp_enqueued_at(headers=headers)
        assert headers["enqueued_at"] == "2026-01-01T00:00:00+00:00"

    def test_non_dict_headers_ignored(self):
        from dev_health_ops.workers.celery_app import _stamp_enqueued_at

        _stamp_enqueued_at(headers=None)  # must not raise


class TestTaskRegistration:
    def test_monitor_task_is_registered(self):
        from dev_health_ops.workers.tasks import monitor_queue_depths as exported

        assert hasattr(exported, "delay")
        assert exported.name == "dev_health_ops.workers.tasks.monitor_queue_depths"

    def test_monitor_task_runs_on_monitoring_queue(self):
        """Dedicated queue: a flooded `default` must not starve telemetry."""
        assert monitor_queue_depths.queue == "monitoring"
