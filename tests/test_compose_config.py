from __future__ import annotations

from pathlib import Path

import yaml

from dev_health_ops.workers.config import task_queues


def test_compose_worker_includes_backfill_queue() -> None:
    compose_path = Path(__file__).resolve().parents[1] / "compose.yml"
    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    worker_command = compose_data["services"]["worker"]["command"]
    worker_command_str = (
        " ".join(worker_command)
        if isinstance(worker_command, list)
        else str(worker_command)
    )

    assert "backfill" in worker_command_str


def test_celery_config_has_backfill_queue() -> None:
    assert "backfill" in task_queues


def test_celery_worker_prefetch_multiplier_is_one() -> None:
    """CHAOS-2277: long-running tasks (sync, stream consumers) + default
    prefetch (4) let reserved slow-queue messages fill the QoS window and
    block fetching from other queues entirely — Sync Now appeared stuck
    until a worker restart released the unacked reservations. One-at-a-time
    fetching keeps cross-queue round-robin fair."""
    from dev_health_ops.workers.config import worker_prefetch_multiplier

    assert worker_prefetch_multiplier == 1
