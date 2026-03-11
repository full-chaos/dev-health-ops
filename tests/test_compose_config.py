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
