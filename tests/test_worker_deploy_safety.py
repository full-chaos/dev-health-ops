from __future__ import annotations

import json
from argparse import Namespace

import pytest


def test_worker_inspect_sanitizes_task_arguments(monkeypatch, capsys) -> None:
    from dev_health_ops.workers import runner
    from dev_health_ops.workers.celery_app import celery_app

    class FakeInspector:
        def active(self) -> dict[str, list[dict[str, object]]]:
            return {
                "worker@node": [
                    {
                        "id": "task-1",
                        "name": "dev_health_ops.workers.tasks.run_sync_config",
                        "args": ["sensitive-value"],
                        "kwargs": {"credential": "sensitive-value"},
                        "argsrepr": "('sensitive-value',)",
                        "kwargsrepr": "{'credential': 'sensitive-value'}",
                        "headers": {"x-provider-credential": "sensitive-value"},
                        "properties": {"correlation_id": "sensitive-value"},
                        "delivery_info": {
                            "routing_key": "sync.github",
                            "redelivered": False,
                        },
                    }
                ]
            }

    def fake_inspect(timeout: float) -> FakeInspector:
        assert timeout == 0.1
        return FakeInspector()

    monkeypatch.setattr(celery_app.control, "inspect", fake_inspect)

    ns = Namespace(state="active", timeout=0.1, output="json")

    assert runner._cmd_inspect(ns) == 0

    output = capsys.readouterr().out
    payload = json.loads(output)
    task = payload["worker@node"][0]
    assert task == {
        "delivery_info": {"redelivered": False, "routing_key": "sync.github"},
        "id": "task-1",
        "name": "dev_health_ops.workers.tasks.run_sync_config",
    }
    assert "sensitive-value" not in output
    assert "args" not in task
    assert "kwargs" not in task
    assert "headers" not in task
    assert "properties" not in task


def test_worker_inspect_quiet_json_restores_otel_on_parse_error(monkeypatch) -> None:
    from dev_health_ops import cli

    monkeypatch.setenv("OTEL_ENABLED", "true")

    with pytest.raises(SystemExit):
        cli.main(["workers", "inspect", "--state", "bogus", "--output", "json"])

    assert cli.os.environ["OTEL_ENABLED"] == "true"


def test_worker_inspect_sanitizes_nested_scheduled_request(monkeypatch, capsys) -> None:
    from dev_health_ops.workers import runner
    from dev_health_ops.workers.celery_app import celery_app

    class FakeInspector:
        def scheduled(self) -> dict[str, list[dict[str, object]]]:
            return {
                "worker@node": [
                    {
                        "eta": "2026-01-01T00:00:00+00:00",
                        "priority": 3,
                        "request": {
                            "id": "task-2",
                            "name": "dev_health_ops.workers.tasks.run_sync_config",
                            "args": ["sensitive-value"],
                            "kwargs": {"credential": "sensitive-value"},
                            "headers": {"x-provider-credential": "sensitive-value"},
                            "delivery_info": {"routing_key": "sync.github"},
                        },
                    }
                ]
            }

    monkeypatch.setattr(celery_app.control, "inspect", lambda timeout: FakeInspector())

    ns = Namespace(state="scheduled", timeout=0.1, output="json")

    assert runner._cmd_inspect(ns) == 0

    output = capsys.readouterr().out
    payload = json.loads(output)
    task = payload["worker@node"][0]
    assert task == {
        "delivery_info": {"routing_key": "sync.github"},
        "eta": "2026-01-01T00:00:00+00:00",
        "id": "task-2",
        "name": "dev_health_ops.workers.tasks.run_sync_config",
        "priority": 3,
    }
    assert "sensitive-value" not in output
    assert "args" not in task
    assert "kwargs" not in task
    assert "headers" not in task


def test_worker_late_ack_exclusions_are_explicit() -> None:
    from dev_health_ops.workers import config
    from dev_health_ops.workers.celery_app import celery_app

    assert config.task_acks_late is False
    assert config.task_reject_on_worker_lost is False
    assert celery_app.conf.task_acks_late is False
    assert celery_app.conf.task_reject_on_worker_lost is False
    assert (
        "dev_health_ops.workers.tasks.phone_home_heartbeat"
        in config.late_ack_excluded_tasks
    )
    assert config.task_annotations[
        "dev_health_ops.workers.tasks.phone_home_heartbeat"
    ] == {"acks_late": False, "reject_on_worker_lost": False}
    assert celery_app.conf.task_annotations[
        "dev_health_ops.workers.tasks.phone_home_heartbeat"
    ] == {"acks_late": False, "reject_on_worker_lost": False}


def test_worker_late_ack_exclusions_match_registered_tasks(monkeypatch) -> None:
    monkeypatch.setenv("OTEL_ENABLED", "false")

    from dev_health_ops.workers import config
    from dev_health_ops.workers.celery_app import celery_app

    missing = set(config.late_ack_excluded_tasks) - set(celery_app.tasks)
    assert missing == set()
