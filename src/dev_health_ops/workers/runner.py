"""CLI runner for background workers."""

import argparse
import contextlib
import io
import json
import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)

_INSPECT_STATES = ("active", "reserved", "scheduled")


def _sanitize_delivery_info(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}

    safe_keys = {"exchange", "routing_key", "priority", "redelivered"}
    return {key: value[key] for key in safe_keys if key in value}


def _sanitize_task(task: object) -> dict[str, object]:
    if not isinstance(task, dict):
        return {"raw_type": type(task).__name__}

    request = task.get("request")
    if isinstance(request, dict):
        sanitized = _sanitize_task(request)
        for key in ("eta", "priority"):
            if key in task:
                sanitized[key] = task[key]
        return sanitized

    sanitized: dict[str, object] = {}
    for key in ("id", "name", "hostname", "time_start", "acknowledged", "worker_pid"):
        if key in task:
            sanitized[key] = task[key]

    delivery_info = _sanitize_delivery_info(task.get("delivery_info"))
    if delivery_info:
        sanitized["delivery_info"] = delivery_info

    return sanitized


def _inspect_worker_tasks(
    state: str, timeout: float
) -> dict[str, list[dict[str, object]]]:
    if state not in _INSPECT_STATES:
        raise ValueError(f"unsupported inspect state: {state}")

    from dev_health_ops.workers.celery_app import celery_app

    inspector = celery_app.control.inspect(timeout=timeout)
    raw = getattr(inspector, state)() or {}
    if not isinstance(raw, dict):
        return {}

    sanitized: dict[str, list[dict[str, object]]] = {}
    for worker, tasks in raw.items():
        if not isinstance(tasks, list):
            sanitized[str(worker)] = []
            continue
        sanitized[str(worker)] = [_sanitize_task(task) for task in tasks]
    return sanitized


def _print_inspect_text(tasks_by_worker: dict[str, list[dict[str, object]]]) -> None:
    if not tasks_by_worker:
        print("No workers responded.")
        return

    for worker in sorted(tasks_by_worker):
        tasks = tasks_by_worker[worker]
        print(f"{worker}: {len(tasks)} task(s)")
        for task in tasks:
            delivery_info = task.get("delivery_info")
            queue = ""
            if isinstance(delivery_info, dict):
                routing_key = delivery_info.get("routing_key")
                if routing_key:
                    queue = f" queue={routing_key}"
            task_name = task.get("name", "<unknown>")
            task_id = task.get("id", "<unknown>")
            print(f"  - {task_name} id={task_id}{queue}")


def _cmd_start_worker(ns: argparse.Namespace) -> int:
    """Start a Celery worker."""
    queues = ns.queues or ["default", "metrics", "sync"]
    concurrency = ns.concurrency

    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "dev_health_ops.workers.celery_app",
        "worker",
        "--loglevel=INFO",
        f"--queues={','.join(queues)}",
    ]

    if concurrency:
        cmd.extend(["--concurrency", str(concurrency)])

    logger.info(f"Starting Celery worker: {' '.join(cmd)}")
    return subprocess.run(cmd).returncode


def _cmd_start_scheduler(ns: argparse.Namespace) -> int:
    """Start the Celery beat scheduler."""
    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "dev_health_ops.workers.celery_app",
        "beat",
        "--loglevel=INFO",
    ]

    logger.info(f"Starting Celery beat: {' '.join(cmd)}")
    return subprocess.run(cmd).returncode


def _cmd_inspect(ns: argparse.Namespace) -> int:
    if ns.output == "json":
        previous_disable = logging.root.manager.disable
        previous_otel_enabled = os.environ.get("OTEL_ENABLED")
        with contextlib.redirect_stdout(io.StringIO()):
            logging.disable(logging.CRITICAL)
            os.environ["OTEL_ENABLED"] = "false"
            try:
                tasks_by_worker = _inspect_worker_tasks(ns.state, ns.timeout)
            finally:
                if previous_otel_enabled is None:
                    os.environ.pop("OTEL_ENABLED", None)
                else:
                    os.environ["OTEL_ENABLED"] = previous_otel_enabled
                logging.disable(previous_disable)
        print(json.dumps(tasks_by_worker, sort_keys=True))
    else:
        tasks_by_worker = _inspect_worker_tasks(ns.state, ns.timeout)
        _print_inspect_text(tasks_by_worker)
    return 0


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    """Register worker commands."""
    worker_parser = subparsers.add_parser("start-worker", help="Start a Celery worker.")
    worker_parser.add_argument(
        "--queues",
        nargs="+",
        help="Queues to consume from (default: default metrics sync)",
    )
    worker_parser.add_argument(
        "--concurrency", type=int, help="Number of concurrent worker processes"
    )
    worker_parser.set_defaults(func=_cmd_start_worker)

    beat_parser = subparsers.add_parser(
        "start-scheduler", help="Start the Celery beat scheduler."
    )
    beat_parser.set_defaults(func=_cmd_start_scheduler)

    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Show sanitized active/reserved/scheduled Celery task state.",
    )
    inspect_parser.add_argument(
        "--state",
        choices=_INSPECT_STATES,
        default="active",
        help="Worker task state to inspect (default: active).",
    )
    inspect_parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Seconds to wait for worker inspect replies (default: 5).",
    )
    inspect_parser.add_argument(
        "--output",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    inspect_parser.set_defaults(func=_cmd_inspect)
