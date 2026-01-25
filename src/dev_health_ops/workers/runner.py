"""CLI runner for background workers."""

import argparse
import subprocess
import sys


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

    print(f"Starting Celery worker: {' '.join(cmd)}")
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

    print(f"Starting Celery beat: {' '.join(cmd)}")
    return subprocess.run(cmd).returncode


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
