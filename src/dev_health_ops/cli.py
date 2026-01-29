#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
from pathlib import Path
from typing import List, Optional


# Runner and registration modules
from dev_health_ops.processors import sync as sync_processor
from dev_health_ops.providers import teams as teams_provider
from dev_health_ops.fixtures import runner as fixtures_runner
from dev_health_ops.work_graph import runner as work_graph_runner
from dev_health_ops.workers import runner as workers_runner
from dev_health_ops.api import runner as api_runner
from dev_health_ops.metrics import (
    job_work_items,
    job_daily,
    job_complexity_db,
    job_dora,
    job_capacity,
)
from dev_health_ops.audit import completeness, schema, perf, coverage

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_dotenv(path: Path) -> int:
    """
    Load a .env file into process environment (without overriding existing vars).
    Keeps dependencies minimal (avoids python-dotenv).
    """
    if not path.exists():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (len(value) >= 2) and ((value[0] == value[-1]) and value[0] in {"'", '"'}):
            value = value[1:-1]
        os.environ[key] = value
        loaded += 1
    return loaded


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dev-health-ops",
        description="Sync git data and compute developer health metrics.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING). Defaults to env LOG_LEVEL or INFO.",
    )
    from dev_health_ops.llm.cli import add_llm_arguments

    add_llm_arguments(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- sync ----
    sync_parser = sub.add_parser("sync", help="Sync data from various sources.")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    # Register sync commands (git, prs, blame, cicd, deployments, incidents)
    sync_processor.register_commands(sync_subparsers)
    # Register team sync
    teams_provider.register_commands(sync_subparsers)
    # Register work-items sync
    job_work_items.register_commands(sync_subparsers)

    # ---- metrics ----
    metrics_parser = sub.add_parser("metrics", help="Compute metrics.")
    metrics_subparsers = metrics_parser.add_subparsers(
        dest="metrics_command", required=True
    )

    job_daily.register_commands(metrics_subparsers)
    job_complexity_db.register_commands(metrics_subparsers)
    job_dora.register_commands(metrics_subparsers)
    job_capacity.register_commands(metrics_subparsers)

    # ---- audit ----
    audit_parser = sub.add_parser("audit", help="Run diagnostic audits.")
    audit_subparsers = audit_parser.add_subparsers(dest="audit_command", required=True)

    completeness.register_commands(audit_subparsers)
    schema.register_commands(audit_subparsers)
    perf.register_commands(audit_subparsers)
    coverage.register_commands(audit_subparsers)

    # ---- fixtures ----
    fixtures_runner.register_commands(sub)

    # ---- api ----
    api_runner.register_commands(sub)

    # ---- work-graph & investment ----
    work_graph_runner.register_commands(sub)

    # ---- workers ----
    workers_parser = sub.add_parser(
        "workers", help="Manage background worker processes."
    )
    workers_subparsers = workers_parser.add_subparsers(
        dest="workers_command", required=True
    )
    workers_runner.register_commands(workers_subparsers)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    if os.getenv("DISABLE_DOTENV", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        _load_dotenv(REPO_ROOT / ".env")

    parser = build_parser()
    ns = parser.parse_args(argv)

    level_name = str(getattr(ns, "log_level", "") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    func = getattr(ns, "func", None)
    if func is None:
        parser.print_help()
        return 2
    if inspect.iscoroutinefunction(func):
        return asyncio.run(func(ns))
    return int(func(ns))


if __name__ == "__main__":
    raise SystemExit(main())
