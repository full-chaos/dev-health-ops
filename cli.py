#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
import subprocess
from pathlib import Path
from typing import List, Optional


# Runner and registration modules
import processors.sync
import providers.teams
import fixtures.runner
import work_graph.runner
import api.runner
import metrics.job_work_items
import metrics.job_daily
import metrics.job_complexity_db
import audit.completeness
import audit.schema
import audit.perf
import audit.coverage

REPO_ROOT = Path(__file__).resolve().parent


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


def _cmd_grafana_up(_ns: argparse.Namespace) -> int:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(REPO_ROOT / "compose.yml"),
        "up",
        "-d",
    ]
    return subprocess.run(cmd, check=False).returncode


def _cmd_grafana_down(_ns: argparse.Namespace) -> int:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(REPO_ROOT / "compose.yml"),
        "down",
    ]
    return subprocess.run(cmd, check=False).returncode


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
    from cli_shared import add_llm_arguments

    add_llm_arguments(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- sync ----
    sync_parser = sub.add_parser("sync", help="Sync data from various sources.")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    # Register sync commands (git, prs, blame, cicd, deployments, incidents)
    processors.sync.register_commands(sync_subparsers)
    # Register team sync
    providers.teams.register_commands(sync_subparsers)
    # Register work-items sync
    metrics.job_work_items.register_commands(sync_subparsers)

    # ---- metrics ----
    metrics_parser = sub.add_parser("metrics", help="Compute metrics.")
    metrics_subparsers = metrics_parser.add_subparsers(
        dest="metrics_command", required=True
    )

    metrics.job_daily.register_commands(metrics_subparsers)
    metrics.job_complexity_db.register_commands(metrics_subparsers)

    # ---- audit ----
    audit_parser = sub.add_parser("audit", help="Run diagnostic audits.")
    audit_subparsers = audit_parser.add_subparsers(dest="audit_command", required=True)

    audit.completeness.register_commands(audit_subparsers)
    audit.schema.register_commands(audit_subparsers)
    audit.perf.register_commands(audit_subparsers)
    audit.coverage.register_commands(audit_subparsers)

    # ---- fixtures ----
    fixtures.runner.register_commands(sub)

    # ---- api ----
    api.runner.register_commands(sub)

    # ---- work-graph & investment ----
    work_graph.runner.register_commands(sub)

    # ---- grafana ----
    graf = sub.add_parser(
        "grafana", help="Start/stop the Grafana + ClickHouse dev stack."
    )
    graf_sub = graf.add_subparsers(dest="grafana_command", required=True)
    graf_up = graf_sub.add_parser(
        "up", help="docker compose up -d for grafana/docker-compose.yml"
    )
    graf_up.set_defaults(func=_cmd_grafana_up)
    graf_down = graf_sub.add_parser(
        "down", help="docker compose down for grafana/docker-compose.yml"
    )
    graf_down.set_defaults(func=_cmd_grafana_down)

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
