from __future__ import annotations

import argparse
import logging
from datetime import timedelta

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)

from .runner import run_backfill_for_config

logger = logging.getLogger(__name__)


def register_backfill_commands(subparsers: argparse._SubParsersAction) -> None:
    backfill_parser = subparsers.add_parser(
        "backfill", help="Historical backfill operations."
    )
    backfill_subparsers = backfill_parser.add_subparsers(
        dest="backfill_command", required=True
    )

    run_parser = backfill_subparsers.add_parser("run", help="Run historical backfill.")
    run_parser.add_argument(
        "--config-id", required=True, help="Sync configuration UUID"
    )
    add_date_range_args(run_parser)
    add_sink_arg(run_parser)
    run_parser.set_defaults(func=_cmd_backfill_run)


def _cmd_backfill_run(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        since = end_day - timedelta(days=backfill_days - 1)
        before = end_day
        org_id = str(getattr(ns, "org", "") or "")
        if not org_id:
            raise ValueError("Organization ID is required")

        def _progress(index: int, total: int, window_since, window_before) -> None:
            print(
                f"Syncing window {index}/{total}: {window_since.isoformat()} to {window_before.isoformat()}..."
            )

        run_backfill_for_config(
            db_url=resolve_sink_uri(ns),
            sync_config_id=ns.config_id,
            org_id=org_id,
            since=since,
            before=before,
            sink=ns.sink,
            chunk_days=7,
            progress_cb=_progress,
        )
        return 0
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        return 1
