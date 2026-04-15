"""CLI job for ``dev-hops metrics release-impact``."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, timedelta

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.release_impact import compute_release_impact_daily
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)

logger = logging.getLogger(__name__)


def _date_range(end_day: date, backfill_days: int) -> list[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


async def run_release_impact_job(
    *,
    db_url: str,
    day: date,
    backfill_days: int = 1,
    recomputation_window_days: int = 7,
    org_id: str = "",
) -> int:
    if not db_url:
        raise ValueError("ClickHouse URI is required (set CLICKHOUSE_URI).")

    sink = ClickHouseMetricsSink(dsn=db_url)
    total = 0

    for d in _date_range(day, backfill_days):
        written = await compute_release_impact_daily(
            ch_client=sink.client,
            sink=sink,
            org_id=org_id,
            day=d,
            recomputation_window_days=recomputation_window_days,
        )
        total += written

    logger.info("release-impact job complete: %d total records written", total)
    return total


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "release-impact",
        help="Compute release impact daily metrics from telemetry signal buckets.",
    )
    add_date_range_args(parser)
    add_sink_arg(parser)
    parser.add_argument(
        "--recomputation-window",
        type=int,
        default=7,
        dest="recomputation_window_days",
        help="Number of days to recompute on each run (default: 7).",
    )
    parser.set_defaults(func=_cmd_release_impact)


async def _cmd_release_impact(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        await run_release_impact_job(
            db_url=resolve_sink_uri(ns),
            day=end_day,
            backfill_days=backfill_days,
            recomputation_window_days=ns.recomputation_window_days,
            org_id=getattr(ns, "org", None) or "",
        )
        return 0
    except Exception as e:
        logger.error("Release impact metrics job failed: %s", e)
        return 1
