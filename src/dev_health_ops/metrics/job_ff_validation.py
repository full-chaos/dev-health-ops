"""CLI job for ``dev-hops metrics validate-flags``."""

from __future__ import annotations

import argparse
import asyncio
import logging

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.ff_validation import (
    ValidationReport,
    format_report,
    validate_flag_pipeline,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.utils.cli import add_sink_arg, validate_sink

logger = logging.getLogger(__name__)


async def run_validate_flags(
    *,
    db_url: str,
    org_id: str = "",
    lookback_days: int = 30,
) -> ValidationReport:
    if not db_url:
        raise ValueError("ClickHouse URI is required (set CLICKHOUSE_URI).")

    sink = ClickHouseMetricsSink(dsn=db_url)
    return await validate_flag_pipeline(
        ch_client=sink.client,
        org_id=org_id,
        lookback_days=lookback_days,
    )


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "validate-flags",
        help="Run feature-flag pipeline validation checks.",
    )
    add_sink_arg(parser)
    parser.add_argument(
        "--lookback",
        type=int,
        default=30,
        dest="lookback_days",
        help="Number of days to inspect (default: 30).",
    )
    parser.set_defaults(func=_cmd_validate_flags)


async def _cmd_validate_flags(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        report = await run_validate_flags(
            db_url=resolve_sink_uri(ns),
            org_id=getattr(ns, "org", None) or "",
            lookback_days=ns.lookback_days,
        )
        print(format_report(report))
        return 1 if report.has_critical else 0
    except Exception as e:
        logger.error("Flag validation failed: %s", e)
        return 1
