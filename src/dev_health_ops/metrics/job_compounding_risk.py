"""Standalone CLI job for Compounding Risk (CHAOS-1641).

Backfills or recomputes ``compounding_risk_daily`` rows for one or more days
WITHOUT re-running the full ``dev-hops metrics daily`` pipeline. Reads the
already-persisted ``repo_metrics_daily`` + ``repo_complexity_daily`` rows
and emits one REPO-scope row per repo per day.

CHAOS-5084/no-straddle (#2275 v2): this job used to ALSO emit team-scope
rows (resolving repo-to-team via ``_merge_repo_to_team``/pattern fallback,
same as job_daily.py's own now-deleted finalize-step writer). Removed:
CompoundingRiskTeamExecutor (Go) is the sole writer for team-scope
compounding_risk_daily now, with no Python compute anywhere, including this
standalone manual-backfill tool -- a second, still-callable Python producer
of a family that becomes native is exactly the straddle this port forbids,
even for a tool that only runs by hand. A manual team-scope backfill is the
Go worker's job now, not this CLI's.

Usage:
    dev-hops metrics compounding-risk --org ORG [--since YYYY-MM-DD] [--before YYYY-MM-DD] [--backfill N]
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.compounding_risk import (
    MISSING_INPUT_REASONS,
    build_compounding_risk_rows_for_day,
    summarize_compounding_risk_diagnostics,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.storage import detect_db_type
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


def _fetch_repo_metrics_for_day(sink: Any, org_id: str, day: date) -> list[Any]:
    """Read the latest ``repo_metrics_daily`` rows for ``day`` as plain dicts.

    Returned objects are duck-typed enough to satisfy
    ``build_compounding_risk_rows_for_day`` — it only reads attributes via
    ``getattr(row, name, None)``.
    """

    class _Row:
        __slots__ = (
            "repo_id",
            "rework_churn_ratio_30d",
            "single_owner_file_ratio_30d",
            "code_ownership_gini",
            "bus_factor",
            "pr_first_review_p90_hours",
        )

        def __init__(self, d: dict[str, Any]) -> None:
            self.repo_id = d.get("repo_id")
            self.rework_churn_ratio_30d = d.get("rework_churn_ratio_30d")
            self.single_owner_file_ratio_30d = d.get("single_owner_file_ratio_30d")
            self.code_ownership_gini = d.get("code_ownership_gini")
            self.bus_factor = d.get("bus_factor")
            self.pr_first_review_p90_hours = d.get("pr_first_review_p90_hours")

    query = """
        SELECT
            repo_id,
            argMax(rework_churn_ratio_30d,    computed_at) AS rework_churn_ratio_30d,
            argMax(single_owner_file_ratio_30d, computed_at) AS single_owner_file_ratio_30d,
            argMax(code_ownership_gini,       computed_at) AS code_ownership_gini,
            argMax(bus_factor,                computed_at) AS bus_factor,
            argMax(pr_first_review_p90_hours, computed_at) AS pr_first_review_p90_hours
        FROM repo_metrics_daily
        WHERE org_id = {org_id:String} AND day = {day:Date}
        GROUP BY repo_id
    """
    raw = sink.query_dicts(query, {"org_id": org_id, "day": day})
    return [_Row(r) for r in raw]


async def run_compounding_risk_job(
    *,
    db_url: str,
    day: date,
    backfill_days: int,
    org_id: str,
) -> int:
    """Recompute and persist ``compounding_risk_daily`` for the date range."""
    if not org_id:
        raise ValueError("--org is required for compounding-risk")
    if not db_url:
        raise ValueError("Database URI is required (set CLICKHOUSE_URI).")
    backend = detect_db_type(db_url)
    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. ClickHouse only (CHAOS-641)."
        )

    sink = ClickHouseMetricsSink(db_url)
    setattr(sink, "org_id", org_id)
    if hasattr(sink, "ensure_tables"):
        sink.ensure_tables()

    computed_at = datetime.now(timezone.utc)

    total_rows = 0
    aggregate_non_null = 0
    aggregate_unknown = 0
    # Seed every fixed reason key at 0 so the aggregate has a stable shape
    # and key order regardless of which reasons actually occur (CHAOS-2888).
    aggregate_reason_counts: dict[str, int] = dict.fromkeys(MISSING_INPUT_REASONS, 0)
    days_with_no_repo_rows: list[str] = []
    for d in _date_range(day, backfill_days):
        repo_rows = _fetch_repo_metrics_for_day(sink, org_id, d)
        if not repo_rows:
            days_with_no_repo_rows.append(d.isoformat())
            logger.info(
                "compounding-risk: no repo_metrics_daily rows for day=%s org_id=%s",
                d.isoformat(),
                org_id,
            )
            continue
        # CHAOS-5084/no-straddle (#2275 v2): this loop used to ALSO load a
        # per-day team_repo_ownership map here and resolve repo-to-team
        # (_merge_repo_to_team over _load_repo_catalog_and_pattern_resolver's
        # run-scoped catalog/resolver, both deleted with this change) to feed
        # build_compounding_risk_rows_for_day's now-removed repo_to_team
        # parameter. Team-scope rows are Go-only now; this loop emits
        # repo-scope rows only.
        rows = build_compounding_risk_rows_for_day(
            sink=sink,
            day=d,
            org_id=org_id,
            repo_metrics_rows=repo_rows,
            computed_at=computed_at,
        )
        if rows:
            sink.write_compounding_risk_daily(rows)
            total_rows += len(rows)
            diagnostics = summarize_compounding_risk_diagnostics(rows)
            aggregate_non_null += diagnostics.non_null_rows
            aggregate_unknown += diagnostics.unknown_rows
            for reason, count in diagnostics.reason_counts.items():
                aggregate_reason_counts[reason] = (
                    aggregate_reason_counts.get(reason, 0) + count
                )
            logger.info(
                "compounding-risk: wrote %d repo-scope rows for day=%s "
                "(non_null=%d, unknown=%d, reasons=%s)",
                len(rows),
                d.isoformat(),
                diagnostics.non_null_rows,
                diagnostics.unknown_rows,
                diagnostics.reason_counts,
            )

    logger.info(
        "compounding-risk: done, %d rows written across %d day(s) "
        "(non_null=%d, unknown=%d, reasons=%s, days_with_no_repo_rows=%s)",
        total_rows,
        max(1, backfill_days),
        aggregate_non_null,
        aggregate_unknown,
        aggregate_reason_counts,
        days_with_no_repo_rows,
    )
    # CHAOS-4243: this used to unconditionally `return 0`, an exit-code
    # habit that silently discarded the row count this function computes
    # (total_rows) and just spent the previous ~15 lines logging. That made
    # `run_compounding_risk_job`'s int return lie about being a row count --
    # every caller, including the CLI wrapper below, always saw 0 regardless
    # of how many rows actually wrote. This function now returns its real
    # written count (matching the sibling "return the real row count"
    # pattern other daily jobs already used); _cmd_compounding_risk below is
    # updated to discard the count and always exit 0 on success, so this
    # change is CLI-exit-code neutral.
    return total_rows


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "compounding-risk",
        help="Compute the Compounding Risk composite from persisted inputs.",
        description=(
            "Reads repo_metrics_daily + repo_complexity_daily for the "
            "given day range and writes compounding_risk_daily."
        ),
    )
    add_date_range_args(p, include_deprecated_aliases=False)
    add_sink_arg(p)
    p.set_defaults(func=_cmd_compounding_risk)


async def _cmd_compounding_risk(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        db_url = resolve_sink_uri(ns)
        day, backfill_days = resolve_date_range(ns)
        org_id = getattr(ns, "org", None) or os.getenv("ORG_ID") or ""
        # CHAOS-4243: run_compounding_risk_job now returns the real rows-
        # written count, not a 0/1 exit code. This CLI wrapper discards the
        # count and always exits 0 on success -- a normal day writing rows
        # is not a CLI failure.
        await run_compounding_risk_job(
            db_url=db_url,
            day=day,
            backfill_days=backfill_days,
            org_id=org_id,
        )
        return 0
    except Exception as exc:
        logger.error("compounding-risk job failed: %s", exc)
        return 1
