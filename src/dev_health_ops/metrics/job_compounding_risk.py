"""Standalone CLI job for Compounding Risk (CHAOS-1641).

Backfills or recomputes ``compounding_risk_daily`` rows for one or more days
WITHOUT re-running the full ``dev-hops metrics daily`` pipeline. Reads the
already-persisted ``repo_metrics_daily`` + ``repo_complexity_daily`` rows
and emits one row per repo (and per team) per day.

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
    build_compounding_risk_rows_for_day,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.providers.teams import build_repo_pattern_resolver
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


async def _load_repo_to_team(sink: Any, org_id: str) -> dict[str, str]:
    """Build a ``{repo_id_str: team_id}`` map via the existing teams resolver."""
    try:
        teams = await sink.get_all_teams()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not load teams for compounding risk: %s", exc)
        return {}
    resolver = build_repo_pattern_resolver(teams or [])

    repos = sink.query_dicts(
        """
        SELECT toString(id) AS repo_id, argMax(repo, last_synced) AS full_name
        FROM repos
        WHERE org_id = {org_id:String}
        GROUP BY org_id, id
        """,
        {"org_id": org_id},
    )
    mapping: dict[str, str] = {}
    for row in repos:
        team_id, _ = resolver.resolve(row.get("full_name"))
        if team_id:
            mapping[row["repo_id"]] = team_id
    return mapping


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

    repo_to_team = await _load_repo_to_team(sink, org_id)
    computed_at = datetime.now(timezone.utc)

    total_rows = 0
    for d in _date_range(day, backfill_days):
        repo_rows = _fetch_repo_metrics_for_day(sink, org_id, d)
        if not repo_rows:
            logger.info(
                "compounding-risk: no repo_metrics_daily rows for day=%s org_id=%s",
                d.isoformat(),
                org_id,
            )
            continue
        rows = build_compounding_risk_rows_for_day(
            sink=sink,
            day=d,
            org_id=org_id,
            repo_metrics_rows=repo_rows,
            computed_at=computed_at,
            repo_to_team=repo_to_team or None,
        )
        if rows:
            sink.write_compounding_risk_daily(rows)
            total_rows += len(rows)
            logger.info(
                "compounding-risk: wrote %d rows for day=%s (repos=%d, teams=%d)",
                len(rows),
                d.isoformat(),
                sum(1 for r in rows if r.scope == "repo"),
                sum(1 for r in rows if r.scope == "team"),
            )

    logger.info(
        "compounding-risk: done, %d rows written across %d day(s)",
        total_rows,
        max(1, backfill_days),
    )
    return 0


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
        return await run_compounding_risk_job(
            db_url=db_url,
            day=day,
            backfill_days=backfill_days,
            org_id=org_id,
        )
    except Exception as exc:
        logger.error("compounding-risk job failed: %s", exc)
        return 1
