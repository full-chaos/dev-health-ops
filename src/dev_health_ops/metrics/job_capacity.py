from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import replace
from datetime import date

from dev_health_ops.metrics.capacity_queries import (
    discover_team_scopes,
    get_backlog_from_sink,
    load_throughput_from_sink,
)
from dev_health_ops.metrics.compute_capacity import (
    ForecastResult,
    forecast_capacity,
)
from dev_health_ops.metrics.schemas import CapacityForecastRecord
from dev_health_ops.metrics.sinks.factory import create_sink

logger = logging.getLogger(__name__)


def _result_to_record(result: ForecastResult) -> CapacityForecastRecord:
    return CapacityForecastRecord(
        forecast_id=result.forecast_id,
        computed_at=result.computed_at,
        team_id=result.team_id,
        work_scope_id=result.work_scope_id,
        backlog_size=result.backlog_size,
        target_items=result.target_items,
        target_date=result.target_date,
        history_days=result.history_days,
        simulation_count=result.simulation_count,
        p50_days=result.p50_days,
        p85_days=result.p85_days,
        p95_days=result.p95_days,
        p50_date=result.p50_date,
        p85_date=result.p85_date,
        p95_date=result.p95_date,
        p50_items=result.p50_items,
        p85_items=result.p85_items,
        p95_items=result.p95_items,
        throughput_mean=result.throughput_mean,
        throughput_stddev=result.throughput_stddev,
        insufficient_history=result.insufficient_history,
        high_variance=result.high_variance,
    )


async def run_capacity_forecast(
    db_url: str,
    org_id: str,
    team_id: str | None = None,
    work_scope_id: str | None = None,
    target_items: int | None = None,
    target_date: date | None = None,
    history_days: int = 90,
    simulations: int = 10000,
    all_teams: bool = False,
    persist: bool = True,
) -> list[ForecastResult]:
    if not org_id:
        raise ValueError("org_id is required for capacity forecast")

    sink = create_sink(db_url)
    try:
        setattr(sink, "org_id", org_id)
        logger.info("Running capacity forecast for org_id=%s", org_id)
        results: list[ForecastResult] = []

        if all_teams:
            scopes = await discover_team_scopes(sink)
            logger.info(f"Discovered {len(scopes)} team/scope combinations")
        else:
            scopes = [(team_id, work_scope_id)]

        for tid, wsid in scopes:
            logger.info(f"Computing forecast for team={tid}, scope={wsid}")

            history = await load_throughput_from_sink(
                sink, team_id=tid, work_scope_id=wsid, history_days=history_days
            )

            if not history.daily_throughputs:
                logger.warning(f"No throughput history for team={tid}, scope={wsid}")
                continue

            backlog = await get_backlog_from_sink(sink, team_id=tid, work_scope_id=wsid)

            items = target_items if target_items else backlog
            if items <= 0:
                logger.warning(f"No target items for team={tid}, scope={wsid}")
                continue

            result = forecast_capacity(
                history=history,
                target_items=items,
                target_date=target_date,
                backlog_size=backlog,
                team_id=tid,
                work_scope_id=wsid,
                simulations=simulations,
            )
            results.append(result)

            if result.insufficient_history:
                logger.warning(
                    f"Insufficient history ({result.history_days} days) for team={tid}"
                )
            if result.high_variance:
                logger.warning(f"High throughput variance detected for team={tid}")

        if persist and results:
            records = [
                replace(r, org_id=org_id)
                for r in (_result_to_record(r) for r in results)
            ]
            sink.write_capacity_forecasts(records)
            logger.info(f"Persisted {len(records)} forecast(s)")

        return results

    finally:
        sink.close()


def _print_forecast(result: ForecastResult) -> None:
    print(f"\n{'=' * 60}")
    print(f"Capacity Forecast: {result.forecast_id[:8]}")
    print(f"{'=' * 60}")
    print(f"Team: {result.team_id or 'All'}")
    print(f"Scope: {result.work_scope_id or 'All'}")
    print(f"Backlog: {result.backlog_size} items")
    print(f"History: {result.history_days} days")
    print(
        f"Throughput: {result.throughput_mean:.2f} ± {result.throughput_stddev:.2f} items/day"
    )

    if result.target_items:
        print(f"\nTarget: Complete {result.target_items} items")
        print(f"  P50 (optimistic):   {result.p50_days} days → {result.p50_date}")
        print(f"  P85 (planning):     {result.p85_days} days → {result.p85_date}")
        print(f"  P95 (conservative): {result.p95_days} days → {result.p95_date}")

    if result.target_date:
        print(f"\nTarget: By {result.target_date}")
        print(f"  P50 (optimistic):   {result.p50_items} items")
        print(f"  P85 (likely):       {result.p85_items} items")
        print(f"  P95 (minimum):      {result.p95_items} items")

    if result.insufficient_history:
        print("\n⚠️  WARNING: Insufficient history for reliable forecast")
    if result.high_variance:
        print("\n⚠️  WARNING: High throughput variance detected")


async def _run_cli(args: argparse.Namespace) -> int:
    from dev_health_ops.metrics.sinks.factory import detect_backend

    detect_backend(args.db)

    target_date = None
    if args.target_date:
        target_date = date.fromisoformat(args.target_date)

    results = await run_capacity_forecast(
        db_url=args.db,
        team_id=args.team_id,
        work_scope_id=args.work_scope_id,
        target_items=args.target_items,
        target_date=target_date,
        history_days=args.history_days,
        simulations=args.simulations,
        all_teams=args.all_teams,
        persist=not args.dry_run,
        org_id=args.org or "",
    )

    if not results:
        print("No forecasts generated. Check logs for warnings.")
        return 1

    for result in results:
        _print_forecast(result)

    return 0


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    capacity_parser = subparsers.add_parser(
        "capacity",
        help="Compute capacity forecasts using Monte Carlo simulation",
    )
    capacity_parser.add_argument(
        "--db",
        required=True,
        help="Database connection string",
    )
    capacity_parser.add_argument(
        "--org",
        help="Organization ID for tenant-scoped forecast queries",
    )
    capacity_parser.add_argument(
        "--team-id",
        help="Filter by team ID",
    )
    capacity_parser.add_argument(
        "--work-scope-id",
        help="Filter by work scope ID (project/board)",
    )
    capacity_parser.add_argument(
        "--target-items",
        type=int,
        help="Number of items to complete (defaults to current backlog)",
    )
    capacity_parser.add_argument(
        "--target-date",
        help="Target deadline (YYYY-MM-DD format)",
    )
    capacity_parser.add_argument(
        "--history-days",
        type=int,
        default=90,
        help="Days of history to use (default: 90)",
    )
    capacity_parser.add_argument(
        "--simulations",
        type=int,
        default=10000,
        help="Number of Monte Carlo simulations (default: 10000)",
    )
    capacity_parser.add_argument(
        "--all-teams",
        action="store_true",
        help="Compute forecasts for all discovered team/scope combinations",
    )
    capacity_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print forecasts without persisting to database",
    )
    capacity_parser.set_defaults(func=lambda args: asyncio.run(_run_cli(args)))
