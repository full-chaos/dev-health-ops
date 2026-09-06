from __future__ import annotations

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
    seed: int | None = None,
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
                seed=seed,
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


# CHAOS-5307: `_print_forecast`/`_run_cli`/`register_commands` (the direct-
# Python-compute `dev-hops metrics capacity` CLI verb, plus its
# console-print helper and asyncio wrapper, both only ever called from the
# CLI handler itself) were deleted here. They were already 100% orphaned
# before this change -- CHAOS-5055/#2232 repointed `cli.py` to register
# `workerctl_dispatch.register_capacity_trigger_command` instead (dispatches
# through `dev-health-workerctl metrics remaining trigger-backstop --family
# capacity`), and nothing anywhere in the repo still called these functions
# or `job_capacity.register_commands` by name (verified by repo-wide search
# before deletion). `run_capacity_forecast` above is NOT dead -- it still
# has live callers (the worker bridge, the GraphQL capacity resolver,
# tests) -- only the unwired CLI wrapper functions were removed.
