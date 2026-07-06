from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any, Protocol

from dev_health_ops.metrics.compute_capacity import ThroughputHistory, ThroughputSample
from dev_health_ops.utils.datetime import utc_today


class CapacityQuerySink(Protocol):
    @property
    def backend_type(self) -> str: ...

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]: ...


async def load_throughput_from_sink(
    sink: CapacityQuerySink,
    team_id: str | None = None,
    work_scope_id: str | None = None,
    history_days: int = 90,
) -> ThroughputHistory:
    backend = sink.backend_type
    start_date = utc_today() - timedelta(days=history_days)

    conditions = [f"day >= '{start_date.isoformat()}'"]
    params = {}
    org_id = str(getattr(sink, "org_id", "") or "")

    if org_id:
        if backend == "clickhouse":
            conditions.append("org_id = {org_id:String}")
        else:
            conditions.append("org_id = :org_id")
        params["org_id"] = org_id

    if team_id:
        if backend == "clickhouse":
            conditions.append("team_id = {team_id:String}")
        else:
            conditions.append("team_id = :team_id")
        params["team_id"] = team_id
    if work_scope_id:
        if backend == "clickhouse":
            conditions.append("work_scope_id = {work_scope_id:String}")
        else:
            conditions.append("work_scope_id = :work_scope_id")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT day, SUM(items_completed) as items_completed
        FROM work_item_metrics_daily FINAL
        WHERE {where_clause}
        GROUP BY day
        ORDER BY day
    """

    rows = await asyncio.to_thread(sink.query_dicts, query, params)

    samples = [
        ThroughputSample(
            day=row["day"]
            if isinstance(row["day"], date)
            else date.fromisoformat(str(row["day"])),
            items_completed=int(row["items_completed"]),
            team_id=team_id,
            work_scope_id=work_scope_id,
        )
        for row in rows
    ]

    return ThroughputHistory(samples)


async def get_backlog_from_sink(
    sink: CapacityQuerySink,
    team_id: str | None = None,
    work_scope_id: str | None = None,
) -> int:
    backend = sink.backend_type
    conditions = []
    params = {}
    org_id = str(getattr(sink, "org_id", "") or "")

    if org_id:
        if backend == "clickhouse":
            conditions.append("org_id = {org_id:String}")
        else:
            conditions.append("org_id = :org_id")
        params["org_id"] = org_id

    if team_id:
        if backend == "clickhouse":
            conditions.append("team_id = {team_id:String}")
        else:
            conditions.append("team_id = :team_id")
        params["team_id"] = team_id
    if work_scope_id:
        if backend == "clickhouse":
            conditions.append("work_scope_id = {work_scope_id:String}")
        else:
            conditions.append("work_scope_id = :work_scope_id")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT sum(wip_count_end_of_day) AS wip_count_end_of_day
        FROM work_item_metrics_daily FINAL
        WHERE {where_clause}
          AND day = (
              SELECT max(day)
              FROM work_item_metrics_daily FINAL
              WHERE {where_clause}
          )
    """

    rows = await asyncio.to_thread(sink.query_dicts, query, params)
    if rows:
        return int(rows[0].get("wip_count_end_of_day") or 0)
    return 0


async def discover_team_scopes(
    sink: CapacityQuerySink,
) -> list[tuple[str | None, str | None]]:
    backend = sink.backend_type
    org_id = str(getattr(sink, "org_id", "") or "")

    if backend == "clickhouse":
        org_filter = "AND org_id = {org_id:String}" if org_id else ""
    else:
        org_filter = "AND org_id = :org_id" if org_id else ""
    params = {"org_id": org_id} if org_id else {}

    query = f"""
        SELECT DISTINCT team_id, work_scope_id
        FROM work_item_metrics_daily FINAL
        WHERE day >= today() - 30
        {org_filter}
    """

    rows = await asyncio.to_thread(sink.query_dicts, query, params)
    return [
        (
            str(row.get("team_id")) if row.get("team_id") is not None else None,
            str(row.get("work_scope_id"))
            if row.get("work_scope_id") is not None
            else None,
        )
        for row in rows
    ]
