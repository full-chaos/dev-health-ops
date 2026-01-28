from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .client import query_dicts
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


async def fetch_work_unit_investments(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    repo_ids: Optional[List[str]],
    limit: int,
    work_unit_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    dialect = sink.dialect
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    # ClickHouse may prefer alias over column names in WHERE; always qualify columns
    # to avoid accidentally referencing argMax(...) aliases.
    filters: List[str] = [
        "work_unit_investments.from_ts < %(end_ts)s",
        "work_unit_investments.to_ts >= %(start_ts)s",
    ]
    if repo_ids:
        filters.append("work_unit_investments.repo_id IN %(repo_ids)s")
        params["repo_ids"] = repo_ids
    if work_unit_id:
        filters.append("work_unit_investments.work_unit_id = %(work_unit_id)s")
        params["work_unit_id"] = work_unit_id
    where_sql = " AND ".join(filters)

    def _am(col: str) -> str:
        return dialect.arg_max(col, "work_unit_investments.computed_at")

    query = f"""
        SELECT
            work_unit_id,
            {_am("work_unit_type")} AS work_unit_type,
            {_am("work_unit_name")} AS work_unit_name,
            {_am("from_ts")} AS from_ts,
            {_am("to_ts")} AS to_ts,
            {_am("repo_id")} AS repo_id,
            {_am("provider")} AS provider,
            {_am("effort_metric")} AS effort_metric,
            {_am("effort_value")} AS effort_value,
            {_am("theme_distribution_json")} AS theme_distribution_json,
            {_am("subcategory_distribution_json")} AS subcategory_distribution_json,
            {_am("structural_evidence_json")} AS structural_evidence_json,
            {_am("evidence_quality")} AS evidence_quality,
            {_am("evidence_quality_band")} AS evidence_quality_band,
            {_am("categorization_status")} AS categorization_status,
            {_am("categorization_run_id")} AS categorization_run_id,
            max(work_unit_investments.computed_at) AS computed_at
        FROM work_unit_investments
        WHERE {where_sql}
        GROUP BY work_unit_id
        ORDER BY effort_value DESC
        LIMIT %(limit)s
    """
    return await query_dicts(sink, query, params)


async def fetch_repo_scopes(
    sink: BaseMetricsSink,
    *,
    repo_ids: Iterable[str],
) -> Dict[str, str]:
    dialect = sink.dialect
    ids = [repo_id for repo_id in repo_ids if repo_id]
    if not ids:
        return {}
    query = f"""
        SELECT
            {dialect.to_string("id")} AS repo_id,
            repo
        FROM repos
        WHERE id IN %(repo_ids)s
    """
    rows = await query_dicts(sink, query, {"repo_ids": ids})
    return {
        str(row.get("repo_id")): str(row.get("repo") or "")
        for row in rows
        if row.get("repo_id")
    }


async def fetch_work_item_team_assignments(
    sink: BaseMetricsSink,
    *,
    work_item_ids: Iterable[str],
) -> Dict[str, Dict[str, str]]:
    dialect = sink.dialect
    ids = [work_item_id for work_item_id in work_item_ids if work_item_id]
    if not ids:
        return {}
    query = f"""
        SELECT
            work_item_id,
            {dialect.arg_max("team_id", "computed_at")} AS team_id,
            {dialect.arg_max("team_name", "computed_at")} AS team_name
        FROM work_item_cycle_times
        WHERE work_item_id IN %(work_item_ids)s
        GROUP BY work_item_id
    """
    rows = await query_dicts(sink, query, {"work_item_ids": ids})
    result: Dict[str, Dict[str, str]] = {}
    for row in rows:
        work_item_id = str(row.get("work_item_id") or "")
        if not work_item_id:
            continue
        team_id = str(row.get("team_id") or "")
        team_name = str(row.get("team_name") or "")
        result[work_item_id] = {"team_id": team_id, "team_name": team_name}
    return result


async def fetch_work_unit_investment_quotes(
    sink: BaseMetricsSink,
    *,
    unit_runs: Iterable[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    pairs = [(unit_id, run_id) for unit_id, run_id in unit_runs if unit_id and run_id]
    if not pairs:
        return []
    query = """
        SELECT
            work_unit_id,
            quote,
            source_type,
            source_id,
            categorization_run_id
        FROM work_unit_investment_quotes
        WHERE (work_unit_id, categorization_run_id) IN %(pairs)s
    """
    return await query_dicts(sink, query, {"pairs": pairs})
