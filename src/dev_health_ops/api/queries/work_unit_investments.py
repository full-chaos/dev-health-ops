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
    query = f"""
        SELECT
            work_unit_id,
            argMax(work_unit_type, work_unit_investments.computed_at) AS work_unit_type,
            argMax(work_unit_name, work_unit_investments.computed_at) AS work_unit_name,
            argMax(from_ts, work_unit_investments.computed_at) AS from_ts,
            argMax(to_ts, work_unit_investments.computed_at) AS to_ts,
            argMax(repo_id, work_unit_investments.computed_at) AS repo_id,
            argMax(provider, work_unit_investments.computed_at) AS provider,
            argMax(effort_metric, work_unit_investments.computed_at) AS effort_metric,
            argMax(effort_value, work_unit_investments.computed_at) AS effort_value,
            argMax(theme_distribution_json, work_unit_investments.computed_at) AS theme_distribution_json,
            argMax(subcategory_distribution_json, work_unit_investments.computed_at) AS subcategory_distribution_json,
            argMax(structural_evidence_json, work_unit_investments.computed_at) AS structural_evidence_json,
            argMax(evidence_quality, work_unit_investments.computed_at) AS evidence_quality,
            argMax(evidence_quality_band, work_unit_investments.computed_at) AS evidence_quality_band,
            argMax(categorization_status, work_unit_investments.computed_at) AS categorization_status,
            argMax(categorization_run_id, work_unit_investments.computed_at) AS categorization_run_id,
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
    ids = [repo_id for repo_id in repo_ids if repo_id]
    if not ids:
        return {}
    query = """
        SELECT
            toString(id) AS repo_id,
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
    ids = [work_item_id for work_item_id in work_item_ids if work_item_id]
    if not ids:
        return {}
    query = """
        SELECT
            work_item_id,
            argMax(team_id, computed_at) AS team_id,
            argMax(team_name, computed_at) AS team_name
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
