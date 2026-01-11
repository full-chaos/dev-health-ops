from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .client import query_dicts


async def fetch_work_unit_investments(
    client: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    repo_ids: Optional[List[str]],
    limit: int,
    work_unit_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    filters: List[str] = ["from_ts < %(end_ts)s", "to_ts >= %(start_ts)s"]
    if repo_ids:
        filters.append("repo_id IN %(repo_ids)s")
        params["repo_ids"] = repo_ids
    if work_unit_id:
        filters.append("work_unit_id = %(work_unit_id)s")
        params["work_unit_id"] = work_unit_id
    where_sql = " AND ".join(filters)
    query = f"""
        SELECT
            work_unit_id,
            argMax(from_ts, computed_at) AS from_ts,
            argMax(to_ts, computed_at) AS to_ts,
            argMax(repo_id, computed_at) AS repo_id,
            argMax(provider, computed_at) AS provider,
            argMax(effort_metric, computed_at) AS effort_metric,
            argMax(effort_value, computed_at) AS effort_value,
            argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
            argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
            argMax(structural_evidence_json, computed_at) AS structural_evidence_json,
            argMax(evidence_quality, computed_at) AS evidence_quality,
            argMax(evidence_quality_band, computed_at) AS evidence_quality_band,
            argMax(categorization_status, computed_at) AS categorization_status,
            argMax(categorization_run_id, computed_at) AS categorization_run_id,
            argMax(computed_at, computed_at) AS computed_at
        FROM work_unit_investments
        WHERE {where_sql}
        GROUP BY work_unit_id
        ORDER BY effort_value DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_work_unit_investment_quotes(
    client: Any,
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
    return await query_dicts(client, query, {"pairs": pairs})
