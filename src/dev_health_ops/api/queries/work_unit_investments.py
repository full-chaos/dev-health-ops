from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any, TypeVar

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts
from .investment import LATEST_WORK_UNIT_INVESTMENTS_CTE
from .investment_membership_scope import record_stale_investment_membership_scope

_LOOKUP_CHUNK_SIZE = 250
T = TypeVar("T")


def _unique_non_empty(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _chunks(values: list[T], size: int = _LOOKUP_CHUNK_SIZE) -> Iterable[list[T]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


async def fetch_work_unit_investments(
    sink: BaseMetricsSink,
    *,
    start_ts: datetime,
    end_ts: datetime,
    repo_ids: list[str] | None,
    limit: int,
    work_unit_id: str | None = None,
    org_id: str = "",
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"start_ts": start_ts, "end_ts": end_ts, "limit": limit}
    params["org_id"] = org_id
    # ClickHouse may prefer alias over column names in WHERE; always qualify columns
    # to avoid accidentally referencing argMax(...) aliases.
    filters: list[str] = [
        "work_unit_investments.from_ts < %(end_ts)s",
        "work_unit_investments.to_ts >= %(start_ts)s",
        "work_unit_investments.org_id = %(org_id)s",
    ]
    if repo_ids:
        filters.append("work_unit_investments.repo_id IN %(repo_ids)s")
        params["repo_ids"] = repo_ids
    if work_unit_id:
        filters.append("work_unit_investments.work_unit_id = %(work_unit_id)s")
        params["work_unit_id"] = work_unit_id
    where_sql = " AND ".join(filters)
    query = f"""
        WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}
        SELECT
            work_unit_id,
            work_unit_type,
            work_unit_name,
            from_ts,
            to_ts,
            repo_id,
            provider,
            effort_metric,
            effort_value,
            theme_distribution_json,
            subcategory_distribution_json,
            structural_evidence_json,
            evidence_quality,
            evidence_quality_band,
            categorization_status,
            categorization_model_version,
            categorization_run_id,
            latest_computed_at AS computed_at
        FROM latest_work_unit_investments AS work_unit_investments
        WHERE {where_sql}
        ORDER BY effort_value DESC, work_unit_id ASC
        LIMIT %(limit)s
    """
    await record_stale_investment_membership_scope(sink, org_id=org_id)
    return await query_dicts(sink, query, params)


async def fetch_repo_scopes(
    sink: BaseMetricsSink,
    *,
    repo_ids: Iterable[str],
    org_id: str = "",
) -> dict[str, str]:
    ids = _unique_non_empty(repo_ids)
    if not ids:
        return {}
    query = """
        SELECT
            toString(id) AS repo_id,
            repo
        FROM repos
        WHERE id IN {repo_ids:Array(String)}
          AND org_id = {org_id:String}
    """
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(ids):
        rows.extend(
            await query_dicts(sink, query, {"repo_ids": chunk, "org_id": org_id})
        )
    return {
        str(row.get("repo_id")): str(row.get("repo") or "")
        for row in rows
        if row.get("repo_id")
    }


async def fetch_work_item_team_assignments(
    sink: BaseMetricsSink,
    *,
    work_item_ids: Iterable[str],
    org_id: str = "",
) -> dict[str, dict[str, str]]:
    ids = _unique_non_empty(work_item_ids)
    if not ids:
        return {}
    query = """
        SELECT
            work_item_id,
            argMax(team_id, computed_at) AS team_id,
            argMax(team_name, computed_at) AS team_name
        FROM work_item_cycle_times
        WHERE work_item_id IN {work_item_ids:Array(String)}
          AND org_id = {org_id:String}
        GROUP BY work_item_id
    """
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(ids):
        rows.extend(
            await query_dicts(sink, query, {"work_item_ids": chunk, "org_id": org_id})
        )
    result: dict[str, dict[str, str]] = {}
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
    unit_runs: Iterable[tuple[str, str]],
    org_id: str = "",
) -> list[dict[str, Any]]:
    pairs = list(
        dict.fromkeys(
            (unit_id, run_id) for unit_id, run_id in unit_runs if unit_id and run_id
        )
    )
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
        WHERE (work_unit_id, categorization_run_id) IN {pairs:Array(Tuple(String, String))}
          AND org_id = {org_id:String}
    """
    rows: list[dict[str, Any]] = []
    for chunk in _chunks(pairs):
        rows.extend(await query_dicts(sink, query, {"pairs": chunk, "org_id": org_id}))
    return rows
