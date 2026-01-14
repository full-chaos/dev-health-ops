from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from .client import query_dicts


async def fetch_last_ingested_at(client: Any) -> Optional[str]:
    query = """
        SELECT max(computed_at) AS last_ingested_at
        FROM repo_metrics_daily
    """
    rows = await query_dicts(client, query, {})
    if not rows:
        return None
    value = rows[0].get("last_ingested_at")
    if value is None:
        return None
    return value


async def fetch_coverage(
    client: Any,
    *,
    start_day: date,
    end_day: date,
) -> Dict[str, float]:
    repos_query = """
        SELECT countDistinct(id) AS total
        FROM repos
    """
    repos_rows = await query_dicts(client, repos_query, {})
    total_repos = float((repos_rows[0].get("total") or 0) if repos_rows else 0)

    covered_query = """
        SELECT countDistinct(repo_id) AS covered
        FROM repo_metrics_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
    """
    covered_rows = await query_dicts(
        client, covered_query, {"start_day": start_day, "end_day": end_day}
    )
    covered = float((covered_rows[0].get("covered") or 0) if covered_rows else 0)
    repos_covered_pct = (covered / total_repos * 100.0) if total_repos else 0.0

    pr_link_query = """
        SELECT
            countIf(work_scope_id != '') AS linked,
            count(*) AS total
        FROM work_item_cycle_times
        WHERE day >= %(start_day)s AND day < %(end_day)s
    """
    pr_rows = await query_dicts(
        client, pr_link_query, {"start_day": start_day, "end_day": end_day}
    )
    linked = float((pr_rows[0].get("linked") or 0) if pr_rows else 0)
    total = float((pr_rows[0].get("total") or 0) if pr_rows else 0)
    prs_linked_pct = (linked / total * 100.0) if total else 0.0

    cycle_query = """
        SELECT
            countIf(cycle_time_hours IS NOT NULL) AS with_cycle,
            count(*) AS total
        FROM work_item_cycle_times
        WHERE day >= %(start_day)s AND day < %(end_day)s
    """
    cycle_rows = await query_dicts(
        client, cycle_query, {"start_day": start_day, "end_day": end_day}
    )
    with_cycle = float((cycle_rows[0].get("with_cycle") or 0) if cycle_rows else 0)
    total_cycle = float((cycle_rows[0].get("total") or 0) if cycle_rows else 0)
    issues_cycle_pct = (with_cycle / total_cycle * 100.0) if total_cycle else 0.0

    return {
        "repos_covered_pct": repos_covered_pct,
        "prs_linked_to_issues_pct": prs_linked_pct,
        "issues_with_cycle_states_pct": issues_cycle_pct,
    }
