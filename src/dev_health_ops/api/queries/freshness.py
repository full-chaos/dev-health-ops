from __future__ import annotations

from datetime import date, datetime

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from .client import query_dicts


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            normalized = value.replace(" ", "T")
            return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _source_status(value: datetime | str | None, *, start_day: date) -> str:
    seen_at = _coerce_datetime(value)
    if seen_at is None:
        return "down"
    if seen_at.date() < start_day:
        return "degraded"
    return "ok"


async def fetch_last_ingested_at(
    sink: BaseMetricsSink, org_id: str = ""
) -> datetime | None:
    query = """
        SELECT maxOrNull(computed_at) AS last_ingested_at
        FROM repo_metrics_daily
        WHERE org_id = %(org_id)s
    """
    rows = await query_dicts(sink, query, {"org_id": org_id})
    if not rows:
        return None
    value = rows[0].get("last_ingested_at")
    if value is None:
        return None
    if isinstance(value, str):
        try:
            normalized = value.replace(" ", "T")
            return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            return None
    return value


async def fetch_coverage(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    end_day: date,
    org_id: str = "",
) -> dict[str, float]:
    repos_query = """
        SELECT countDistinct(id) AS total
        FROM repos
        WHERE org_id = %(org_id)s
    """
    repos_rows = await query_dicts(sink, repos_query, {"org_id": org_id})
    total_repos = float((repos_rows[0].get("total") or 0) if repos_rows else 0)

    covered_query = """
        SELECT countDistinct(repo_id) AS covered
        FROM repo_metrics_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
    """
    covered_rows = await query_dicts(
        sink,
        covered_query,
        {"start_day": start_day, "end_day": end_day, "org_id": org_id},
    )
    covered = float((covered_rows[0].get("covered") or 0) if covered_rows else 0)
    repos_covered_pct = (covered / total_repos * 100.0) if total_repos else 0.0

    pr_link_query = """
        SELECT
            countIf(work_scope_id != '') AS linked,
            count(*) AS total
        FROM work_item_cycle_times
        WHERE day >= %(start_day)s AND day < %(end_day)s
          AND org_id = %(org_id)s
    """
    pr_rows = await query_dicts(
        sink,
        pr_link_query,
        {"start_day": start_day, "end_day": end_day, "org_id": org_id},
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
          AND org_id = %(org_id)s
    """
    cycle_rows = await query_dicts(
        sink,
        cycle_query,
        {"start_day": start_day, "end_day": end_day, "org_id": org_id},
    )
    with_cycle = float((cycle_rows[0].get("with_cycle") or 0) if cycle_rows else 0)
    total_cycle = float((cycle_rows[0].get("total") or 0) if cycle_rows else 0)
    issues_cycle_pct = (with_cycle / total_cycle * 100.0) if total_cycle else 0.0

    return {
        "repos_covered_pct": repos_covered_pct,
        "prs_linked_to_issues_pct": prs_linked_pct,
        "issues_with_cycle_states_pct": issues_cycle_pct,
    }


async def fetch_source_statuses(
    sink: BaseMetricsSink,
    *,
    start_day: date,
    org_id: str = "",
) -> dict[str, str]:
    query = """
        SELECT source, max(last_seen_at) AS last_seen_at
        FROM (
            SELECT lower(provider) AS source, max(last_synced) AS last_seen_at
            FROM repos
            WHERE org_id = %(org_id)s
              AND lower(provider) NOT IN ('', 'unknown', 'synthetic')
            GROUP BY source

            UNION ALL

            SELECT lower(provider) AS source, max(last_synced) AS last_seen_at
            FROM work_items
            WHERE org_id = %(org_id)s
              AND lower(provider) NOT IN ('', 'unknown', 'synthetic')
            GROUP BY source

            UNION ALL

            SELECT 'ci' AS source, max(last_synced) AS last_seen_at
            FROM ci_pipeline_runs
            WHERE org_id = %(org_id)s
            HAVING count() > 0
        )
        GROUP BY source
        ORDER BY source
    """
    rows = await query_dicts(sink, query, {"org_id": org_id})
    return {
        str(row["source"]): _source_status(row.get("last_seen_at"), start_day=start_day)
        for row in rows
        if row.get("source")
    }
