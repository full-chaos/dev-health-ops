"""Home metrics resolver for GraphQL API."""

from __future__ import annotations

import logging
from typing import Any

from ..authz import require_org_id
from ..context import GraphQLContext

logger = logging.getLogger(__name__)


async def resolve_home(
    context: GraphQLContext,
    filters: Any | None = None,
) -> dict[str, Any]:
    """
    Resolve home dashboard metrics.

    This provides a GraphQL interface to the /api/v1/home endpoint data.

    Args:
        context: GraphQL request context.
        filters: Optional filters to apply.

    Returns:
        Dict with freshness, deltas, summary, tiles, constraint, and events.
    """
    require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    from dev_health_ops.api.queries.client import query_dicts
    from dev_health_ops.api.queries.metrics import (
        _INVESTMENT_THEME_LABELS,
        canonical_investment_theme_sql,
    )
    from dev_health_ops.investment_taxonomy import THEMES

    # Get freshness data
    freshness_sql = """
        SELECT
            max(computed_at) as last_ingested_at
        FROM investment_metrics_daily
        WHERE day >= today() - 30
          AND org_id = %(org_id)s
    """
    freshness_rows = await query_dicts(
        client, freshness_sql, {"org_id": context.org_id}
    )
    last_ingested = None
    if freshness_rows and freshness_rows[0].get("last_ingested_at"):
        last_ingested = freshness_rows[0]["last_ingested_at"]

    # Get metric deltas (comparing current period to previous)
    deltas_sql = """
        SELECT
            'throughput' as metric,
            'Throughput' as label,
            count(DISTINCT work_unit_id) as value,
            'units' as unit
        FROM work_unit_investments
        WHERE from_ts >= today() - 30
        AND from_ts < today()
        AND org_id = %(org_id)s
        UNION ALL
        SELECT
            'pr_rework_ratio' as metric,
            'PR Rework Ratio' as label,
            SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0) * 100.0 as value,
            '%' as unit
        FROM repo_metrics_daily
        WHERE day >= today() - 30
        AND day < today()
        AND org_id = %(org_id)s
    """
    delta_rows = await query_dicts(client, deltas_sql, {"org_id": context.org_id})

    canonical_theme_expr = canonical_investment_theme_sql("investment_area")
    rework_theme_sql = f"""
        SELECT
            canonical_theme AS theme,
            sum(work_items_completed) AS allocation,
            sum(prs_merged) AS prs_merged,
            sum(churn_loc) AS churn_loc
        FROM (
            SELECT
                day,
                repo_id,
                team_id,
                {canonical_theme_expr} AS canonical_theme,
                project_stream,
                argMax(work_items_completed, computed_at) AS work_items_completed,
                argMax(prs_merged, computed_at) AS prs_merged,
                argMax(churn_loc, computed_at) AS churn_loc
            FROM investment_metrics_daily
            WHERE day >= today() - 30
              AND day < today()
              AND org_id = %(org_id)s
            GROUP BY day, repo_id, team_id, canonical_theme, project_stream
        )
        WHERE canonical_theme != ''
        GROUP BY canonical_theme
        ORDER BY allocation DESC
    """
    rework_theme_rows = await query_dicts(
        client, rework_theme_sql, {"org_id": context.org_id}
    )

    deltas = []
    for row in delta_rows:
        deltas.append(
            {
                "metric": row.get("metric", ""),
                "label": row.get("label", ""),
                "value": float(row.get("value", 0)),
                "unit": row.get("unit", ""),
                "delta_pct": 0.0,  # Would need previous period comparison
                "spark": [],
            }
        )

    canonical_rework_theme_rows = [
        row for row in rework_theme_rows if str(row.get("theme") or "") in THEMES
    ]
    total_allocation = sum(
        float(row.get("allocation") or 0.0) for row in canonical_rework_theme_rows
    )
    rework_theme_allocation = []
    for row in canonical_rework_theme_rows:
        theme = str(row.get("theme") or "")
        allocation = float(row.get("allocation") or 0.0)
        rework_theme_allocation.append(
            {
                "theme": theme,
                "label": _INVESTMENT_THEME_LABELS[theme],
                "allocation": allocation,
                "allocation_pct": (allocation / total_allocation * 100.0)
                if total_allocation
                else 0.0,
                "prs_merged": int(row.get("prs_merged") or 0),
                "churn_loc": int(row.get("churn_loc") or 0),
            }
        )

    return {
        "freshness": {
            "last_ingested_at": last_ingested,
            "sources": {},
            "coverage": {
                "repos_covered_pct": 0.0,
                "prs_linked_to_issues_pct": 0.0,
                "issues_with_cycle_states_pct": 0.0,
            },
        },
        "deltas": deltas,
        "rework_theme_allocation": rework_theme_allocation,
        "summary": [],
        "tiles": {},
        "constraint": {
            "title": "",
            "claim": "",
            "evidence": [],
            "experiments": [],
        },
        "events": [],
    }
