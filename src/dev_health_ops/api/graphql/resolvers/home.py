"""Home metrics resolver for GraphQL API."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from ..authz import require_org_id
from ..context import GraphQLContext

logger = logging.getLogger(__name__)


async def resolve_home(
    context: GraphQLContext,
    filters: Optional[Any] = None,
) -> Dict[str, Any]:
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

    # Get freshness data
    freshness_sql = """
        SELECT
            max(ingested_at) as last_ingested_at
        FROM investment_metrics_daily
        WHERE day >= today() - 30
    """
    freshness_rows = await query_dicts(client, freshness_sql, {})
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
    """
    delta_rows = await query_dicts(client, deltas_sql, {})

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
