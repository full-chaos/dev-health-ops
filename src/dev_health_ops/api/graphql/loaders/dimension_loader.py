"""Dimension value loader for catalog queries."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..sql.compiler import CatalogValuesRequest, compile_catalog_values
from ..sql.validate import Dimension

if TYPE_CHECKING:
    from ..models.inputs import FilterInput


logger = logging.getLogger(__name__)


async def load_dimension_values(
    client: Any,
    dimension: str,
    org_id: str,
    limit: int = 100,
    timeout: int = 30,
    filters: Optional["FilterInput"] = None,  # NEW: Filter support
) -> List[Dict[str, Any]]:
    """
    Load distinct values for a dimension.

    Args:
        client: ClickHouse client instance.
        dimension: The dimension to load values for.
        org_id: Organization ID for scoping.
        limit: Maximum number of values to return.
        timeout: Query timeout in seconds.
        filters: Optional FilterInput to narrow down dimension values.

    Returns:
        List of dicts with 'value' and 'count' keys.
    """
    from dev_health_ops.api.queries.client import query_dicts

    request = CatalogValuesRequest(
        dimension=dimension,
        limit=limit,
    )

    sql, params = compile_catalog_values(request, org_id, timeout, filters=filters)

    logger.debug(
        "Loading dimension values for %s, org_id=%s, limit=%d, filters=%s",
        dimension,
        org_id,
        limit,
        filters,
    )

    try:
        rows = await query_dicts(client, sql, params)
        return [
            {"value": row.get("value", ""), "count": row.get("count", 0)}
            for row in rows
        ]
    except Exception as e:
        logger.warning("Failed to load dimension values: %s", e)
        return []


def get_dimension_descriptions() -> Dict[str, str]:
    """Get descriptions for all available dimensions."""
    return {
        Dimension.TEAM.value: "Team identifier for grouping work",
        Dimension.REPO.value: "Repository identifier",
        Dimension.AUTHOR.value: "Author/contributor identifier",
        Dimension.WORK_TYPE.value: "Type of work item (issue, PR, etc.)",
        Dimension.THEME.value: "Investment theme category",
        Dimension.SUBCATEGORY.value: "Investment subcategory",
    }


def get_measure_descriptions() -> Dict[str, str]:
    """Get descriptions for all available measures."""
    from ..sql.validate import Measure

    return {
        Measure.COUNT.value: "Count of work units",
        Measure.CHURN_LOC.value: "Lines of code changed",
        Measure.CYCLE_TIME_HOURS.value: "Average cycle time in hours",
        Measure.THROUGHPUT.value: "Distinct work units completed",
    }
