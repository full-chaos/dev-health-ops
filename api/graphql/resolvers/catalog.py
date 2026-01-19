"""Catalog resolver for GraphQL analytics API."""

from __future__ import annotations

import logging
from typing import Optional, TYPE_CHECKING

from ..authz import require_org_id
from ..context import GraphQLContext
from ..cost import DEFAULT_LIMITS
from ..loaders.dimension_loader import (
    get_dimension_descriptions,
    get_measure_descriptions,
    load_dimension_values,
)
from ..models.inputs import DimensionInput
from ..models.outputs import (
    CatalogDimension,
    CatalogLimits,
    CatalogMeasure,
    CatalogResult,
    CatalogValueItem,
)
from ..sql.validate import Dimension, Measure

if TYPE_CHECKING:
    from ..models.inputs import FilterInput


logger = logging.getLogger(__name__)


async def resolve_catalog(
    context: GraphQLContext,
    dimension: Optional[DimensionInput] = None,
    filters: Optional["FilterInput"] = None,  # NEW: Filter support
) -> CatalogResult:
    """
    Resolve catalog query.

    Returns available dimensions, measures, limits, and optionally
    distinct values for a specific dimension.

    Args:
        context: GraphQL request context with org_id.
        dimension: Optional dimension to fetch values for.
        filters: Optional filters to narrow down dimension values.

    Returns:
        CatalogResult with dimensions, measures, limits, and optional values.
    """
    org_id = require_org_id(context)

    # Build dimension list
    dim_descriptions = get_dimension_descriptions()
    dimensions = [
        CatalogDimension(name=d.value, description=dim_descriptions.get(d.value, ""))
        for d in Dimension
    ]

    # Build measure list
    measure_descriptions = get_measure_descriptions()
    measures = [
        CatalogMeasure(name=m.value, description=measure_descriptions.get(m.value, ""))
        for m in Measure
    ]

    # Build limits
    limits = CatalogLimits(
        max_days=DEFAULT_LIMITS.max_days,
        max_buckets=DEFAULT_LIMITS.max_buckets,
        max_top_n=DEFAULT_LIMITS.max_top_n,
        max_sankey_nodes=DEFAULT_LIMITS.max_sankey_nodes,
        max_sankey_edges=DEFAULT_LIMITS.max_sankey_edges,
        max_sub_requests=DEFAULT_LIMITS.max_sub_requests,
    )

    # Optionally fetch dimension values
    values = None
    if dimension is not None and context.client is not None:
        try:
            raw_values = await load_dimension_values(
                client=context.client,
                dimension=dimension.value,
                org_id=org_id,
                limit=100,
                filters=filters,
            )
            values = [
                CatalogValueItem(value=v["value"], count=v["count"]) for v in raw_values
            ]
        except Exception as e:
            logger.warning("Failed to load dimension values: %s", e)
            values = []

    return CatalogResult(
        dimensions=dimensions,
        measures=measures,
        limits=limits,
        values=values,
    )
