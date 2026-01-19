"""GraphQL schema definition for analytics API."""

from __future__ import annotations

import logging
from typing import Optional

import strawberry
from strawberry.types import Info

from .context import GraphQLContext
from .models.inputs import AnalyticsRequestInput, DimensionInput, FilterInput
from .models.outputs import AnalyticsResult, CatalogResult
from .resolvers.analytics import resolve_analytics
from .resolvers.catalog import resolve_catalog


logger = logging.getLogger(__name__)


def get_context(info: Info) -> GraphQLContext:
    """Extract GraphQL context from request info."""
    return info.context


@strawberry.type
class Query:
    """Root query type for analytics API."""

    @strawberry.field(
        description="Get catalog of available dimensions, measures, and limits"
    )
    async def catalog(
        self,
        info: Info,
        org_id: str,
        dimension: Optional[DimensionInput] = None,
        filters: Optional[FilterInput] = None,  # NEW: Filter support
    ) -> CatalogResult:
        """
        Fetch catalog information.

        Args:
            org_id: Required organization ID for scoping.
            dimension: Optional dimension to fetch distinct values for.
            filters: Optional filters to narrow down dimension values.

        Returns:
            CatalogResult with dimensions, measures, limits, and optional values.
        """
        context = get_context(info)
        # Override org_id from argument (the schema requires it)
        context.org_id = org_id
        return await resolve_catalog(context, dimension, filters=filters)

    @strawberry.field(description="Run batch analytics queries")
    async def analytics(
        self,
        info: Info,
        org_id: str,
        batch: AnalyticsRequestInput,
    ) -> AnalyticsResult:
        """
        Execute batch analytics queries.

        Args:
            org_id: Required organization ID for scoping.
            batch: Batch request with timeseries, breakdowns, and optional sankey.

        Returns:
            AnalyticsResult with all query results.
        """
        context = get_context(info)
        # Override org_id from argument (the schema requires it)
        context.org_id = org_id
        return await resolve_analytics(context, batch)


# Create the Strawberry schema
schema = strawberry.Schema(query=Query)
