"""GraphQL schema definition for analytics API."""

from __future__ import annotations

import logging
from typing import Optional

import strawberry
from strawberry.types import Info

from .context import GraphQLContext
from .models.inputs import (
    AnalyticsRequestInput,
    CapacityForecastFilterInput,
    CapacityForecastInput,
    DimensionInput,
    FilterInput,
    WorkGraphEdgeFilterInput,
)
from .models.outputs import (
    AnalyticsResult,
    CapacityForecast,
    CapacityForecastConnection,
    CatalogResult,
    HomeResult,
    OpportunitiesResult,
    PersonResult,
    PersonSearchResult,
    WorkGraphEdgesResult,
)
from .resolvers.analytics import resolve_analytics
from .resolvers.catalog import resolve_catalog
from .subscriptions import Subscription


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

    @strawberry.field(description="Get home dashboard metrics")
    async def home(
        self,
        info: Info,
        org_id: str,
        filters: Optional[FilterInput] = None,
    ) -> HomeResult:
        """
        Fetch home dashboard metrics and freshness info.

        Args:
            org_id: Required organization ID for scoping.
            filters: Optional filters to apply.

        Returns:
            HomeResult with freshness and metric deltas.
        """
        from .resolvers.home import resolve_home
        from .models.outputs import Freshness, MetricDelta, HomeResult as HR

        context = get_context(info)
        context.org_id = org_id
        data = await resolve_home(context, filters)

        return HR(
            freshness=Freshness(
                last_ingested_at=str(data["freshness"]["last_ingested_at"])
                if data["freshness"]["last_ingested_at"]
                else None,
            ),
            deltas=[
                MetricDelta(
                    metric=d["metric"],
                    label=d["label"],
                    value=d["value"],
                    unit=d["unit"],
                    delta_pct=d["delta_pct"],
                    spark=[],
                )
                for d in data["deltas"]
            ],
        )

    @strawberry.field(description="Search for people")
    async def people(
        self,
        info: Info,
        query: str,
        limit: int = 20,
    ) -> list[PersonSearchResult]:
        """
        Search for people by name or email.

        Args:
            query: Search query string.
            limit: Maximum results to return.

        Returns:
            List of matching people.
        """
        # Placeholder - would integrate with people service
        return []

    @strawberry.field(description="Get person details")
    async def person(
        self,
        info: Info,
        person_id: str,
    ) -> Optional[PersonResult]:
        """
        Get detailed information about a person.

        Args:
            person_id: Unique person identifier.

        Returns:
            Person details or None if not found.
        """
        # Placeholder - would integrate with people service
        return None

    @strawberry.field(description="Get opportunities/focus cards")
    async def opportunities(
        self,
        info: Info,
        org_id: str,
        filters: Optional[FilterInput] = None,
    ) -> OpportunitiesResult:
        """
        Fetch opportunity cards for the organization.

        Args:
            org_id: Required organization ID.
            filters: Optional filters to apply.

        Returns:
            OpportunitiesResult with opportunity cards.
        """
        from .models.outputs import OpportunitiesResult as OR

        # Placeholder - would integrate with opportunities service
        return OR(items=[])

    @strawberry.field(description="Query work graph edges with optional filters")
    async def work_graph_edges(
        self,
        info: Info,
        org_id: str,
        filters: Optional[WorkGraphEdgeFilterInput] = None,
    ) -> WorkGraphEdgesResult:
        from .resolvers.work_graph import resolve_work_graph_edges

        context = get_context(info)
        context.org_id = org_id
        return await resolve_work_graph_edges(context, filters)

    @strawberry.field(description="Compute capacity forecast on-demand")
    async def capacity_forecast(
        self,
        info: Info,
        org_id: str,
        input: Optional[CapacityForecastInput] = None,
    ) -> Optional[CapacityForecast]:
        from .resolvers.capacity import resolve_capacity_forecast

        context = get_context(info)
        context.org_id = org_id
        return await resolve_capacity_forecast(context, input)

    @strawberry.field(description="List persisted capacity forecasts")
    async def capacity_forecasts(
        self,
        info: Info,
        org_id: str,
        filters: Optional[CapacityForecastFilterInput] = None,
    ) -> CapacityForecastConnection:
        from .resolvers.capacity import resolve_capacity_forecasts

        context = get_context(info)
        context.org_id = org_id
        return await resolve_capacity_forecasts(context, filters)


# Create the Strawberry schema with subscription support
schema = strawberry.Schema(query=Query, subscription=Subscription)
