"""GraphQL schema definition for analytics API."""

from __future__ import annotations

import logging

import strawberry
from strawberry.types import Info

from .context import GraphQLContext
from .extensions import OrgIdAuthExtension
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
    WorkGraphEdgesResult,
)
from .resolvers.analytics import resolve_analytics
from .resolvers.catalog import resolve_catalog
from .resolvers.reports import (
    CloneSavedReportInput,
    CreateSavedReportInput,
    ReportRunConnection,
    ReportRunType,
    SavedReportConnection,
    SavedReportType,
    UpdateSavedReportInput,
    resolve_clone_saved_report,
    resolve_create_saved_report,
    resolve_delete_saved_report,
    resolve_report_runs,
    resolve_saved_report,
    resolve_saved_reports,
    resolve_trigger_report,
    resolve_update_saved_report,
)
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
        dimension: DimensionInput | None = None,
        filters: FilterInput | None = None,
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
        # org_id is already validated and written to context by OrgIdAuthExtension.
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
        return await resolve_analytics(context, batch)

    @strawberry.field(description="Get home dashboard metrics")
    async def home(
        self,
        info: Info,
        org_id: str,
        filters: FilterInput | None = None,
    ) -> HomeResult:
        """
        Fetch home dashboard metrics and freshness info.

        Args:
            org_id: Required organization ID for scoping.
            filters: Optional filters to apply.

        Returns:
            HomeResult with freshness and metric deltas.
        """
        from .models.outputs import Freshness, MetricDelta
        from .models.outputs import HomeResult as HR
        from .resolvers.home import resolve_home

        context = get_context(info)
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

    @strawberry.field(description="Query work graph edges with optional filters")
    async def work_graph_edges(
        self,
        info: Info,
        org_id: str,
        filters: WorkGraphEdgeFilterInput | None = None,
    ) -> WorkGraphEdgesResult:
        from .resolvers.work_graph import resolve_work_graph_edges

        context = get_context(info)
        return await resolve_work_graph_edges(context, filters)

    @strawberry.field(description="List saved reports for an organization")
    async def saved_reports(
        self,
        info: Info,
        org_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> SavedReportConnection:
        return await resolve_saved_reports(org_id, limit, offset)

    @strawberry.field(description="Get a saved report by ID")
    async def saved_report(
        self,
        info: Info,
        org_id: str,
        report_id: str,
    ) -> SavedReportType | None:
        return await resolve_saved_report(org_id, report_id)

    @strawberry.field(description="List report runs for a saved report")
    async def report_runs(
        self,
        info: Info,
        org_id: str,
        report_id: str,
        limit: int = 50,
    ) -> ReportRunConnection:
        return await resolve_report_runs(org_id, report_id, limit)

    @strawberry.field(description="Compute capacity forecast on-demand")
    async def capacity_forecast(
        self,
        info: Info,
        org_id: str,
        input: CapacityForecastInput | None = None,
    ) -> CapacityForecast | None:
        from .resolvers.capacity import resolve_capacity_forecast

        context = get_context(info)
        return await resolve_capacity_forecast(context, input)

    @strawberry.field(description="List persisted capacity forecasts")
    async def capacity_forecasts(
        self,
        info: Info,
        org_id: str,
        filters: CapacityForecastFilterInput | None = None,
    ) -> CapacityForecastConnection:
        from .resolvers.capacity import resolve_capacity_forecasts

        context = get_context(info)
        return await resolve_capacity_forecasts(context, filters)


@strawberry.type
class Mutation:
    @strawberry.mutation(description="Create a new saved report")
    async def create_saved_report(
        self,
        info: Info,
        org_id: str,
        input: CreateSavedReportInput,
    ) -> SavedReportType:
        return await resolve_create_saved_report(org_id, input)

    @strawberry.mutation(description="Update an existing saved report")
    async def update_saved_report(
        self,
        info: Info,
        org_id: str,
        report_id: str,
        input: UpdateSavedReportInput,
    ) -> SavedReportType | None:
        return await resolve_update_saved_report(org_id, report_id, input)

    @strawberry.mutation(description="Delete a saved report")
    async def delete_saved_report(
        self,
        info: Info,
        org_id: str,
        report_id: str,
    ) -> bool:
        return await resolve_delete_saved_report(org_id, report_id)

    @strawberry.mutation(description="Clone a saved report with optional overrides")
    async def clone_saved_report(
        self,
        info: Info,
        org_id: str,
        input: CloneSavedReportInput,
    ) -> SavedReportType | None:
        return await resolve_clone_saved_report(org_id, input)

    @strawberry.mutation(description="Trigger a manual report execution")
    async def trigger_report(
        self,
        info: Info,
        org_id: str,
        report_id: str,
    ) -> ReportRunType | None:
        return await resolve_trigger_report(org_id, report_id)


schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
    extensions=[OrgIdAuthExtension],
)
