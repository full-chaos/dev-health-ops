"""GraphQL schema definition for analytics API."""

from __future__ import annotations

import logging

import strawberry
from strawberry.types import Info

from .context import GraphQLContext
from .extensions import ConfiguredValidationRules, OrgIdAuthExtension
from .models.ai import (
    AiAttributedPrsResult,
    AIComparison,
    AIDateRangeInput,
    AIGovernanceSummary,
    AIImpactSummary,
    AIOpportunitiesResult,
    AIReviewLoadResult,
    AIRiskBreakdownResult,
    AIScopeInput,
    AIWorkflowDrilldownResult,
    AIWorkflowRootTypeInput,
)
from .models.data_health import DataHealth
from .models.improve import ExperimentsResult
from .models.inputs import (
    AnalyticsRequestInput,
    CapacityForecastFilterInput,
    CapacityForecastInput,
    DimensionInput,
    FilterInput,
    OperatingReviewInput,
    ProductTelemetryDashboardInput,
    SecurityAlertFilterInput,
    SecurityPaginationInput,
    ThroughputForecastInput,
    WorkGraphEdgeFilterInput,
)
from .models.outputs import (
    AnalyticsResult,
    CapacityForecast,
    CapacityForecastConnection,
    CatalogResult,
    HomeResult,
    OperatingReview,
    ProductTelemetryDashboardType,
    ProductTelemetryPlatformDashboardType,
    SecurityAlertConnection,
    SecurityOverview,
    ThroughputForecast,
    WorkGraphEdgesResult,
)
from .models.recommendations import (
    Recommendation,
    WindowInput,
)
from .resolvers.ai import (
    resolve_ai_attributed_prs,
    resolve_ai_comparison,
    resolve_ai_governance_summary,
    resolve_ai_impact_summary,
    resolve_ai_opportunities,
    resolve_ai_review_load,
    resolve_ai_risk_breakdown,
    resolve_ai_workflow_drilldown,
)
from .resolvers.analytics import resolve_analytics
from .resolvers.bus_factor import resolve_bus_factor
from .resolvers.catalog import resolve_catalog
from .resolvers.cognitive_load import resolve_cognitive_load
from .resolvers.complexity import resolve_complexity_timeseries, resolve_hotspots
from .resolvers.compounding_risk import resolve_compounding_risk
from .resolvers.data_health import resolve_data_health
from .resolvers.product_telemetry import (
    resolve_product_telemetry_dashboard,
    resolve_product_telemetry_platform_dashboard,
)
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
from .resolvers.review_edges import resolve_review_edges
from .subscriptions import Subscription
from .types.bus_factor import BusFactor, BusFactorScopeInput
from .types.cognitive_load import (
    CognitiveLoadInput,
    CognitiveLoadResult,
)
from .types.complexity import (
    ComplexityTimeseriesInput,
    ComplexityTimeseriesResult,
    HotspotsInput,
    HotspotsResult,
)
from .types.compounding_risk import (
    CompoundingRiskFilterInput,
    CompoundingRiskResult,
)
from .types.review_edges import (
    ReviewEdgesInput,
    ReviewEdgesResult,
)

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

    @strawberry.field(description="Get first-party product telemetry dashboard metrics")
    async def product_telemetry_dashboard(
        self,
        info: Info,
        org_id: str,
        input: ProductTelemetryDashboardInput,
    ) -> ProductTelemetryDashboardType:
        context = get_context(info)
        return await resolve_product_telemetry_dashboard(context, input)

    @strawberry.field(
        description=(
            "Cross-org product telemetry dashboard for platform/super admins. "
            "Requires is_superuser. Returns global aggregates plus a top-orgs "
            "rollup with org names resolved from Postgres."
        )
    )
    async def product_telemetry_platform_dashboard(
        self,
        info: Info,
        input: ProductTelemetryDashboardInput,
    ) -> ProductTelemetryPlatformDashboardType:
        context = get_context(info)
        return await resolve_product_telemetry_platform_dashboard(context, input)

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
        from .models.outputs import Freshness, MetricDelta, ReworkThemeAllocation
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
            rework_theme_allocation=[
                ReworkThemeAllocation(
                    theme=row["theme"],
                    label=row["label"],
                    allocation=row["allocation"],
                    allocation_pct=row["allocation_pct"],
                    prs_merged=row["prs_merged"],
                    churn_loc=row["churn_loc"],
                )
                for row in data.get("rework_theme_allocation", [])
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

    @strawberry.field(description="Paginated list of security alerts")
    async def security_alerts(
        self,
        info: Info,
        org_id: str,
        filters: SecurityAlertFilterInput | None = None,
        pagination: SecurityPaginationInput | None = None,
    ) -> SecurityAlertConnection:
        from .resolvers.security import resolve_security_alerts

        context = get_context(info)
        return await resolve_security_alerts(context, org_id, filters, pagination)

    @strawberry.field(description="Aggregated security posture for the dashboard")
    async def security_overview(
        self,
        info: Info,
        org_id: str,
        filters: SecurityAlertFilterInput | None = None,
    ) -> SecurityOverview:
        from .resolvers.security import resolve_security_overview

        context = get_context(info)
        return await resolve_security_overview(context, org_id, filters)

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

    @strawberry.field(description="Compute throughput-based capacity forecast")
    async def throughput_forecast(
        self,
        info: Info,
        org_id: str,
        input: ThroughputForecastInput,
    ) -> ThroughputForecast | None:
        from .resolvers.forecast import resolve_throughput_forecast

        context = get_context(info)
        return await resolve_throughput_forecast(context, input)

    @strawberry.field(description="Weekly Engineering Operating Review")
    async def operating_review(
        self,
        info: Info,
        org_id: str,
        input: OperatingReviewInput,
    ) -> OperatingReview:
        from .resolvers.operating_review import resolve_operating_review

        context = get_context(info)
        return await resolve_operating_review(context, input)

    @strawberry.field(description="Operator data-health and trust surface")
    async def data_health(
        self,
        info: Info,
        team: strawberry.ID,
    ) -> DataHealth:
        context = get_context(info)
        return await resolve_data_health(context, str(team))

    @strawberry.field(
        description="Repository ownership concentration and bus-factor summary."
    )
    async def bus_factor(
        self,
        info: Info,
        org_id: str,
        scope: BusFactorScopeInput | None = None,
    ) -> BusFactor:
        context = get_context(info)
        return await resolve_bus_factor(context, org_id, scope)

    @strawberry.field(
        description=(
            "Compounding Risk composite: churn × complexity × ownership "
            "× review-latency. Inspectable score with persisted weights, "
            "thresholds, raw inputs, and normalized components."
        )
    )
    async def compounding_risk(
        self,
        info: Info,
        org_id: str,
        filter: CompoundingRiskFilterInput | None = None,  # noqa: A002
    ) -> CompoundingRiskResult:
        context = get_context(info)
        return await resolve_compounding_risk(context, org_id, filter)

    @strawberry.field(
        description=(
            "Cyclomatic complexity trend by repo or file. Reads from "
            "append-only ``repo_complexity_daily`` / ``file_complexity_snapshots`` "
            "tables — no recomputation, pure surface of persisted data."
        )
    )
    async def complexity_timeseries(
        self,
        info: Info,
        input: ComplexityTimeseriesInput,
    ) -> ComplexityTimeseriesResult:
        context = get_context(info)
        return await resolve_complexity_timeseries(context, input)

    @strawberry.field(
        description=(
            "Top file hotspots ranked by risk_score (churn x complexity x "
            "ownership concentration). Reads from the append-only "
            "``file_hotspot_daily`` table."
        )
    )
    async def hotspots(
        self,
        info: Info,
        input: HotspotsInput,
    ) -> HotspotsResult:
        context = get_context(info)
        return await resolve_hotspots(context, input)

    @strawberry.field(
        description=(
            "Daily cognitive-load signals (PR interruption, context spread, "
            "review request load, after-hours and weekend commit ratios). "
            "Reads from ``user_metrics_daily`` and ``team_metrics_daily`` — "
            "no recomputation, pure surface of persisted metrics."
        )
    )
    async def cognitive_load(
        self,
        info: Info,
        input: CognitiveLoadInput,
    ) -> CognitiveLoadResult:
        context = get_context(info)
        return await resolve_cognitive_load(context, input)

    @strawberry.field(
        description=(
            "Reviewer-to-author collaboration edges from ``review_edges_daily``. "
            "Ordered by review count descending.  Use ``repoIds`` to narrow to "
            "specific repositories.  Org-scoped; no recomputation."
        )
    )
    async def review_edges(
        self,
        info: Info,
        input: ReviewEdgesInput,
    ) -> ReviewEdgesResult:
        context = get_context(info)
        return await resolve_review_edges(context, input)

    @strawberry.field(
        description="Latest rule-based recommendations for a team within a lookback window."
    )
    async def recommendations(
        self,
        info: Info,
        org_id: str,
        team: strawberry.ID,
        window: WindowInput,
    ) -> list[Recommendation]:
        from .resolvers.recommendations import resolve_recommendations

        context = get_context(info)
        return await resolve_recommendations(context, str(team), window)

    @strawberry.field(
        description=(
            "Experiments derived from opportunity suggested_experiments (CHAOS-2219). "
            "v1: computed at query-time — no persistence table. "
            "Each experiment is a typed promotion of a suggestion string with "
            "hypothesis / metric / owner / stop_condition. "
            "``derived_from_opportunities`` is False when the opportunities "
            "service was unavailable; items will be empty in that case."
        )
    )
    async def experiments(
        self,
        info: Info,
        org_id: str,
        filters: FilterInput | None = None,
    ) -> ExperimentsResult:
        from .resolvers.improve import resolve_experiments

        context = get_context(info)
        return await resolve_experiments(context, filters)

    @strawberry.field(
        description="AI workflow impact summary across the requested time range."
    )
    async def ai_impact_summary(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIScopeInput | None = None,
    ) -> AIImpactSummary:
        context = get_context(info)
        return await resolve_ai_impact_summary(context, date_range, scope)

    @strawberry.field(
        description="Side-by-side AI-assisted vs non-AI baseline comparison."
    )
    async def ai_comparison(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIScopeInput | None = None,
    ) -> AIComparison:
        context = get_context(info)
        return await resolve_ai_comparison(context, date_range, scope)

    @strawberry.field(
        description="Per-bucket AI review-load breakdown with amplification."
    )
    async def ai_review_load(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIScopeInput | None = None,
    ) -> AIReviewLoadResult:
        context = get_context(info)
        return await resolve_ai_review_load(context, date_range, scope)

    @strawberry.field(
        description="Per-bucket AI risk breakdown (rework, revert, test gaps, incidents)."
    )
    async def ai_risk_breakdown(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIScopeInput | None = None,
    ) -> AIRiskBreakdownResult:
        context = get_context(info)
        return await resolve_ai_risk_breakdown(context, date_range, scope)

    @strawberry.field(
        description=(
            "AI automation opportunity recommendations. "
            "Returns an empty, stable contract until the detector "
            "ships (CHAOS-1586)."
        )
    )
    async def ai_opportunities(
        self,
        info: Info,
        org_id: str,
        scope: AIScopeInput | None = None,
        limit: int = 25,
    ) -> AIOpportunitiesResult:
        context = get_context(info)
        return await resolve_ai_opportunities(context, scope, limit)

    @strawberry.field(
        description="AI governance coverage and recent policy violations."
    )
    async def ai_governance_summary(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIScopeInput | None = None,
        violation_limit: int = 100,
    ) -> AIGovernanceSummary:
        context = get_context(info)
        return await resolve_ai_governance_summary(
            context, date_range, scope, violation_limit
        )

    @strawberry.field(
        description=(
            "Drilldown into AI workflow evidence rooted at an issue, "
            "PR, or work_unit. Returns Work Graph nodes and edges with "
            "provenance and short evidence references."
        )
    )
    async def ai_workflow_drilldown(
        self,
        info: Info,
        org_id: str,
        root_type: AIWorkflowRootTypeInput,
        root_id: str,
        depth: int = 3,
        limit: int = 100,
    ) -> AIWorkflowDrilldownResult:
        context = get_context(info)
        return await resolve_ai_workflow_drilldown(
            context, root_type, root_id, depth, limit
        )

    @strawberry.field(
        description=(
            "List AI-attributed pull requests in the requested window so "
            "the UI can offer a concrete drilldown selector. Rows come "
            "from ai_attribution_resolved joined to git_pull_requests; "
            "no aggregation, no fabrication."
        )
    )
    async def ai_attributed_prs(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIScopeInput | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> AiAttributedPrsResult:
        context = get_context(info)
        return await resolve_ai_attributed_prs(
            context, date_range, scope, limit, offset
        )


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
    extensions=[
        OrgIdAuthExtension,
        ConfiguredValidationRules,
    ],
)
