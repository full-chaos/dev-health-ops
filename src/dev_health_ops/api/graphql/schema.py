"""GraphQL schema definition for analytics API."""

from __future__ import annotations

import logging
from typing import NoReturn

import strawberry
from strawberry.types import Info

from .context import GraphQLContext
from .extensions import ConfiguredValidationRules, OrgIdAuthExtension
from .models.ai import (
    AiAttributedPrsResult,
    AIAttributionOverviewResult,
    AIAttributionScopeInput,
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
from .models.improve import ExperimentsResult, ImproveOpportunitiesResult
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
    FeatureFlagEventsResult,
    FeatureFlagRegistryResult,
    HomeResult,
    OperatingReview,
    ProductTelemetryDashboardType,
    ProductTelemetryPlatformDashboardType,
    SecurityAlertConnection,
    SecurityOverview,
    ThroughputForecast,
    WorkGraphArtifactsResult,
    WorkGraphEdgesResult,
    WorkGraphFlowResult,
    WorkItemTeamAttribution,
    WorkUnitTeamAttribution,
)
from .models.pr import PullRequestDetail
from .models.recommendations import (
    Recommendation,
    WindowInput,
)
from .resolvers.analytics import resolve_analytics
from .resolvers.dev_evidence import (
    resolve_dev_data_health,
    resolve_dev_evidence_search,
)
from .resolvers.dev_metric import resolve_dev_metric, resolve_dev_metric_catalog
from .resolvers.dev_scope import resolve_dev_scope_search
from .resolvers.dev_status_change import (
    resolve_dev_change_summary,
    resolve_dev_status_snapshot,
)
from .resolvers.dev_work_graph import resolve_dev_work_graph_neighbors
from .resolvers.product_telemetry import (
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
from .types.dev_evidence import (
    DevDataHealthInput,
    DevDataHealthResult,
    DevEvidenceSearchInput,
    DevEvidenceSearchResult,
)
from .types.dev_metric import (
    DevMetricCatalog,
    DevMetricCatalogInput,
    DevMetricQueryInput,
    DevMetricResult,
)
from .types.dev_scope import DevScopeSearchInput, DevScopeSearchResult
from .types.dev_status_change import (
    DevChangeSummary,
    DevChangeSummaryInput,
    DevStatusSnapshot,
    DevStatusSnapshotInput,
)
from .types.dev_work_graph import (
    DevWorkGraphNeighborsInput,
    DevWorkGraphNeighborsResult,
)
from .types.review_edges import (
    ReviewEdgesInput,
    ReviewEdgesResult,
)
from .types.testops_risk import TestOpsRiskInput, TestOpsRiskResult

logger = logging.getLogger(__name__)


class GoServedOperationUnavailableError(RuntimeError):
    """A query-api-served operation reached its Python field, which has no implementation.

    Its own exception type rather than a bare RuntimeError so an operator can
    grep for it, an alert can match on it, and a future test can assert it
    without matching on message text.
    """


def _raise_served_by_query_api(operation: str, org_id: str, info: Info) -> NoReturn:
    """Fail loudly for an operation query-api owns.

    ONE structured error line before raising, per the standing telemetry rule:
    the exception reaches the client as a GraphQL error with no server context,
    so without this the operator sees a 500 with nothing pointing at the actual
    cause -- which is always deploy skew, never a bug in the request.
    """
    logger.error(
        "graphql.operation_served_by_query_api",
        extra={
            "operation": operation,
            # The argument as sent, NOT an authorization decision -- these
            # resolvers never scoped off it (require_org_id did), and it is
            # logged only because a digest miss is usually org-agnostic while a
            # routing-row gap is usually not, and knowing which narrows it fast.
            "requested_org_id": org_id,
            "path": str(info.path) if info is not None else "",
            "reason": (
                "query-api owns this operation and it has no Python implementation; "
                "reaching this resolver means the Go "
                "dispatcher did not intercept -- check the go_api_routing_state "
                "row for this operation, then the registered document digest"
            ),
        },
    )
    raise GoServedOperationUnavailableError(
        f"{operation} is served by query-api and has no Python implementation. "
        "The Go dispatcher did not intercept this request: verify the "
        "go_api_routing_state row for this operation is present with "
        "mode 'canary' or 'primary', and that the client's query document digest matches the one "
        "query-api registers (cmd/query-api/query_route.go)."
    )


def get_context(info: Info) -> GraphQLContext:
    """Extract GraphQL context from request info."""
    return info.context


@strawberry.type
class Query:
    """Root query type for analytics API."""

    @strawberry.field(
        description=(
            "Search authorized Ask Dev V1 direct-scope entities. Results are "
            "tenant-scoped, deterministic, and capped at 25 candidates."
        )
    )
    async def dev_scope_search(
        self,
        info: Info,
        org_id: str,
        input: DevScopeSearchInput,
    ) -> DevScopeSearchResult:
        return await resolve_dev_scope_search(get_context(info), input)

    @strawberry.field(
        description="List the exact authorized Ask Dev V1 metric registry."
    )
    async def dev_metric_catalog(
        self,
        info: Info,
        org_id: str,
        input: DevMetricCatalogInput | None = None,
    ) -> DevMetricCatalog:
        return await resolve_dev_metric_catalog(get_context(info), input)

    @strawberry.field(
        description=(
            "Query one registered Ask Dev V1 metric through the shared bounded "
            "service, including prior-equivalent comparison and source state."
        )
    )
    async def dev_metric(
        self,
        info: Info,
        org_id: str,
        input: DevMetricQueryInput,
    ) -> DevMetricResult:
        return await resolve_dev_metric(get_context(info), input)

    @strawberry.field(
        description=(
            "Search bounded authorized Ask Dev evidence. Source text is "
            "sanitized and explicitly untrusted; results are capped at 25. "
            "Requires the canonical explicit-enable ask_dev entitlement."
        )
    )
    async def dev_evidence_search(
        self,
        info: Info,
        org_id: str,
        input: DevEvidenceSearchInput,
    ) -> DevEvidenceSearchResult:
        return await resolve_dev_evidence_search(get_context(info), input)

    @strawberry.field(
        description=(
            "Report source-specific Ask Dev coverage, freshness, failures, "
            "and whether required sources permit a complete answer. Requires "
            "the canonical explicit-enable ask_dev entitlement."
        )
    )
    async def dev_data_health(
        self,
        info: Info,
        org_id: str,
        input: DevDataHealthInput,
    ) -> DevDataHealthResult:
        return await resolve_dev_data_health(get_context(info), input)

    @strawberry.field(
        description=(
            "Return declared status separately from deterministic, evidence-backed "
            "completion using the versioned Ask Dev status rules."
        )
    )
    async def dev_status_snapshot(
        self,
        info: Info,
        org_id: str,
        input: DevStatusSnapshotInput,
    ) -> DevStatusSnapshot:
        return await resolve_dev_status_snapshot(get_context(info), input)

    @strawberry.field(
        description=(
            "Return reproducible observed changes across explicit equal-duration "
            "windows without upgrading correlation to cause."
        )
    )
    async def dev_change_summary(
        self,
        info: Info,
        org_id: str,
        input: DevChangeSummaryInput,
    ) -> DevChangeSummary:
        return await resolve_dev_change_summary(get_context(info), input)

    @strawberry.field(
        description=(
            "Return persisted, tenant-scoped work-graph neighbors with depth fixed "
            "to one and code-owned relationship/result bounds."
        )
    )
    async def dev_work_graph_neighbors(
        self,
        info: Info,
        org_id: str,
        input: DevWorkGraphNeighborsInput,
    ) -> DevWorkGraphNeighborsResult:
        return await resolve_dev_work_graph_neighbors(get_context(info), input)

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
        _raise_served_by_query_api("catalog", org_id, info)

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
        _raise_served_by_query_api("productTelemetryDashboard", org_id, info)

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
        _raise_served_by_query_api("workGraphEdges", org_id, info)

    @strawberry.field(
        description=(
            "Pull request detail by stable id ({repo_id}#pr{number}) from "
            "persisted ClickHouse PR, review, commit, and Work Graph tables."
        )
    )
    async def pr(
        self,
        info: Info,
        org_id: str,
        id: strawberry.ID,
    ) -> PullRequestDetail | None:
        _raise_served_by_query_api("pr", org_id, info)

    @strawberry.field(
        description="Per-node-type inflow/outflow over the full work graph"
    )
    async def work_graph_flow(
        self,
        info: Info,
        org_id: str,
        filters: WorkGraphEdgeFilterInput | None = None,
    ) -> WorkGraphFlowResult:
        _raise_served_by_query_api("workGraphFlow", org_id, info)

    @strawberry.field(
        description="Top-N work graph nodes ranked by degree over the full graph"
    )
    async def work_graph_artifacts(
        self,
        info: Info,
        org_id: str,
        filters: WorkGraphEdgeFilterInput | None = None,
    ) -> WorkGraphArtifactsResult:
        _raise_served_by_query_api("workGraphArtifacts", org_id, info)

    @strawberry.field(description="List feature flags from the ClickHouse registry")
    async def feature_flags(
        self,
        info: Info,
        org_id: str,
        provider: str | None = None,
        project: str | None = None,
        include_archived: bool | None = False,
        limit: int = 1000,
    ) -> FeatureFlagRegistryResult:
        _raise_served_by_query_api("featureFlags", org_id, info)

    @strawberry.field(description="List feature flag state-change events")
    async def feature_flag_events(
        self,
        info: Info,
        org_id: str,
        flag_key: str | None = None,
        environment: str | None = None,
        limit: int = 1000,
    ) -> FeatureFlagEventsResult:
        _raise_served_by_query_api("featureFlagEvents", org_id, info)

    @strawberry.field(
        description=(
            "Team-attribution provenance per work item (source/confidence/"
            "evidence/is_primary) — CHAOS-2600"
        )
    )
    async def work_item_team_attributions(
        self,
        info: Info,
        org_id: str,
        work_item_ids: list[str] | None = None,
        team_id: str | None = None,
    ) -> list[WorkItemTeamAttribution]:
        from .resolvers.team_attribution import (
            resolve_work_item_team_attributions,
        )

        context = get_context(info)
        return await resolve_work_item_team_attributions(
            context, work_item_ids=work_item_ids, team_id=team_id
        )

    @strawberry.field(
        description=(
            "The owning team per work UNIT (investment cluster), collapsed from "
            "its member work-item attributions by source precedence — CHAOS-2600"
        )
    )
    async def work_unit_team_attributions(
        self,
        info: Info,
        org_id: str,
        work_unit_ids: list[str] | None = None,
        team_id: str | None = None,
    ) -> list[WorkUnitTeamAttribution]:
        _raise_served_by_query_api("workUnitTeamAttributions", org_id, info)

    @strawberry.field(description="Paginated list of security alerts")
    async def security_alerts(
        self,
        info: Info,
        org_id: str,
        filters: SecurityAlertFilterInput | None = None,
        pagination: SecurityPaginationInput | None = None,
    ) -> SecurityAlertConnection:
        _raise_served_by_query_api("securityAlerts", org_id, info)

    @strawberry.field(description="Aggregated security posture for the dashboard")
    async def security_overview(
        self,
        info: Info,
        org_id: str,
        filters: SecurityAlertFilterInput | None = None,
    ) -> SecurityOverview:
        _raise_served_by_query_api("securityOverview", org_id, info)

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

    # The fields whose bodies call _raise_served_by_query_api are SERVED BY
    # query-api, not by Python.
    #
    # The DECLARATIONS below are load-bearing and must not be deleted. They are
    # what `dev_health_ops.api.graphql.export_schema` emits into
    # contracts/graphql/v1/schema.graphql, which is gqlgen's input SDL for
    # query-api, web's codegen schema, AND half of both planes' routing key --
    # routeswitch.PostgresSwitch looks up go_api_routing_state by
    # (schema_digest, document_digest, selected_operation). Measured:
    # removing a field registration changes the export and its sha256, which
    # would invalidate the routing row of EVERY registered operation at once.
    #
    # The BODIES raise. They have no Python implementation, so there is no
    # Python fallback for these operations. Every other delegated operation
    # keeps its fallback.
    #
    # In normal operation nothing below ever runs: go_api_dispatcher intercepts
    # at the HTTP layer, before Strawberry executes, whenever the operation's
    # go_api_routing_state row is in mode canary or primary. Reaching one of these raises means the
    # dispatcher did NOT intercept, which is one of exactly three things: the
    # routing row is missing or not in mode canary or primary, the request's document digest
    # did not match what query-api registers (a web query-text change deployed
    # without its ops counterpart), or query-api answered non-200. All three are
    # deploy skew or outage, and all three are worth a loud failure: the alternative is
    # returning null or an empty connection, which renders as "no data" and is
    # indistinguishable from a genuinely empty scope.
    @strawberry.field(description="Compute capacity forecast on-demand")
    async def capacity_forecast(
        self,
        info: Info,
        org_id: str,
        input: CapacityForecastInput | None = None,
    ) -> CapacityForecast | None:
        _raise_served_by_query_api("capacityForecast", org_id, info)

    @strawberry.field(description="List persisted capacity forecasts")
    async def capacity_forecasts(
        self,
        info: Info,
        org_id: str,
        filters: CapacityForecastFilterInput | None = None,
    ) -> CapacityForecastConnection:
        _raise_served_by_query_api("capacityForecasts", org_id, info)

    @strawberry.field(description="Compute throughput-based capacity forecast")
    async def throughput_forecast(
        self,
        info: Info,
        org_id: str,
        input: ThroughputForecastInput,
    ) -> ThroughputForecast | None:
        _raise_served_by_query_api("throughputForecast", org_id, info)

    @strawberry.field(description="Weekly Engineering Operating Review")
    async def operating_review(
        self,
        info: Info,
        org_id: str,
        input: OperatingReviewInput,
    ) -> OperatingReview:
        _raise_served_by_query_api("operatingReview", org_id, info)

    @strawberry.field(description="Operator data-health and trust surface")
    async def data_health(
        self,
        info: Info,
        team: strawberry.ID,
    ) -> DataHealth:
        _raise_served_by_query_api(
            "dataHealth", getattr(get_context(info), "org_id", ""), info
        )

    @strawberry.field(
        description="Repository ownership concentration and bus-factor summary."
    )
    async def bus_factor(
        self,
        info: Info,
        org_id: str,
        scope: BusFactorScopeInput | None = None,
    ) -> BusFactor:
        _raise_served_by_query_api("busFactor", org_id, info)

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
        _raise_served_by_query_api("compoundingRisk", org_id, info)

    @strawberry.field(
        description=(
            "Persisted TestOps Delivery Risk metrics from release confidence, "
            "quality drag, and pipeline stability tables."
        )
    )
    async def testops_risk(
        self,
        info: Info,
        org_id: str,
        input: TestOpsRiskInput,
    ) -> TestOpsRiskResult:
        from .resolvers.testops_risk import resolve_testops_risk

        context = get_context(info)
        return await resolve_testops_risk(context, org_id, input)

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
        _raise_served_by_query_api("complexityTimeseries", input.org_id, info)

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
        _raise_served_by_query_api("hotspots", input.org_id, info)

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
        _raise_served_by_query_api("cognitiveLoad", input.org_id, info)

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
        _raise_served_by_query_api("reviewEdges", input.org_id, info)

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
        _raise_served_by_query_api("experiments", org_id, info)

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
        _raise_served_by_query_api("aiImpactSummary", org_id, info)

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
        _raise_served_by_query_api("aiComparison", org_id, info)

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
        _raise_served_by_query_api("aiReviewLoad", org_id, info)

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
        _raise_served_by_query_api("aiRiskBreakdown", org_id, info)

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
        _raise_served_by_query_api("aiOpportunities", org_id, info)

    @strawberry.field(
        description=(
            "Non-AI flow opportunity recommendations for the Improve surface "
            "(CHAOS-2220). Fires threshold rules over repo and team metrics "
            "(review latency, cycle time, rework, WIP, throughput, churn, "
            "change failure) and returns scored candidates. "
            "An empty list means all metrics are within thresholds — not an error."
        )
    )
    async def improve_opportunities(
        self,
        info: Info,
        scope: AIScopeInput | None = None,
        limit: int = 10,
        window_days: int = 30,
    ) -> ImproveOpportunitiesResult:
        _raise_served_by_query_api("improveOpportunities", "", info)

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
        _raise_served_by_query_api("aiGovernanceSummary", org_id, info)

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
        _raise_served_by_query_api("aiWorkflowDrilldown", org_id, info)

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
        _raise_served_by_query_api("aiAttributedPrs", org_id, info)

    @strawberry.field(
        description=(
            "AI attribution mix and provenance evidence for the requested "
            "window. Reads ai_attribution_resolved only - the highest-"
            "precedence, non-superseded signal per subject - so every row "
            "carries source, confidence, and evidence. Does not include a "
            "synthesized human bucket; use aiImpactSummary for the full "
            "AI-vs-human PR split."
        )
    )
    async def ai_attribution_overview(
        self,
        info: Info,
        org_id: str,
        date_range: AIDateRangeInput,
        scope: AIAttributionScopeInput | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> AIAttributionOverviewResult:
        _raise_served_by_query_api("aiAttributionOverview", org_id, info)


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
    extensions=[
        OrgIdAuthExtension,
        ConfiguredValidationRules,
    ],
)
