from __future__ import annotations

from hashlib import sha256

from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDashboardRange,
    load_product_telemetry_dashboard,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import ProductTelemetryDashboardInput
from ..models.outputs import (
    ProductTelemetryChartInteractionType,
    ProductTelemetryClientErrorType,
    ProductTelemetryDailyActiveUsersType,
    ProductTelemetryDashboardType,
    ProductTelemetryFeatureViewType,
    ProductTelemetryFilterChangeType,
    ProductTelemetryRouteUsageType,
    ProductTelemetrySessionSummaryType,
)


def _product_telemetry_org_hash(org_id: str) -> str:
    return sha256(org_id.encode()).hexdigest()


async def resolve_product_telemetry_dashboard(
    context: GraphQLContext,
    input: ProductTelemetryDashboardInput,
) -> ProductTelemetryDashboardType:
    org_id = require_org_id(context)
    client = context.client
    if client is None:
        raise RuntimeError("Database client not available")
    if input.start_date > input.end_date:
        raise ValueError("start_date must be before or equal to end_date")

    dashboard = await load_product_telemetry_dashboard(
        client,
        org_id_hash=_product_telemetry_org_hash(org_id),
        date_range=ProductTelemetryDashboardRange(
            start_date=input.start_date,
            end_date=input.end_date,
        ),
    )

    return ProductTelemetryDashboardType(
        daily_active_users=[
            ProductTelemetryDailyActiveUsersType(
                day=item.day,
                active_anonymous_users=item.active_anonymous_users,
            )
            for item in dashboard.daily_active_users
        ],
        top_routes=[
            ProductTelemetryRouteUsageType(
                route_pattern=item.route_pattern,
                events=item.events,
                sessions=item.sessions,
                anonymous_users=item.anonymous_users,
            )
            for item in dashboard.top_routes
        ],
        feature_views=[
            ProductTelemetryFeatureViewType(
                feature=item.feature,
                surface=item.surface,
                views=item.views,
                anonymous_users=item.anonymous_users,
            )
            for item in dashboard.feature_views
        ],
        filter_changes=[
            ProductTelemetryFilterChangeType(
                view=item.view,
                filter_key=item.filter_key,
                changes=item.changes,
                avg_value_count=item.avg_value_count,
            )
            for item in dashboard.filter_changes
        ],
        chart_interactions=[
            ProductTelemetryChartInteractionType(
                chart=item.chart,
                action=item.action,
                surface=item.surface,
                interactions=item.interactions,
                sessions=item.sessions,
            )
            for item in dashboard.chart_interactions
        ],
        client_errors=[
            ProductTelemetryClientErrorType(
                route_pattern=item.route_pattern,
                boundary=item.boundary,
                error_class=item.error_class,
                errors=item.errors,
                affected_anonymous_users=item.affected_anonymous_users,
            )
            for item in dashboard.client_errors
        ],
        session_summary=ProductTelemetrySessionSummaryType(
            p50_duration_ms=dashboard.session_summary.p50_duration_ms,
            p75_duration_ms=dashboard.session_summary.p75_duration_ms,
            p90_duration_ms=dashboard.session_summary.p90_duration_ms,
            p95_duration_ms=dashboard.session_summary.p95_duration_ms,
            avg_pages_viewed=dashboard.session_summary.avg_pages_viewed,
            avg_interactions=dashboard.session_summary.avg_interactions,
        ),
    )
