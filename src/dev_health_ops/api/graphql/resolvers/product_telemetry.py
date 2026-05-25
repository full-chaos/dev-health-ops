from __future__ import annotations

from hashlib import sha256

from sqlalchemy import select

from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDashboardRange,
    ProductTelemetryPlatformDashboard,
    ProductTelemetryTopOrg,
    load_product_telemetry_dashboard,
    load_product_telemetry_platform_dashboard,
)

from ..authz import require_org_id, require_platform_admin
from ..context import GraphQLContext
from ..models.inputs import ProductTelemetryDashboardInput
from ..models.outputs import (
    ProductTelemetryChartInteractionType,
    ProductTelemetryClientErrorType,
    ProductTelemetryDailyActiveUsersType,
    ProductTelemetryDashboardType,
    ProductTelemetryFeatureViewType,
    ProductTelemetryFilterChangeType,
    ProductTelemetryPlatformDashboardType,
    ProductTelemetryPlatformTotalsType,
    ProductTelemetryRouteUsageType,
    ProductTelemetrySessionSummaryType,
    ProductTelemetryTopOrgType,
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


async def _load_org_hash_index() -> dict[str, dict[str, str]]:
    """Build a ``{org_id_hash: {org_id, slug, name}}`` lookup from Postgres.

    Loads every organization row (orgs table is small even for large
    deployments) and hashes ``str(org.id)`` with the same recipe used by the
    per-org resolver, so the resulting map can be joined against ClickHouse
    rollups by hash. Unknown hashes — events ingested from orgs that no
    longer exist in Postgres — stay unresolved and are surfaced as hash-only
    rows in the UI.
    """
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.models.users import Organization

    index: dict[str, dict[str, str]] = {}
    async with get_postgres_session() as session:
        result = await session.execute(
            select(Organization.id, Organization.slug, Organization.name)
        )
        for org_id, slug, name in result.all():
            org_id_str = str(org_id)
            org_id_hash = _product_telemetry_org_hash(org_id_str)
            index[org_id_hash] = {
                "org_id": org_id_str,
                "slug": str(slug or ""),
                "name": str(name or ""),
            }
    return index


def _resolve_top_orgs(
    top_orgs: list[ProductTelemetryTopOrg],
    org_index: dict[str, dict[str, str]],
) -> list[ProductTelemetryTopOrgType]:
    """Attach Postgres-side org names to ClickHouse top-org rollups by hash.

    Pure function so the merge is unit-testable without touching Postgres.
    """
    resolved: list[ProductTelemetryTopOrgType] = []
    for org in top_orgs:
        match = org_index.get(org.org_id_hash)
        resolved.append(
            ProductTelemetryTopOrgType(
                org_id_hash=org.org_id_hash,
                events=org.events,
                sessions=org.sessions,
                anonymous_users=org.anonymous_users,
                org_id=(match or {}).get("org_id"),
                org_slug=(match or {}).get("slug") or None,
                org_name=(match or {}).get("name") or None,
            )
        )
    return resolved


def _platform_dashboard_to_graphql(
    dashboard: ProductTelemetryPlatformDashboard,
    org_index: dict[str, dict[str, str]],
) -> ProductTelemetryPlatformDashboardType:
    return ProductTelemetryPlatformDashboardType(
        totals=ProductTelemetryPlatformTotalsType(
            active_orgs=dashboard.totals.active_orgs,
            anonymous_users=dashboard.totals.anonymous_users,
            sessions=dashboard.totals.sessions,
            events=dashboard.totals.events,
        ),
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
        top_orgs=_resolve_top_orgs(dashboard.top_orgs, org_index),
    )


async def resolve_product_telemetry_platform_dashboard(
    context: GraphQLContext,
    input: ProductTelemetryDashboardInput,
) -> ProductTelemetryPlatformDashboardType:
    """Cross-org product telemetry dashboard for platform/super admins.

    Requires ``context.user.is_superuser``. Aggregates across every tenant
    and returns a top-orgs roll-up with names resolved from Postgres.
    """
    require_platform_admin(context)
    client = context.client
    if client is None:
        raise RuntimeError("Database client not available")
    if input.start_date > input.end_date:
        raise ValueError("start_date must be before or equal to end_date")

    dashboard = await load_product_telemetry_platform_dashboard(
        client,
        date_range=ProductTelemetryDashboardRange(
            start_date=input.start_date,
            end_date=input.end_date,
        ),
    )

    org_index = await _load_org_hash_index() if dashboard.top_orgs else {}
    return _platform_dashboard_to_graphql(dashboard, org_index)
