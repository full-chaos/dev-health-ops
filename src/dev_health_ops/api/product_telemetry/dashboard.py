from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from dev_health_ops.api.queries.client import query_dicts, require_clickhouse_backend


@dataclass(frozen=True)
class ProductTelemetryDashboardRange:
    """Half-open dashboard range: start_date is inclusive, end_date is exclusive."""

    start_date: date
    end_date: date


@dataclass(frozen=True)
class ProductTelemetryDailyActiveUsers:
    day: date
    active_anonymous_users: int


@dataclass(frozen=True)
class ProductTelemetryRouteUsage:
    route_pattern: str
    events: int
    sessions: int
    anonymous_users: int


@dataclass(frozen=True)
class ProductTelemetryFeatureView:
    feature: str
    surface: str
    views: int
    anonymous_users: int


@dataclass(frozen=True)
class ProductTelemetryFilterChange:
    view: str
    filter_key: str
    changes: int
    avg_value_count: float | None


@dataclass(frozen=True)
class ProductTelemetryChartInteraction:
    chart: str
    action: str
    surface: str
    interactions: int
    sessions: int


@dataclass(frozen=True)
class ProductTelemetryClientError:
    route_pattern: str
    boundary: str
    error_class: str
    errors: int
    affected_anonymous_users: int


@dataclass(frozen=True)
class ProductTelemetrySessionSummary:
    p50_duration_ms: int | None = None
    p75_duration_ms: int | None = None
    p90_duration_ms: int | None = None
    p95_duration_ms: int | None = None
    avg_pages_viewed: float | None = None
    avg_interactions: float | None = None


@dataclass(frozen=True)
class ProductTelemetryDashboard:
    daily_active_users: list[ProductTelemetryDailyActiveUsers] = field(
        default_factory=list
    )
    top_routes: list[ProductTelemetryRouteUsage] = field(default_factory=list)
    feature_views: list[ProductTelemetryFeatureView] = field(default_factory=list)
    filter_changes: list[ProductTelemetryFilterChange] = field(default_factory=list)
    chart_interactions: list[ProductTelemetryChartInteraction] = field(
        default_factory=list
    )
    client_errors: list[ProductTelemetryClientError] = field(default_factory=list)
    session_summary: ProductTelemetrySessionSummary = field(
        default_factory=ProductTelemetrySessionSummary
    )


DAILY_ACTIVE_USERS_SQL = """
SELECT
    toDate(occurred_at) AS day,
    uniqExact(anonymous_user_id) AS active_anonymous_users
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY day
ORDER BY day
"""

TOP_ROUTES_SQL = """
SELECT
    route_pattern,
    count() AS events,
    uniqExact(session_id) AS sessions,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND name = 'page_viewed'
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY route_pattern
ORDER BY events DESC
LIMIT 25
"""

FEATURE_VIEWS_SQL = """
SELECT
    JSONExtractString(payload_json, 'feature') AS feature,
    JSONExtractString(payload_json, 'surface') AS surface,
    count() AS views,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND name = 'feature_viewed'
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY feature, surface
ORDER BY views DESC
"""

FILTER_CHANGES_SQL = """
SELECT
    JSONExtractString(payload_json, 'view') AS view,
    JSONExtractString(payload_json, 'filterKey') AS filter_key,
    count() AS changes,
    avg(JSONExtractInt(payload_json, 'valueCount')) AS avg_value_count
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND name = 'filter_changed'
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY view, filter_key
ORDER BY changes DESC
"""

CHART_INTERACTIONS_SQL = """
SELECT
    JSONExtractString(payload_json, 'chart') AS chart,
    JSONExtractString(payload_json, 'action') AS action,
    JSONExtractString(payload_json, 'surface') AS surface,
    count() AS interactions,
    uniqExact(session_id) AS sessions
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND name = 'chart_interacted'
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY chart, action, surface
ORDER BY interactions DESC
"""

CLIENT_ERRORS_SQL = """
SELECT
    route_pattern,
    JSONExtractString(payload_json, 'boundary') AS boundary,
    JSONExtractString(payload_json, 'errorClass') AS error_class,
    count() AS errors,
    uniqExact(anonymous_user_id) AS affected_anonymous_users
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND name = 'client_error'
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
GROUP BY route_pattern, boundary, error_class
ORDER BY errors DESC
"""

SESSION_SUMMARY_SQL = """
SELECT
    quantile(0.5)(JSONExtractInt(payload_json, 'durationMs')) AS p50_duration_ms,
    quantile(0.75)(JSONExtractInt(payload_json, 'durationMs')) AS p75_duration_ms,
    quantile(0.9)(JSONExtractInt(payload_json, 'durationMs')) AS p90_duration_ms,
    quantile(0.95)(JSONExtractInt(payload_json, 'durationMs')) AS p95_duration_ms,
    avg(JSONExtractInt(payload_json, 'pagesViewed')) AS avg_pages_viewed,
    avg(JSONExtractInt(payload_json, 'interactions')) AS avg_interactions
FROM product_telemetry_events
WHERE org_id_hash = %(org_id_hash)s
  AND name = 'session_ended'
  AND occurred_at >= %(start)s
  AND occurred_at < %(end)s
"""


def _params(
    org_id_hash: str, date_range: ProductTelemetryDashboardRange
) -> dict[str, Any]:
    return {
        "org_id_hash": org_id_hash,
        "start": date_range.start_date,
        "end": date_range.end_date,
    }


async def load_product_telemetry_dashboard(
    client: Any,
    org_id_hash: str,
    date_range: ProductTelemetryDashboardRange,
) -> ProductTelemetryDashboard:
    require_clickhouse_backend(client)
    params = _params(org_id_hash, date_range)

    (
        daily_rows,
        route_rows,
        feature_rows,
        filter_rows,
        chart_rows,
        error_rows,
        session_rows,
    ) = await asyncio.gather(
        query_dicts(client, DAILY_ACTIVE_USERS_SQL, params),
        query_dicts(client, TOP_ROUTES_SQL, params),
        query_dicts(client, FEATURE_VIEWS_SQL, params),
        query_dicts(client, FILTER_CHANGES_SQL, params),
        query_dicts(client, CHART_INTERACTIONS_SQL, params),
        query_dicts(client, CLIENT_ERRORS_SQL, params),
        query_dicts(client, SESSION_SUMMARY_SQL, params),
    )

    return ProductTelemetryDashboard(
        daily_active_users=[
            ProductTelemetryDailyActiveUsers(
                day=row["day"],
                active_anonymous_users=int(row.get("active_anonymous_users") or 0),
            )
            for row in daily_rows
        ],
        top_routes=[
            ProductTelemetryRouteUsage(
                route_pattern=str(row.get("route_pattern") or ""),
                events=int(row.get("events") or 0),
                sessions=int(row.get("sessions") or 0),
                anonymous_users=int(row.get("anonymous_users") or 0),
            )
            for row in route_rows
        ],
        feature_views=[
            ProductTelemetryFeatureView(
                feature=str(row.get("feature") or ""),
                surface=str(row.get("surface") or ""),
                views=int(row.get("views") or 0),
                anonymous_users=int(row.get("anonymous_users") or 0),
            )
            for row in feature_rows
        ],
        filter_changes=[
            ProductTelemetryFilterChange(
                view=str(row.get("view") or ""),
                filter_key=str(row.get("filter_key") or ""),
                changes=int(row.get("changes") or 0),
                avg_value_count=_optional_float(row.get("avg_value_count")),
            )
            for row in filter_rows
        ],
        chart_interactions=[
            ProductTelemetryChartInteraction(
                chart=str(row.get("chart") or ""),
                action=str(row.get("action") or ""),
                surface=str(row.get("surface") or ""),
                interactions=int(row.get("interactions") or 0),
                sessions=int(row.get("sessions") or 0),
            )
            for row in chart_rows
        ],
        client_errors=[
            ProductTelemetryClientError(
                route_pattern=str(row.get("route_pattern") or ""),
                boundary=str(row.get("boundary") or ""),
                error_class=str(row.get("error_class") or ""),
                errors=int(row.get("errors") or 0),
                affected_anonymous_users=int(row.get("affected_anonymous_users") or 0),
            )
            for row in error_rows
        ],
        session_summary=_session_summary(session_rows),
    )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result):
        return None
    return result


def _optional_int(value: Any) -> int | None:
    value = _optional_float(value)
    if value is None:
        return None
    return int(value)


def _session_summary(rows: list[dict[str, Any]]) -> ProductTelemetrySessionSummary:
    if not rows:
        return ProductTelemetrySessionSummary()
    row = rows[0]
    return ProductTelemetrySessionSummary(
        p50_duration_ms=_optional_int(row.get("p50_duration_ms")),
        p75_duration_ms=_optional_int(row.get("p75_duration_ms")),
        p90_duration_ms=_optional_int(row.get("p90_duration_ms")),
        p95_duration_ms=_optional_int(row.get("p95_duration_ms")),
        avg_pages_viewed=_optional_float(row.get("avg_pages_viewed")),
        avg_interactions=_optional_float(row.get("avg_interactions")),
    )
