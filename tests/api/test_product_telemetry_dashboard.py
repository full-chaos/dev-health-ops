from __future__ import annotations

from datetime import date
from math import nan
from typing import Any

import pytest

from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDashboardRange,
    load_product_telemetry_dashboard,
)


class FakeQueryClient:
    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []


async def fake_query_dicts(
    client: FakeQueryClient, sql: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    client.calls.append((sql, params))
    if "active_anonymous_users" in sql:
        return [{"day": date(2026, 5, 24), "active_anonymous_users": 7}]
    if "name = 'page_viewed'" in sql:
        return [
            {
                "route_pattern": "/metrics",
                "events": 9,
                "sessions": 3,
                "anonymous_users": 2,
            }
        ]
    if "name = 'feature_viewed'" in sql:
        return [
            {
                "feature": "investment",
                "surface": "dashboard",
                "views": 5,
                "anonymous_users": 2,
            }
        ]
    if "name = 'filter_changed'" in sql:
        return [
            {
                "view": "metrics",
                "filter_key": "team",
                "changes": 4,
                "avg_value_count": 1.5,
            }
        ]
    if "name = 'chart_interacted'" in sql:
        return [
            {
                "chart": "quadrant",
                "action": "hover",
                "surface": "metrics",
                "interactions": 8,
                "sessions": 2,
            }
        ]
    if "name = 'client_error'" in sql:
        return [
            {
                "route_pattern": "/metrics",
                "boundary": "chart",
                "error_class": "RenderError",
                "errors": 2,
                "affected_anonymous_users": 1,
            }
        ]
    if "name = 'session_ended'" in sql:
        return [
            {
                "p50_duration_ms": 1000,
                "p75_duration_ms": 1500,
                "p90_duration_ms": 2500,
                "p95_duration_ms": 3000,
                "avg_pages_viewed": 4.0,
                "avg_interactions": 11.0,
            }
        ]
    return []


@pytest.mark.asyncio
async def test_load_product_telemetry_dashboard_queries_all_sections(
    monkeypatch,
) -> None:
    client = FakeQueryClient()
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.dashboard.query_dicts",
        fake_query_dicts,
    )

    result = await load_product_telemetry_dashboard(
        client,
        org_id_hash="org_hash_123",
        date_range=ProductTelemetryDashboardRange(
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
        ),
    )

    assert result.daily_active_users[0].active_anonymous_users == 7
    assert result.top_routes[0].route_pattern == "/metrics"
    assert result.feature_views[0].feature == "investment"
    assert result.filter_changes[0].filter_key == "team"
    assert result.chart_interactions[0].chart == "quadrant"
    assert result.client_errors[0].error_class == "RenderError"
    assert result.session_summary.p95_duration_ms == 3000
    assert len(client.calls) == 7
    assert all(call[1]["org_id_hash"] == "org_hash_123" for call in client.calls)
    assert all("product_telemetry_events" in call[0] for call in client.calls)
    assert all("occurred_at >= %(start)s" in call[0] for call in client.calls)
    assert all("occurred_at < %(end)s" in call[0] for call in client.calls)


async def fake_query_dicts_with_empty_session_summary(
    client: FakeQueryClient, sql: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    client.calls.append((sql, params))
    if "name = 'session_ended'" in sql:
        return [
            {
                "p50_duration_ms": nan,
                "p75_duration_ms": nan,
                "p90_duration_ms": nan,
                "p95_duration_ms": nan,
                "avg_pages_viewed": nan,
                "avg_interactions": nan,
            }
        ]
    return []


@pytest.mark.asyncio
async def test_load_product_telemetry_dashboard_normalizes_empty_session_summary(
    monkeypatch,
) -> None:
    client = FakeQueryClient()
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.dashboard.query_dicts",
        fake_query_dicts_with_empty_session_summary,
    )

    result = await load_product_telemetry_dashboard(
        client,
        org_id_hash="org_hash_123",
        date_range=ProductTelemetryDashboardRange(
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
        ),
    )

    assert result.session_summary.p50_duration_ms is None
    assert result.session_summary.p95_duration_ms is None
    assert result.session_summary.avg_pages_viewed is None
    assert result.session_summary.avg_interactions is None
