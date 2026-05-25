from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDashboardRange,
    load_product_telemetry_platform_dashboard,
)


class FakeQueryClient:
    backend_type = "clickhouse"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []


async def fake_query_dicts(
    client: FakeQueryClient, sql: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    client.calls.append((sql, params))
    if "active_orgs" in sql:
        return [
            {
                "active_orgs": 4,
                "anonymous_users": 120,
                "sessions": 340,
                "events": 5000,
            }
        ]
    if "active_anonymous_users" in sql:
        return [{"day": date(2026, 5, 24), "active_anonymous_users": 42}]
    if "name = 'page_viewed'" in sql:
        return [
            {
                "route_pattern": "/metrics",
                "events": 90,
                "sessions": 33,
                "anonymous_users": 25,
            }
        ]
    if "name = 'feature_viewed'" in sql:
        return [
            {
                "feature": "investment",
                "surface": "dashboard",
                "views": 50,
                "anonymous_users": 30,
            }
        ]
    if "name = 'filter_changed'" in sql:
        return [
            {
                "view": "metrics",
                "filter_key": "team",
                "changes": 14,
                "avg_value_count": 1.7,
            }
        ]
    if "name = 'chart_interacted'" in sql:
        return [
            {
                "chart": "quadrant",
                "action": "hover",
                "surface": "metrics",
                "interactions": 80,
                "sessions": 20,
            }
        ]
    if "name = 'client_error'" in sql:
        return [
            {
                "route_pattern": "/metrics",
                "boundary": "chart",
                "error_class": "RenderError",
                "errors": 6,
                "affected_anonymous_users": 3,
            }
        ]
    if "name = 'session_ended'" in sql:
        return [
            {
                "p50_duration_ms": 1100,
                "p75_duration_ms": 1600,
                "p90_duration_ms": 2600,
                "p95_duration_ms": 3100,
                "avg_pages_viewed": 4.2,
                "avg_interactions": 11.5,
            }
        ]
    if "GROUP BY org_id_hash" in sql:
        return [
            {
                "org_id_hash": "hash_a",
                "events": 3000,
                "sessions": 180,
                "anonymous_users": 60,
            },
            {
                "org_id_hash": "hash_b",
                "events": 2000,
                "sessions": 160,
                "anonymous_users": 60,
            },
        ]
    return []


@pytest.mark.asyncio
async def test_load_platform_dashboard_runs_all_sections_without_org_filter(
    monkeypatch,
) -> None:
    client = FakeQueryClient()
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.dashboard.query_dicts",
        fake_query_dicts,
    )

    result = await load_product_telemetry_platform_dashboard(
        client,
        date_range=ProductTelemetryDashboardRange(
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
        ),
    )

    # totals roll-up
    assert result.totals.active_orgs == 4
    assert result.totals.anonymous_users == 120
    assert result.totals.sessions == 340
    assert result.totals.events == 5000

    # section payloads still resolve correctly
    assert result.daily_active_users[0].active_anonymous_users == 42
    assert result.top_routes[0].route_pattern == "/metrics"
    assert result.feature_views[0].feature == "investment"
    assert result.filter_changes[0].filter_key == "team"
    assert result.chart_interactions[0].chart == "quadrant"
    assert result.client_errors[0].error_class == "RenderError"
    assert result.session_summary.p95_duration_ms == 3100

    # top-orgs rollup is ordered by events desc and stays hash-only at this layer
    assert [o.org_id_hash for o in result.top_orgs] == ["hash_a", "hash_b"]
    assert result.top_orgs[0].events == 3000
    assert result.top_orgs[1].sessions == 160
    assert result.top_orgs[0].org_id is None
    assert result.top_orgs[0].org_name is None

    # 9 queries fire: totals + 7 sections + top_orgs
    assert len(client.calls) == 9
    for sql, params in client.calls:
        assert "org_id_hash = %(org_id_hash)s" not in sql, (
            "platform dashboard must not enforce per-org filter: "
            f"{sql.strip().splitlines()[0]}"
        )
        assert params == {
            "start": date(2026, 5, 1),
            "end": date(2026, 5, 25),
        }


@pytest.mark.asyncio
async def test_load_platform_dashboard_handles_empty_top_orgs(monkeypatch) -> None:
    async def empty_query_dicts(
        client: FakeQueryClient, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        client.calls.append((sql, params))
        return []

    client = FakeQueryClient()
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.dashboard.query_dicts",
        empty_query_dicts,
    )

    result = await load_product_telemetry_platform_dashboard(
        client,
        date_range=ProductTelemetryDashboardRange(
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
        ),
    )

    assert result.totals.active_orgs == 0
    assert result.totals.events == 0
    assert result.top_orgs == []
    assert result.daily_active_users == []
    assert result.session_summary.p50_duration_ms is None
