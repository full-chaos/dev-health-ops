from __future__ import annotations

from datetime import date
from hashlib import sha256

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import ProductTelemetryDashboardInput
from dev_health_ops.api.graphql.resolvers.product_telemetry import (
    resolve_product_telemetry_dashboard,
)
from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryDailyActiveUsers,
    ProductTelemetryDashboard,
    ProductTelemetrySessionSummary,
)


@pytest.mark.asyncio
async def test_resolve_product_telemetry_dashboard_requires_org_and_maps_sections(
    monkeypatch,
) -> None:
    expected_org_id_hash = sha256(b"org_raw_123").hexdigest()

    async def fake_loader(client, org_id_hash, date_range):
        assert client is fake_client
        assert org_id_hash == expected_org_id_hash
        assert date_range.start_date == date(2026, 5, 1)
        assert date_range.end_date == date(2026, 5, 25)
        return ProductTelemetryDashboard(
            daily_active_users=[
                ProductTelemetryDailyActiveUsers(
                    day=date(2026, 5, 24), active_anonymous_users=7
                )
            ],
            top_routes=[],
            feature_views=[],
            filter_changes=[],
            chart_interactions=[],
            client_errors=[],
            session_summary=ProductTelemetrySessionSummary(),
        )

    fake_client = object()
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry.load_product_telemetry_dashboard",
        fake_loader,
    )

    context = GraphQLContext(
        org_id="org_raw_123", db_url="clickhouse://test", client=fake_client
    )
    result = await resolve_product_telemetry_dashboard(
        context,
        ProductTelemetryDashboardInput(
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
        ),
    )

    assert result.daily_active_users[0].active_anonymous_users == 7
    assert result.session_summary.avg_pages_viewed is None
