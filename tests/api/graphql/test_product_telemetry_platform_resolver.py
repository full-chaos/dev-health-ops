from __future__ import annotations

from datetime import date
from hashlib import sha256
from typing import Any, cast

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.errors import AuthorizationError
from dev_health_ops.api.graphql.models.inputs import ProductTelemetryDashboardInput
from dev_health_ops.api.graphql.resolvers.product_telemetry import (
    _resolve_top_orgs,
    resolve_product_telemetry_platform_dashboard,
)
from dev_health_ops.api.product_telemetry.dashboard import (
    ProductTelemetryPlatformDashboard,
    ProductTelemetryPlatformTotals,
    ProductTelemetryRouteUsage,
    ProductTelemetrySessionSummary,
    ProductTelemetryTopOrg,
)


class _FakeUser:
    def __init__(self, is_superuser: bool) -> None:
        self.is_superuser = is_superuser


def _superuser_context() -> GraphQLContext:
    # Platform admins can have no org_id; the context relaxes the gate for
    # is_superuser so cross-org queries are reachable.
    return GraphQLContext(
        org_id="",
        db_url="clickhouse://test",
        client=object(),
        user=cast(Any, _FakeUser(is_superuser=True)),
    )


def _input() -> ProductTelemetryDashboardInput:
    return ProductTelemetryDashboardInput(
        start_date=date(2026, 5, 1), end_date=date(2026, 5, 25)
    )


def test_resolve_top_orgs_attaches_postgres_org_names() -> None:
    org_a_id = "00000000-0000-0000-0000-000000000001"
    org_b_id = "00000000-0000-0000-0000-000000000002"
    hash_a = sha256(org_a_id.encode()).hexdigest()
    hash_b = sha256(org_b_id.encode()).hexdigest()

    index = {
        hash_a: {"org_id": org_a_id, "slug": "acme", "name": "Acme Corp"},
        hash_b: {"org_id": org_b_id, "slug": "globex", "name": "Globex"},
    }

    rows = [
        ProductTelemetryTopOrg(
            org_id_hash=hash_a, events=300, sessions=20, anonymous_users=10
        ),
        ProductTelemetryTopOrg(
            org_id_hash=hash_b, events=200, sessions=12, anonymous_users=8
        ),
        ProductTelemetryTopOrg(
            org_id_hash="unknown_hash", events=50, sessions=3, anonymous_users=2
        ),
    ]

    resolved = _resolve_top_orgs(rows, index)

    assert resolved[0].org_id == org_a_id
    assert resolved[0].org_name == "Acme Corp"
    assert resolved[0].org_slug == "acme"
    assert resolved[0].events == 300
    assert resolved[1].org_id == org_b_id
    assert resolved[1].org_name == "Globex"
    # Unknown hash stays hash-only
    assert resolved[2].org_id_hash == "unknown_hash"
    assert resolved[2].org_id is None
    assert resolved[2].org_name is None
    assert resolved[2].org_slug is None


def test_resolve_top_orgs_treats_blank_postgres_name_as_unresolved() -> None:
    """Empty strings from Postgres should not surface as visible org names."""
    org_id = "00000000-0000-0000-0000-000000000003"
    org_hash = sha256(org_id.encode()).hexdigest()

    index = {org_hash: {"org_id": org_id, "slug": "", "name": ""}}
    rows = [
        ProductTelemetryTopOrg(
            org_id_hash=org_hash, events=5, sessions=1, anonymous_users=1
        )
    ]
    resolved = _resolve_top_orgs(rows, index)

    assert resolved[0].org_id == org_id
    assert resolved[0].org_name is None
    assert resolved[0].org_slug is None


@pytest.mark.asyncio
async def test_resolve_platform_dashboard_rejects_non_superuser() -> None:
    context = GraphQLContext(
        org_id="org_raw_123",
        db_url="clickhouse://test",
        client=object(),
        user=cast(Any, _FakeUser(is_superuser=False)),
    )
    with pytest.raises(AuthorizationError):
        await resolve_product_telemetry_platform_dashboard(context, _input())


@pytest.mark.asyncio
async def test_resolve_platform_dashboard_rejects_unauthenticated() -> None:
    context = GraphQLContext(
        org_id="org_raw_123",
        db_url="clickhouse://test",
        client=object(),
        user=None,
    )
    with pytest.raises(AuthorizationError):
        await resolve_product_telemetry_platform_dashboard(context, _input())


@pytest.mark.asyncio
async def test_resolve_platform_dashboard_returns_payload_and_resolves_org_names(
    monkeypatch,
) -> None:
    org_id = "00000000-0000-0000-0000-000000000010"
    org_hash = sha256(org_id.encode()).hexdigest()

    async def fake_loader(client, date_range):
        return ProductTelemetryPlatformDashboard(
            totals=ProductTelemetryPlatformTotals(
                active_orgs=2,
                anonymous_users=50,
                sessions=120,
                events=900,
            ),
            daily_active_users=[],
            top_routes=[
                ProductTelemetryRouteUsage(
                    route_pattern="/metrics",
                    events=10,
                    sessions=5,
                    anonymous_users=4,
                )
            ],
            feature_views=[],
            filter_changes=[],
            chart_interactions=[],
            client_errors=[],
            session_summary=ProductTelemetrySessionSummary(),
            top_orgs=[
                ProductTelemetryTopOrg(
                    org_id_hash=org_hash,
                    events=600,
                    sessions=80,
                    anonymous_users=30,
                ),
                ProductTelemetryTopOrg(
                    org_id_hash="unknown_hash",
                    events=300,
                    sessions=40,
                    anonymous_users=20,
                ),
            ],
        )

    async def fake_index():
        return {
            org_hash: {"org_id": org_id, "slug": "acme", "name": "Acme Corp"},
        }

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry"
        ".load_product_telemetry_platform_dashboard",
        fake_loader,
    )
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry._load_org_hash_index",
        fake_index,
    )

    result = await resolve_product_telemetry_platform_dashboard(
        _superuser_context(), _input()
    )

    assert result.totals.active_orgs == 2
    assert result.totals.events == 900
    assert result.top_routes[0].route_pattern == "/metrics"
    assert len(result.top_orgs) == 2
    assert result.top_orgs[0].org_id == org_id
    assert result.top_orgs[0].org_name == "Acme Corp"
    assert result.top_orgs[0].events == 600
    assert result.top_orgs[1].org_id is None
    assert result.top_orgs[1].org_name is None


@pytest.mark.asyncio
async def test_resolve_platform_dashboard_skips_postgres_lookup_when_no_top_orgs(
    monkeypatch,
) -> None:
    """If ClickHouse returns no top-orgs we must not hit Postgres at all."""
    postgres_called = False

    async def fake_loader(client, date_range):
        return ProductTelemetryPlatformDashboard(
            totals=ProductTelemetryPlatformTotals(),
            top_orgs=[],
        )

    async def fake_index():
        nonlocal postgres_called
        postgres_called = True
        return {}

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry"
        ".load_product_telemetry_platform_dashboard",
        fake_loader,
    )
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry._load_org_hash_index",
        fake_index,
    )

    result = await resolve_product_telemetry_platform_dashboard(
        _superuser_context(), _input()
    )

    assert result.top_orgs == []
    assert postgres_called is False


@pytest.mark.asyncio
async def test_resolve_platform_dashboard_rejects_inverted_date_range(
    monkeypatch,
) -> None:
    async def fake_loader(client, date_range):  # pragma: no cover - unreachable
        raise AssertionError("loader should not run for invalid input")

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.product_telemetry"
        ".load_product_telemetry_platform_dashboard",
        fake_loader,
    )

    bad_input = ProductTelemetryDashboardInput(
        start_date=date(2026, 5, 25), end_date=date(2026, 5, 1)
    )
    with pytest.raises(ValueError):
        await resolve_product_telemetry_platform_dashboard(
            _superuser_context(), bad_input
        )
