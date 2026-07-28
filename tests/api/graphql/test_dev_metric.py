from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.metrics.service import (
    MetricSourceRef,
    MetricSourceState,
    RawMetricResult,
    RawMetricRow,
)
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers import dev_entitlement
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.api.services.auth import AuthenticatedUser

UTC = timezone.utc
ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
OTHER_ORG_ID = "80d529e0-3c06-4597-8480-794fd02328b6"

_CATALOG_QUERY = """
query MetricCatalog($orgId: String!) {
  devMetricCatalog(orgId: $orgId) {
    registryVersion
    metrics { metricId definitionVersion sourceTable queryVersion }
  }
}
"""

_METRIC_QUERY = """
query Metric($orgId: String!, $input: DevMetricQueryInput!) {
  devMetric(orgId: $orgId, input: $input) {
    definition { metricId unit aggregation definitionVersion sourceVersion queryVersion }
    state
    freshness
    values { value comparisonValue }
    currentWindowStart
    currentWindowEnd
    comparisonWindowStart
    comparisonWindowEnd
    sourceRefs { refId sourceTable sourceVersion queryVersion }
  }
}
"""


def _context(org_id: str = ORG_ID, *, authenticated: bool = True) -> GraphQLContext:
    user = None
    if authenticated:
        user = AuthenticatedUser(
            user_id="user-a",
            email="member@example.com",
            org_id=org_id,
            role="member",
            token_version=7,
        )
    return GraphQLContext(
        org_id=org_id,
        db_url="clickhouse://test",
        client=object(),
        user=user,
    )


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ORGANIZATION,
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 8, tzinfo=UTC),
            timezone="UTC",
        ),
        comparison_range=DevTimeRange(
            start=datetime(2026, 6, 24, tzinfo=UTC),
            end=datetime(2026, 7, 1, tzinfo=UTC),
            timezone="UTC",
        ),
    )


@pytest.fixture
def _allow_ask_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def fake_session():
        yield object()

    async def allow(_session, _org_id, _feature_key):
        return True

    monkeypatch.setattr(dev_entitlement, "get_postgres_session", fake_session)
    monkeypatch.setattr(dev_entitlement, "is_org_feature_enabled_async", allow)


@pytest.mark.asyncio
async def test_graphql_catalog_exposes_exactly_the_shared_eight(_allow_ask_dev) -> None:
    result = await schema.execute(
        _CATALOG_QUERY,
        variable_values={"orgId": ORG_ID},
        context_value=_context(),
    )

    assert result.errors is None
    assert result.data is not None
    catalog = result.data["devMetricCatalog"]
    assert catalog["registryVersion"] == "ask-dev-metrics.v1"
    assert [item["metricId"] for item in catalog["metrics"]] == [
        "ITEMS_COMPLETED",
        "CYCLE_TIME_P50_HOURS",
        "AVG_WIP",
        "DEPLOYMENTS_COUNT",
        "CHANGE_FAILURE_RATE",
        "INVESTMENT_ALLOCATION_PCT",
        "CYCLOMATIC_PER_KLOC",
        "COMPOUNDING_RISK_SCORE",
    ]
    assert len(catalog["metrics"]) == 8


@pytest.mark.asyncio
async def test_graphql_metric_uses_shared_service_and_prior_window(
    monkeypatch: pytest.MonkeyPatch,
    _allow_ask_dev,
) -> None:
    class FakeSource:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def watermark(self, org_id, definition, scope):
            assert org_id == ORG_ID
            return "watermark-1"

        async def query(
            self,
            org_id,
            definition,
            scope,
            *,
            comparison,
            include_series,
            max_series_points,
        ):
            assert max_series_points == 90
            day = date(2026, 6, 30) if comparison else date(2026, 7, 7)
            watermark = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
            return RawMetricResult(
                rows=(RawMetricRow(dimensions=(), value=0.125 if comparison else 0.2),),
                watermark=watermark,
                latest_materialized_day=day,
                source_state=MetricSourceState.AVAILABLE,
                covered_days=7,
                expected_days=7,
                source_refs=(
                    MetricSourceRef(
                        ref_id="metric-source:test",
                        source_table=definition.source_table,
                        source_version=definition.source_version,
                        watermark=watermark,
                        query_version=definition.query_version,
                    ),
                ),
            )

    async def fake_scope(_context, _input):
        return _scope()

    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_metric.ClickHouseMetricSource",
        FakeSource,
    )
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_metric._resolve_scope", fake_scope
    )

    result = await schema.execute(
        _METRIC_QUERY,
        variable_values={
            "orgId": ORG_ID,
            "input": {
                "metricId": "CHANGE_FAILURE_RATE",
                "scope": {
                    "directScope": "ORGANIZATION",
                    "startDate": "2026-07-01",
                    "endDate": "2026-07-08",
                },
            },
        },
        context_value=_context(),
    )

    assert result.errors is None
    assert result.data is not None
    metric = result.data["devMetric"]
    assert metric["values"] == [{"value": 0.2, "comparisonValue": 0.125}]
    assert metric["definition"] == {
        "metricId": "CHANGE_FAILURE_RATE",
        "unit": "ratio",
        "aggregation": "sum_failed_deployments_divided_by_sum_deployments",
        "definitionVersion": "change_failure_rate.v1",
        "sourceVersion": "deploy_metrics_daily.v1",
        "queryVersion": "query-change_failure_rate.v1",
    }
    assert metric["comparisonWindowStart"] == "2026-06-24T00:00:00+00:00"
    assert metric["comparisonWindowEnd"] == "2026-07-01T00:00:00+00:00"
    assert metric["sourceRefs"][0]["sourceTable"] == "deploy_metrics_daily"


@pytest.mark.asyncio
async def test_graphql_metric_catalog_rejects_unauthenticated_and_cross_tenant() -> (
    None
):
    unauthenticated = await schema.execute(
        _CATALOG_QUERY,
        variable_values={"orgId": ORG_ID},
        context_value=_context(authenticated=False),
    )
    cross_tenant = await schema.execute(
        _CATALOG_QUERY,
        variable_values={"orgId": OTHER_ORG_ID},
        context_value=_context(),
    )

    assert unauthenticated.errors is not None
    assert cross_tenant.errors is not None


@pytest.mark.asyncio
async def test_graphql_metric_catalog_requires_ask_dev_entitlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_session():
        yield object()

    async def deny(_session, _org_id, _feature_key):
        return False

    monkeypatch.setattr(dev_entitlement, "get_postgres_session", fake_session)
    monkeypatch.setattr(dev_entitlement, "is_org_feature_enabled_async", deny)
    result = await schema.execute(
        _CATALOG_QUERY,
        variable_values={"orgId": ORG_ID},
        context_value=_context(),
    )

    assert result.errors is not None
    assert result.data is None


@pytest.mark.asyncio
async def test_shared_entitlement_boundary_uses_canonical_feature_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    @asynccontextmanager
    async def fake_session():
        yield object()

    async def decide(session, org_id, feature_key):
        observed.update(session=session, org_id=org_id, feature_key=feature_key)
        return True

    monkeypatch.setattr(dev_entitlement, "get_postgres_session", fake_session)
    monkeypatch.setattr(dev_entitlement, "is_org_feature_enabled_async", decide)

    await dev_entitlement.require_ask_dev_entitlement(
        "70d529e0-3c06-4597-8480-794fd02328b6"
    )

    assert str(observed["org_id"]) == "70d529e0-3c06-4597-8480-794fd02328b6"
    assert observed["feature_key"] == "ask_dev"
