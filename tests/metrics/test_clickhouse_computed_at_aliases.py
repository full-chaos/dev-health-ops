from __future__ import annotations

from datetime import date
from typing import cast
from unittest.mock import MagicMock

import pytest

from dev_health_ops.audit.ai_governance.loaders import AIGovernanceLoader
from dev_health_ops.metrics.loaders.ai_impact import AIImpactClickHouseLoader
from dev_health_ops.metrics.opportunities.ai_detector import AIOpportunityDetector
from dev_health_ops.metrics.sinks.clickhouse.core import ClickHouseCore


def _assert_computed_at_alias_safe(query: str) -> None:
    assert "argMax(" in query
    assert " AS computed_at" in query
    assert "max(computed_at) AS computed_at" not in query
    assert ", computed_at)" not in query


@pytest.mark.asyncio
async def test_ai_impact_query_qualifies_computed_at(monkeypatch: pytest.MonkeyPatch):
    captured = {"query": ""}

    async def fake_query_dicts(_client, query, _params):
        captured["query"] = query
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    await AIImpactClickHouseLoader(MagicMock(), org_id="org-a").load_ai_impact_metrics(
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
    )

    _assert_computed_at_alias_safe(captured["query"])
    assert "FROM ai_impact_metrics_daily AS metrics" in captured["query"]
    assert "max(metrics.computed_at) AS computed_at" in captured["query"]


@pytest.mark.asyncio
async def test_ai_opportunity_query_qualifies_computed_at(
    monkeypatch: pytest.MonkeyPatch,
):
    captured = {"query": ""}

    async def fake_query_dicts(_client, query, _params):
        if "ai_impact_metrics_daily" in query:
            captured["query"] = query
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    _assert_computed_at_alias_safe(captured["query"])
    assert "FROM ai_impact_metrics_daily AS metrics" in captured["query"]
    assert "max(metrics.computed_at) AS computed_at" in captured["query"]


def test_ai_governance_coverage_query_qualifies_computed_at():
    captured = {"query": ""}

    class FakeClient:
        def query_dicts(self, query, _params):
            captured["query"] = query
            return []

    AIGovernanceLoader(FakeClient()).load_coverage(
        org_id="org-a",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
    )

    _assert_computed_at_alias_safe(captured["query"])
    assert "FROM ai_governance_coverage_daily AS coverage" in captured["query"]
    assert "max(coverage.computed_at) AS computed_at" in captured["query"]


def test_latest_repo_metrics_query_qualifies_computed_at():
    query = ClickHouseCore.latest_repo_metrics_query(
        cast(ClickHouseCore, object()),
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
    )

    _assert_computed_at_alias_safe(query)
    assert "FROM repo_metrics_daily AS metrics" in query
    assert "max(metrics.computed_at) AS computed_at" in query
