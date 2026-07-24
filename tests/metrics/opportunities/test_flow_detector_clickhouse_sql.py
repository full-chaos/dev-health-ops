from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.metrics.opportunities.flow_detector import FlowOpportunityDetector


@pytest.mark.asyncio
async def test_detector_metric_queries_dedup_all_rerunnable_daily_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queries: list[str] = []

    async def fake_query_dicts(
        _client: Any, query: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        queries.append(" ".join(query.split()))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    await FlowOpportunityDetector(MagicMock()).detect("org-test")

    repo_query = next(query for query in queries if "FROM repo_metrics_daily" in query)
    work_item_query = next(
        query for query in queries if "FROM work_item_metrics_daily" in query
    )

    assert "ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day" in repo_query
    assert "FROM work_item_metrics_daily FINAL" in work_item_query
