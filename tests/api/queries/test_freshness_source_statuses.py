from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.queries import freshness as freshness_queries
from dev_health_ops.api.queries.freshness import fetch_source_statuses
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


@pytest.mark.asyncio
async def test_source_statuses_derive_from_ingested_provider_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query_dicts(
        sink: BaseMetricsSink,
        query: str,
        parameters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        assert "repos" in query
        assert "work_items" in query
        assert "ci_pipeline_runs" in query
        assert parameters == {"org_id": "org-test"}
        return [
            {
                "source": "github",
                "last_seen_at": datetime(2026, 6, 10, tzinfo=timezone.utc),
            },
            {
                "source": "jira",
                "last_seen_at": datetime(2026, 5, 20, tzinfo=timezone.utc),
            },
            {"source": "ci", "last_seen_at": None},
        ]

    monkeypatch.setattr(freshness_queries, "query_dicts", fake_query_dicts)

    statuses = await fetch_source_statuses(
        MagicMock(spec=BaseMetricsSink),
        start_day=date(2026, 6, 1),
        org_id="org-test",
    )

    assert statuses == {"ci": "down", "github": "ok", "jira": "degraded"}
