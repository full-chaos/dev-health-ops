from __future__ import annotations

from datetime import datetime, timezone
from typing import TypedDict, cast

import pytest

import dev_health_ops.api.queries.work_unit_investments as work_unit_investments
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


class _CapturedQuery(TypedDict):
    query: str
    params: dict[str, object]


@pytest.mark.asyncio
async def test_work_unit_investments_query_qualifies_columns(monkeypatch):
    captured: _CapturedQuery = {"query": "", "params": {}}

    async def _fake_query_dicts(_client, query: str, params):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(work_unit_investments, "query_dicts", _fake_query_dicts)

    start_ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end_ts = datetime(2025, 1, 2, tzinfo=timezone.utc)
    await work_unit_investments.fetch_work_unit_investments(
        cast(BaseMetricsSink, object()),
        start_ts=start_ts,
        end_ts=end_ts,
        repo_ids=None,
        limit=10,
    )

    assert "work_unit_investments.from_ts" in captured["query"]
    assert "work_unit_investments.to_ts" in captured["query"]
    assert "argMax(from_ts, work_unit_investments.computed_at)" in captured["query"]
    assert "argMax(computed_at, computed_at)" not in captured["query"]
