from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

from dev_health_ops.api.queries import aggregated_flame
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def _capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {"query": "", "params": {}}

    async def fake_query_dicts(_sink: Any, query: str, params: dict[str, Any]):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(aggregated_flame, "query_dicts", fake_query_dicts)
    return captured


def _assert_primary_attribution_sql(query: str) -> None:
    assert "FROM work_item_cycle_times AS wct FINAL" in query
    assert "FROM work_item_team_attributions FINAL" in query
    assert "is_primary = 1" in query
    assert "LEFT JOIN" in query
    assert "t.work_item_id = wct.work_item_id" in query
    assert "'Unassigned'" in query
    assert "t.team_id IS NOT NULL" not in query
    assert "work_item_metrics_daily" not in query


@pytest.mark.asyncio
async def test_throughput_fallback_uses_primary_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch)

    await aggregated_flame.fetch_throughput(
        cast(BaseMetricsSink, object()),
        start_day=date(2026, 1, 1),
        end_day=date(2026, 2, 1),
        team_id="team-a",
        limit=20,
        org_id="org-a",
    )

    _assert_primary_attribution_sql(str(captured["query"]))
    assert "t.team_id = %(team_id)s" in str(captured["query"])
    assert captured["params"]["org_id"] == "org-a"


@pytest.mark.asyncio
async def test_throughput_by_type_uses_primary_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch)

    await aggregated_flame.fetch_throughput_by_type(
        cast(BaseMetricsSink, object()),
        start_day=date(2026, 1, 1),
        end_day=date(2026, 2, 1),
        team_id="team-a",
        limit=20,
        org_id="org-a",
    )

    _assert_primary_attribution_sql(str(captured["query"]))
    assert "coalesce(nullIf(wct.type, ''), 'unclassified')" in str(captured["query"])
