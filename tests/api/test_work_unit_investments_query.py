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
    assert "%(start_ts)s" in captured["query"]
    assert "%(end_ts)s" in captured["query"]
    assert "{start_ts:DateTime}" not in captured["query"]
    assert "{end_ts:DateTime}" not in captured["query"]
    assert "latest_work_unit_investments AS" in captured["query"]
    assert "membership_scoped_work_unit_ids AS" in captured["query"]
    assert "latest_computed_at AS computed_at" in captured["query"]
    assert "argMax(computed_at, computed_at)" not in captured["query"]
    assert "ORDER BY effort_value DESC, work_unit_id ASC" in captured["query"]


@pytest.mark.asyncio
async def test_work_unit_lookup_queries_use_typed_array_params(monkeypatch):
    captured: list[_CapturedQuery] = []

    async def _fake_query_dicts(_client, query: str, params):
        captured.append({"query": query, "params": params})
        return []

    monkeypatch.setattr(work_unit_investments, "query_dicts", _fake_query_dicts)

    await work_unit_investments.fetch_work_item_team_assignments(
        cast(BaseMetricsSink, object()),
        work_item_ids=["linear:CHAOS-1", "linear:CHAOS-2", "linear:CHAOS-1"],
        org_id="org-1",
    )
    await work_unit_investments.fetch_work_unit_investment_quotes(
        cast(BaseMetricsSink, object()),
        unit_runs=[("linear:CHAOS-1", "run-1"), ("linear:CHAOS-2", "run-2")],
        org_id="org-1",
    )

    assert "work_item_id IN {work_item_ids:Array(String)}" in captured[0]["query"]
    assert "FROM work_item_team_attributions FINAL" in captured[0]["query"]
    assert "is_primary = 1" in captured[0]["query"]
    assert "(work_item_id, computed_at) IN" in captured[0]["query"]
    assert "max(computed_at)" in captured[0]["query"]
    assert "work_item_cycle_times" not in captured[0]["query"]
    assert "%(work_item_ids)s" not in captured[0]["query"]
    assert captured[0]["params"]["work_item_ids"] == [
        "linear:CHAOS-1",
        "linear:CHAOS-2",
    ]
    assert (
        "(work_unit_id, categorization_run_id) IN {pairs:Array(Tuple(String, String))}"
        in captured[1]["query"]
    )
    assert "%(pairs)s" not in captured[1]["query"]


@pytest.mark.asyncio
async def test_work_unit_lookup_queries_chunk_large_id_sets(monkeypatch):
    captured: list[_CapturedQuery] = []

    async def _fake_query_dicts(_client, query: str, params):
        captured.append({"query": query, "params": params})
        return []

    monkeypatch.setattr(work_unit_investments, "query_dicts", _fake_query_dicts)

    work_item_ids = [f"linear:CHAOS-{index}" for index in range(251)]
    await work_unit_investments.fetch_work_item_team_assignments(
        cast(BaseMetricsSink, object()), work_item_ids=work_item_ids, org_id="org-1"
    )

    assert len(captured) == 2
    first_chunk = cast(list[str], captured[0]["params"]["work_item_ids"])
    second_chunk = cast(list[str], captured[1]["params"]["work_item_ids"])
    assert len(first_chunk) == 250
    assert len(second_chunk) == 1
