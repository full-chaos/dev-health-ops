from __future__ import annotations

from datetime import date, datetime
from typing import Any, cast

import pytest

from dev_health_ops.api.queries import people
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


@pytest.mark.asyncio
async def test_person_issues_read_team_identity_from_wita(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_query_dicts(_sink: Any, query: str, params: dict[str, Any]):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(people, "query_dicts", fake_query_dicts)

    await people.fetch_person_issues(
        cast(BaseMetricsSink, object()),
        identities=["dev@example.com"],
        start_day=date(2026, 1, 1),
        end_day=date(2026, 1, 31),
        limit=25,
        org_id="org-1",
    )

    query = captured["query"]
    assert "FROM work_item_cycle_times AS wct FINAL" in query
    assert "FROM work_item_team_attributions FINAL" in query
    assert "LEFT JOIN" in query
    assert "nullIf(t.team_id, '') AS team_id" in query
    assert "wct.team_id" not in query
    assert captured["params"]["identities"] == ["dev@example.com"]


@pytest.mark.asyncio
async def test_person_issues_cursor_filters_qualified_completed_at(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_query_dicts(_sink: Any, query: str, params: dict[str, Any]):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(people, "query_dicts", fake_query_dicts)

    await people.fetch_person_issues(
        cast(BaseMetricsSink, object()),
        identities=["dev@example.com"],
        start_day=date(2026, 1, 1),
        end_day=date(2026, 1, 31),
        limit=25,
        cursor=datetime(2026, 1, 15),
        org_id="org-1",
    )

    assert "AND wct.completed_at < %(cursor)s" in captured["query"]
    assert captured["params"]["cursor"] == datetime(2026, 1, 15)
