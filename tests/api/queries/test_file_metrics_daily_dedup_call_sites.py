"""CHAOS-4459 (codex review round 2): the file_metrics_daily readers codex
found still summing raw rows -- api/queries/sankey.py's fetch_hotspot_rows
and api/queries/aggregated_flame.py's fetch_code_hotspots -- must dedup on
read, matching tests/api/queries/test_heatmap_file_metrics_dedup.py's
pattern for the readers already fixed in round 1.
"""

from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.api.queries.aggregated_flame import fetch_code_hotspots
from dev_health_ops.api.queries.sankey import fetch_hotspot_rows


class _FakeSink:
    """Captures every query_dicts() call; returns canned rows in sequence."""

    def __init__(self, responses: list[list[dict]]) -> None:
        self._responses = list(responses)
        self.queries: list[str] = []

    def query_dicts(self, query: str, params: dict) -> list[dict]:
        self.queries.append(query)
        if not self._responses:
            return []
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_fetch_code_hotspots_dedups_file_metrics_daily_on_read() -> None:
    sink = _FakeSink(responses=[[]])

    await fetch_code_hotspots(
        sink,
        start_day=date(2026, 8, 20),
        end_day=date(2026, 8, 28),
        org_id="70d529e0-3c06-4597-8480-794fd02328b6",
    )

    assert len(sink.queries) == 1
    query = sink.queries[0]
    assert "LIMIT 1 BY org_id, repo_id, day, path" in query, (
        f"query does not dedup file_metrics_daily before summing churn:\n{query}"
    )
    assert "ORDER BY computed_at DESC" in query


@pytest.mark.asyncio
async def test_fetch_hotspot_rows_dedups_file_metrics_daily_on_read() -> None:
    sink = _FakeSink(responses=[[]])

    await fetch_hotspot_rows(
        sink,  # type: ignore[arg-type]
        start_day=date(2026, 8, 20),
        end_day=date(2026, 8, 28),
        scope_filter="",
        scope_params={},
        limit=10,
        org_id="70d529e0-3c06-4597-8480-794fd02328b6",
    )

    assert len(sink.queries) == 1
    query = sink.queries[0]
    # All three references to file_metrics_daily in this query (the two
    # quantile CTEs and the main SELECT) must dedup.
    assert query.count("LIMIT 1 BY org_id, repo_id, day, path") == 3, (
        f"expected all 3 file_metrics_daily reads to dedup, got:\n{query}"
    )
    assert "ORDER BY computed_at DESC" in query
