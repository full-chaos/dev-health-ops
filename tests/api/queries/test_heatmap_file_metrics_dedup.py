"""CHAOS-4459 (codex review, P1): heatmap.py's file_metrics_daily readers
must dedup on read, not just have a registry entry.

Mirrors tests/test_storage.py's
test_clickhouse_complexity_snapshots_dedup_compute_generations: a mocked
sink captures the literal query text sent to ClickHouse, so this proves the
CALL SITE actually routes through dedup_from() -- registering the table in
clickhouse_dedup.py alone (test_clickhouse_dedup_file_metrics_registration.py)
would not catch a call site that forgot to use it.
"""

from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.api.queries.heatmap import (
    fetch_hotspot_evidence,
    fetch_hotspot_risk,
)


class _FakeSink:
    """Captures every query_dicts() call; returns canned rows in sequence."""

    def __init__(self, responses: list[list[dict]]) -> None:
        self._responses = list(responses)
        self.queries: list[str] = []
        self.params: list[dict] = []

    def query_dicts(self, query: str, params: dict) -> list[dict]:
        self.queries.append(query)
        self.params.append(params)
        if not self._responses:
            return []
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_fetch_hotspot_risk_dedups_file_metrics_daily_on_read() -> None:
    sink = _FakeSink(
        responses=[
            [{"file_key": "repo:path/to/file.py", "total": 3.5}],
            [
                {
                    "week": date(2026, 8, 24),
                    "file_key": "repo:path/to/file.py",
                    "value": 3.5,
                }
            ],
        ]
    )

    await fetch_hotspot_risk(
        sink,  # type: ignore[arg-type]
        start_day=date(2026, 8, 20),
        end_day=date(2026, 8, 28),
        scope_filter="",
        scope_params={},
        limit=10,
        org_id="70d529e0-3c06-4597-8480-794fd02328b6",
    )

    assert len(sink.queries) == 2, (
        "fetch_hotspot_risk must issue exactly its two queries"
    )
    for query in sink.queries:
        # dedup_from()'s exact output shape (clickhouse_dedup.py) -- a raw
        # `FROM file_metrics_daily` here is the codex-flagged regression.
        assert "LIMIT 1 BY org_id, repo_id, day, path" in query, (
            f"query does not dedup file_metrics_daily before aggregating:\n{query}"
        )
        assert "ORDER BY computed_at DESC" in query
        assert "AS file_metrics_daily" in query


@pytest.mark.asyncio
async def test_fetch_hotspot_evidence_dedups_file_metrics_daily_on_read() -> None:
    sink = _FakeSink(responses=[[]])

    await fetch_hotspot_evidence(
        sink,  # type: ignore[arg-type]
        week_start=date(2026, 8, 20),
        week_end=date(2026, 8, 27),
        file_key="repo:path/to/file.py",
        scope_filter="",
        scope_params={},
        limit=10,
        org_id="70d529e0-3c06-4597-8480-794fd02328b6",
    )

    assert len(sink.queries) == 1
    query = sink.queries[0]
    assert "LIMIT 1 BY org_id, repo_id, day, path" in query, (
        f"query does not dedup file_metrics_daily before returning evidence rows:\n{query}"
    )
    assert "ORDER BY computed_at DESC" in query
