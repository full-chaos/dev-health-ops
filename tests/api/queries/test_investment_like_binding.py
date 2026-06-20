"""Regression test for CHAOS-2566.

fetch_mock_fixture_investment_row_count builds a ClickHouse query that contains
LIKE literals with bare % characters.  clickhouse-connect's finalize_query
applies pyformat %-substitution to any query that uses %(name)s params; a bare
'%mock%' is misread as a positional conversion and raises:

    TypeError: not enough arguments for format string

The fix doubles the percent signs (%%mock%%) so they survive pyformat expansion
as literal single-percent patterns in the final SQL.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from clickhouse_connect.driver.binding import finalize_query

import dev_health_ops.api.queries.investment as investment_module


def test_mock_fixture_like_patterns_survive_pyformat_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """finalize_query must not raise and must produce single-% LIKE patterns."""
    captured: dict[str, Any] = {}

    async def _stub_query_dicts(
        _sink: Any, query: str, params: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(investment_module, "query_dicts", _stub_query_dicts)

    start_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_ts = datetime(2024, 2, 1, tzinfo=timezone.utc)
    org_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    result = asyncio.run(
        investment_module.fetch_mock_fixture_investment_row_count(
            sink=None,  # type: ignore[arg-type]  # stub never touches sink
            start_ts=start_ts,
            end_ts=end_ts,
            scope_filter="",
            scope_params={},
            org_id=org_id,
        )
    )

    assert result == 0
    assert "query" in captured, "stub was not called"

    query: str = captured["query"]
    params: dict[str, Any] = captured["params"] or {}

    # Pre-fix this raised TypeError: not enough arguments for format string
    finalized = finalize_query(query, params)

    # After pyformat expansion %% collapses to %; the finalized SQL must contain
    # the single-percent LIKE patterns that ClickHouse will evaluate.
    assert "LIKE '%mock%'" in finalized
    assert "LIKE '%synthetic%'" in finalized
    assert "LIKE '%fixture%'" in finalized

    # Named params must also be substituted correctly.
    assert "%(start_ts)s" not in finalized
    assert "%(end_ts)s" not in finalized
    assert "%(org_id)s" not in finalized
