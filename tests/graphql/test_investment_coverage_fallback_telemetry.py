"""CHAOS-4241 (team-lead ruling): the investment Sankey coverage query's
``except -> coverage=None`` fallback swallowed a real, unrelated SQL bug (an
ambiguous ``repo_id`` identifier) for an unknown period -- the coverage query
had apparently never executed successfully against real ClickHouse before a
live test caught it, and every failure was silent (a structured log line,
but no counter, easy to miss). This must be loud: a raising coverage query
increments ``devhealth_graphql_resolver_fallback_total`` and logs a
structured ``investment_coverage.query_failed`` line, while the UI-facing
behavior is unchanged (coverage still degrades to ``None``, never a 500).

Mirrors the ``_RecordingCounter`` pattern in
``tests/api/services/test_home_cache_invalidation.py`` (the same shape of
fix for CHAOS-4226's cache-epoch bypass counter).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    DateRangeInput,
    DimensionInput,
    MeasureInput,
    SankeyRequestInput,
)
from dev_health_ops.api.graphql.resolvers import analytics as analytics_resolver


class _RecordingCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, str], float]] = []
        self._pending: dict[str, str] = {}

    def labels(self, **values: str) -> _RecordingCounter:
        self._pending = values
        return self

    def inc(self, amount: float = 1) -> None:
        self.calls.append((dict(self._pending), amount))


def _sankey_batch() -> AnalyticsRequestInput:
    return AnalyticsRequestInput(
        sankey=SankeyRequestInput(
            path=[DimensionInput.THEME, DimensionInput.TEAM],
            measure=MeasureInput.COUNT,
            date_range=DateRangeInput(
                start_date=date(2026, 6, 1),
                end_date=date(2026, 6, 17),
            ),
            use_investment=True,
        ),
        use_investment=True,
    )


@pytest.mark.asyncio
async def test_coverage_query_failure_is_counted_and_logged_loudly(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    counter = _RecordingCounter()
    monkeypatch.setattr(
        analytics_resolver, "INVESTMENT_COVERAGE_QUERY_FAILED_TOTAL", counter
    )

    async def fake_query_dicts(_client: Any, sql: str, _params: dict[str, Any]):
        # "assigned_team" only appears in the coverage query's SELECT list
        # (see tests/api/test_analytics_repo_filter_resolution.py for the
        # same marker) -- raise there specifically so the node/edge Sankey
        # queries still resolve normally and this failure is isolated to
        # the coverage fallback path under test.
        if "assigned_team" in sql:
            raise RuntimeError("ambiguous identifier 'repo_id' (simulated)")
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    context = GraphQLContext(
        org_id="org-telemetry-1", db_url="clickhouse://test", client=object()
    )

    with caplog.at_level(
        logging.ERROR, logger="dev_health_ops.api.graphql.resolvers.analytics"
    ):
        result = await analytics_resolver.resolve_analytics(context, _sankey_batch())

    # UI-facing behavior is unchanged: a degraded empty coverage, not a 500.
    assert result.sankey is not None
    assert result.sankey.coverage is None

    # The fallback itself must be loud.
    assert counter.calls == [
        ({"resolver": "investment_coverage", "reason": "RuntimeError"}, 1)
    ]
    failures = [
        r
        for r in caplog.records
        if r.getMessage() == "investment_coverage.query_failed"
    ]
    assert len(failures) == 1, "expected exactly one structured failure log line"
    # `extra={...}` fields land as dynamic LogRecord attributes -- not part
    # of logging.LogRecord's static shape, so read via __dict__ rather than
    # attribute access (which mypy correctly flags as attr-defined).
    record = failures[0].__dict__
    assert record["resolver"] == "investment_coverage"
    assert record["org_id"] == "org-telemetry-1"
    assert record["measure"] == "count"
    assert record["use_investment"] is True
    assert "ambiguous identifier" in record["error"]
