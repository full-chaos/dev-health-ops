"""Tests for CHAOS-3304's ``native_team_workload.ClickHouseTeamWorkloadSource``.

Mirrors ``test_native_evidence.py``'s ``FakeSink`` pattern: a fake client
records every query/params pair and returns canned rows, proving the query
shape (org_id + team_id scoping, one call per method, no concurrent
fan-out) and the investment-bucket accounting (an unrecognized
``investment_area`` must land in ``unclassified_units``, never be dropped).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.native_team_workload import ClickHouseTeamWorkloadSource

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_START = _NOW - timedelta(days=14)
_ORG_ID = "org-1"
_TEAM_ID = "team-1"


class FakeSink:
    def __init__(self, rows_by_query: dict[str, list[dict[str, Any]]]) -> None:
        self._rows_by_query = rows_by_query
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((query, params))
        for marker, rows in self._rows_by_query.items():
            if marker in query:
                return rows
        return []


@pytest.mark.asyncio
async def test_cognitive_load_scopes_every_query_by_org_and_team() -> None:
    sink = FakeSink(
        {
            "user_metrics_daily": [
                {
                    "pr_interruption_load": 24.0,
                    "context_spread_count": 6.0,
                    "review_request_load": 18.0,
                    "sample_days": 14,
                }
            ],
            "team_metrics_daily": [
                {
                    "after_hours_commit_ratio": 0.3,
                    "weekend_commit_ratio": 0.1,
                    "sample_days": 14,
                }
            ],
        }
    )
    source = ClickHouseTeamWorkloadSource(sink)
    result = await source.cognitive_load(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert result.measured is True
    assert result.after_hours_commit_ratio == 0.3
    assert result.review_request_load == 18.0
    assert result.pr_interruption_load == 24.0
    assert len(sink.calls) == 2  # one user-level + one team-level query
    for _, params in sink.calls:
        assert params["org_id"] == _ORG_ID
        assert params["team_id"] == _TEAM_ID


@pytest.mark.asyncio
async def test_cognitive_load_no_rows_reports_unmeasured() -> None:
    sink = FakeSink({})
    source = ClickHouseTeamWorkloadSource(sink)
    result = await source.cognitive_load(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert result.measured is False
    assert result.after_hours_commit_ratio is None


@pytest.mark.asyncio
async def test_active_contributor_count_returns_distinct_author_count() -> None:
    sink = FakeSink({"user_metrics_daily": [{"active_contributor_count": 7}]})
    source = ClickHouseTeamWorkloadSource(sink)
    count = await source.active_contributor_count(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert count == 7


@pytest.mark.asyncio
async def test_active_contributor_count_no_rows_is_none_not_zero() -> None:
    """A genuinely absent count is ``None`` (denominator unavailable), never
    a fabricated ``0`` that would read as "measured, zero active
    contributors".
    """

    sink = FakeSink({})
    source = ClickHouseTeamWorkloadSource(sink)
    count = await source.active_contributor_count(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert count is None


@pytest.mark.asyncio
async def test_investment_mix_buckets_unrecognized_area_as_unclassified() -> None:
    """An ``investment_area`` string that does not resolve to one of the
    four canonical buckets must be counted in ``unclassified_units`` --
    never silently dropped (unlike ``operating_review._investment_units``,
    which this module deliberately does not reuse -- see its own
    docstring).
    """

    sink = FakeSink(
        {
            "investment_metrics_daily": [
                {"investment_area": "new value", "delivery_units": 40.0},
                {"investment_area": "ktlo", "delivery_units": 30.0},
                {"investment_area": "security", "delivery_units": 10.0},
                {"investment_area": "infra", "delivery_units": 10.0},
                {"investment_area": "some_future_category", "delivery_units": 10.0},
            ]
        }
    )
    source = ClickHouseTeamWorkloadSource(sink)
    result = await source.investment_mix(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert result.measured is True
    assert result.new_value_units == 40.0
    assert result.ktlo_units == 30.0
    assert result.security_units == 10.0
    assert result.infra_units == 10.0
    assert result.unclassified_units == 10.0
    assert result.total_units == 100.0
    assert result.classification_coverage == pytest.approx(0.9)


@pytest.mark.asyncio
async def test_investment_mix_no_rows_reports_unmeasured() -> None:
    sink = FakeSink({})
    source = ClickHouseTeamWorkloadSource(sink)
    result = await source.investment_mix(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert result.measured is False
    assert result.total_units == 0.0


@pytest.mark.asyncio
async def test_investment_mix_query_failure_reports_unmeasured_not_raise() -> None:
    class RaisingSink:
        def query_dicts(
            self, query: str, params: dict[str, Any]
        ) -> list[dict[str, Any]]:
            raise RuntimeError("connection reset")

    source = ClickHouseTeamWorkloadSource(RaisingSink())
    result = await source.investment_mix(
        org_id=_ORG_ID, team_id=_TEAM_ID, start=_START, end=_NOW
    )
    assert result.measured is False
