from datetime import datetime, timezone

import pytest

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
from dev_health_ops.api.models.schemas import (
    ConstraintCard,
    Coverage,
    Freshness,
    HomeResponse,
    MetricDelta,
    SparkPoint,
)
from dev_health_ops.api.services.cache import TTLCache
from dev_health_ops.api.services.opportunities import build_opportunities_response


def _home_with_deltas(deltas: list[MetricDelta]) -> HomeResponse:
    return HomeResponse(
        freshness=Freshness(
            last_ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            sources={"github": "ok", "gitlab": "ok", "jira": "ok", "ci": "ok"},
            coverage=Coverage(
                repos_covered_pct=100.0,
                prs_linked_to_issues_pct=100.0,
                issues_with_cycle_states_pct=100.0,
            ),
        ),
        deltas=deltas,
        summary=[],
        tiles={},
        constraint=ConstraintCard(
            title="Constraint",
            claim="No constraint",
            evidence=[],
            experiments=[],
        ),
        events=[],
    )


def _delta(metric: str, label: str, delta_pct: float) -> MetricDelta:
    return MetricDelta(
        metric=metric,
        label=label,
        value=1.0,
        unit="items",
        delta_pct=delta_pct,
        spark=[SparkPoint(ts=datetime(2024, 1, 1, tzinfo=timezone.utc), value=1.0)],
    )


@pytest.mark.asyncio
async def test_opportunities_use_metric_specific_experiments(monkeypatch):
    async def _fake_home(**_):
        return _home_with_deltas(
            [
                _delta("cycle_time", "Cycle Time", 24.0),
                _delta("review_latency", "Review Latency", 12.0),
            ]
        )

    monkeypatch.setattr(
        "dev_health_ops.api.services.opportunities.build_home_response", _fake_home
    )

    result = await build_opportunities_response(
        db_url="clickhouse://test",
        filters=MetricFilter(
            time=TimeFilter(range_days=30, compare_days=30),
            scope=ScopeFilter(level="team", ids=["team-a"]),
        ),
        cache=TTLCache(ttl_seconds=1),
        org_id="test-org",
    )

    by_metric = {
        item.evidence_links[0].split("metric=", 1)[1].split("&", 1)[0]: item
        for item in result.items
    }
    assert by_metric["cycle_time"].suggested_experiments == [
        "Trace the oldest active items to their current waiting state.",
        "Split one long-running item into the next reviewable slice.",
    ]
    assert by_metric["review_latency"].suggested_experiments == [
        "Reserve a daily review block for PRs waiting longest for first response.",
        "Pair authors with likely reviewers before opening complex PRs.",
    ]
