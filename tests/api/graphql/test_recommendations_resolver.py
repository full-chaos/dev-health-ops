"""Integration tests for the recommendations GraphQL resolver.

Tests are fixture-backed (no live ClickHouse required): query_dicts is
patched to return controlled row sets, and we assert the resolver maps
them to the correct Strawberry output shape, including evidence
references.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

strawberry = pytest.importorskip("strawberry")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_context():
    ctx = MagicMock()
    ctx.org_id = "test-org"
    ctx.db_url = "clickhouse://localhost:8123/default"
    ctx.client = MagicMock()
    return ctx


EVIDENCE_LIST = [
    {
        "team_id": "team-alpha",
        "metric_table": "work_item_metrics_daily",
        "field": "wip_count",
        "window_start": "2026-04-01",
        "window_end": "2026-04-07",
        "value": 14.0,
    },
    {
        "team_id": "team-alpha",
        "metric_table": "work_item_metrics_daily",
        "field": "throughput",
        "window_start": "2026-04-01",
        "window_end": "2026-04-07",
        "value": 2.0,
    },
]


FIXTURE_ROWS = [
    {
        "team_id": "team-alpha",
        "org_id": "test-org",
        "rule_id": "saturation",
        "fired": True,
        "severity": "critical",
        "title": "Team is saturating. Reduce active work before adding scope.",
        "rationale": "WIP has been rising while throughput remains flat for 2 cycles.",
        "success_criterion": "WIP trend turns negative or throughput trend turns positive in 2 cycles",
        "evidence_json": json.dumps(EVIDENCE_LIST),
        "window_start": "2026-04-01",
        "window_end": "2026-04-07",
        "computed_at": datetime(2026, 4, 8, 3, 0, 0, tzinfo=timezone.utc),
    },
    {
        "team_id": "team-alpha",
        "org_id": "test-org",
        "rule_id": "thrash",
        "window_end": "2026-04-07",
        "fired": True,
        "severity": "warning",
        "title": "Thrash likely. Inspect hotspots and rework loops.",
        "rationale": "High churn detected with low delivery ratio over the last 7 days.",
        "success_criterion": "Churn drops OR throughput rises in 2 cycles",
        "evidence_json": json.dumps([EVIDENCE_LIST[0]]),
        "window_start": "2026-04-01",
        "computed_at": datetime(2026, 4, 8, 3, 0, 0, tzinfo=timezone.utc),
    },
]


# ---------------------------------------------------------------------------
# Shape / field tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_resolve_recommendations_returns_list(mock_context):
    """Resolver returns a non-empty list when rows are present."""
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=FIXTURE_ROWS,
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=1, unit=WindowUnit.WEEK),
        )

    assert isinstance(result, list)
    assert len(result) == 2


@pytest.mark.anyio
async def test_resolve_recommendations_field_mapping(mock_context):
    """Fields map correctly from DB row to Recommendation type."""
    from dev_health_ops.api.graphql.models.recommendations import (
        Severity,
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=[FIXTURE_ROWS[0]],
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=1, unit=WindowUnit.WEEK),
        )

    assert len(result) == 1
    rec = result[0]
    assert rec.rule_id == "saturation"
    assert rec.team_id == "team-alpha"
    assert rec.org_id == "test-org"
    assert rec.severity == Severity.CRITICAL
    assert rec.title == "Team is saturating. Reduce active work before adding scope."
    assert rec.success_criterion.startswith("WIP trend")
    assert isinstance(rec.computed_at, datetime)
    assert rec.computed_at.tzinfo is not None


@pytest.mark.anyio
async def test_resolve_recommendations_evidence_references_resolve(mock_context):
    """Evidence list is deserialised; each EvidenceRef carries required fields."""
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=[FIXTURE_ROWS[0]],
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=1, unit=WindowUnit.WEEK),
        )

    rec = result[0]
    assert len(rec.evidence) == 2
    ev = rec.evidence[0]
    # Keys match canonical EvidenceRef field names exactly (as per engine sink contract)
    assert ev.metric_table == "work_item_metrics_daily"
    assert ev.field == "wip_count"
    assert ev.value == 14.0
    assert ev.team_id == "team-alpha"
    assert isinstance(ev.window_start, date)
    assert isinstance(ev.window_end, date)


@pytest.mark.anyio
async def test_resolve_recommendations_empty_on_no_rows(mock_context):
    """Resolver returns an empty list when the query returns nothing."""
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-beta",
            window=WindowInput(value=7, unit=WindowUnit.DAY),
        )

    assert result == []


@pytest.mark.anyio
async def test_resolve_recommendations_tolerates_db_error(mock_context):
    """Resolver swallows DB exceptions and returns an empty list (graceful degradation)."""
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        side_effect=RuntimeError("ClickHouse unavailable"),
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=4, unit=WindowUnit.WEEK),
        )

    assert result == []


@pytest.mark.anyio
async def test_resolve_recommendations_multiple_rules(mock_context):
    """All fired rules within the window are returned."""
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=FIXTURE_ROWS,
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=2, unit=WindowUnit.CYCLE),
        )

    rule_ids = [r.rule_id for r in result]
    assert "saturation" in rule_ids
    assert "thrash" in rule_ids


@pytest.mark.anyio
async def test_resolve_recommendations_unknown_severity_falls_back(mock_context):
    """An unrecognised severity value defaults to WARNING without raising."""
    from dev_health_ops.api.graphql.models.recommendations import (
        Severity,
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    bad_row = {**FIXTURE_ROWS[0], "severity": "ultra-critical"}

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        new_callable=AsyncMock,
        return_value=[bad_row],
    ):
        result = await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=1, unit=WindowUnit.WEEK),
        )

    assert result[0].severity == Severity.WARNING


def test_window_to_dates_caps_read_at_today_plus_one(monkeypatch):
    """Read cap == utc_today()+1 so the writer's today+1 rows are read same-day.

    Regression for the CHAOS-2373 convention mismatch: the scheduled writer
    persists ``window_end == as_of_day + 1`` (it anchors ``now = as_of_day + 1``
    so the loader's exclusive ``day < window_end`` still reads the finalized
    partition). The resolver previously capped at ``today`` and excluded those
    freshest rows, leaving recommendations() one finalize-day stale. The cap is
    now inclusive of ``today + 1``, mirroring filtering.time_window's
    ``end_day = utc_today() + 1`` so this surface agrees with the home surface.
    """
    from datetime import date, timedelta

    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers import recommendations as resolver_mod
    from dev_health_ops.api.graphql.resolvers.recommendations import _window_to_dates

    today = date(2026, 4, 8)
    monkeypatch.setattr(resolver_mod, "utc_today", lambda: today)

    # window_end is today + 1 (matches the writer + filtering.time_window).
    ws_week, we_week = _window_to_dates(WindowInput(value=2, unit=WindowUnit.WEEK))
    assert we_week == today + timedelta(days=1)
    # window_start is anchored to *today* (not the bumped cap) so the lookback
    # span is unchanged: 2 weeks == 14 days.
    assert ws_week == today - timedelta(days=14)

    # Unit math is preserved across day / week / cycle.
    ws_day, we_day = _window_to_dates(WindowInput(value=7, unit=WindowUnit.DAY))
    assert (we_day, ws_day) == (today + timedelta(days=1), today - timedelta(days=7))
    ws_cyc, we_cyc = _window_to_dates(WindowInput(value=2, unit=WindowUnit.CYCLE))
    assert (we_cyc, ws_cyc) == (today + timedelta(days=1), today - timedelta(days=28))


@pytest.mark.anyio
async def test_resolve_recommendations_raises_without_client(mock_context):
    """RuntimeError is raised when context.client is None (misconfigured server)."""
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    mock_context.client = None

    with pytest.raises(RuntimeError, match="Database client not available"):
        await resolve_recommendations(
            mock_context,
            team="team-alpha",
            window=WindowInput(value=1, unit=WindowUnit.WEEK),
        )
