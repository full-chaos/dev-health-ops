"""Tests for the Improve → Experiments GraphQL resolver (CHAOS-2219).

Covers:
 - ID stability: same (metric, suggestion) always produces the same id
   regardless of the parent opportunity card's daily rank position.
 - Happy path: items are derived with correct fields from OpportunityCard data.
 - Degraded path: build_opportunities_response failure → empty items,
   derived_from_opportunities=False.
 - Filter translation: scope filter is forwarded into MetricFilter correctly.
"""

from __future__ import annotations

import types
from typing import Any
from unittest.mock import AsyncMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.improve import (
    _stable_experiment_id,
    resolve_experiments,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context(org_id: str = "test-org") -> GraphQLContext:
    """Build a minimal GraphQLContext sufficient for the experiments resolver."""
    return GraphQLContext(org_id=org_id, db_url="clickhouse://localhost/test")


def _make_card(
    card_id: str,
    title: str,
    suggestions: list[str],
) -> Any:
    """Build a lightweight OpportunityCard stand-in."""
    card = types.SimpleNamespace()
    card.id = card_id
    card.title = title
    card.suggested_experiments = suggestions
    return card


def _make_opportunities_response(cards: list[Any]) -> Any:
    resp = types.SimpleNamespace()
    resp.items = cards
    return resp


# ---------------------------------------------------------------------------
# ID stability
# ---------------------------------------------------------------------------


def test_stable_id_same_inputs_always_equal() -> None:
    """The same (metric, suggestion) always yields the same ID."""
    id1 = _stable_experiment_id("cycle_time", "Split long-running items into slices.")
    id2 = _stable_experiment_id("cycle_time", "Split long-running items into slices.")
    assert id1 == id2


def test_stable_id_invariant_to_card_rank(monkeypatch: pytest.MonkeyPatch) -> None:
    """ID must not change when the opportunity card's rank (card.id) changes.

    Simulates the daily rank-shift: the same suggestion appearing in
    card opp-1 today and opp-3 tomorrow should produce the same experiment id.
    """
    suggestion = "Reserve a daily review block for PRs waiting longest."
    metric = "review_latency"

    id_rank1 = _stable_experiment_id(metric, suggestion)
    id_rank3 = _stable_experiment_id(metric, suggestion)

    # Both calls use (metric, suggestion) — the card rank is NOT in the hash.
    assert id_rank1 == id_rank3
    assert len(id_rank1) == 16  # 16-hex-char prefix


def test_stable_id_differs_for_different_suggestions() -> None:
    """Different suggestion texts produce different IDs."""
    id_a = _stable_experiment_id("cycle_time", "Suggestion A")
    id_b = _stable_experiment_id("cycle_time", "Suggestion B")
    assert id_a != id_b


def test_stable_id_differs_for_different_metrics() -> None:
    """Same suggestion text under a different metric produces a different ID."""
    id_ct = _stable_experiment_id("cycle_time", "Trace oldest active items.")
    id_rl = _stable_experiment_id("review_latency", "Trace oldest active items.")
    assert id_ct != id_rl


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_returns_correct_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Derived experiments carry the correct hypothesis, metric, and status."""
    cards = [
        _make_card(
            "opp-1",
            "Reduce Cycle Time",
            ["Trace oldest active items.", "Split a long-running item."],
        ),
        _make_card(
            "opp-2",
            "Reduce Review Latency",
            ["Reserve a daily review block."],
        ),
    ]
    fake_response = _make_opportunities_response(cards)

    monkeypatch.setattr(
        "dev_health_ops.api.services.opportunities.build_opportunities_response",
        AsyncMock(return_value=fake_response),
    )

    ctx = _make_context()
    result = await resolve_experiments(ctx)

    assert result.derived_from_opportunities is True
    assert len(result.items) == 3

    # Map by hypothesis for order-independent assertions.
    by_hyp = {e.hypothesis: e for e in result.items}

    exp_ct = by_hyp["Trace oldest active items."]
    assert exp_ct.metric == "cycle_time"
    assert exp_ct.status.value == "suggested"
    assert exp_ct.opportunity_id == "opp-1"
    assert exp_ct.owner == ""
    assert exp_ct.stop_condition == ""
    assert exp_ct.start_date is None
    assert exp_ct.stop_date is None
    assert exp_ct.outcome is None

    exp_rl = by_hyp["Reserve a daily review block."]
    assert exp_rl.metric == "review_latency"
    assert exp_rl.opportunity_id == "opp-2"


@pytest.mark.asyncio
async def test_happy_path_ids_are_content_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """IDs must be derivable from (metric, suggestion) alone — not from card rank."""
    suggestion = "Trace oldest active items."
    metric_key = "cycle_time"

    # Simulate the card appearing at rank opp-1 today …
    cards_today = [_make_card("opp-1", "Reduce Cycle Time", [suggestion])]
    # … and at rank opp-5 tomorrow due to metric fluctuations.
    cards_tomorrow = [_make_card("opp-5", "Reduce Cycle Time", [suggestion])]

    async def _fake_response(cards):
        async def _inner(**_kw):
            return _make_opportunities_response(cards)

        return _inner

    monkeypatch.setattr(
        "dev_health_ops.api.services.opportunities.build_opportunities_response",
        AsyncMock(return_value=_make_opportunities_response(cards_today)),
    )
    ctx = _make_context()
    result_today = await resolve_experiments(ctx)

    monkeypatch.setattr(
        "dev_health_ops.api.services.opportunities.build_opportunities_response",
        AsyncMock(return_value=_make_opportunities_response(cards_tomorrow)),
    )
    result_tomorrow = await resolve_experiments(ctx)

    assert result_today.items[0].id == result_tomorrow.items[0].id
    # ID must also equal the direct helper output.
    assert result_today.items[0].id == _stable_experiment_id(metric_key, suggestion)


# ---------------------------------------------------------------------------
# Degraded path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_degraded_path_on_service_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When build_opportunities_response raises, return empty + derived_from_opportunities=False."""
    monkeypatch.setattr(
        "dev_health_ops.api.services.opportunities.build_opportunities_response",
        AsyncMock(side_effect=RuntimeError("ClickHouse timeout")),
    )

    ctx = _make_context()
    result = await resolve_experiments(ctx)

    assert result.derived_from_opportunities is False
    assert result.items == []


# ---------------------------------------------------------------------------
# Filter translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scope_filter_is_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Scope filter from GraphQL FilterInput is forwarded to MetricFilter."""
    from dev_health_ops.api.graphql.models.inputs import (
        FilterInput,
        ScopeFilterInput,
        ScopeLevelInput,
    )

    captured: dict = {}

    async def _capture_filters(**kwargs: Any):
        captured["filters"] = kwargs.get("filters")
        return _make_opportunities_response([])

    monkeypatch.setattr(
        "dev_health_ops.api.services.opportunities.build_opportunities_response",
        _capture_filters,
    )

    scope_input = ScopeFilterInput(level=ScopeLevelInput.TEAM, ids=["team-alpha"])
    filters = FilterInput(scope=scope_input)

    ctx = _make_context()
    await resolve_experiments(ctx, filters=filters)

    assert captured["filters"] is not None
    mf = captured["filters"]
    assert mf.scope.level == "team"
    assert mf.scope.ids == ["team-alpha"]
