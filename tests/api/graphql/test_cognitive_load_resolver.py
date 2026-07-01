"""Resolver tests for cognitiveLoad (CHAOS-2077).

Tests exercise the resolver against a mocked ClickHouse client and verify:

* Empty state returns empty signals list (``totalDays=0``).
* Signals are built correctly from user_metrics_daily rows.
* Team metrics (after_hours / weekend ratios) are merged on day.
* Merge is over the UNION of days: a day present only in team_metrics (and
  absent from user_metrics) is still emitted, with zero user-side signals and
  the available team ratios.
* Days in user_metrics with no matching team_metrics row produce null ratios.
* Both ClickHouse queries deduplicate append-only rows via
  ``argMax(<col>, computed_at)`` before aggregating (asserted on SQL text,
  since the mocked client cannot execute argMax).
* The org-id gate raises ``AuthorizationError`` when ``context.org_id`` is
  missing.
* The ``team_id`` filter passes through to both SQL queries.
* The ``repo_id`` filter passes through to the user-metrics query only
  (``team_metrics_daily`` has no ``repo_id`` column).

All tests are read-only; no ClickHouse tables are modified.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.cognitive_load import resolve_cognitive_load
from dev_health_ops.api.graphql.types.cognitive_load import CognitiveLoadInput

ORG_ID = "org-cogload-test"
DAY_1 = date(2026, 5, 1)
DAY_2 = date(2026, 5, 2)
SINCE = date(2026, 5, 1)
UNTIL = date(2026, 5, 31)


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


def _ctx() -> GraphQLContext:
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/d")
    ctx.client = MagicMock(spec=["query"])
    return ctx


def _qresult(columns: list[str], rows: list[list[Any]]) -> Any:
    result = MagicMock()
    result.column_names = columns
    result.result_rows = rows
    return result


def _setup_client(client: Any, responses: list[Any]) -> None:
    """Make ``client.query`` return successive responses per call."""
    client.query.side_effect = responses


def _squash_ws(sql: str) -> str:
    """Collapse runs of whitespace to a single space for robust SQL matching.

    The resolver aligns ``argMax(...)`` columns with padding spaces; this lets
    assertions match the logical SQL without coupling to exact alignment.
    """
    return re.sub(r"\s+", " ", sql)


def _input(
    team_id: str | None = None, repo_id: str | None = None
) -> CognitiveLoadInput:
    return CognitiveLoadInput(
        org_id=ORG_ID,
        since_date=SINCE,
        until_date=UNTIL,
        team_id=team_id,
        repo_id=repo_id,
    )


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_empty_state() -> None:
    """Empty user_metrics returns empty signals (totalDays=0)."""
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult([], []),  # user_metrics_daily → no rows
            _qresult([], []),  # team_metrics_daily → no rows
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    assert result.org_id == ORG_ID
    assert result.team_id is None
    assert result.signals == []
    assert result.total_days == 0


# ---------------------------------------------------------------------------
# Happy-path: user metrics only (team metrics empty)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_user_metrics_no_team_metrics() -> None:
    """When team_metrics has no rows, ratios are null for all signals."""
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, 12, 45, 3]]),
            _qresult([], []),  # team_metrics → empty
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    assert len(result.signals) == 1
    sig = result.signals[0]
    assert sig.day == DAY_1
    assert sig.pr_interruption_load == pytest.approx(12.0)
    assert sig.context_spread_count == pytest.approx(45.0)
    assert sig.review_request_load == pytest.approx(3.0)
    assert sig.after_hours_commit_ratio is None
    assert sig.weekend_commit_ratio is None
    assert result.total_days == 1


# ---------------------------------------------------------------------------
# Happy-path: user + team metrics merged on day
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_merges_team_ratios_on_day() -> None:
    """Team metrics are merged correctly when the day matches."""
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    team_cols = ["day", "after_hours_commit_ratio", "weekend_commit_ratio"]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, 10, 50, 5], [DAY_2, 8, 30, 2]]),
            _qresult(team_cols, [[DAY_1, 0.42, 0.31]]),  # only DAY_1 has team row
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    assert len(result.signals) == 2
    s1, s2 = result.signals[0], result.signals[1]

    # DAY_1 — team row present
    assert s1.day == DAY_1
    assert s1.pr_interruption_load == pytest.approx(10.0)
    assert s1.after_hours_commit_ratio == pytest.approx(0.42)
    assert s1.weekend_commit_ratio == pytest.approx(0.31)

    # DAY_2 — no matching team row → null ratios
    assert s2.day == DAY_2
    assert s2.pr_interruption_load == pytest.approx(8.0)
    assert s2.after_hours_commit_ratio is None
    assert s2.weekend_commit_ratio is None

    assert result.total_days == 2


# ---------------------------------------------------------------------------
# Null / zero tolerance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_null_user_values_default_to_zero() -> None:
    """Null column values in user_metrics degrade to 0.0 (not crashes)."""
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, None, None, None]]),
            _qresult([], []),
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    sig = result.signals[0]
    assert sig.pr_interruption_load == pytest.approx(0.0)
    assert sig.context_spread_count == pytest.approx(0.0)
    assert sig.review_request_load == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_cognitive_load_null_team_ratios_propagate_as_none() -> None:
    """Null team ratio values remain null (not coerced to 0)."""
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    team_cols = ["day", "after_hours_commit_ratio", "weekend_commit_ratio"]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, 5, 20, 1]]),
            _qresult(team_cols, [[DAY_1, None, None]]),
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    sig = result.signals[0]
    assert sig.after_hours_commit_ratio is None
    assert sig.weekend_commit_ratio is None


# ---------------------------------------------------------------------------
# team_id filter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_team_id_reflected_in_result() -> None:
    """team_id from input is echoed in the result and passed to both queries."""
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, 3, 7, 1]]),
            _qresult([], []),
        ],
    )

    result = await resolve_cognitive_load(ctx, _input(team_id="team-alpha"))

    assert result.team_id == "team-alpha"
    # Both queries must embed the team_id filter — check via call args.
    assert ctx.client.query.call_count == 2
    first_query: str = ctx.client.query.call_args_list[0].args[0]
    second_query: str = ctx.client.query.call_args_list[1].args[0]
    assert "team_id" in first_query
    assert "team_id" in second_query


# ---------------------------------------------------------------------------
# repo_id filter (CHAOS-2386)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_repo_id_filters_user_query_only() -> None:
    """repo_id is embedded in the user-metrics query only.

    ``team_metrics_daily`` has no ``repo_id`` column, so the team-metrics
    query must remain unfiltered by repo even when ``repo_id`` is supplied —
    regression test for CHAOS-2386 (the resolver previously had no repo_id
    field/predicate at all, making the UI repo control a no-op). The
    predicate casts the UUID column via ``toString(...)`` before comparing,
    so a non-UUID value degrades to a no-match rather than a ClickHouse
    ``CANNOT_PARSE_UUID`` exception (mirrors ``resolvers/complexity.py``).
    """
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, 4, 9, 2]]),
            _qresult([], []),
        ],
    )

    await resolve_cognitive_load(
        ctx, _input(repo_id="3fa85f64-5717-4562-b3fc-2c963f66afa6")
    )

    assert ctx.client.query.call_count == 2
    first_query: str = ctx.client.query.call_args_list[0].args[0]
    second_query: str = ctx.client.query.call_args_list[1].args[0]
    assert "toString(repo_id) = {repo_id:String}" in first_query
    assert "repo_id" not in second_query


@pytest.mark.asyncio
async def test_cognitive_load_no_repo_id_omits_predicate() -> None:
    """When repo_id is absent, no repo_id predicate is added to either query."""
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_cognitive_load(ctx, _input())

    first_query: str = ctx.client.query.call_args_list[0].args[0]
    assert "toString(repo_id) = {repo_id:String}" not in first_query


# ---------------------------------------------------------------------------
# Union-of-days merge (finding 3): day only in team_metrics still emitted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_day_only_in_team_metrics_is_emitted() -> None:
    """A day present in team_metrics but absent from user_metrics is emitted
    with zero user-side signals + the available team ratios.

    Regression test for finding 3: the merge must span the UNION of days from
    both result sets, not just the user rows.
    """
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    team_cols = ["day", "after_hours_commit_ratio", "weekend_commit_ratio"]
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_1, 10, 50, 5]]),  # only DAY_1
            _qresult(team_cols, [[DAY_2, 0.40, 0.25]]),  # only DAY_2 (e.g. weekend)
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    # Union of {DAY_1} and {DAY_2} → both days present, sorted ascending.
    assert result.total_days == 2
    by_day = {s.day: s for s in result.signals}
    assert set(by_day) == {DAY_1, DAY_2}

    # DAY_1 — user signals present, no team row → null ratios
    assert by_day[DAY_1].pr_interruption_load == pytest.approx(10.0)
    assert by_day[DAY_1].after_hours_commit_ratio is None

    # DAY_2 — NO user row → zeros, but team ratios present
    assert by_day[DAY_2].pr_interruption_load == pytest.approx(0.0)
    assert by_day[DAY_2].context_spread_count == pytest.approx(0.0)
    assert by_day[DAY_2].review_request_load == pytest.approx(0.0)
    assert by_day[DAY_2].after_hours_commit_ratio == pytest.approx(0.40)
    assert by_day[DAY_2].weekend_commit_ratio == pytest.approx(0.25)


@pytest.mark.asyncio
async def test_cognitive_load_signals_sorted_by_day() -> None:
    """Signals are returned in ascending day order even when source rows are
    interleaved across the two result sets."""
    ctx = _ctx()
    user_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
    ]
    team_cols = ["day", "after_hours_commit_ratio", "weekend_commit_ratio"]
    day_0 = date(2026, 4, 30)
    _setup_client(
        ctx.client,
        [
            _qresult(user_cols, [[DAY_2, 1, 1, 1]]),  # later day from user side
            _qresult(team_cols, [[day_0, 0.1, 0.2]]),  # earlier day from team side
        ],
    )

    result = await resolve_cognitive_load(ctx, _input())

    assert [s.day for s in result.signals] == [day_0, DAY_2]


# ---------------------------------------------------------------------------
# Dedup: argMax(..., computed_at) before SUM/AVG (finding 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_user_query_dedups_via_argmax_computed_at() -> None:
    """The user_metrics query must collapse to the latest row per logical key
    via argMax(<col>, computed_at) before SUMming.

    The mocked client cannot execute argMax, so we assert on the emitted SQL:
    it must argMax every metric over computed_at and GROUP BY the full key
    (day, repo_id, author_email) in an inner subquery, then SUM by day.
    """
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_cognitive_load(ctx, _input())

    # Column alignment in the SQL may insert extra spaces before ``computed_at``;
    # normalize runs of whitespace before matching so the assertion is robust.
    user_query: str = _squash_ws(ctx.client.query.call_args_list[0].args[0])
    assert "argMax(pr_interruption_load, computed_at)" in user_query
    assert "argMax(context_spread_count, computed_at)" in user_query
    assert "argMax(review_request_load, computed_at)" in user_query
    # Inner grouping on the full append-only key + outer SUM by day.
    assert "GROUP BY day, repo_id, author_email" in user_query
    assert "SUM(pr_interruption_load)" in user_query


@pytest.mark.asyncio
async def test_cognitive_load_team_query_dedups_via_argmax_computed_at() -> None:
    """The team_metrics query must collapse to the latest row per
    (day, team_id) via argMax(<col>, computed_at) before AVGing."""
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_cognitive_load(ctx, _input())

    team_query: str = _squash_ws(ctx.client.query.call_args_list[1].args[0])
    assert "argMax(after_hours_commit_ratio, computed_at)" in team_query
    assert "argMax(weekend_commit_ratio, computed_at)" in team_query
    assert "GROUP BY day, team_id" in team_query
    assert "AVG(after_hours_commit_ratio)" in team_query


# ---------------------------------------------------------------------------
# Org-id gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_raises_on_missing_context_org() -> None:
    """``require_org_id`` raises ``AuthorizationError`` when org_id is absent."""
    from dev_health_ops.api.graphql.errors import AuthorizationError

    ctx = _ctx()
    object.__setattr__(ctx, "org_id", "")

    with pytest.raises(AuthorizationError):
        await resolve_cognitive_load(ctx, _input())


# ---------------------------------------------------------------------------
# Two ClickHouse queries are always fired
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_always_fires_two_queries() -> None:
    """Resolver always issues exactly 2 ClickHouse queries: user then team."""
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_cognitive_load(ctx, _input())

    assert ctx.client.query.call_count == 2
