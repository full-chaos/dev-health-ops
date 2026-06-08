"""Resolver tests for reviewEdges (CHAOS-2077).

Tests exercise the resolver against a mocked ClickHouse client and verify:

* Empty state returns ``ReviewEdgesResult(edges=[], totalCount=0)``.
* Rows are mapped correctly from ClickHouse column names.
* The query deduplicates append-only rows via ``argMax(reviews_count,
  computed_at)`` grouped by the full key BEFORE ordering/limiting (asserted on
  SQL text, since the mocked client cannot execute argMax).
* The row limit is clamped to ``MAX_REVIEW_EDGES_ROWS`` and applied AFTER dedup.
* Optional ``repo_ids`` filter is included in the SQL when supplied.
* ``repo_id`` is ``None`` when the column value is absent/empty.
* The org-id gate raises ``AuthorizationError`` when ``context.org_id`` is
  missing.

All tests are read-only; no ClickHouse tables are modified.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.review_edges import (
    MAX_REVIEW_EDGES_ROWS,
    resolve_review_edges,
)
from dev_health_ops.api.graphql.types.review_edges import ReviewEdgesInput

ORG_ID = "org-review-edges-test"
DAY = date(2026, 5, 10)
SINCE = date(2026, 5, 1)
UNTIL = date(2026, 5, 31)

EDGE_COLS = ["reviewer", "author", "reviews_count", "day", "repo_id"]


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


def _input(
    *,
    repo_ids: list[str] | None = None,
    limit: int = 500,
) -> ReviewEdgesInput:
    return ReviewEdgesInput(
        org_id=ORG_ID,
        since_date=SINCE,
        until_date=UNTIL,
        repo_ids=repo_ids,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_empty_state() -> None:
    """Empty table returns stable zero-row contract."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    result = await resolve_review_edges(ctx, _input())

    assert result.edges == []
    assert result.total_count == 0


# ---------------------------------------------------------------------------
# Happy-path: column mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_maps_columns_correctly() -> None:
    """All columns map to the correct GraphQL type fields."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult(
        EDGE_COLS,
        [["reviewer@example.com", "author@example.com", 7, DAY, "repo-uuid-abc"]],
    )

    result = await resolve_review_edges(ctx, _input())

    assert len(result.edges) == 1
    edge = result.edges[0]
    assert edge.reviewer == "reviewer@example.com"
    assert edge.author == "author@example.com"
    assert edge.reviews_count == 7
    assert edge.day == DAY
    assert edge.repo_id == "repo-uuid-abc"
    assert result.total_count == 1


@pytest.mark.asyncio
async def test_review_edges_multiple_rows_all_returned() -> None:
    """Multiple edge rows are all included in the result."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult(
        EDGE_COLS,
        [
            ["a@x.com", "b@x.com", 5, DAY, "repo-1"],
            ["c@x.com", "d@x.com", 3, DAY, "repo-2"],
            ["a@x.com", "c@x.com", 1, DAY, "repo-1"],
        ],
    )

    result = await resolve_review_edges(ctx, _input())

    assert len(result.edges) == 3
    assert result.total_count == 3


# ---------------------------------------------------------------------------
# Null / missing repo_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_null_repo_id_becomes_none() -> None:
    """Empty/null repo_id column maps to None in the response."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult(
        EDGE_COLS,
        [["r@x.com", "a@x.com", 2, DAY, None]],
    )

    result = await resolve_review_edges(ctx, _input())

    assert result.edges[0].repo_id is None


@pytest.mark.asyncio
async def test_review_edges_empty_string_repo_id_becomes_none() -> None:
    """Empty-string repo_id is treated the same as null → None."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult(
        EDGE_COLS,
        [["r@x.com", "a@x.com", 2, DAY, ""]],
    )

    result = await resolve_review_edges(ctx, _input())

    assert result.edges[0].repo_id is None


# ---------------------------------------------------------------------------
# repo_ids filter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_repo_ids_filter_appears_in_query() -> None:
    """When repo_ids are supplied, the SQL includes the IN filter clause."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input(repo_ids=["repo-a", "repo-b"]))

    query: str = ctx.client.query.call_args.args[0]
    assert "repo_ids" in query


@pytest.mark.asyncio
async def test_review_edges_no_repo_ids_filter_absent_from_query() -> None:
    """When repo_ids is None, the IN filter must NOT appear in the SQL."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input(repo_ids=None))

    query: str = ctx.client.query.call_args.args[0]
    assert "repo_ids" not in query


# ---------------------------------------------------------------------------
# Dedup: argMax(reviews_count, computed_at) before ORDER/LIMIT (finding 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_dedups_via_argmax_computed_at() -> None:
    """The query must collapse review_edges_daily to the latest row per
    (repo_id, reviewer, author, day) via argMax(reviews_count, computed_at)
    BEFORE ordering and limiting.

    The mocked client cannot execute argMax, so we assert on the emitted SQL:
    it must argMax reviews_count over computed_at, GROUP BY the full key, and
    only then ORDER BY reviews_count DESC. Regression test for finding 2:
    without dedup, a backfilled day's edge would be counted twice.
    """
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input())

    query: str = ctx.client.query.call_args.args[0]
    assert "argMax(reviews_count, computed_at)" in query
    assert "GROUP BY repo_id, reviewer, author, day" in query
    # Dedup subquery must precede the ranking/cap.
    idx_group = query.index("GROUP BY repo_id, reviewer, author, day")
    idx_order = query.index("ORDER BY reviews_count DESC")
    assert idx_group < idx_order, "dedup GROUP BY must come before ORDER BY"


# ---------------------------------------------------------------------------
# Row limit clamping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_limit_clamped_to_max() -> None:
    """Limits above MAX_REVIEW_EDGES_ROWS are silently clamped."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input(limit=MAX_REVIEW_EDGES_ROWS + 99999))

    query: str = ctx.client.query.call_args.args[0]
    assert f"LIMIT {MAX_REVIEW_EDGES_ROWS}" in query


@pytest.mark.asyncio
async def test_review_edges_limit_below_one_clamped_to_one() -> None:
    """Limits of 0 or negative are clamped to 1."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input(limit=0))

    query: str = ctx.client.query.call_args.args[0]
    assert "LIMIT 1" in query


# ---------------------------------------------------------------------------
# Org-id gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_raises_on_missing_context_org() -> None:
    """``require_org_id`` raises ``AuthorizationError`` when org_id is absent."""
    from dev_health_ops.api.graphql.errors import AuthorizationError

    ctx = _ctx()
    object.__setattr__(ctx, "org_id", "")

    with pytest.raises(AuthorizationError):
        await resolve_review_edges(ctx, _input())


# ---------------------------------------------------------------------------
# Single ClickHouse query per call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_fires_exactly_one_query() -> None:
    """ReviewEdges resolver issues exactly one ClickHouse query per call."""
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input())

    assert ctx.client.query.call_count == 1
