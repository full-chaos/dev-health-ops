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
async def test_review_edges_repo_ids_filter_resolves_slugs_or_uuids() -> None:
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(
        ctx, _input(repo_ids=["3fa85f64-5717-4562-b3fc-2c963f66afa6"])
    )

    query: str = ctx.client.query.call_args.args[0]
    assert "repo_id IN (" in query
    assert "SELECT id FROM repos" in query
    assert "org_id = {org_id:String}" in query
    assert "repo IN {repo_ids:Array(String)}" in query
    assert "toString(id) IN {repo_ids:Array(String)}" in query


@pytest.mark.asyncio
async def test_review_edges_repo_ids_filter_accepts_slug_from_filter_options() -> None:
    ctx = _ctx()
    slug = "full-chaos/dev-health-ops"
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input(repo_ids=[slug]))

    query: str = ctx.client.query.call_args.args[0]
    params: dict[str, Any] = ctx.client.query.call_args.kwargs["parameters"]
    assert "repo_id IN (" in query
    assert params["repo_ids"] == [slug]


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
# Deterministic tie-break (CHAOS-4421 / CHAOS-4368 Part A)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_edges_order_by_has_deterministic_tie_break() -> None:
    """``ORDER BY reviews_count DESC`` alone has no tie-breaker, so
    ClickHouse does not guarantee a stable row order/set among rows with
    equal ``reviews_count`` -- particularly at a ``LIMIT`` boundary (see
    ClickHouse's own ORDER BY docs: "If ... rows have the same value ... the
    resulting order of such rows is undefined and may be non-deterministic").
    A non-deterministic row set makes stage-2 dual-run parity proof (the
    comparator) meaningless -- comparing two non-deterministic outputs never
    proves the Go and Python implementations agree.

    This is a red-first regression test: before the fix, the emitted SQL was
    ``ORDER BY reviews_count DESC`` with no further key, so this assertion
    failed (the tie-break columns were entirely absent from the ORDER BY
    clause). The fix appends the resolver's own GROUP BY key -- ``repo_id,
    reviewer, author, day`` -- which is already a total order over the
    deduplicated row set (each combination of those four columns identifies
    exactly one row after the argMax/GROUP BY dedup above), so no synthetic
    tiebreaker column is introduced.
    """
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    await resolve_review_edges(ctx, _input())

    query: str = ctx.client.query.call_args.args[0]
    assert "ORDER BY reviews_count DESC, repo_id, reviewer, author, day" in query, (
        "ORDER BY must carry a full deterministic tie-break key "
        "(repo_id, reviewer, author, day) after reviews_count DESC, "
        "not just reviews_count DESC alone -- see CHAOS-4421."
    )
    # The tie-break key must appear strictly after the dedup GROUP BY (same
    # ordering-of-clauses invariant the existing dedup test pins) and
    # strictly before LIMIT, so it actually governs which rows the cap keeps.
    idx_group = query.index("GROUP BY repo_id, reviewer, author, day")
    idx_order = query.index(
        "ORDER BY reviews_count DESC, repo_id, reviewer, author, day"
    )
    idx_limit = query.index("LIMIT")
    assert idx_group < idx_order < idx_limit


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
