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
    """team_id from input is echoed in the result and passed to the query.

    CHAOS-4365: a single-team query (team_id set, repo_id NOT set) now reads
    team_cognitive_load_daily directly -- ONE query, not the old two-query
    merge (see test_cognitive_load_single_team_reads_new_table_not_the_old_
    tainted_columns below for why: the old merge filtered on user_metrics_
    daily/team_metrics_daily's own team_id column, which CHAOS-4396 found
    can be empty/impure for a real org).
    """
    ctx = _ctx()
    team_load_cols = [
        "day",
        "pr_interruption_load",
        "context_spread_count",
        "review_request_load",
        "after_hours_commit_ratio",
        "weekend_commit_ratio",
    ]
    _setup_client(
        ctx.client,
        [_qresult(team_load_cols, [[DAY_1, 3, 7, 1, 0.2, 0.1]])],
    )

    result = await resolve_cognitive_load(ctx, _input(team_id="team-alpha"))

    assert result.team_id == "team-alpha"
    assert ctx.client.query.call_count == 1
    query: str = ctx.client.query.call_args_list[0].args[0]
    assert "team_cognitive_load_daily" in query
    assert "team_id" in query
    assert len(result.signals) == 1
    assert result.signals[0].pr_interruption_load == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_cognitive_load_single_team_reads_new_table_not_the_old_tainted_columns() -> (
    None
):
    """RED before the fix: a single-team query returned EMPTY on a real org
    even though the org-wide query worked, because the old path filtered
    user_metrics_daily/team_metrics_daily on their OWN team_id column --
    CHAOS-4396 found that column can fall back to author-membership
    resolution or stay unset for a native org whose teams have empty
    repo_patterns. team_cognitive_load_daily (CHAOS-4365 item 2) is already
    team-scoped and OWNERSHIP-resolved (CHAOS-4321) at write time, so this
    asserts the single-team path queries THAT table, never the tainted
    user_metrics_daily/team_metrics_daily columns -- pinning the fix by
    checking which table the resolver actually reads, not just the output
    shape (a mock returning data either way can't distinguish "read the
    right table" from "read the wrong table that happened to have rows").
    """
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], [])])

    await resolve_cognitive_load(ctx, _input(team_id="team-alpha"))

    assert ctx.client.query.call_count == 1
    query: str = ctx.client.query.call_args_list[0].args[0]
    assert "team_cognitive_load_daily" in query
    assert "user_metrics_daily" not in query
    assert "team_metrics_daily" not in query


@pytest.mark.asyncio
async def test_cognitive_load_single_team_query_bundles_nullable_ratios_in_one_argmax_tuple() -> (
    None
):
    """Codex R1 (P2): the ratio columns are Nullable(Float64) -- None means
    unmeasured, distinct from a measured 0.0 (migration 081). A bare
    argMax(nullable_col, computed_at) PER COLUMN independently skips NULL
    arguments, so a day recomputed from "measured" to "unmeasured" (the
    latest row's ratio genuinely NULL) would keep returning a STALE
    non-null ratio from an older row instead of the latest row's true
    NULL. The query must bundle every field into ONE
    argMax(tuple(...), computed_at) so the whole row is picked atomically
    from the single latest computed_at -- same fix as compounding_risk.py's
    _fetch_latest_rows. Asserted on SQL text (the mocked client cannot
    execute ClickHouse's real argMax NULL-skipping semantics)."""
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], [])])

    await resolve_cognitive_load(ctx, _input(team_id="team-alpha"))

    query: str = _squash_ws(ctx.client.query.call_args_list[0].args[0])
    assert "argMax( tuple(" in query or "argMax(tuple(" in query
    # Each nullable ratio field must appear INSIDE the tuple(...) argument,
    # not as its own separate argMax(...) call -- a naive per-column bare
    # argMax(after_hours_commit_ratio, computed_at) would also satisfy a
    # weaker "argMax appears and after_hours_commit_ratio appears somewhere"
    # check, so this greps specifically for a bare per-column call, which
    # must NOT be present.
    assert "argMax(after_hours_commit_ratio, computed_at)" not in query
    assert "argMax(weekend_commit_ratio, computed_at)" not in query


@pytest.mark.asyncio
async def test_cognitive_load_team_and_repo_id_combined_uses_the_old_merge_path() -> (
    None
):
    """team_id AND repo_id both set: team_cognitive_load_daily has no
    repo_id dimension to filter by, so this falls through to the original
    two-query merge over user_metrics_daily/team_metrics_daily (repo_id
    narrows the user-metrics query only, per CHAOS-2386) -- unchanged by
    the CHAOS-4365 single-team fix.
    """
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    result = await resolve_cognitive_load(
        ctx, _input(team_id="team-alpha", repo_id="acme/repo")
    )

    assert result.team_id == "team-alpha"
    assert ctx.client.query.call_count == 2
    first_query: str = ctx.client.query.call_args_list[0].args[0]
    second_query: str = ctx.client.query.call_args_list[1].args[0]
    assert "team_cognitive_load_daily" not in first_query
    assert "team_cognitive_load_daily" not in second_query
    assert "team_id" in first_query
    assert "team_id" in second_query


# ---------------------------------------------------------------------------
# repo_id filter (CHAOS-2386)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cognitive_load_repo_id_filters_user_query_only() -> None:
    """The GraphQL ``repoId`` filter narrows the user-metrics query only.

    ``team_metrics_daily`` gained a ``repo_id`` column (CHAOS-4329) that the
    team-metrics query now groups/sums by internally, but
    ``_fetch_team_metrics`` still does not accept a ``repoId`` FILTER — it
    always aggregates across every repo a team owns, unlike the user-metrics
    query below, which narrows to one repo when ``repoId`` is supplied.
    Regression test for CHAOS-2386 (the resolver previously had no repo_id
    field/predicate at all, making the UI repo control a no-op). The
    predicate resolves against an org-scoped subquery over ``repos`` (by
    UUID or slug) rather than comparing the UUID column directly against
    the parameter, so a non-UUID value degrades to a no-match rather than a
    ClickHouse ``CANNOT_PARSE_UUID`` exception (mirrors
    ``resolvers/complexity.py``'s org-scoped repo-label lookup).
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
    assert "repo_id IN (" in first_query
    assert "SELECT id FROM repos" in first_query
    assert "org_id = {org_id:String}" in first_query
    assert "repo = {repo_id:String}" in first_query
    assert "toString(id) = {repo_id:String}" in first_query
    # The team query groups/sums by repo_id (CHAOS-4329) but never takes the
    # GraphQL repoId as a FILTER -- the {repo_id:String} bind parameter (the
    # thing the user-metrics query above is filtered by) must not appear.
    assert "{repo_id:String}" not in second_query
    assert "repos" not in second_query


@pytest.mark.asyncio
async def test_cognitive_load_repo_id_accepts_slug_from_filter_options() -> None:
    """repo_id also accepts a repos.repo full_name slug (CHAOS-2386 acceptance).

    ``/api/v1/filters/options`` populates the web repo picker from
    ``repos.repo`` slugs (e.g. "org/repo"), not UUIDs — a normal UI repo
    selection sends a slug, not a UUID. The predicate must resolve either
    form via the org-scoped ``repos`` subquery so a real UI selection
    actually narrows the query, not just a UUID passed directly.
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
            _qresult(user_cols, [[DAY_1, 6, 11, 3]]),
            _qresult([], []),
        ],
    )

    result = await resolve_cognitive_load(
        ctx, _input(repo_id="full-chaos/dev-health-ops")
    )

    assert len(result.signals) == 1
    first_query: str = ctx.client.query.call_args_list[0].args[0]
    first_params: dict = ctx.client.query.call_args_list[0].kwargs["parameters"]
    assert "repo_id IN (" in first_query
    # The slug is passed through unmodified as the single {repo_id:String}
    # parameter binding — the resolver does not attempt to detect/parse the
    # input shape itself; ClickHouse's OR of repo/toString(id) resolves it.
    assert first_params["repo_id"] == "full-chaos/dev-health-ops"


@pytest.mark.asyncio
async def test_cognitive_load_no_repo_id_omits_predicate() -> None:
    """When repo_id is absent, no repo_id predicate is added to either query."""
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_cognitive_load(ctx, _input())

    first_query: str = ctx.client.query.call_args_list[0].args[0]
    assert "repo_id IN (" not in first_query


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
    (day, team_id, repo_id) via argMax(<col>, computed_at) (CHAOS-4329: adds
    repo_id to the dedup key), SUM the additive counts across a team's
    repos, recompute the ratio from those sums, then AVG across teams."""
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_cognitive_load(ctx, _input())

    team_query: str = _squash_ws(ctx.client.query.call_args_list[1].args[0])
    assert "argMax(commits_count, computed_at)" in team_query
    assert "argMax(after_hours_commits_count, computed_at)" in team_query
    assert "argMax(weekend_commits_count, computed_at)" in team_query
    assert "GROUP BY day, team_id, repo_id" in team_query
    assert "GROUP BY day, team_id" in team_query
    assert "total_after_hours_commits / total_commits" in team_query
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
