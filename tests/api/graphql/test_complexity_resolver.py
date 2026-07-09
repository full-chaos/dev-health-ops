"""Resolver tests for Complexity timeseries + hotspots (CHAOS-1756).

These tests exercise the resolvers against a mocked ClickHouse client and
verify:

* empty state returns ``ComplexityTimeseriesResult(points=[], totalScope=0)``
  and ``HotspotsResult(rows=[])``,
* repo-scope timeseries maps all column names correctly,
* file-scope timeseries uses ``as_of_day`` / ``file_path`` correctly,
* hotspot rows map ``risk_score``, ``evidenceUrl``, and nullable
  ``blameConcentration`` correctly,
* the org-id gate raises ``AuthorizationError`` on mismatch,
* the row limit is clamped to ``MAX_ROWS`` / ``MAX_HOTSPOTS_ROWS``.

All tests are read-only — no ClickHouse tables are modified.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.complexity import (
    MAX_HOTSPOTS_ROWS,
    MAX_ROWS,
    MAX_TIMESERIES_POINTS,
    resolve_complexity_timeseries,
    resolve_hotspots,
)
from dev_health_ops.api.graphql.types.complexity import (
    ComplexityScope,
    ComplexityTimeseriesInput,
    HotspotsInput,
    TimeGranularity,
)

ORG_ID = "org-complexity-test"
NOW = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
DAY = date(2026, 5, 20)
SINCE = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
UNTIL = datetime(2026, 5, 20, 23, 59, 59, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Test infrastructure (mirrors test_compounding_risk_resolver.py)
# ---------------------------------------------------------------------------


def _ctx() -> GraphQLContext:
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/d")
    ctx.client = MagicMock(spec=["query"])
    return ctx


def _qresult(columns: list[str], rows: list[list[Any]]) -> Any:
    """Build a fake clickhouse_connect QueryResult-like object."""
    result = MagicMock()
    result.column_names = columns
    result.result_rows = rows
    return result


def _setup_client(client: Any, responses: list[Any]) -> None:
    """Make ``client.query`` return ``responses[i]`` on call ``i``."""
    client.query.side_effect = responses


def _assert_repo_ids_slug_or_uuid_predicate(query: str) -> None:
    assert "repo_id IN (" in query
    assert "SELECT id FROM repos" in query
    assert "org_id = {org_id:String}" in query
    assert "repo IN {repo_ids:Array(String)}" in query
    assert "toString(id) IN {repo_ids:Array(String)}" in query


def _timeseries_input(
    *,
    scope: ComplexityScope = ComplexityScope.REPO,
    granularity: TimeGranularity = TimeGranularity.DAY,
    repo_ids: list[str] | None = None,
    limit: int | None = None,
) -> ComplexityTimeseriesInput:
    return ComplexityTimeseriesInput(
        org_id=ORG_ID,
        since_utc=SINCE,
        until_utc=UNTIL,
        granularity=granularity,
        scope=scope,
        repo_ids=repo_ids,
        limit=limit,
    )


def _hotspots_input(
    *,
    repo_ids: list[str] | None = None,
    limit: int | None = None,
) -> HotspotsInput:
    return HotspotsInput(
        org_id=ORG_ID,
        since_utc=SINCE,
        until_utc=UNTIL,
        repo_ids=repo_ids,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# complexityTimeseries — empty state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_empty_state_repo_scope() -> None:
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult([], []),  # repo_complexity_daily → no rows
            _qresult([], []),  # repo labels lookup
        ],
    )

    result = await resolve_complexity_timeseries(ctx, _timeseries_input())

    assert result.points == []
    assert result.total_scope == 0


@pytest.mark.asyncio
async def test_timeseries_empty_state_file_scope() -> None:
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult([], []),  # file_complexity_snapshots → no rows
            # FILE-scope skips the repo-labels join — only 1 query fires.
        ],
    )

    result = await resolve_complexity_timeseries(
        ctx, _timeseries_input(scope=ComplexityScope.FILE)
    )

    assert result.points == []
    assert result.total_scope == 0


# ---------------------------------------------------------------------------
# complexityTimeseries — repo scope happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_repo_scope_maps_columns_correctly() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "loc_total",
        "cyclomatic_total",
        "cyclomatic_per_kloc",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    row = [DAY, "repo-abc", 50000, 3200, 64.0, 42, 7]

    _setup_client(
        ctx.client,
        [
            _qresult(columns, [row]),
            _qresult(["repo_id", "full_name"], [["repo-abc", "acme/backend"]]),
        ],
    )

    result = await resolve_complexity_timeseries(ctx, _timeseries_input())

    assert len(result.points) == 1
    pt = result.points[0]
    assert pt.point_date == DAY
    assert pt.scope_id == "repo-abc"
    assert pt.scope_name == "acme/backend"
    assert pt.loc_total == 50000
    assert pt.cyclomatic_per_kloc == pytest.approx(64.0)
    assert pt.cyclomatic_total == 3200
    assert pt.cyclomatic_avg is None  # not stored per repo row
    assert pt.high_complexity_functions == 42
    assert pt.very_high_complexity_functions == 7
    assert result.total_scope == 1


@pytest.mark.asyncio
async def test_timeseries_repo_scope_fallback_scope_name_when_label_missing() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "loc_total",
        "cyclomatic_total",
        "cyclomatic_per_kloc",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(columns, [[DAY, "repo-xyz", 1000, 80, 80.0, 5, 0]]),
            _qresult([], []),  # label lookup returns nothing
        ],
    )

    result = await resolve_complexity_timeseries(ctx, _timeseries_input())

    # Falls back to repo_id when full_name is not in catalog.
    assert result.points[0].scope_name == "repo-xyz"


@pytest.mark.asyncio
async def test_timeseries_null_columns_propagate_as_none() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "loc_total",
        "cyclomatic_total",
        "cyclomatic_per_kloc",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(columns, [[DAY, "repo-1", None, None, None, None, None]]),
            _qresult([], []),
        ],
    )

    result = await resolve_complexity_timeseries(ctx, _timeseries_input())

    pt = result.points[0]
    assert pt.loc_total is None
    assert pt.cyclomatic_total is None
    assert pt.cyclomatic_per_kloc is None
    assert pt.high_complexity_functions is None
    assert pt.very_high_complexity_functions is None


@pytest.mark.asyncio
async def test_timeseries_total_scope_counts_distinct_scope_ids() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "loc_total",
        "cyclomatic_total",
        "cyclomatic_per_kloc",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    rows = [
        [DAY, "repo-1", 1000, 80, 80.0, 5, 0],
        [date(2026, 5, 19), "repo-1", 1000, 79, 79.0, 5, 0],  # same repo, different day
        [DAY, "repo-2", 2000, 160, 80.0, 10, 1],
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(columns, rows),
            _qresult([], []),
        ],
    )

    result = await resolve_complexity_timeseries(ctx, _timeseries_input())

    assert len(result.points) == 3
    # Two distinct repos → totalScope == 2
    assert result.total_scope == 2


# ---------------------------------------------------------------------------
# complexityTimeseries — file scope happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_file_scope_maps_columns_correctly() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "file_path",
        "cyclomatic_total",
        "cyclomatic_avg",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    row = [DAY, "repo-1", "src/auth/login.py", 120, 6.5, 3, 0]

    _setup_client(
        ctx.client,
        [
            _qresult(columns, [row]),
            # FILE-scope skips the repo-labels join — only 1 query fires.
        ],
    )

    result = await resolve_complexity_timeseries(
        ctx, _timeseries_input(scope=ComplexityScope.FILE)
    )

    assert len(result.points) == 1
    pt = result.points[0]
    assert pt.point_date == DAY
    assert pt.scope_id == "repo-1/src/auth/login.py"
    assert pt.scope_name == "src/auth/login.py"
    assert pt.loc_total is None  # not in file_complexity_snapshots
    assert pt.cyclomatic_per_kloc is None  # not in file_complexity_snapshots
    assert pt.cyclomatic_total == 120
    assert pt.cyclomatic_avg == pytest.approx(6.5)
    assert pt.high_complexity_functions == 3
    assert pt.very_high_complexity_functions == 0


@pytest.mark.asyncio
async def test_timeseries_file_scope_skips_repo_label_join() -> None:
    """FILE-scope must NOT call _load_repo_labels.

    Regression test for the dead-code path flagged by github-code-quality +
    CodeQL on PR #769: previously FILE-scope built ``repo_ids_seen`` and queried
    repo labels, but the result was never read (scopeName is derived from
    file_path). The unused query added a wasted ClickHouse round-trip per call.
    """
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "file_path",
        "cyclomatic_total",
        "cyclomatic_avg",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(columns, [[DAY, "repo-1", "src/a.py", 10, 2.0, 0, 0]]),
        ],
    )

    await resolve_complexity_timeseries(
        ctx, _timeseries_input(scope=ComplexityScope.FILE)
    )

    # Exactly one ClickHouse call — the file_complexity_snapshots fetch.
    # If a future change re-adds the repo-labels join, this assertion fails
    # and forces the author to justify the extra round-trip.
    assert ctx.client.query.call_count == 1, (
        f"FILE-scope made {ctx.client.query.call_count} ClickHouse queries; "
        "expected 1 (no repo-label join)."
    )


# ---------------------------------------------------------------------------
# complexityTimeseries — WEEK granularity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_week_granularity_truncates_to_monday() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "loc_total",
        "cyclomatic_total",
        "cyclomatic_per_kloc",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(
                columns,
                [[date(2026, 5, 18), "repo-1", 1000, 80, 80.0, 5, 0]],
            ),
            _qresult([], []),
        ],
    )

    result = await resolve_complexity_timeseries(
        ctx, _timeseries_input(granularity=TimeGranularity.WEEK)
    )

    assert result.points[0].point_date == date(2026, 5, 18)


@pytest.mark.asyncio
async def test_timeseries_week_granularity_buckets_in_clickhouse() -> None:
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_complexity_timeseries(
        ctx, _timeseries_input(granularity=TimeGranularity.WEEK)
    )

    first_call_query: str = ctx.client.query.call_args_list[0].args[0]
    assert "toStartOfWeek(day, 1) AS day" in first_call_query
    assert (
        "argMax(cyclomatic_total,               (day, computed_at))" in first_call_query
    )
    assert "GROUP BY day, repo_id" in first_call_query


@pytest.mark.asyncio
async def test_timeseries_week_granularity_returns_one_point_per_repo_week() -> None:
    ctx = _ctx()
    columns = [
        "day",
        "repo_id",
        "loc_total",
        "cyclomatic_total",
        "cyclomatic_per_kloc",
        "high_complexity_functions",
        "very_high_complexity_functions",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(columns, [[date(2026, 5, 18), "repo-1", 1000, 80, 80.0, 5, 0]]),
            _qresult([], []),
        ],
    )

    result = await resolve_complexity_timeseries(
        ctx, _timeseries_input(granularity=TimeGranularity.WEEK)
    )

    assert [(p.scope_id, p.point_date) for p in result.points] == [
        ("repo-1", date(2026, 5, 18))
    ]


# ---------------------------------------------------------------------------
# complexityTimeseries — row limit clamping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_limit_is_clamped_by_scope_bucket_point_cap() -> None:
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    result = await resolve_complexity_timeseries(
        ctx, _timeseries_input(limit=MAX_ROWS + 99999)
    )

    first_call_query: str = ctx.client.query.call_args_list[0].args[0]
    first_call_params: dict[str, Any] = ctx.client.query.call_args_list[0].kwargs[
        "parameters"
    ]
    expected_scope_limit = MAX_TIMESERIES_POINTS // 20
    assert "LIMIT {limit:UInt32}" in first_call_query
    assert first_call_params["limit"] == expected_scope_limit
    assert result.points == []


@pytest.mark.asyncio
async def test_timeseries_point_bound_limits_multi_day_multi_repo_window() -> None:
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_complexity_timeseries(ctx, _timeseries_input(limit=MAX_ROWS))

    first_call_params: dict[str, Any] = ctx.client.query.call_args_list[0].kwargs[
        "parameters"
    ]
    assert first_call_params["limit"] * 20 <= MAX_TIMESERIES_POINTS


@pytest.mark.asyncio
async def test_timeseries_limit_selects_scopes_not_earliest_rows() -> None:
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    await resolve_complexity_timeseries(ctx, _timeseries_input(limit=10))

    first_call_query: str = ctx.client.query.call_args_list[0].args[0]
    first_call_params: dict[str, Any] = ctx.client.query.call_args_list[0].kwargs[
        "parameters"
    ]
    assert "GROUP BY day, repo_id" in first_call_query
    assert "SELECT repo_id" in first_call_query
    assert "latest_complexity" in first_call_query
    assert "ORDER BY day, repo_id\nLIMIT" not in first_call_query
    assert first_call_params["limit"] == 10


@pytest.mark.asyncio
async def test_repo_ids_filter_resolves_slugs_or_uuids_at_all_user_filter_sites() -> (
    None
):
    ctx = _ctx()

    _setup_client(ctx.client, [_qresult([], []), _qresult([], []), _qresult([], [])])

    await resolve_complexity_timeseries(
        ctx,
        _timeseries_input(repo_ids=["3fa85f64-5717-4562-b3fc-2c963f66afa6"]),
    )
    await resolve_complexity_timeseries(
        ctx,
        _timeseries_input(
            scope=ComplexityScope.FILE,
            repo_ids=["3fa85f64-5717-4562-b3fc-2c963f66afa6"],
        ),
    )
    await resolve_hotspots(
        ctx, _hotspots_input(repo_ids=["3fa85f64-5717-4562-b3fc-2c963f66afa6"])
    )

    repo_query: str = ctx.client.query.call_args_list[0].args[0]
    file_query: str = ctx.client.query.call_args_list[1].args[0]
    hotspots_query: str = ctx.client.query.call_args_list[2].args[0]
    _assert_repo_ids_slug_or_uuid_predicate(repo_query)
    _assert_repo_ids_slug_or_uuid_predicate(file_query)
    _assert_repo_ids_slug_or_uuid_predicate(hotspots_query)


@pytest.mark.asyncio
async def test_repo_ids_filter_accepts_slug_from_filter_options() -> None:
    ctx = _ctx()
    slug = "full-chaos/dev-health-ops"
    _setup_client(ctx.client, [_qresult([], []), _qresult([], []), _qresult([], [])])

    await resolve_complexity_timeseries(ctx, _timeseries_input(repo_ids=[slug]))
    await resolve_complexity_timeseries(
        ctx, _timeseries_input(scope=ComplexityScope.FILE, repo_ids=[slug])
    )
    await resolve_hotspots(ctx, _hotspots_input(repo_ids=[slug]))

    for call in ctx.client.query.call_args_list:
        query: str = call.args[0]
        params: dict[str, Any] = call.kwargs["parameters"]
        _assert_repo_ids_slug_or_uuid_predicate(query)
        assert params["repo_ids"] == [slug]


# ---------------------------------------------------------------------------
# hotspots — empty state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hotspots_empty_state() -> None:
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult([], []),  # file_hotspot_daily → no rows
            _qresult([], []),  # repo labels lookup
        ],
    )

    result = await resolve_hotspots(ctx, _hotspots_input())

    assert result.rows == []


# ---------------------------------------------------------------------------
# hotspots — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hotspots_maps_columns_correctly() -> None:
    ctx = _ctx()
    columns = [
        "repo_id",
        "file_path",
        "churn_loc_30d",
        "churn_commits_30d",
        "cyclomatic_total",
        "cyclomatic_avg",
        "blame_concentration",
        "risk_score",
    ]
    row = ["repo-1", "src/core/engine.py", 4200, 38, 95, 9.5, 0.72, 0.88]

    _setup_client(
        ctx.client,
        [
            _qresult(columns, [row]),
            _qresult(["repo_id", "full_name"], [["repo-1", "acme/core"]]),
        ],
    )

    result = await resolve_hotspots(ctx, _hotspots_input())

    assert len(result.rows) == 1
    r = result.rows[0]
    assert r.file_path == "src/core/engine.py"
    assert r.repo_id == "repo-1"
    assert r.repo_name == "acme/core"
    assert r.churn_loc_30d == 4200
    assert r.churn_commits_30d == 38
    assert r.cyclomatic_total == 95
    assert r.cyclomatic_avg == pytest.approx(9.5)
    assert r.blame_concentration == pytest.approx(0.72)
    assert r.risk_score == pytest.approx(0.88)
    # urllib.parse.quote keeps '/' safe by default; the path separator is
    # readable and still valid as a query-string value.
    assert r.evidence_url == "/code?file=src/core/engine.py"


@pytest.mark.asyncio
async def test_hotspots_null_blame_concentration_propagates_as_none() -> None:
    ctx = _ctx()
    columns = [
        "repo_id",
        "file_path",
        "churn_loc_30d",
        "churn_commits_30d",
        "cyclomatic_total",
        "cyclomatic_avg",
        "blame_concentration",
        "risk_score",
    ]
    row = ["repo-1", "src/main.py", 100, 5, 20, 2.0, None, 0.3]

    _setup_client(
        ctx.client,
        [
            _qresult(columns, [row]),
            _qresult([], []),
        ],
    )

    result = await resolve_hotspots(ctx, _hotspots_input())

    assert result.rows[0].blame_concentration is None


@pytest.mark.asyncio
async def test_hotspots_evidence_url_encodes_special_chars() -> None:
    ctx = _ctx()
    columns = [
        "repo_id",
        "file_path",
        "churn_loc_30d",
        "churn_commits_30d",
        "cyclomatic_total",
        "cyclomatic_avg",
        "blame_concentration",
        "risk_score",
    ]
    row = ["repo-1", "src/module with spaces/file.py", 10, 1, 5, 1.0, None, 0.1]

    _setup_client(
        ctx.client,
        [
            _qresult(columns, [row]),
            _qresult([], []),
        ],
    )

    result = await resolve_hotspots(ctx, _hotspots_input())

    # Spaces must be percent-encoded in the deeplink.
    assert result.rows[0].evidence_url is not None
    assert " " not in result.rows[0].evidence_url
    assert "%20" in result.rows[0].evidence_url


@pytest.mark.asyncio
async def test_hotspots_fallback_repo_name_when_label_missing() -> None:
    ctx = _ctx()
    columns = [
        "repo_id",
        "file_path",
        "churn_loc_30d",
        "churn_commits_30d",
        "cyclomatic_total",
        "cyclomatic_avg",
        "blame_concentration",
        "risk_score",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(columns, [["repo-unknown", "src/x.py", 10, 1, 5, 1.0, None, 0.1]]),
            _qresult([], []),  # no catalog row returned
        ],
    )

    result = await resolve_hotspots(ctx, _hotspots_input())

    # Falls back to repo_id when full_name is not in the catalog.
    assert result.rows[0].repo_name == "repo-unknown"


# ---------------------------------------------------------------------------
# hotspots — row limit clamping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hotspots_limit_is_clamped_to_max_hotspots_rows() -> None:
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult([], []), _qresult([], [])])

    result = await resolve_hotspots(
        ctx, _hotspots_input(limit=MAX_HOTSPOTS_ROWS + 99999)
    )

    first_call_query: str = ctx.client.query.call_args_list[0].args[0]
    assert f"LIMIT {MAX_HOTSPOTS_ROWS}" in first_call_query
    assert result.rows == []


# ---------------------------------------------------------------------------
# Org-id gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_org_id_gate_raises_on_missing_context_org() -> None:
    """``require_org_id`` raises when context.org_id is missing."""
    from dev_health_ops.api.graphql.errors import AuthorizationError

    # Bypass __post_init__ validation by patching after construction.
    ctx = _ctx()
    object.__setattr__(ctx, "org_id", "")

    with pytest.raises(AuthorizationError):
        await resolve_complexity_timeseries(ctx, _timeseries_input())


@pytest.mark.asyncio
async def test_hotspots_org_id_gate_raises_on_missing_context_org() -> None:
    """``require_org_id`` raises when context.org_id is missing."""
    from dev_health_ops.api.graphql.errors import AuthorizationError

    ctx = _ctx()
    object.__setattr__(ctx, "org_id", "")

    with pytest.raises(AuthorizationError):
        await resolve_hotspots(ctx, _hotspots_input())


# ---------------------------------------------------------------------------
# Schema surface sanity
# ---------------------------------------------------------------------------


def test_complexity_scope_enum_values() -> None:
    """ComplexityScope must expose REPO and FILE (no person-level scope)."""
    values = {s.value for s in ComplexityScope}
    assert values == {"repo", "file"}


def test_time_granularity_enum_values() -> None:
    """TimeGranularity must expose DAY and WEEK."""
    values = {g.value for g in TimeGranularity}
    assert values == {"day", "week"}
