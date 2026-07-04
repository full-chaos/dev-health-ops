"""Resolver tests for Compounding Risk (CHAOS-1642).

These tests exercise the resolver against a mocked ClickHouse client and
verify:

* the latest day is resolved when ``filter.day`` is omitted,
* per-repo rows surface the persisted score, severity, components, weights,
  thresholds, and computed_at,
* per-team breakout averages repo rows via the configured repo→team map,
* nulls propagate as ``None`` (data unavailable, not zero),
* the resolver is read-only and never recomputes the composite.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.compounding_risk import (
    resolve_compounding_risk,
)
from dev_health_ops.api.graphql.types.compounding_risk import (
    CompoundingRiskFilterInput,
    CompoundingRiskScope,
    CompoundingRiskScopeEntity,
    CompoundingRiskSeverity,
)

ORG_ID = "org-test"
NOW = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
DAY = date(2026, 5, 20)
PARTIAL_DAY = date(2026, 5, 21)


def _ctx() -> GraphQLContext:
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/d")
    # ``spec=["query"]`` restricts the mock so ``api.queries.client.query_dicts``
    # skips the dsn-based per-thread client path AND the ``sink.query_dicts``
    # path and falls through to ``sink.query(query, parameters=params)`` —
    # the surface these tests have always asserted against.
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


def _assert_scope_id_slug_or_uuid_predicate(query: str) -> None:
    assert "scope_id IN (" in query
    assert "SELECT toString(id) FROM repos" in query
    assert "org_id = {org_id:String}" in query
    assert "repo IN {repo_ids:Array(String)}" in query
    assert "toString(id) IN {repo_ids:Array(String)}" in query


# ---------------------------------------------------------------------------
# Day resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_state_when_no_data_for_org() -> None:
    ctx = _ctx()
    # First call: latest-day lookup → empty.
    _setup_client(ctx.client, [_qresult(["day"], [])])

    result = await resolve_compounding_risk(ctx, ORG_ID)

    assert result.org_id == ORG_ID
    assert result.rows == []
    assert result.trend == []


@pytest.mark.asyncio
async def test_empty_state_when_no_scored_day_for_org() -> None:
    ctx = _ctx()
    _setup_client(ctx.client, [_qresult(["day"], [[None]])])

    result = await resolve_compounding_risk(ctx, ORG_ID)

    latest_query: str = ctx.client.query.call_args_list[0].args[0]
    assert "missing_scores = 0" in latest_query
    assert result.org_id == ORG_ID
    assert result.rows == []
    assert result.trend == []


@pytest.mark.asyncio
async def test_latest_day_skips_partial_null_day_and_returns_complete_score() -> None:
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult(
                columns,
                [["repo-1", 0.4996, "elevated", 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, NOW]],
            ),
            _qresult(["repo_id", "full_name"], [["repo-1", "acme/backend"]]),
            _qresult(["day", "avg_score"], [[DAY, 0.4996], [PARTIAL_DAY, None]]),
        ],
    )

    result = await resolve_compounding_risk(ctx, ORG_ID)

    latest_query: str = ctx.client.query.call_args_list[0].args[0]
    latest_params: dict[str, Any] = ctx.client.query.call_args_list[0].kwargs[
        "parameters"
    ]
    assert "maxOrNull(day) AS day" in latest_query
    assert "argMax(compounding_risk, computed_at) AS score" in latest_query
    assert "countIf(score IS NULL) AS missing_scores" in latest_query
    assert "missing_scores = 0" in latest_query
    assert latest_params == {"org_id": ORG_ID, "scope": "repo"}
    assert result.rows[0].day == DAY
    assert result.rows[0].score == pytest.approx(0.4996)


@pytest.mark.asyncio
async def test_uses_filter_day_when_provided_and_skips_latest_lookup() -> None:
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult(  # repo rows for the explicit day
                [
                    "scope_id",
                    "score",
                    "severity",
                    "latest_computed_at",
                    "w_churn",
                    "w_complexity",
                    "w_ownership",
                    "w_review",
                    "threshold_elevated",
                    "threshold_high",
                ],
                [["repo-1", 0.5, "elevated", NOW, 0.3, 0.3, 0.2, 0.2, 0.4, 0.65]],
            ),
            _qresult([], []),  # repos label lookup
            _qresult(["day", "avg_score"], [[DAY, 0.5]]),  # trend
        ],
    )

    result = await resolve_compounding_risk(
        ctx, ORG_ID, CompoundingRiskFilterInput(day=DAY)
    )

    # First query must NOT be the latest-day lookup.
    first_query = ctx.client.query.call_args_list[0].args[0]
    assert "max(day)" not in first_query
    assert len(result.rows) == 1
    assert result.rows[0].day == DAY


# ---------------------------------------------------------------------------
# Repo breakout — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repo_breakout_surfaces_full_audit_trail() -> None:
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "churn_norm",
        "complexity_norm",
        "ownership_norm",
        "review_norm",
        "rework_churn",
        "complexity_delta",
        "bus_factor",
        "ownership_gini",
        "single_owner_ratio",
        "review_latency_p90h",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    repo_row = [
        "repo-1",
        0.72,
        "high",
        0.8,
        0.7,
        0.6,
        0.5,
        0.18,
        0.12,
        4.0,
        0.55,
        0.65,
        30.0,
        0.30,
        0.30,
        0.20,
        0.20,
        0.40,
        0.65,
        NOW,
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),  # latest-day lookup
            _qresult(columns, [repo_row]),  # repo rows
            _qresult(  # repo labels
                ["repo_id", "full_name"], [["repo-1", "acme/backend"]]
            ),
            _qresult(["day", "avg_score"], [[DAY, 0.72]]),  # trend
        ],
    )

    result = await resolve_compounding_risk(ctx, ORG_ID)

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.scope == CompoundingRiskScope.REPO
    assert row.scope_id == "repo-1"
    assert row.scope_label == "acme/backend"
    assert row.score == pytest.approx(0.72)
    assert row.severity == CompoundingRiskSeverity.HIGH
    # Components fully populated
    assert row.components.churn_norm == pytest.approx(0.8)
    assert row.components.complexity_delta == pytest.approx(0.12)
    assert row.components.review_latency_p90h == pytest.approx(30.0)
    # Audit trail
    assert (
        row.weights.churn
        + row.weights.complexity
        + row.weights.ownership
        + row.weights.review
        == pytest.approx(1.0)
    )
    assert row.thresholds.elevated == pytest.approx(0.40)
    assert row.thresholds.high == pytest.approx(0.65)


@pytest.mark.asyncio
async def test_null_score_maps_to_unknown_severity() -> None:
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult(
                columns,
                [["repo-1", None, "unknown", 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, NOW]],
            ),
            _qresult([], []),
            _qresult([], []),
        ],
    )

    result = await resolve_compounding_risk(ctx, ORG_ID)
    assert result.rows[0].score is None
    assert result.rows[0].severity == CompoundingRiskSeverity.UNKNOWN


@pytest.mark.asyncio
async def test_repo_ids_filter_resolves_slugs_or_uuids_in_latest_and_trend_queries() -> (
    None
):
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult([], []),
            _qresult([], []),
        ],
    )

    await resolve_compounding_risk(
        ctx,
        ORG_ID,
        CompoundingRiskFilterInput(
            day=DAY, repo_ids=["3fa85f64-5717-4562-b3fc-2c963f66afa6"]
        ),
    )

    latest_query: str = ctx.client.query.call_args_list[0].args[0]
    trend_query: str = ctx.client.query.call_args_list[1].args[0]
    _assert_scope_id_slug_or_uuid_predicate(latest_query)
    _assert_scope_id_slug_or_uuid_predicate(trend_query)


@pytest.mark.asyncio
async def test_latest_day_selection_is_repo_scope_and_filter_isolated() -> None:
    ctx = _ctx()
    slug = "full-chaos/dev-health-ops"
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult([], []),
            _qresult([], []),
        ],
    )

    await resolve_compounding_risk(
        ctx, ORG_ID, CompoundingRiskFilterInput(repo_ids=[slug])
    )

    latest_query: str = ctx.client.query.call_args_list[0].args[0]
    latest_params: dict[str, Any] = ctx.client.query.call_args_list[0].kwargs[
        "parameters"
    ]
    assert "AND scope = {scope:String}" in latest_query
    _assert_scope_id_slug_or_uuid_predicate(latest_query)
    assert latest_params == {"org_id": ORG_ID, "scope": "repo", "repo_ids": [slug]}


@pytest.mark.asyncio
async def test_latest_day_selection_is_team_scope_and_filter_isolated() -> None:
    ctx = _ctx()
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult([], []),
            _qresult([], []),
            _qresult([], []),
            _qresult([], []),
        ],
    )

    await resolve_compounding_risk(
        ctx,
        ORG_ID,
        CompoundingRiskFilterInput(
            breakout=CompoundingRiskScope.TEAM, team_ids=["team-A"]
        ),
    )

    latest_query: str = ctx.client.query.call_args_list[0].args[0]
    latest_params: dict[str, Any] = ctx.client.query.call_args_list[0].kwargs[
        "parameters"
    ]
    assert "AND scope = {scope:String}" in latest_query
    assert "AND scope_id IN {scope_ids:Array(String)}" in latest_query
    assert latest_params == {
        "org_id": ORG_ID,
        "scope": "team",
        "scope_ids": ["team-A"],
    }


@pytest.mark.asyncio
async def test_repo_ids_filter_accepts_slug_from_filter_options() -> None:
    ctx = _ctx()
    slug = "full-chaos/dev-health-ops"
    _setup_client(
        ctx.client,
        [
            _qresult([], []),
            _qresult([], []),
        ],
    )

    await resolve_compounding_risk(
        ctx, ORG_ID, CompoundingRiskFilterInput(day=DAY, repo_ids=[slug])
    )

    for call in ctx.client.query.call_args_list:
        query: str = call.args[0]
        params: dict[str, Any] = call.kwargs["parameters"]
        _assert_scope_id_slug_or_uuid_predicate(query)
        assert params["repo_ids"] == [slug]


# ---------------------------------------------------------------------------
# Team breakout — persisted team rows preferred, fallback to read-time aggregation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_team_breakout_averages_repos_via_repo_to_team_map() -> None:
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "churn_norm",
        "complexity_norm",
        "ownership_norm",
        "review_norm",
        "rework_churn",
        "complexity_delta",
        "bus_factor",
        "ownership_gini",
        "single_owner_ratio",
        "review_latency_p90h",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    base_weights = [0.30, 0.30, 0.20, 0.20]
    thresholds = [0.40, 0.65]
    rows = [
        [
            "repo-1",
            0.80,
            "high",
            0.9,
            0.7,
            0.8,
            0.6,
            0.20,
            0.15,
            3.0,
            0.6,
            0.7,
            50.0,
            *base_weights,
            *thresholds,
            NOW,
        ],
        [
            "repo-2",
            0.40,
            "elevated",
            0.5,
            0.4,
            0.4,
            0.3,
            0.10,
            0.05,
            5.0,
            0.4,
            0.4,
            20.0,
            *base_weights,
            *thresholds,
            NOW,
        ],
    ]
    teams_rows = [["team-A", "Platform", ["repo-1", "repo-2"]]]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult([], []),  # team-scope query returns empty → fallback to repo agg
            _qresult(columns, rows),  # repo rows for the fallback aggregation
            _qresult(
                ["id", "name", "repo_patterns"], teams_rows
            ),  # teams for repo→team map
            _qresult(["day", "avg_score"], [[DAY, 0.6]]),  # trend
        ],
    )

    result = await resolve_compounding_risk(
        ctx, ORG_ID, CompoundingRiskFilterInput(breakout=CompoundingRiskScope.TEAM)
    )

    assert len(result.rows) == 1
    team_row = result.rows[0]
    assert team_row.scope == CompoundingRiskScope.TEAM
    assert team_row.scope_id == "team-A"
    assert team_row.scope_label == "Platform"
    # Average of 0.80 and 0.40 == 0.60
    assert team_row.score == pytest.approx(0.60)
    # 0.60 is in [0.40, 0.65) → elevated
    assert team_row.severity == CompoundingRiskSeverity.ELEVATED
    # Component averages
    assert team_row.components.churn_norm == pytest.approx(0.70)
    assert team_row.components.review_latency_p90h == pytest.approx(35.0)


@pytest.mark.asyncio
async def test_team_breakout_filters_by_team_ids() -> None:
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    base = [0.30, 0.30, 0.20, 0.20, 0.40, 0.65]
    rows = [
        ["repo-1", 0.8, "high", *base, NOW],
        ["repo-2", 0.4, "elevated", *base, NOW],
    ]
    teams_rows = [
        ["team-A", "A", ["repo-1"]],
        ["team-B", "B", ["repo-2"]],
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult([], []),  # team-scope query empty → fallback
            _qresult(columns, rows),  # repo rows for fallback aggregation
            _qresult(["id", "name", "repo_patterns"], teams_rows),
            _qresult([], []),  # trend
        ],
    )

    result = await resolve_compounding_risk(
        ctx,
        ORG_ID,
        CompoundingRiskFilterInput(
            breakout=CompoundingRiskScope.TEAM, team_ids=["team-B"]
        ),
    )
    assert [r.scope_id for r in result.rows] == ["team-B"]


@pytest.mark.asyncio
async def test_team_breakout_uses_persisted_team_rows_when_available() -> None:
    """When ``compounding_risk_daily`` has scope='team' rows, the resolver
    surfaces them directly instead of aggregating from repo rows."""
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "churn_norm",
        "complexity_norm",
        "ownership_norm",
        "review_norm",
        "rework_churn",
        "complexity_delta",
        "bus_factor",
        "ownership_gini",
        "single_owner_ratio",
        "review_latency_p90h",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    base_weights = [0.30, 0.30, 0.20, 0.20]
    thresholds = [0.40, 0.65]
    persisted_team_rows = [
        [
            "team-X",
            0.55,
            "elevated",
            0.7,
            0.5,
            0.4,
            0.3,
            0.12,
            0.08,
            4.0,
            0.5,
            0.55,
            36.0,
            *base_weights,
            *thresholds,
            NOW,
        ],
    ]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult(columns, persisted_team_rows),  # team-scope rows EXIST
            _qresult(  # teams query for label lookup
                ["id", "name", "repo_patterns"],
                [["team-X", "Platform", []]],
            ),
            _qresult([], []),  # trend
        ],
    )

    result = await resolve_compounding_risk(
        ctx, ORG_ID, CompoundingRiskFilterInput(breakout=CompoundingRiskScope.TEAM)
    )

    assert len(result.rows) == 1
    team_row = result.rows[0]
    assert team_row.scope == CompoundingRiskScope.TEAM
    assert team_row.scope_id == "team-X"
    assert team_row.scope_label == "Platform"
    # Persisted score is surfaced as-is, NOT recomputed by the resolver.
    assert team_row.score == pytest.approx(0.55)
    # Persisted severity is also pass-through.
    assert team_row.severity == CompoundingRiskSeverity.ELEVATED
    # Components and audit-trail come from the persisted row.
    assert team_row.components.churn_norm == pytest.approx(0.70)
    assert team_row.weights.churn == pytest.approx(0.30)


# ---------------------------------------------------------------------------
# Trend
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trend_window_is_bounded() -> None:
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    base = [0.30, 0.30, 0.20, 0.20, 0.40, 0.65]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult(columns, [["repo-1", 0.5, "elevated", *base, NOW]]),
            _qresult(["repo_id", "full_name"], [["repo-1", "acme/r1"]]),
            _qresult(
                ["day", "avg_score"],
                [[DAY, 0.5], [date(2026, 5, 19), 0.45]],
            ),
        ],
    )

    result = await resolve_compounding_risk(
        ctx, ORG_ID, CompoundingRiskFilterInput(trend_days=99999)
    )

    # Even with a huge requested trend_days, resolver clamps to MAX_TREND_DAYS.
    assert len(result.trend) == 2
    assert result.trend[0].day == DAY
    # Verify the trend query was issued with a bounded date window.
    last_call_args = ctx.client.query.call_args_list[-1]
    params = last_call_args.kwargs.get("parameters") or last_call_args.args[1]
    delta_days = (params["end"] - params["start"]).days
    assert delta_days <= 365  # MAX_TREND_DAYS


# ---------------------------------------------------------------------------
# Scope guardrails
# ---------------------------------------------------------------------------


def test_scope_input_has_no_developer_option() -> None:
    """Per the no-surveillance contract, person scope is intentionally absent.

    This is a static check on the enum surface — UI is also expected to gate
    its scope picker.
    """
    values = {s.value for s in CompoundingRiskScope}
    assert values == {"repo", "team"}


# ---------------------------------------------------------------------------
# scope_entity resolved display name (Framework A7 / A8)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scope_entity_carries_resolved_display_name() -> None:
    """scope_entity must expose the human label, not the raw scope_id (A7)."""
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    base = [0.30, 0.30, 0.20, 0.20, 0.4, 0.65]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult(columns, [["repo-1", 0.5, "elevated", *base, NOW]]),
            _qresult(["repo_id", "full_name"], [["repo-1", "acme/backend"]]),
            _qresult(["day", "avg_score"], [[DAY, 0.5]]),
        ],
    )
    result = await resolve_compounding_risk(ctx, ORG_ID)
    assert len(result.rows) == 1
    row = result.rows[0]

    assert isinstance(row.scope_entity, CompoundingRiskScopeEntity)
    assert row.scope_entity.id == "repo-1"
    assert row.scope_entity.display_name == "acme/backend"
    # scope_entity.displayName must equal scope_label for consistency
    assert row.scope_entity.display_name == row.scope_label


@pytest.mark.asyncio
async def test_scope_entity_display_name_is_not_bare_uuid_when_label_resolved() -> None:
    """A8: when a label lookup succeeds, displayName must not be a bare UUID."""
    import re

    uuid_scope_id = "698c1234-abcd-0000-0000-000000000000"
    ctx = _ctx()
    columns = [
        "scope_id",
        "score",
        "severity",
        "w_churn",
        "w_complexity",
        "w_ownership",
        "w_review",
        "threshold_elevated",
        "threshold_high",
        "latest_computed_at",
    ]
    base = [0.30, 0.30, 0.20, 0.20, 0.4, 0.65]
    _setup_client(
        ctx.client,
        [
            _qresult(["day"], [[DAY]]),
            _qresult(columns, [[uuid_scope_id, 0.7, "high", *base, NOW]]),
            _qresult(
                ["repo_id", "full_name"],
                [[uuid_scope_id, "my-org/api-service"]],
            ),
            _qresult(["day", "avg_score"], [[DAY, 0.7]]),
        ],
    )
    result = await resolve_compounding_risk(ctx, ORG_ID)
    assert len(result.rows) == 1
    row = result.rows[0]

    uuid_re = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
    )
    assert row.scope_entity.id == uuid_scope_id
    assert row.scope_entity.display_name == "my-org/api-service"
    assert not uuid_re.match(row.scope_entity.display_name)
