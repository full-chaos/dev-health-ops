"""Per-resolver SQL fixture registry for EXPLAIN-based validation (CHAOS-1752).

Each entry in :data:`ALL_RESOLVER_SQL_FIXTURES` is an async function that
takes a :class:`CapturingSink`, invokes every SQL-emitting helper in a single
resolver with representative arguments, and returns. The fixture's purpose
is to leave a recorded list of ``(sql, params)`` calls on the sink that
``test_resolver_sql_explain.py`` then validates with ``EXPLAIN SYNTAX``.

Contract for new resolvers
--------------------------
Every new GraphQL resolver that emits SQL **must** register a fixture here
that exercises each of its SQL-emitting helpers with a representative
parameter set. The fixture is part of the resolver's acceptance criteria,
not an optional extra. CI fails if EXPLAIN catches an unknown identifier,
illegal aggregation, or any other parse-time error in the recorded SQL.

Design notes
------------
* Helpers must look identical to production: never call resolver code
  through wrappers that mutate SQL. The whole point is to validate the
  *exact* SQL the resolver emits.
* When a helper takes ``context: GraphQLContext`` we wrap the capturing
  sink in :class:`FakeGraphQLContext`. When it takes ``client: Any`` we
  pass the sink directly. The two paths converge inside ``query_dicts``.
* The recorded ``(sql, params)`` pairs are blind to mutual exclusion —
  every code branch worth validating should be hit (e.g. with/without
  filters, with/without scope_ids).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import date, timedelta

from _sql_explain_helpers import (  # type: ignore[import-not-found]
    CapturingSink,
    FakeGraphQLContext,
)

SAMPLE_ORG_ID = "00000000-0000-0000-0000-000000000001"
SAMPLE_TEAM_ID = "team-alpha"
SAMPLE_REPO_ID = "11111111-1111-1111-1111-111111111111"
SAMPLE_DAY = date(2026, 5, 21)

ResolverSQLFixture = Callable[[CapturingSink], Awaitable[None]]


# ---------------------------------------------------------------------------
# compounding_risk (CHAOS-1642 / CHAOS-1751 — origin of this whole exercise)
# ---------------------------------------------------------------------------


async def _fixture_compounding_risk(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.resolvers.compounding_risk import (
        _fetch_latest_rows,
        _fetch_repo_trend,
        _latest_day_for_org,
        _load_repo_labels,
        _load_team_assignments,
    )

    await _latest_day_for_org(sink, SAMPLE_ORG_ID)

    # Both scope=repo and scope=team paths plus with/without scope_ids filter
    # — bug #2 (max(computed_at) AS computed_at) lives in this query.
    await _fetch_latest_rows(
        sink,
        org_id=SAMPLE_ORG_ID,
        day=SAMPLE_DAY,
        scope="repo",
        scope_ids=None,
    )
    await _fetch_latest_rows(
        sink,
        org_id=SAMPLE_ORG_ID,
        day=SAMPLE_DAY,
        scope="repo",
        scope_ids=[SAMPLE_REPO_ID],
    )
    await _fetch_latest_rows(
        sink,
        org_id=SAMPLE_ORG_ID,
        day=SAMPLE_DAY,
        scope="team",
        scope_ids=None,
    )

    await _fetch_repo_trend(
        sink, SAMPLE_ORG_ID, SAMPLE_DAY, trend_days=30, repo_ids=None
    )
    await _fetch_repo_trend(
        sink,
        SAMPLE_ORG_ID,
        SAMPLE_DAY,
        trend_days=30,
        repo_ids=[SAMPLE_REPO_ID],
    )

    # Bug #3 (toString(repo_id) AS repo_id from `repos` table that has `id`,
    # `repo`) lives here.
    await _load_repo_labels(sink, SAMPLE_ORG_ID, [SAMPLE_REPO_ID])
    await _load_team_assignments(sink, SAMPLE_ORG_ID)


# ---------------------------------------------------------------------------
# forecast
# ---------------------------------------------------------------------------


async def _fixture_forecast(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.resolvers.forecast import (
        _load_incident_overlay,
        _load_review_overlay,
        _load_throughput_history,
        _load_work_item_overlay,
    )

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)
    history_weeks = 12

    # Without work_scope_id.
    await _load_throughput_history(
        context,
        team_id=SAMPLE_TEAM_ID,
        work_scope_id=None,
        history_weeks=history_weeks,
    )
    await _load_work_item_overlay(
        context,
        team_id=SAMPLE_TEAM_ID,
        work_scope_id=None,
        history_weeks=history_weeks,
    )
    # With work_scope_id branch.
    await _load_throughput_history(
        context,
        team_id=SAMPLE_TEAM_ID,
        work_scope_id="scope-1",
        history_weeks=history_weeks,
    )
    await _load_work_item_overlay(
        context,
        team_id=SAMPLE_TEAM_ID,
        work_scope_id="scope-1",
        history_weeks=history_weeks,
    )
    await _load_review_overlay(context, history_weeks=history_weeks)
    await _load_incident_overlay(context, history_weeks=history_weeks)


# ---------------------------------------------------------------------------
# home
# ---------------------------------------------------------------------------


async def _fixture_home(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.resolvers.home import resolve_home

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)
    await resolve_home(context)


# ---------------------------------------------------------------------------
# capacity
# ---------------------------------------------------------------------------


async def _fixture_capacity(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.models.inputs import CapacityForecastFilterInput
    from dev_health_ops.api.graphql.resolvers.capacity import (
        resolve_capacity_forecasts,
    )

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)

    # No filters — bare query.
    await resolve_capacity_forecasts(context, filters=None)

    # Each filter branch contributes a different WHERE clause.
    filters = CapacityForecastFilterInput(
        team_id=SAMPLE_TEAM_ID,
        work_scope_id="scope-1",
        from_date=SAMPLE_DAY - timedelta(days=30),
        to_date=SAMPLE_DAY,
        limit=25,
    )
    await resolve_capacity_forecasts(context, filters=filters)


# ---------------------------------------------------------------------------
# work_graph
# ---------------------------------------------------------------------------


async def _fixture_work_graph(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.models.inputs import (
        WorkGraphEdgeFilterInput,
        WorkGraphEdgeTypeInput,
        WorkGraphNodeTypeInput,
    )
    from dev_health_ops.api.graphql.resolvers.work_graph import (
        resolve_work_graph_edges,
    )

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)

    await resolve_work_graph_edges(context, filters=None)

    filters = WorkGraphEdgeFilterInput(
        repo_ids=[SAMPLE_REPO_ID],
        source_type=WorkGraphNodeTypeInput.ISSUE,
        target_type=WorkGraphNodeTypeInput.PR,
        edge_type=WorkGraphEdgeTypeInput.IMPLEMENTS,
        node_id="node-1",
        limit=100,
    )
    await resolve_work_graph_edges(context, filters=filters)


# ---------------------------------------------------------------------------
# recommendations
# ---------------------------------------------------------------------------


async def _fixture_recommendations(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.models.recommendations import (
        WindowInput,
        WindowUnit,
    )
    from dev_health_ops.api.graphql.resolvers.recommendations import (
        resolve_recommendations,
    )

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)
    window = WindowInput(value=4, unit=WindowUnit.WEEK)
    await resolve_recommendations(context, team=SAMPLE_TEAM_ID, window=window)


# ---------------------------------------------------------------------------
# operating_review
# ---------------------------------------------------------------------------


async def _fixture_operating_review(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.resolvers.operating_review import (
        _fetch_period_rows,
    )
    from dev_health_ops.api.queries.client import query_dicts

    # Sample week_start = Monday.
    week_start = date(2026, 5, 18)
    await _fetch_period_rows(
        sink,
        query_dicts,
        org_id=SAMPLE_ORG_ID,
        team_id=SAMPLE_TEAM_ID,
        start=week_start,
    )


# ---------------------------------------------------------------------------
# security
# ---------------------------------------------------------------------------


async def _fixture_security(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.models.inputs import (
        SecurityAlertFilterInput,
        SecurityPaginationInput,
        SecuritySeverityInput,
        SecuritySourceInput,
        SecurityStateInput,
    )
    from dev_health_ops.api.graphql.resolvers.security import (
        resolve_security_alerts,
        resolve_security_overview,
    )

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)

    # Bare alert list.
    await resolve_security_alerts(context, org_id=SAMPLE_ORG_ID)

    # Every filter branch is included in the WHERE clause builder; covering
    # them here makes sure no branch ships a column the schema does not have.
    filters = SecurityAlertFilterInput(
        open_only=False,
        states=[SecurityStateInput.OPEN, SecurityStateInput.FIXED],
        repo_ids=[SAMPLE_REPO_ID],
        severities=[
            SecuritySeverityInput.CRITICAL,
            SecuritySeverityInput.HIGH,
        ],
        sources=[SecuritySourceInput.DEPENDABOT],
        since=SAMPLE_DAY - timedelta(days=30),
        until=SAMPLE_DAY,
        search="cve",
    )
    pagination = SecurityPaginationInput(first=50, after="0")
    await resolve_security_alerts(
        context,
        org_id=SAMPLE_ORG_ID,
        filters=filters,
        pagination=pagination,
    )

    # open_only branch overrides explicit states.
    open_only_filters = SecurityAlertFilterInput(open_only=True)
    await resolve_security_alerts(
        context, org_id=SAMPLE_ORG_ID, filters=open_only_filters
    )

    # Overview fires four queries (kpis, breakdown, top repos, trend).
    await resolve_security_overview(context, org_id=SAMPLE_ORG_ID, filters=filters)


# ---------------------------------------------------------------------------
# bus_factor (via OwnershipClickHouseLoader)
# ---------------------------------------------------------------------------


async def _fixture_bus_factor(sink: CapturingSink) -> None:
    from dev_health_ops.metrics.loaders.ownership import OwnershipClickHouseLoader

    loader = OwnershipClickHouseLoader(sink, org_id=SAMPLE_ORG_ID)

    # team_id path triggers _repo_ids_for_team plus the main stats query.
    await loader.load_commit_ownership_stats(team_id=SAMPLE_TEAM_ID)
    # repo_id direct path.
    await loader.load_commit_ownership_stats(repo_id=uuid.UUID(SAMPLE_REPO_ID))
    # Unscoped org-wide path.
    await loader.load_commit_ownership_stats()


# ---------------------------------------------------------------------------
# data_health
# ---------------------------------------------------------------------------


async def _fixture_data_health(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.resolvers.data_health import (
        _coverage_rows,
        _observed_identities,
    )

    context = FakeGraphQLContext(client=sink, org_id=SAMPLE_ORG_ID)
    await _observed_identities(context, org_id=SAMPLE_ORG_ID, team=SAMPLE_TEAM_ID)
    await _coverage_rows(context, org_id=SAMPLE_ORG_ID, team=SAMPLE_TEAM_ID)


# ---------------------------------------------------------------------------
# analytics — compile_* functions return SQL deterministically; harvest by
# calling them directly with sample requests (no capturing sink needed).
# ---------------------------------------------------------------------------


async def _fixture_analytics(sink: CapturingSink) -> None:
    from dev_health_ops.api.graphql.sql.compiler import (
        BreakdownRequest,
        CatalogValuesRequest,
        FlowMatrixRequest,
        SankeyRequest,
        TimeseriesRequest,
        compile_breakdown,
        compile_catalog_values,
        compile_flow_matrix,
        compile_sankey,
        compile_timeseries,
    )

    end = SAMPLE_DAY
    start = SAMPLE_DAY - timedelta(days=30)

    # Source-table-routing matrix:
    #   investment dims (theme/subcategory/work_type) → work_unit_investments
    #   non-investment dims (team/repo/author)         → investment_metrics_daily
    # The two backing tables expose different measure columns; restrict each
    # dimension to the measure set the matching template can actually project.
    # Known gaps for the investment path are tracked in CHAOS-1754: throughput,
    # churn_loc, and cycle_time_hours mappings on work_unit_investments don't
    # match any real column. Until that is fixed, only count is exercised
    # against investment dimensions here.
    NON_INVESTMENT_DIMS = ["team", "repo"]
    NON_INVESTMENT_MEASURES = ["count", "throughput", "cycle_time_hours", "churn_loc"]
    INVESTMENT_DIMS = ["theme", "subcategory", "work_type"]
    INVESTMENT_MEASURES = ["count"]

    matrix: list[tuple[list[str], list[str]]] = [
        (NON_INVESTMENT_DIMS, NON_INVESTMENT_MEASURES),
        (INVESTMENT_DIMS, INVESTMENT_MEASURES),
    ]

    for dims, measures in matrix:
        for dim in dims:
            for measure in measures:
                ts_req = TimeseriesRequest(
                    dimension=dim,
                    measure=measure,
                    interval="day",
                    start_date=start,
                    end_date=end,
                )
                sql, params = compile_timeseries(ts_req, SAMPLE_ORG_ID)
                sink.calls.append((sql, params))

                bd_req = BreakdownRequest(
                    dimension=dim,
                    measure=measure,
                    start_date=start,
                    end_date=end,
                    top_n=10,
                )
                sql, params = compile_breakdown(bd_req, SAMPLE_ORG_ID)
                sink.calls.append((sql, params))

    sankey_req = SankeyRequest(
        path=["team", "repo"],
        measure="count",
        start_date=start,
        end_date=end,
    )
    nodes_qs, edges_qs = compile_sankey(sankey_req, SAMPLE_ORG_ID)
    sink.calls.extend(nodes_qs)
    sink.calls.extend(edges_qs)

    for fm_dim in ("team", "repo", "work_type"):
        fm_req = FlowMatrixRequest(
            dimension=fm_dim,
            measure="count",
            start_date=start,
            end_date=end,
        )
        nodes_qs, edges_qs = compile_flow_matrix(fm_req, SAMPLE_ORG_ID)
        sink.calls.extend(nodes_qs)
        sink.calls.extend(edges_qs)

    for dim in NON_INVESTMENT_DIMS + INVESTMENT_DIMS:
        cv_req = CatalogValuesRequest(dimension=dim, limit=50)
        sql, params = compile_catalog_values(cv_req, SAMPLE_ORG_ID)
        sink.calls.append((sql, params))


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


ALL_RESOLVER_SQL_FIXTURES: list[tuple[str, ResolverSQLFixture]] = [
    ("compounding_risk", _fixture_compounding_risk),
    ("forecast", _fixture_forecast),
    ("home", _fixture_home),
    ("capacity", _fixture_capacity),
    ("work_graph", _fixture_work_graph),
    ("recommendations", _fixture_recommendations),
    ("operating_review", _fixture_operating_review),
    ("security", _fixture_security),
    ("bus_factor", _fixture_bus_factor),
    ("data_health", _fixture_data_health),
    ("analytics", _fixture_analytics),
]
"""Registry of ``(resolver_name, fixture)`` pairs.

When adding a new resolver that emits ClickHouse SQL, add an entry here. The
fixture body should call every helper that builds a query, with sample
arguments that cover the major WHERE/branch differences.
"""
