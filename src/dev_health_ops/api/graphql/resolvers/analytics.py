"""Analytics resolver for GraphQL analytics API."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from datetime import date, datetime, time, timezone
from typing import Any, cast
from uuid import UUID

from dev_health_ops.api.queries.investment import (
    LATEST_WORK_UNIT_INVESTMENTS_CTE,
    fetch_investment_quality_stats,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..cost import (
    DEFAULT_LIMITS,
    validate_buckets,
    validate_date_range,
    validate_sankey_limits,
    validate_sub_request_count,
    validate_top_n,
)
from ..models.inputs import (
    AnalyticsRequestInput,
    BreakdownRequestInput,
    FilterInput,
    ScopeFilterInput,
    TimeseriesRequestInput,
    WhatFilterInput,
)
from ..models.outputs import (
    AnalyticsResult,
    BreakdownItem,
    BreakdownResult,
    EvidenceQualityStats,
    FlowMatrixResult,
    SankeyCoverage,
    SankeyEdge,
    SankeyNode,
    SankeyResult,
    TimeseriesBucket,
    TimeseriesResult,
)
from ..sql.compiler import (
    BreakdownRequest,
    FlowMatrixRequest,
    SankeyRequest,
    TimeseriesRequest,
    compile_breakdown,
    compile_flow_matrix,
    compile_sankey,
    compile_timeseries,
)
from ..sql.filter_translation import translate_filters

logger = logging.getLogger(__name__)

_UNMATCHED_REPO_FILTER_ID = "00000000-0000-0000-0000-000000000000"


def _as_uuid_string(value: str) -> str | None:
    try:
        return str(UUID(value))
    except (TypeError, ValueError):
        return None


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


async def _resolve_repo_filter_refs(
    client: Any,
    *,
    org_id: str,
    repo_refs: list[str],
) -> list[str]:
    from dev_health_ops.api.queries.client import query_dicts

    cleaned_refs = [str(ref).strip() for ref in repo_refs if str(ref).strip()]
    repo_names = [ref for ref in cleaned_refs if _as_uuid_string(ref) is None]

    repo_ids_by_name: dict[str, str] = {}
    if repo_names:
        rows = await query_dicts(
            client,
            """
            SELECT
                toString(id) AS repo_id,
                repo
            FROM repos
            WHERE org_id = %(org_id)s
              AND lower(repo) IN %(repo_names)s
            """,
            {
                "org_id": org_id,
                "repo_names": _dedupe_preserving_order(
                    [name.lower() for name in repo_names]
                ),
            },
        )
        for row in rows:
            repo_name = str(row.get("repo") or "").lower()
            repo_id = _as_uuid_string(str(row.get("repo_id") or ""))
            if repo_name and repo_id:
                repo_ids_by_name[repo_name] = repo_id

    resolved: list[str] = []
    for ref in cleaned_refs:
        repo_id = _as_uuid_string(ref)
        if repo_id is not None:
            resolved.append(repo_id)
            continue
        resolved.append(repo_ids_by_name.get(ref.lower(), _UNMATCHED_REPO_FILTER_ID))

    return _dedupe_preserving_order(resolved)


async def _resolve_analytics_repo_filters(
    client: Any,
    *,
    org_id: str,
    filters: FilterInput | None,
) -> FilterInput | None:
    if filters is None:
        return None

    scope = filters.scope
    what = filters.what

    if scope is not None and scope.level.value == "repo" and scope.ids:
        scope = ScopeFilterInput(
            level=scope.level,
            ids=await _resolve_repo_filter_refs(
                client, org_id=org_id, repo_refs=scope.ids
            ),
        )

    if what is not None and what.repos:
        what = WhatFilterInput(
            repos=await _resolve_repo_filter_refs(
                client, org_id=org_id, repo_refs=what.repos
            ),
            services=what.services,
        )

    return FilterInput(
        scope=scope,
        who=filters.who,
        what=what,
        why=filters.why,
        how=filters.how,
    )


def _analytics_quality_window(batch: AnalyticsRequestInput) -> tuple[date, date] | None:
    if batch.breakdowns:
        date_range = batch.breakdowns[0].date_range
        return date_range.start_date, date_range.end_date
    if batch.timeseries:
        date_range = batch.timeseries[0].date_range
        return date_range.start_date, date_range.end_date
    return None


async def _resolve_evidence_quality_stats(
    client: Any,
    batch: AnalyticsRequestInput,
    org_id: str,
    filters: FilterInput | None,
) -> EvidenceQualityStats | None:
    if not bool(batch.use_investment):
        return None
    window = _analytics_quality_window(batch)
    if window is None:
        return None

    start_date, end_date = window
    scope_filter = ""
    scope_params: dict[str, Any] = {}
    team_scope_ids: list[str] | None = None
    themes: list[str] | None = None

    if filters is not None:
        if filters.scope is not None and filters.scope.ids:
            if filters.scope.level.value == "team":
                team_scope_ids = filters.scope.ids
            elif filters.scope.level.value == "repo":
                scope_filter += " AND work_unit_investments.repo_id IN %(scope_ids)s"
                scope_params["scope_ids"] = filters.scope.ids
        if filters.what is not None and filters.what.repos:
            scope_filter += " AND work_unit_investments.repo_id IN %(repo_filter_ids)s"
            scope_params["repo_filter_ids"] = filters.what.repos
        if filters.why is not None and filters.why.work_category:
            themes = filters.why.work_category

    row = await fetch_investment_quality_stats(
        client,
        start_ts=datetime.combine(start_date, time.min, tzinfo=timezone.utc),
        end_ts=datetime.combine(end_date, time.min, tzinfo=timezone.utc),
        scope_filter=scope_filter,
        scope_params=scope_params,
        org_id=org_id,
        themes=themes,
        team_scope_ids=team_scope_ids,
    )
    if not row:
        return EvidenceQualityStats()

    band_counts = {
        "high": int(row.get("high_count") or 0),
        "moderate": int(row.get("moderate_count") or 0),
        "low": int(row.get("low_count") or 0),
        "very_low": int(row.get("very_low_count") or 0),
        "unknown": int(row.get("unknown_count") or 0),
    }
    known_count = int(row.get("quality_known_count") or 0)
    mean_value = row.get("quality_mean")
    stddev_value = row.get("quality_stddev")
    return EvidenceQualityStats(
        mean=float(mean_value) if mean_value is not None and known_count > 0 else None,
        stddev=float(stddev_value)
        if stddev_value is not None and known_count > 0
        else None,
        total=int(row.get("total") or 0),
        band_counts=cast(Any, band_counts),
    )


async def _execute_sankey_inner(
    client: Any,
    nodes_queries: list[tuple[str, dict[str, Any]]],
    edges_queries: list[tuple[str, dict[str, Any]]],
) -> tuple[list[SankeyNode], list[SankeyEdge]]:
    """Execute all node and edge queries concurrently and aggregate results."""
    from dev_health_ops.api.queries.client import query_dicts

    async def _nodes() -> list[SankeyNode]:
        results = await asyncio.gather(
            *(query_dicts(client, sql, params) for sql, params in nodes_queries)
        )
        out: list[SankeyNode] = []
        for rows in results:
            if not rows:
                continue
            for row in rows:
                dim = str(row.get("dimension", ""))
                node_id = str(row.get("node_id", ""))
                value = float(row.get("value") or 0)
                out.append(
                    SankeyNode(
                        id=f"{dim}:{node_id}",
                        label=node_id,
                        dimension=dim,
                        value=value,
                    )
                )
        return out

    async def _edges() -> list[SankeyEdge]:
        results = await asyncio.gather(
            *(query_dicts(client, sql, params) for sql, params in edges_queries)
        )
        out: list[SankeyEdge] = []
        for rows in results:
            if not rows:
                continue
            for row in rows:
                source_dim = str(row.get("source_dimension", ""))
                target_dim = str(row.get("target_dimension", ""))
                source = str(row.get("source", ""))
                target = str(row.get("target", ""))
                value = float(row.get("value") or 0)
                out.append(
                    SankeyEdge(
                        source=f"{source_dim}:{source}",
                        target=f"{target_dim}:{target}",
                        value=value,
                    )
                )
        return out

    nodes_task = _nodes()
    edges_task = _edges()
    nodes, edges = await asyncio.gather(nodes_task, edges_task)
    return nodes, edges


async def _execute_timeseries_query(
    client: Any,
    ts_req: TimeseriesRequestInput,
    org_id: str,
    timeout: int,
    use_investment: bool,
    filters: Any | None,
) -> list[TimeseriesResult]:
    """Execute a single timeseries query and return results."""
    from dev_health_ops.api.queries.client import query_dicts

    start = ts_req.date_range.start_date
    end = ts_req.date_range.end_date

    request = TimeseriesRequest(
        dimension=ts_req.dimension.value,
        measure=ts_req.measure.value,
        interval=ts_req.interval.value,
        start_date=start,
        end_date=end,
        use_investment=use_investment,
    )

    sql, params = compile_timeseries(request, org_id, timeout, filters=filters)

    rows = await query_dicts(client, sql, params)
    grouped: dict[str, list[TimeseriesBucket]] = {}

    for row in rows:
        dim_val = str(row.get("dimension_value", ""))
        bucket_date = row.get("bucket")
        value = float(row.get("value") or 0)

        if dim_val not in grouped:
            grouped[dim_val] = []

        if isinstance(bucket_date, date):
            grouped[dim_val].append(TimeseriesBucket(date=bucket_date, value=value))

    return [
        TimeseriesResult(
            dimension=ts_req.dimension.name,
            dimension_value=dim_val,
            measure=ts_req.measure.name,
            buckets=buckets,
        )
        for dim_val, buckets in grouped.items()
    ]


async def _resolve_breakdown_labels(
    client: Any,
    *,
    dimension: str,
    org_id: str,
    keys: list[str],
) -> dict[str, str]:
    """Resolve repo/team breakdown keys to display names (Framework A7/A8).

    Only the ``repo`` and ``team`` dimensions carry stable ids that can leak as
    raw UUIDs; other dimensions (theme, work_type, ...) are already human text.
    Best-effort: returns an empty map on failure so callers fall back.
    """
    from dev_health_ops.api.services.identity import resolve_scope_display_names

    if dimension not in {"repo", "team"}:
        return {}
    return await resolve_scope_display_names(
        client,
        org_id=org_id,
        scope="team" if dimension == "team" else "repo",
        ids=keys,
    )


def _build_breakdown_item(
    key: str,
    value: float,
    label_map: dict[str, str],
) -> BreakdownItem:
    from dev_health_ops.api.services.identity import looks_like_uuid

    resolved = label_map.get(key)
    if resolved and not looks_like_uuid(resolved):
        label: str | None = resolved
    elif key and not looks_like_uuid(key):
        # Already-human key (e.g. investment-path repo slug, theme name).
        label = key
    else:
        # A8: never surface a bare UUID; emit a controlled short token and let
        # the client render its Unresolved badge.
        token = key.replace("-", "")[:8]
        label = f"#{token}" if token else None
    return BreakdownItem(key=key, value=value, label=label)


async def _execute_breakdown_query(
    client: Any,
    bd_req: BreakdownRequestInput,
    org_id: str,
    timeout: int,
    use_investment: bool,
    filters: Any | None,
) -> BreakdownResult:
    """Execute a single breakdown query and return results."""
    from dev_health_ops.api.queries.client import query_dicts

    start = bd_req.date_range.start_date
    end = bd_req.date_range.end_date

    request = BreakdownRequest(
        dimension=bd_req.dimension.value,
        measure=bd_req.measure.value,
        start_date=start,
        end_date=end,
        top_n=bd_req.top_n,
        use_investment=use_investment,
    )

    sql, params = compile_breakdown(request, org_id, timeout, filters=filters)

    rows = await query_dicts(client, sql, params)

    # Resolve repo/team dimension keys to human display names server-side so the
    # client never renders a raw id as the primary label (Framework A7/A8).
    keys = [str(row.get("dimension_value", "")) for row in rows]
    label_map = await _resolve_breakdown_labels(
        client, dimension=bd_req.dimension.value, org_id=org_id, keys=keys
    )
    items = [
        _build_breakdown_item(
            str(row.get("dimension_value", "")),
            float(row.get("value") or 0),
            label_map,
        )
        for row in rows
    ]

    return BreakdownResult(
        dimension=bd_req.dimension.name,
        measure=bd_req.measure.name,
        items=items,
    )


async def resolve_analytics(
    context: GraphQLContext,
    batch: AnalyticsRequestInput,
) -> AnalyticsResult:
    """
    Resolve batch analytics query.

    Validates cost limits, compiles SQL, executes queries IN PARALLEL,
    and returns results.

    Args:
        context: GraphQL request context with org_id and client.
        batch: Batch request with timeseries, breakdowns, and optional sankey.

    Returns:
        AnalyticsResult with all query results.

    Raises:
        CostLimitExceededError: If any cost limit is exceeded.
        ValidationError: If any input is invalid.
    """
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    # Validate sub-request count
    validate_sub_request_count(
        timeseries_count=len(batch.timeseries),
        breakdowns_count=len(batch.breakdowns),
        has_sankey=batch.sankey is not None,
        has_flow_matrix=batch.flow_matrix is not None,
    )

    timeout = DEFAULT_LIMITS.query_timeout_seconds

    # Validate all requests upfront before executing any queries
    for ts_req in batch.timeseries:
        validate_date_range(ts_req.date_range.start_date, ts_req.date_range.end_date)
        validate_buckets(
            ts_req.date_range.start_date,
            ts_req.date_range.end_date,
            ts_req.interval.value,
        )

    for bd_req in batch.breakdowns:
        validate_date_range(bd_req.date_range.start_date, bd_req.date_range.end_date)
        validate_top_n(bd_req.top_n)

    if batch.sankey is not None:
        validate_date_range(
            batch.sankey.date_range.start_date, batch.sankey.date_range.end_date
        )
        validate_sankey_limits(batch.sankey.max_nodes, batch.sankey.max_edges)

    if batch.flow_matrix is not None:
        validate_date_range(
            batch.flow_matrix.date_range.start_date,
            batch.flow_matrix.date_range.end_date,
        )
        validate_sankey_limits(batch.flow_matrix.max_nodes, batch.flow_matrix.max_edges)

    # Build list of all query coroutines for parallel execution
    use_investment = bool(batch.use_investment)
    resolved_filters = await _resolve_analytics_repo_filters(
        client, org_id=org_id, filters=batch.filters
    )

    timeseries_coros: list[Coroutine[Any, Any, list[TimeseriesResult]]] = [
        _execute_timeseries_query(
            client,
            ts_req,
            org_id,
            timeout,
            use_investment,
            resolved_filters,
        )
        for ts_req in batch.timeseries
    ]

    breakdown_coros: list[Coroutine[Any, Any, BreakdownResult]] = [
        _execute_breakdown_query(
            client,
            bd_req,
            org_id,
            timeout,
            use_investment,
            resolved_filters,
        )
        for bd_req in batch.breakdowns
    ]

    # Execute all timeseries and breakdown queries in parallel
    all_results = await asyncio.gather(
        *timeseries_coros,
        *breakdown_coros,
        return_exceptions=True,
    )

    # Split results back into timeseries and breakdowns
    num_timeseries = len(batch.timeseries)
    timeseries_raw = all_results[:num_timeseries]
    breakdown_raw = all_results[num_timeseries:]

    # Process timeseries results (flatten nested lists)
    timeseries_results: list[TimeseriesResult] = []
    for i, result in enumerate(timeseries_raw):
        if isinstance(result, Exception):
            logger.error("Timeseries query %d failed: %s", i, result)
            raise result
        if not isinstance(result, list):
            raise TypeError(
                f"Unexpected timeseries result type: {type(result).__name__}"
            )
        timeseries_results.extend(result)

    # Process breakdown results
    breakdown_results: list[BreakdownResult] = []
    for i, result in enumerate(breakdown_raw):
        if isinstance(result, Exception):
            logger.error("Breakdown query %d failed: %s", i, result)
            raise result
        if not isinstance(result, BreakdownResult):
            raise TypeError(
                f"Unexpected breakdown result type: {type(result).__name__}"
            )
        breakdown_results.append(result)

    sankey_result: SankeyResult | None = None

    # Execute sankey query (already validated above)
    if batch.sankey is not None:
        sk_req = batch.sankey
        start = sk_req.date_range.start_date
        end = sk_req.date_range.end_date

        request = SankeyRequest(
            path=[d.value for d in sk_req.path],
            measure=sk_req.measure.value,
            start_date=start,
            end_date=end,
            max_nodes=sk_req.max_nodes,
            max_edges=sk_req.max_edges,
            use_investment=sk_req.use_investment
            if sk_req.use_investment is not None
            else batch.use_investment,
        )

        nodes_queries, edges_queries = compile_sankey(
            request, org_id, timeout, filters=resolved_filters
        )

        nodes: list[SankeyNode] = []
        edges: list[SankeyEdge] = []

        try:
            # Execute nodes and edges queries concurrently via asyncio.gather.
            try:
                nodes, edges = await _execute_sankey_inner(
                    client,
                    nodes_queries,
                    edges_queries,
                )
            except Exception as exc:
                logger.error("Sankey query failed: %s", exc)
                nodes, edges = [], []

            # Calculate coverage metrics if requested
            coverage: SankeyCoverage | None = None
            if batch.sankey is not None:
                # Use a specific coverage query
                # We need to calculate % of units with assigned team and assigned repo
                from ..sql.validate import Dimension

                team_col = Dimension.db_column(
                    Dimension.TEAM, use_investment=bool(request.use_investment)
                )
                repo_col = Dimension.db_column(
                    Dimension.REPO, use_investment=bool(request.use_investment)
                )

                table = (
                    "latest_work_unit_investments AS work_unit_investments"
                    if request.use_investment
                    else "investment_metrics_daily"
                )

                base_table = table
                date_filter = "day >= %(start_date)s AND day <= %(end_date)s"
                joins = ""
                if request.use_investment:
                    date_filter = (
                        "work_unit_investments.from_ts < %(end_date)s "
                        "AND work_unit_investments.to_ts >= %(start_date)s"
                    )
                    joins = """
                        LEFT JOIN (
                            SELECT
                                work_unit_id,
                                argMax(team, cnt) AS team_label,
                                argMax(team_id, cnt) AS team_id
                            FROM (
                                SELECT
                                    work_unit_investments.work_unit_id AS work_unit_id,
                                    t.team_id AS team_id,
                                    ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) AS team,
                                    countIf(ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, '')) IS NOT NULL) AS cnt
                                FROM latest_work_unit_investments AS work_unit_investments
                                ARRAY JOIN arrayDistinct(arrayConcat(
                                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                                    [work_unit_investments.work_unit_id]
                                )) AS issue_id
                                LEFT JOIN (
                                    SELECT
                                        work_item_id,
                                        argMax(team_id, computed_at) AS team_id,
                                        argMax(team_name, computed_at) AS team_name
                                    FROM work_item_cycle_times
                                    WHERE org_id = %(org_id)s
                                    GROUP BY work_item_id
                                ) AS t ON t.work_item_id = issue_id
                                GROUP BY work_unit_id, team_id, team
                            )
                            GROUP BY work_unit_id
                        ) AS ut ON ut.work_unit_id = work_unit_investments.work_unit_id
                        LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)
                        """

                assigned_team_expr = f"lower(ifNull(nullIf({team_col}, ''), 'unassigned')) != 'unassigned'"
                assigned_repo_expr = f"{repo_col} IS NOT NULL"
                if request.use_investment:
                    assigned_repo_expr = f"lower({repo_col}) != 'unassigned'"

                with_clause = (
                    f"WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE}"
                    if request.use_investment
                    else ""
                )
                source_alias = (
                    "work_unit_investments" if request.use_investment else table
                )
                # Qualify org_id only on the investment path, which JOINs other
                # tables (repos/teams) that also carry org_id; the daily-table path
                # is single-table, so an unqualified column is unambiguous.
                org_filter = (
                    f"{source_alias}.org_id = %(org_id)s"
                    if request.use_investment
                    else "org_id = %(org_id)s"
                )

                coverage_filters = (
                    FilterInput(
                        scope=resolved_filters.scope,
                        what=resolved_filters.what,
                    )
                    if resolved_filters is not None
                    else None
                )
                coverage_filter_clause, coverage_filter_params = translate_filters(
                    coverage_filters,
                    use_investment=bool(request.use_investment),
                    team_column=team_col,
                    repo_column="work_unit_investments.repo_id"
                    if request.use_investment
                    else repo_col,
                    author_column="work_unit_investments.author_id"
                    if request.use_investment
                    else "author_id",
                )

                coverage_sql = f"""
                    {with_clause}
                    SELECT
                        count() as total,
                        countIf({assigned_team_expr}) as assigned_team,
                        countIf({assigned_repo_expr}) as assigned_repo
                    FROM {base_table}
                    {joins}
                    WHERE {date_filter}
                      AND {org_filter}
                      {coverage_filter_clause}
                """

                cov_params = {
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "org_id": org_id,
                }
                cov_params.update(coverage_filter_params)

                try:
                    c_rows = await query_dicts(client, coverage_sql, cov_params)
                    if c_rows:
                        total = float(c_rows[0].get("total", 0))
                        assigned_team = float(c_rows[0].get("assigned_team", 0))
                        assigned_repo = float(c_rows[0].get("assigned_repo", 0))

                        coverage = SankeyCoverage(
                            team_coverage=assigned_team / total if total > 0 else 0,
                            repo_coverage=assigned_repo / total if total > 0 else 0,
                        )
                except Exception as e:
                    logger.error("Coverage query failed: %s", e)
                    # Don't fail the whole request for metrics
                    coverage = SankeyCoverage(team_coverage=0, repo_coverage=0)

            sankey_result = SankeyResult(nodes=nodes, edges=edges, coverage=coverage)

        except Exception as e:
            logger.error("Sankey query failed: %s", e)
            # Prevent crash by returning empty result
            sankey_result = SankeyResult(nodes=[], edges=[], coverage=None)

    flow_matrix_result: FlowMatrixResult | None = None

    if batch.flow_matrix is not None:
        fm_req = batch.flow_matrix
        fm_request = FlowMatrixRequest(
            dimension=fm_req.dimension.value,
            measure=fm_req.measure.value,
            start_date=fm_req.date_range.start_date,
            end_date=fm_req.date_range.end_date,
            max_nodes=fm_req.max_nodes,
            max_edges=fm_req.max_edges,
            use_investment=fm_req.use_investment
            if fm_req.use_investment is not None
            else batch.use_investment,
        )

        fm_nodes_queries, fm_edges_queries = compile_flow_matrix(
            fm_request, org_id, timeout, filters=resolved_filters
        )

        try:
            fm_nodes, fm_edges = await _execute_sankey_inner(
                client,
                fm_nodes_queries,
                fm_edges_queries,
            )
        except Exception as exc:
            logger.error("FlowMatrix query failed: %s", exc)
            fm_nodes, fm_edges = [], []

        flow_matrix_result = FlowMatrixResult(nodes=fm_nodes, edges=fm_edges)

    evidence_quality_stats = await _resolve_evidence_quality_stats(
        client, batch, org_id, resolved_filters
    )
    evidence_quality_distribution = (
        evidence_quality_stats.band_counts
        if evidence_quality_stats is not None
        else None
    )

    return AnalyticsResult(
        timeseries=timeseries_results,
        breakdowns=breakdown_results,
        sankey=sankey_result,
        flow_matrix=flow_matrix_result,
        evidence_quality_distribution=evidence_quality_distribution,
        evidence_quality_stats=evidence_quality_stats,
    )
