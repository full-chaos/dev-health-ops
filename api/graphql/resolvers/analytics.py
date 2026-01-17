"""Analytics resolver for GraphQL analytics API."""

from __future__ import annotations

import logging
from datetime import date
from typing import Dict, List, Optional

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
from ..models.inputs import AnalyticsRequestInput
from ..models.outputs import (
    AnalyticsResult,
    BreakdownItem,
    BreakdownResult,
    SankeyCoverage,
    SankeyEdge,
    SankeyNode,
    SankeyResult,
    TimeseriesBucket,
    TimeseriesResult,
)
from ..sql.compiler import (
    BreakdownRequest,
    SankeyRequest,
    TimeseriesRequest,
    compile_breakdown,
    compile_sankey,
    compile_timeseries,
)


logger = logging.getLogger(__name__)


async def resolve_analytics(
    context: GraphQLContext,
    batch: AnalyticsRequestInput,
) -> AnalyticsResult:
    """
    Resolve batch analytics query.

    Validates cost limits, compiles SQL, executes queries, and returns results.

    Args:
        context: GraphQL request context with org_id and client.
        batch: Batch request with timeseries, breakdowns, and optional sankey.

    Returns:
        AnalyticsResult with all query results.

    Raises:
        CostLimitExceededError: If any cost limit is exceeded.
        ValidationError: If any input is invalid.
    """
    from api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    # Validate sub-request count
    validate_sub_request_count(
        timeseries_count=len(batch.timeseries),
        breakdowns_count=len(batch.breakdowns),
        has_sankey=batch.sankey is not None,
    )

    timeseries_results: List[TimeseriesResult] = []
    breakdown_results: List[BreakdownResult] = []
    sankey_result: Optional[SankeyResult] = None

    timeout = DEFAULT_LIMITS.query_timeout_seconds

    # Execute timeseries queries
    for ts_req in batch.timeseries:
        start = ts_req.date_range.start_date
        end = ts_req.date_range.end_date

        validate_date_range(start, end)
        validate_buckets(start, end, ts_req.interval.value)

        request = TimeseriesRequest(
            dimension=ts_req.dimension.value,
            measure=ts_req.measure.value,
            interval=ts_req.interval.value,
            start_date=start,
            end_date=end,
            use_investment=batch.use_investment,
        )

        sql, params = compile_timeseries(request, org_id, timeout)

        try:
            rows = await query_dicts(client, sql, params)
            # Group by dimension_value
            grouped: Dict[str, List[TimeseriesBucket]] = {}
            for row in rows:
                dim_val = str(row.get("dimension_value", ""))
                bucket_date = row.get("bucket")
                value = float(row.get("value", 0))

                if dim_val not in grouped:
                    grouped[dim_val] = []

                if isinstance(bucket_date, date):
                    grouped[dim_val].append(
                        TimeseriesBucket(date=bucket_date, value=value)
                    )

            for dim_val, buckets in grouped.items():
                timeseries_results.append(
                    TimeseriesResult(
                        dimension=ts_req.dimension.value,
                        dimension_value=dim_val,
                        measure=ts_req.measure.value,
                        buckets=buckets,
                    )
                )
        except Exception as e:
            logger.error("Timeseries query failed: %s", e)
            raise

    # Execute breakdown queries
    for bd_req in batch.breakdowns:
        start = bd_req.date_range.start_date
        end = bd_req.date_range.end_date

        validate_date_range(start, end)
        validate_top_n(bd_req.top_n)

        request = BreakdownRequest(
            dimension=bd_req.dimension.value,
            measure=bd_req.measure.value,
            start_date=start,
            end_date=end,
            top_n=bd_req.top_n,
            use_investment=batch.use_investment,
        )

        sql, params = compile_breakdown(request, org_id, timeout)

        try:
            rows = await query_dicts(client, sql, params)
            items = [
                BreakdownItem(
                    key=str(row.get("dimension_value", "")),
                    value=float(row.get("value", 0)),
                )
                for row in rows
            ]
            breakdown_results.append(
                BreakdownResult(
                    dimension=bd_req.dimension.value,
                    measure=bd_req.measure.value,
                    items=items,
                )
            )
        except Exception as e:
            logger.error("Breakdown query failed: %s", e)
            raise

    # Execute sankey query
    if batch.sankey is not None:
        sk_req = batch.sankey
        start = sk_req.date_range.start_date
        end = sk_req.date_range.end_date

        validate_date_range(start, end)
        validate_sankey_limits(sk_req.max_nodes, sk_req.max_edges)

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

        nodes_queries, edges_queries = compile_sankey(request, org_id, timeout)

        nodes: List[SankeyNode] = []
        edges: List[SankeyEdge] = []

        try:
            # Execute nodes query
            try:
                for sql, params in nodes_queries:
                    rows = await query_dicts(client, sql, params)
                    if not rows:
                        continue
                    for row in rows:
                        dim = str(row.get("dimension", ""))
                        node_id = str(row.get("node_id", ""))
                        value = float(row.get("value", 0))
                        nodes.append(
                            SankeyNode(
                                id=f"{dim}:{node_id}",
                                label=node_id,
                                dimension=dim,
                                value=value,
                            )
                        )
            except Exception as e:
                logger.error("Sankey nodes query failed: %s", e)
                # Continue with empty nodes is better than crashing?
                # For Sankey, if nodes fail, edges likely will too or be useless.

            # Execute edges queries
            try:
                for sql, params in edges_queries:
                    rows = await query_dicts(client, sql, params)
                    if not rows:
                        continue
                    for row in rows:
                        source_dim = str(row.get("source_dimension", ""))
                        target_dim = str(row.get("target_dimension", ""))
                        source = str(row.get("source", ""))
                        target = str(row.get("target", ""))
                        value = float(row.get("value", 0))

                        edges.append(
                            SankeyEdge(
                                source=f"{source_dim}:{source}",
                                target=f"{target_dim}:{target}",
                                value=value,
                            )
                        )
            except Exception as e:
                logger.error("Sankey edges query failed: %s", e)

            # Calculate coverage metrics if requested
            coverage: Optional[SankeyCoverage] = None
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
                    "work_unit_investments"
                    if request.use_investment
                    else "investment_metrics_daily"
                )

                base_table = table
                joins = ""
                if request.use_investment:
                    joins = """
                        LEFT JOIN (SELECT id, name as team_label FROM organizations_teams WHERE org_id = %(org_id)s) t 
                        ON work_unit_investments.team_id = t.id
                        LEFT JOIN (
                            SELECT start_date, work_unit_id, grand_parent_name, parent_name, name 
                            FROM investment_hierarchy 
                            WHERE org_id = %(org_id)s
                        ) ih ON work_unit_investments.work_unit_id = ih.work_unit_id 
                        AND work_unit_investments.active_date = ih.start_date
                        LEFT JOIN (SELECT id, name as repo_name FROM organizations_repos WHERE org_id = %(org_id)s) r
                        ON work_unit_investments.repo_id = r.id
                        """

                coverage_sql = f"""
                    SELECT 
                        count() as total,
                        countIf({team_col} != 'Unassigned') as assigned_team,
                        countIf({repo_col} != 'Unassigned') as assigned_repo
                    FROM {base_table}
                    {joins}
                    WHERE org_id = %(org_id)s
                    AND active_date >= %(start_date)s
                    AND active_date <= %(end_date)s
                """

                cov_params = {
                    "org_id": org_id,
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                }

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

    return AnalyticsResult(
        timeseries=timeseries_results,
        breakdowns=breakdown_results,
        sankey=sankey_result,
    )
