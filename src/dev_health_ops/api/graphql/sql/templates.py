"""SQL templates for GraphQL analytics queries.

All templates use parameterized queries - no string interpolation of user values.
Column names are validated against allowlists before being inserted.
"""

from __future__ import annotations

from typing import Any

from .validate import BucketInterval, Dimension, Measure


def timeseries_template(
    dimension: Dimension,
    measure: Measure,
    interval: BucketInterval,
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
) -> str:
    """Generate SQL template for timeseries query."""
    dim_col = Dimension.db_column(dimension, use_investment=use_investment)
    measure_expr = Measure.db_expression(measure, use_investment=use_investment)
    trunc_unit = BucketInterval.date_trunc_unit(interval)

    # Extract date column from filter for truncating
    date_col = date_filter.split(" ")[0]

    return f"""
SELECT
    date_trunc('{trunc_unit}', {date_col}) AS bucket,
    {dim_col} AS dimension_value,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
  AND {source_table}.org_id = %(org_id)s
{filter_clause}
GROUP BY bucket, dimension_value
ORDER BY bucket ASC, value DESC
SETTINGS max_execution_time = %(timeout)s
"""


def breakdown_template(
    dimension: Dimension,
    measure: Measure,
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
) -> str:
    """Generate SQL template for breakdown (top-N aggregation) query."""
    dim_col = Dimension.db_column(dimension, use_investment=use_investment)
    measure_expr = Measure.db_expression(measure, use_investment=use_investment)

    return f"""
SELECT
    {dim_col} AS dimension_value,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
  AND {source_table}.org_id = %(org_id)s
{filter_clause}
GROUP BY dimension_value
ORDER BY value DESC
LIMIT %(top_n)s
SETTINGS max_execution_time = %(timeout)s
"""


def sankey_nodes_template(
    dimensions: list[Dimension],
    measure: Measure,
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
) -> str:
    """Generate SQL template for Sankey nodes query."""
    measure_expr = Measure.db_expression(measure, use_investment=use_investment)

    union_parts = []
    for dim in dimensions:
        dim_col = Dimension.db_column(dim, use_investment=use_investment)
        part = f"""
SELECT
    '{dim.value.upper()}' AS dimension,
    toString({dim_col}) AS node_id,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
  AND {source_table}.org_id = %(org_id)s
{filter_clause}
GROUP BY node_id
ORDER BY value DESC
LIMIT %(limit_per_dim)s
"""
        union_parts.append(part)

    template = " UNION ALL ".join(union_parts)
    return f"""
{template}
SETTINGS max_execution_time = %(timeout)s
"""


def sankey_edges_template(
    source_dim: Dimension,
    target_dim: Dimension,
    measure: Measure,
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
) -> str:
    """Generate SQL template for Sankey edges query."""
    source_col = Dimension.db_column(source_dim, use_investment=use_investment)
    target_col = Dimension.db_column(target_dim, use_investment=use_investment)
    measure_expr = Measure.db_expression(measure, use_investment=use_investment)

    return f"""
SELECT
    '{source_dim.value.upper()}' AS source_dimension,
    '{target_dim.value.upper()}' AS target_dimension,
    toString({source_col}) AS source,
    toString({target_col}) AS target,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
  AND {source_table}.org_id = %(org_id)s
{filter_clause}
  AND {source_col} IS NOT NULL
  AND {target_col} IS NOT NULL
GROUP BY source, target
ORDER BY value DESC
LIMIT %(max_edges)s
SETTINGS max_execution_time = %(timeout)s
"""


def flow_matrix_team_nodes_template() -> str:
    """Nodes query for TEAM flow matrix, sourced from investment_metrics_daily.

    Sums work items completed per team in the window. Uses the same table
    as the edge self-join so node ids and edge endpoints stay consistent.
    """
    return """
SELECT
    'TEAM' AS dimension,
    toString(team_id) AS node_id,
    SUM(work_items_completed) AS value
FROM investment_metrics_daily
WHERE day >= %(start_date)s AND day <= %(end_date)s
  AND investment_metrics_daily.org_id = %(org_id)s
  AND team_id IS NOT NULL
  AND team_id != ''
GROUP BY node_id
ORDER BY value DESC
LIMIT %(limit_per_dim)s
SETTINGS max_execution_time = %(timeout)s
"""


def flow_matrix_team_edges_template() -> str:
    """Asymmetric weighted cross-team edges from investment_metrics_daily.

    A self-join on (repo_id, day) surfaces every pair of teams working on the
    same repo on the same day. Each edge (A, B) is weighted by A's own
    work_items_completed on the shared (repo, day) — not A×B — so the matrix
    is asymmetric: edge (A, B) ≠ edge (B, A) whenever the two teams
    contribute different volumes.

    Semantic: "team A's work in shared space with B". The asymmetry powers
    the chord's directional modes:
      - Outflow[i][j] = team i's work volume where j is also present
      - Inflow[i][j]  = team j's work volume where i is also present
      - Net[i][j]     = positive surplus when i contributes more than j

    Self-loops (a.team_id = b.team_id) are excluded server-side; the frontend
    drops them as well but filtering early keeps the edge list small.
    """
    return """
SELECT
    'TEAM' AS source_dimension,
    'TEAM' AS target_dimension,
    toString(a.team_id) AS source,
    toString(b.team_id) AS target,
    SUM(a.work_items_completed) AS value
FROM investment_metrics_daily AS a
INNER JOIN investment_metrics_daily AS b
  ON a.repo_id = b.repo_id
  AND a.day = b.day
  AND a.org_id = b.org_id
WHERE a.day >= %(start_date)s AND a.day <= %(end_date)s
  AND a.org_id = %(org_id)s
  AND a.team_id IS NOT NULL AND a.team_id != ''
  AND b.team_id IS NOT NULL AND b.team_id != ''
  AND a.team_id != b.team_id
GROUP BY source, target
ORDER BY value DESC
LIMIT %(max_edges)s
SETTINGS max_execution_time = %(timeout)s
"""


def catalog_values_template(
    dimension: Dimension,
    source_table: str = "investment_metrics_daily",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
    **kwargs: Any,
) -> str:
    """Generate SQL template for fetching distinct dimension values."""
    dim_col = Dimension.db_column(dimension, use_investment=use_investment)

    return f"""
SELECT
    toString({dim_col}) AS value,
    COUNT(*) AS count
FROM {source_table}
{extra_clauses}
WHERE {dim_col} IS NOT NULL
  AND {source_table}.org_id = %(org_id)s
  AND toString({dim_col}) != ''
{filter_clause}
GROUP BY value
ORDER BY count DESC
LIMIT %(limit)s
SETTINGS max_execution_time = %(timeout)s
"""
