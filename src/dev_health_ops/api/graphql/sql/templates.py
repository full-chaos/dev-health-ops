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
    """Nodes query for TEAM flow matrix, sourced from work_item_cycle_times.

    Counts distinct work items per team in the window. The cycle_times table
    carries the canonical per-work-item team assignment (one row per work item
    per completed day) with real team diversity, unlike investment_metrics_daily
    which aggregates and may collapse to a single team in sparse data.
    """
    return """
SELECT
    'TEAM' AS dimension,
    toString(team_id) AS node_id,
    uniqExact(work_item_id) AS value
FROM work_item_cycle_times
WHERE day >= %(start_date)s AND day <= %(end_date)s
  AND work_item_cycle_times.org_id = %(org_id)s
  AND team_id IS NOT NULL
  AND team_id != ''
GROUP BY node_id
ORDER BY value DESC
LIMIT %(limit_per_dim)s
SETTINGS max_execution_time = %(timeout)s
"""


def flow_matrix_team_edges_template() -> str:
    """Asymmetric cross-team edges from work_item_cycle_times.

    Self-joins on (work_scope_id, day, org_id) so every pair of teams that
    completed work in the same scope on the same day becomes an edge. The
    edge value is `uniqExact(a.work_item_id)` — the count of SOURCE team's
    distinct work items in that shared cell, not the cartesian product.

    Because edge (A, B) counts A's items and edge (B, A) counts B's items,
    the matrix is asymmetric whenever the two teams contribute different
    volumes. That unlocks the chord's directional modes:
      - Outflow[i][j] = team i's work count where j is also present
      - Inflow[i][j]  = team j's work count where i is also present
      - Net[i][j]     = positive surplus when i outpaces j in shared scopes

    Semantic: "team i's contribution in scopes also touched by j". Not a
    handoff (schema doesn't encode those natively), but a real directional
    signal that populates on any org with cross-team repo sharing.
    """
    return """
SELECT
    'TEAM' AS source_dimension,
    'TEAM' AS target_dimension,
    toString(a.team_id) AS source,
    toString(b.team_id) AS target,
    uniqExact(a.work_item_id) AS value
FROM work_item_cycle_times AS a
INNER JOIN work_item_cycle_times AS b
  ON a.work_scope_id = b.work_scope_id
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
