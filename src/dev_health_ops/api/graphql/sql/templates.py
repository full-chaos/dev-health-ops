"""SQL templates for GraphQL analytics queries.

All templates use parameterized queries - no string interpolation of user values.
Column names are validated against allowlists before being inserted.
"""

from __future__ import annotations

from typing import Any, List, TYPE_CHECKING

from .validate import BucketInterval, Dimension, Measure

if TYPE_CHECKING:
    from ...sql.base_dialect import SqlDialect


def timeseries_template(
    dimension: Dimension,
    measure: Measure,
    interval: BucketInterval,
    dialect: "SqlDialect",
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
    timeout: int = 30,
) -> str:
    """Generate SQL template for timeseries query."""
    dim_col = Dimension.db_column(dimension, dialect, use_investment=use_investment)
    measure_expr = Measure.db_expression(
        measure, dialect, use_investment=use_investment
    )

    # Extract date column from filter for truncating
    date_col = date_filter.split(" ")[0]
    bucket_expr = dialect.date_trunc(interval.value, date_col)

    return f"""
SELECT
    {bucket_expr} AS bucket,
    {dim_col} AS dimension_value,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
{filter_clause}
GROUP BY bucket, dimension_value
ORDER BY bucket ASC, value DESC
{dialect.query_settings(timeout)}
"""


def breakdown_template(
    dimension: Dimension,
    measure: Measure,
    dialect: "SqlDialect",
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
    timeout: int = 30,
) -> str:
    """Generate SQL template for breakdown (top-N aggregation) query."""
    dim_col = Dimension.db_column(dimension, dialect, use_investment=use_investment)
    measure_expr = Measure.db_expression(
        measure, dialect, use_investment=use_investment
    )

    return f"""
SELECT
    {dim_col} AS dimension_value,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
{filter_clause}
GROUP BY dimension_value
ORDER BY value DESC
LIMIT %(top_n)s
{dialect.query_settings(timeout)}
"""


def sankey_nodes_template(
    dimensions: List[Dimension],
    measure: Measure,
    dialect: "SqlDialect",
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
) -> str:
    """Generate SQL template for Sankey nodes query."""
    measure_expr = Measure.db_expression(
        measure, dialect, use_investment=use_investment
    )

    union_parts = []
    for dim in dimensions:
        dim_col = Dimension.db_column(dim, dialect, use_investment=use_investment)
        part = f"""
SELECT
    '{dim.value.upper()}' AS dimension,
    {dialect.to_string(dim_col)} AS node_id,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
{filter_clause}
GROUP BY node_id
ORDER BY value DESC
LIMIT %(limit_per_dim)s
"""
        union_parts.append(part)

    return " UNION ALL ".join(union_parts)


def sankey_edges_template(
    source_dim: Dimension,
    target_dim: Dimension,
    measure: Measure,
    dialect: "SqlDialect",
    source_table: str = "investment_metrics_daily",
    date_filter: str = "day >= %(start_date)s AND day <= %(end_date)s",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
) -> str:
    """Generate SQL template for Sankey edges query."""
    source_col = Dimension.db_column(source_dim, dialect, use_investment=use_investment)
    target_col = Dimension.db_column(target_dim, dialect, use_investment=use_investment)
    measure_expr = Measure.db_expression(
        measure, dialect, use_investment=use_investment
    )

    return f"""
SELECT
    '{source_dim.value.upper()}' AS source_dimension,
    '{target_dim.value.upper()}' AS target_dimension,
    {dialect.to_string(source_col)} AS source,
    {dialect.to_string(target_col)} AS target,
    {measure_expr} AS value
FROM {source_table}
{extra_clauses}
WHERE {date_filter}
{filter_clause}
  AND {source_col} IS NOT NULL
  AND {target_col} IS NOT NULL
GROUP BY source, target
ORDER BY value DESC
LIMIT %(max_edges)s
"""


def catalog_values_template(
    dimension: Dimension,
    dialect: "SqlDialect",
    source_table: str = "investment_metrics_daily",
    extra_clauses: str = "",
    use_investment: bool = False,
    filter_clause: str = "",  # NEW: scope/category filters
    **kwargs: Any,
) -> str:
    """Generate SQL template for fetching distinct dimension values."""
    dim_col = Dimension.db_column(dimension, dialect, use_investment=use_investment)
    dim_str = dialect.to_string(dim_col)

    return f"""
SELECT
    {dim_str} AS value,
    COUNT(*) AS count
FROM {source_table}
{extra_clauses}
WHERE {dim_col} IS NOT NULL
  AND {dim_str} != ''
{filter_clause}
GROUP BY value
ORDER BY count DESC
LIMIT %(limit)s
"""
