"""SQL templates for GraphQL analytics queries.

All templates use parameterized queries - no string interpolation of user values.
Column names are validated against allowlists before being inserted.
"""

from __future__ import annotations

from .validate import BucketInterval, Dimension, Measure


def timeseries_template(
    dimension: Dimension,
    measure: Measure,
    interval: BucketInterval,
) -> str:
    """
    Generate SQL template for timeseries query.

    The template uses ClickHouse date_trunc for bucketing and
    expects parameters: org_id, start_date, end_date.

    Args:
        dimension: Validated dimension enum.
        measure: Validated measure enum.
        interval: Validated bucket interval enum.

    Returns:
        Parameterized SQL string.
    """
    dim_col = Dimension.db_column(dimension)
    measure_expr = Measure.db_expression(measure)
    trunc_unit = BucketInterval.date_trunc_unit(interval)

    return f"""
SELECT
    date_trunc('{trunc_unit}', event_date) AS bucket,
    {dim_col} AS dimension_value,
    {measure_expr} AS value
FROM work_unit_daily
WHERE org_id = {{org_id:String}}
  AND event_date >= {{start_date:Date}}
  AND event_date <= {{end_date:Date}}
GROUP BY bucket, dimension_value
ORDER BY bucket ASC, value DESC
SETTINGS max_execution_time = {{timeout:UInt32}}
"""


def breakdown_template(
    dimension: Dimension,
    measure: Measure,
) -> str:
    """
    Generate SQL template for breakdown (top-N aggregation) query.

    The template expects parameters: org_id, start_date, end_date, top_n.

    Args:
        dimension: Validated dimension enum.
        measure: Validated measure enum.

    Returns:
        Parameterized SQL string.
    """
    dim_col = Dimension.db_column(dimension)
    measure_expr = Measure.db_expression(measure)

    return f"""
SELECT
    {dim_col} AS dimension_value,
    {measure_expr} AS value
FROM work_unit_daily
WHERE org_id = {{org_id:String}}
  AND event_date >= {{start_date:Date}}
  AND event_date <= {{end_date:Date}}
GROUP BY dimension_value
ORDER BY value DESC
LIMIT {{top_n:UInt32}}
SETTINGS max_execution_time = {{timeout:UInt32}}
"""


def sankey_nodes_template(
    dimensions: list[Dimension],
    measure: Measure,
) -> str:
    """
    Generate SQL template for Sankey nodes query.

    Fetches distinct values for each dimension in the path.
    The template expects parameters: org_id, start_date, end_date, limit_per_dim.

    Args:
        dimensions: List of validated dimension enums.
        measure: Validated measure enum.

    Returns:
        Parameterized SQL string.
    """
    # Build UNION ALL for each dimension
    union_parts = []
    for i, dim in enumerate(dimensions):
        dim_col = Dimension.db_column(dim)
        measure_expr = Measure.db_expression(measure)
        part = f"""
SELECT
    '{dim.value}' AS dimension,
    {dim_col} AS node_id,
    {measure_expr} AS value
FROM work_unit_daily
WHERE org_id = {{org_id:String}}
  AND event_date >= {{start_date:Date}}
  AND event_date <= {{end_date:Date}}
GROUP BY node_id
ORDER BY value DESC
LIMIT {{limit_per_dim:UInt32}}
"""
        union_parts.append(part)

    template = " UNION ALL ".join(union_parts)
    return f"""
{template}
SETTINGS max_execution_time = {{timeout:UInt32}}
"""


def sankey_edges_template(
    source_dim: Dimension,
    target_dim: Dimension,
    measure: Measure,
) -> str:
    """
    Generate SQL template for Sankey edges query.

    Computes aggregated flow between source and target dimensions.
    The template expects parameters: org_id, start_date, end_date, max_edges.

    Args:
        source_dim: Source dimension enum.
        target_dim: Target dimension enum.
        measure: Validated measure enum.

    Returns:
        Parameterized SQL string.
    """
    source_col = Dimension.db_column(source_dim)
    target_col = Dimension.db_column(target_dim)
    measure_expr = Measure.db_expression(measure)

    return f"""
SELECT
    {source_col} AS source,
    {target_col} AS target,
    {measure_expr} AS value
FROM work_unit_daily
WHERE org_id = {{org_id:String}}
  AND event_date >= {{start_date:Date}}
  AND event_date <= {{end_date:Date}}
  AND {source_col} IS NOT NULL
  AND {target_col} IS NOT NULL
GROUP BY source, target
ORDER BY value DESC
LIMIT {{max_edges:UInt32}}
SETTINGS max_execution_time = {{timeout:UInt32}}
"""


def catalog_values_template(dimension: Dimension) -> str:
    """
    Generate SQL template for fetching distinct dimension values.

    Used by the catalog resolver to show available values.
    The template expects parameters: org_id, limit.

    Args:
        dimension: Validated dimension enum.

    Returns:
        Parameterized SQL string.
    """
    dim_col = Dimension.db_column(dimension)

    return f"""
SELECT
    {dim_col} AS value,
    COUNT(*) AS count
FROM work_unit_daily
WHERE org_id = {{org_id:String}}
  AND {dim_col} IS NOT NULL
  AND {dim_col} != ''
GROUP BY value
ORDER BY count DESC
LIMIT {{limit:UInt32}}
SETTINGS max_execution_time = {{timeout:UInt32}}
"""
