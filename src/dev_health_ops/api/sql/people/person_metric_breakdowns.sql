SELECT
    {group_expr} AS label,
    {aggregator}({column}) AS value
FROM {table}
{join_clause}
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND {identity_column} IN %(identities)s
  {extra_where}
GROUP BY {group_expr}
ORDER BY value DESC
LIMIT %(limit)s
