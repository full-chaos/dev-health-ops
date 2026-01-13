SELECT
    {aggregator}({column}) AS value
FROM {table}
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND {identity_column} IN %(identities)s
  {extra_where}
