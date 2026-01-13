SELECT
    lower(if(type = '' OR type IS NULL, 'unknown', type)) AS key,
    if(type = '' OR type IS NULL, 'Unknown', type) AS name,
    count() AS value
FROM work_item_cycle_times
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND assignee IN %(identities)s
GROUP BY key, name
ORDER BY value DESC
