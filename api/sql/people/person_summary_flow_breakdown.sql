SELECT
    'Active' AS stage,
    avg(cycle_time_hours) AS value,
    'hours' AS unit
FROM work_item_cycle_times
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND assignee IN %(identities)s
  AND cycle_time_hours IS NOT NULL

UNION ALL

SELECT
    'Waiting' AS stage,
    avg(lead_time_hours - cycle_time_hours) AS value,
    'hours' AS unit
FROM work_item_cycle_times
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND assignee IN %(identities)s
  AND lead_time_hours IS NOT NULL
  AND cycle_time_hours IS NOT NULL
