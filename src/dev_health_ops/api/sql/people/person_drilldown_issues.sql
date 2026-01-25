SELECT
    work_item_id,
    provider,
    status,
    team_id,
    cycle_time_hours,
    lead_time_hours,
    started_at,
    completed_at
FROM work_item_cycle_times
WHERE day >= %(start_day)s AND day < %(end_day)s
  AND assignee IN %(identities)s
  {cursor_filter}
ORDER BY completed_at DESC
LIMIT %(limit)s
